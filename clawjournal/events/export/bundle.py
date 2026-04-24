"""Replay-export bundle assembly (phase-1 plan 07).

``export_session_bundle`` is the orchestrator. The pipeline:

1. Resolve the session_key on event_sessions; collect children if requested.
2. Bridge to workbench and apply the share-time gates: source + projects
   confirmation (config), excluded_projects (per-session), hold-state via
   ``release_gate_blockers``, and a session-level findings gate (any
   ``status='open'`` finding blocks).
3. Read events / overrides / token_usage / cost_anomalies / incidents.
4. Anonymize raw_ref source paths (and snippet keys for the same path).
5. Apply the same share-time redaction layers to event payloads, override
   payloads, and source snippets: custom strings, blocked domains,
   anonymizer, then findings-backed deterministic/PII redactions. The
   findings gate in step 2 blocks unresolved findings; accepted findings
   are redacted and ignored findings are left intact.
6. Assemble the bundle dict, compute manifest sha256.
7. Run TruffleHog on the assembled bundle JSON; on a finding, write a
   manifest-only artifact and exit 2.
8. Atomic write (tmp + fsync + rename).

The implementation deliberately does NOT extend the existing
``export_share_to_disk`` (workbench-detail-keyed): see plan 07
"Relationship to existing bundle-export". Instead it reuses the same
primitives in the same order from a parallel wrapper.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from clawjournal.events.capabilities import capabilities_json
from clawjournal.events.view import fetch_vendor_line
from clawjournal.redaction.anonymizer import Anonymizer
from clawjournal.redaction import trufflehog as th

BUNDLE_SCHEMA_VERSION = "1.0"
RECORDER_SCHEMA_VERSION = "1.0"
EXPORT_BUNDLE_FORMAT = "events-bundle"

_DEFAULT_EXPORT_DIRNAME = "exports"
_BUNDLE_FILENAME_PREFIX = "clawjournal-bundle-"
_PRE_PUBLISH_SIZE_WARN_BYTES = 50 * 1024 * 1024
_SNIPPET_UNAVAILABLE_SENTINEL = "source-unavailable-at-export"
_REDACTED_PATH_SENTINEL = "[REDACTED_PATH]"


class ExportError(Exception):
    """Generic export failure (validation, disk I/O, etc.)."""


class ExportGateBlocked(ExportError):
    """A share-time gate (hold-state, projects, findings) blocked the export.

    ``exit_code`` and ``message`` mirror the CLI surface so callers can
    map directly to ``sys.exit`` / ``stderr``.
    """

    def __init__(self, exit_code: int, message: str) -> None:
        super().__init__(message)
        self.exit_code = exit_code
        self.message = message


@dataclass
class ExportSummary:
    bundle_path: Path | None
    sha256: str | None
    blocked: bool
    block_reason: str | None
    session_keys: list[str] = field(default_factory=list)
    event_count: int = 0
    override_count: int = 0
    token_usage_count: int = 0
    cost_anomaly_count: int = 0
    incident_count: int = 0
    snippet_count: int = 0
    snippet_unavailable_count: int = 0
    redaction_summary: dict[str, Any] = field(default_factory=dict)
    trufflehog: dict[str, Any] | None = None
    bundle_size_bytes: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "bundle_path": str(self.bundle_path) if self.bundle_path else None,
            "sha256": self.sha256,
            "blocked": self.blocked,
            "block_reason": self.block_reason,
            "session_keys": list(self.session_keys),
            "event_count": self.event_count,
            "override_count": self.override_count,
            "token_usage_count": self.token_usage_count,
            "cost_anomaly_count": self.cost_anomaly_count,
            "incident_count": self.incident_count,
            "snippet_count": self.snippet_count,
            "snippet_unavailable_count": self.snippet_unavailable_count,
            "redaction_summary": dict(self.redaction_summary),
            "trufflehog": self.trufflehog,
            "bundle_size_bytes": self.bundle_size_bytes,
        }


# --------------------------------------------------------------------------- #
# session resolution + gates
# --------------------------------------------------------------------------- #


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _resolve_event_session(
    conn: sqlite3.Connection, session_key: str
) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT id, session_key, parent_session_key, client, client_version, "
        "started_at, ended_at, status "
        "FROM event_sessions WHERE session_key = ?",
        (session_key,),
    ).fetchone()


def _children_of(
    conn: sqlite3.Connection, parent_session_key: str
) -> list[sqlite3.Row]:
    return list(
        conn.execute(
            "SELECT id, session_key, parent_session_key, client, client_version, "
            "started_at, ended_at, status "
            "FROM event_sessions WHERE parent_session_key = ? "
            "ORDER BY started_at IS NULL, started_at, session_key",
            (parent_session_key,),
        )
    )


def _workbench_rows_for(
    conn: sqlite3.Connection, session_key: str
) -> list[sqlite3.Row]:
    """Find workbench `sessions` rows matching this session_key.

    ADR-001 §Step 3 puts ``sessions.session_key`` as the bridge column.
    Returns 0/1/2+ rows; the caller maps to the three-branch policy.
    """
    try:
        return list(
            conn.execute(
                "SELECT session_id, hold_state, embargo_until, project, source "
                "FROM sessions WHERE session_key = ?",
                (session_key,),
            )
        )
    except sqlite3.OperationalError:
        # Workbench schema not initialized (events-only DB) — treat as
        # "no workbench row" branch.
        return []


def _enforce_session_gates(
    conn: sqlite3.Connection,
    *,
    session_key: str,
    label: str,
    allow_no_workbench_row: bool,
    excluded_projects: list[str],
) -> sqlite3.Row | None:
    """Enforce hold-state + project + findings gates for a single session.

    Returns the matched workbench row (or None when allow_no_workbench_row
    permitted an events-only session). Raises ExportGateBlocked otherwise.
    """
    from clawjournal.workbench.index import (
        release_gate_blockers,
        session_matches_excluded_projects,
    )

    rows = _workbench_rows_for(conn, session_key)

    if len(rows) == 0:
        if not allow_no_workbench_row:
            raise ExportGateBlocked(
                2,
                f"{label} {session_key!r} has no workbench `sessions` row "
                "and has not been through human review. Run `clawjournal scan` "
                "to bring it into the workbench, or pass "
                "`--allow-no-workbench-row` to opt past this gate.",
            )
        return None

    if len(rows) > 1:
        raise ExportGateBlocked(
            2,
            f"{label} {session_key!r} matches multiple workbench `sessions` "
            "rows — resolve the ambiguity manually before exporting.",
        )

    row = rows[0]
    sid = row["session_id"]

    blockers = release_gate_blockers(conn, [sid])
    if blockers:
        b = blockers[0]
        state = b.get("hold_state", "?")
        embargo = b.get("embargo_until")
        embargo_hint = f" (embargo_until={embargo})" if embargo else ""
        raise ExportGateBlocked(
            2,
            f"{label} {session_key!r} blocked by hold-state {state!r}"
            f"{embargo_hint}. Run `clawjournal hold release {sid}` to clear.",
        )

    if excluded_projects and session_matches_excluded_projects(
        {"project": row["project"], "source": row["source"]},
        excluded_projects,
    ):
        raise ExportGateBlocked(
            2,
            f"{label} {session_key!r} belongs to an excluded project "
            f"({row['project']!r}). Adjust `clawjournal config --exclude` "
            "or remove the project from the exclusion list.",
        )

    open_findings = conn.execute(
        "SELECT COUNT(*) FROM findings WHERE session_id = ? AND status = 'open'",
        (sid,),
    ).fetchone()[0]
    if open_findings > 0:
        raise ExportGateBlocked(
            2,
            f"{label} {session_key!r} has {open_findings} unresolved finding(s). "
            "Resolve them in the workbench (or via the findings UI) before "
            "exporting.",
        )

    return row


def _enforce_global_config_gates(config: dict[str, Any]) -> None:
    """Enforce config-level gates (source confirmed, projects confirmed)."""
    from clawjournal.cli import _is_explicit_source_choice

    source = config.get("source")
    if not _is_explicit_source_choice(source):
        raise ExportGateBlocked(
            2,
            "Source scope is not confirmed. Run "
            "`clawjournal config --source <claude|codex|all|...>` first.",
        )

    if not config.get("projects_confirmed", False):
        raise ExportGateBlocked(
            2,
            "Project scope is not confirmed. Review with `clawjournal list` "
            "and either exclude folders via "
            "`clawjournal config --exclude '<name>'` or accept the full set "
            "via `clawjournal config --confirm-projects`.",
        )


# --------------------------------------------------------------------------- #
# data loaders
# --------------------------------------------------------------------------- #


def _load_events(conn: sqlite3.Connection, session_ids: list[int]) -> list[dict]:
    if not session_ids:
        return []
    placeholders = ",".join("?" * len(session_ids))
    rows = conn.execute(
        f"""
        SELECT e.id, e.session_id, es.session_key, e.type, e.event_key,
               e.event_at, e.source, e.source_path, e.source_offset, e.seq,
               e.client, e.confidence, e.lossiness, e.raw_json
          FROM events e
          JOIN event_sessions es ON es.id = e.session_id
         WHERE e.session_id IN ({placeholders})
         ORDER BY es.session_key, e.event_at IS NULL, e.event_at,
                  e.source_path, e.source_offset, e.seq
        """,
        session_ids,
    ).fetchall()
    return [dict(r) for r in rows]


def _load_overrides(conn: sqlite3.Connection, session_ids: list[int]) -> list[dict]:
    if not session_ids:
        return []
    placeholders = ",".join("?" * len(session_ids))
    try:
        rows = conn.execute(
            f"""
            SELECT eo.session_id, es.session_key, eo.event_key, eo.type,
                   eo.source, eo.confidence, eo.lossiness, eo.event_at,
                   eo.payload_json, eo.origin, eo.created_at
              FROM event_overrides eo
              JOIN event_sessions es ON es.id = eo.session_id
             WHERE eo.session_id IN ({placeholders})
             ORDER BY es.session_key, eo.event_at IS NULL, eo.event_at,
                      eo.event_key
            """,
            session_ids,
        ).fetchall()
    except sqlite3.OperationalError:
        return []  # event_overrides not yet created
    return [dict(r) for r in rows]


def _load_token_usage(conn: sqlite3.Connection, session_ids: list[int]) -> list[dict]:
    if not session_ids:
        return []
    placeholders = ",".join("?" * len(session_ids))
    try:
        rows = conn.execute(
            f"""
            SELECT tu.event_id, tu.session_id, es.session_key,
                   e.source, e.source_path, e.source_offset, e.seq,
                   tu.model, tu.model_family, tu.model_tier, tu.model_provider,
                   tu.input, tu.output, tu.cache_read, tu.cache_write,
                   tu.reasoning, tu.service_tier, tu.data_source,
                   tu.cost_estimate, tu.pricing_table_version, tu.event_at
              FROM token_usage tu
              JOIN events e ON e.id = tu.event_id
              JOIN event_sessions es ON es.id = tu.session_id
             WHERE tu.session_id IN ({placeholders})
             ORDER BY es.session_key, tu.event_at IS NULL, tu.event_at,
                      tu.event_id
            """,
            session_ids,
        ).fetchall()
    except sqlite3.OperationalError:
        return []
    return [dict(r) for r in rows]


def _load_cost_anomalies(
    conn: sqlite3.Connection, session_ids: list[int]
) -> list[dict]:
    if not session_ids:
        return []
    placeholders = ",".join("?" * len(session_ids))
    try:
        rows = conn.execute(
            f"""
            SELECT ca.id, ca.session_id, es.session_key, ca.turn_event_id,
                   ca.kind, ca.confidence, ca.evidence_json, ca.created_at,
                   e.source       AS turn_source,
                   e.source_path  AS turn_source_path,
                   e.source_offset AS turn_source_offset,
                   e.seq          AS turn_seq
              FROM cost_anomalies ca
              JOIN event_sessions es ON es.id = ca.session_id
              LEFT JOIN events e ON e.id = ca.turn_event_id
             WHERE ca.session_id IN ({placeholders})
             ORDER BY es.session_key, ca.id
            """,
            session_ids,
        ).fetchall()
    except sqlite3.OperationalError:
        return []
    return [dict(r) for r in rows]


def _load_incidents(conn: sqlite3.Connection, session_ids: list[int]) -> list[dict]:
    if not session_ids:
        return []
    placeholders = ",".join("?" * len(session_ids))
    try:
        rows = conn.execute(
            f"""
            SELECT i.id, i.session_id, es.session_key, i.kind,
                   i.first_event_id, i.last_event_id,
                   i.evidence_json, i.count, i.confidence, i.created_at,
                   first_e.source       AS first_source,
                   first_e.source_path  AS first_source_path,
                   first_e.source_offset AS first_source_offset,
                   first_e.seq          AS first_seq,
                   last_e.source        AS last_source,
                   last_e.source_path   AS last_source_path,
                   last_e.source_offset AS last_source_offset,
                   last_e.seq           AS last_seq
              FROM incidents i
              JOIN event_sessions es ON es.id = i.session_id
              JOIN events first_e ON first_e.id = i.first_event_id
              JOIN events last_e  ON last_e.id  = i.last_event_id
             WHERE i.session_id IN ({placeholders})
             ORDER BY es.session_key, i.id
            """,
            session_ids,
        ).fetchall()
    except sqlite3.OperationalError:
        return []
    return [dict(r) for r in rows]


# --------------------------------------------------------------------------- #
# redaction passes
# --------------------------------------------------------------------------- #


@dataclass
class _RedactionCounts:
    secrets: int = 0
    by_type: dict[str, int] = field(default_factory=dict)

    def record(self, log: list[dict]) -> None:
        for entry in log:
            t = entry.get("type", "unknown")
            self.by_type[t] = self.by_type.get(t, 0) + 1
        self.secrets += len(log)

    def add(self, type_name: str, count: int) -> None:
        if count <= 0:
            return
        self.by_type[type_name] = self.by_type.get(type_name, 0) + count
        self.secrets += count


@dataclass
class _BundleRedactor:
    conn: sqlite3.Connection
    anonymizer: Anonymizer
    custom_strings: list[str]
    user_allowlist: list[dict] | None
    blocked_domains: list[str]
    counts: _RedactionCounts
    workbench_session_ids: dict[str, str] = field(default_factory=dict)
    _reviewed_replacements_cache: dict[str, list[tuple[str, str, str]]] = field(
        default_factory=dict
    )

    def __post_init__(self) -> None:
        from clawjournal.workbench.index import _compile_blocked_domain_pattern

        self._domain_patterns = [
            pattern
            for pattern in (
                _compile_blocked_domain_pattern(domain)
                for domain in (self.blocked_domains or [])
            )
            if pattern is not None
        ]

    def text(
        self,
        text: str | None,
        *,
        session_key: str | None,
        field: str,
    ) -> str | None:
        if text is None:
            return text

        out = text
        if self.custom_strings:
            from clawjournal.redaction.secrets import redact_custom_strings

            out, n = redact_custom_strings(out, self.custom_strings)
            self.counts.add("custom", n)

        if self._domain_patterns:
            from clawjournal.workbench.index import _redact_blocked_domains_in_value

            out, n, log = _redact_blocked_domains_in_value(
                out,
                self._domain_patterns,
                field=field,
            )
            self.counts.record(log)

        out = self.anonymizer.text(out)

        workbench_session_id = (
            self.workbench_session_ids.get(session_key)
            if session_key is not None
            else None
        )
        out = self._apply_reviewed_entity_replacements(
            out,
            workbench_session_id=workbench_session_id,
        )
        out = self._apply_findings_backed_redactions(
            out,
            workbench_session_id=workbench_session_id,
        )
        return out

    def _apply_reviewed_entity_replacements(
        self,
        text: str,
        *,
        workbench_session_id: str | None,
    ) -> str:
        if not workbench_session_id:
            return text

        replacements = self._reviewed_replacements(workbench_session_id)
        if not replacements:
            return text

        import re

        out = text
        for entity_text, replacement, entity_type in replacements:
            if len(entity_text) < 3:
                continue
            pattern = re.compile(
                rf"(?<!\w){re.escape(entity_text)}(?!\w)",
                re.IGNORECASE,
            )
            out, n = pattern.subn(replacement, out)
            self.counts.add(entity_type or "reviewed_finding", n)
        return out

    def _reviewed_replacements(
        self,
        workbench_session_id: str,
    ) -> list[tuple[str, str, str]]:
        cached = self._reviewed_replacements_cache.get(workbench_session_id)
        if cached is not None:
            return cached

        replacements: list[tuple[str, str, str]] = []
        try:
            row = self.conn.execute(
                "SELECT blob_path FROM sessions WHERE session_id = ?",
                (workbench_session_id,),
            ).fetchone()
        except sqlite3.OperationalError:
            self._reviewed_replacements_cache[workbench_session_id] = replacements
            return replacements

        blob_path = row["blob_path"] if row is not None else None
        if not blob_path:
            self._reviewed_replacements_cache[workbench_session_id] = replacements
            return replacements

        try:
            blob = json.loads(Path(blob_path).read_text(encoding="utf-8"))
        except (OSError, TypeError, json.JSONDecodeError):
            self._reviewed_replacements_cache[workbench_session_id] = replacements
            return replacements

        try:
            from clawjournal.findings import (
                _resolve_field_text,
                hash_entity,
                load_findings_from_db,
            )
            from clawjournal.redaction.pii import replacement_for_type

            findings = load_findings_from_db(
                self.conn,
                workbench_session_id,
                status_filter={"accepted"},
            )
        except (sqlite3.OperationalError, ImportError):
            self._reviewed_replacements_cache[workbench_session_id] = replacements
            return replacements

        seen: set[str] = set()
        for finding in findings:
            source_text = _resolve_field_text(blob, finding)
            if source_text is None:
                continue
            start = max(0, finding.offset)
            end = min(len(source_text), start + max(0, finding.length))
            if start >= end:
                continue
            entity_text = source_text[start:end]
            if hash_entity(entity_text) != finding.entity_hash:
                continue
            if entity_text in seen:
                continue
            seen.add(entity_text)
            replacements.append(
                (
                    entity_text,
                    replacement_for_type(finding.entity_type),
                    finding.entity_type or "reviewed_finding",
                )
            )

        replacements.sort(key=lambda item: len(item[0]), reverse=True)
        self._reviewed_replacements_cache[workbench_session_id] = replacements
        return replacements

    def _apply_findings_backed_redactions(
        self,
        text: str,
        *,
        workbench_session_id: str | None,
    ) -> str:
        # No workbench row → no per-session findings can match; run the
        # pure deterministic pass directly rather than building a
        # synthetic blob around a placeholder session_id.
        if workbench_session_id is None:
            from clawjournal.redaction.secrets import redact_text

            out, _count, log = redact_text(
                text,
                user_allowlist=self.user_allowlist,
            )
            self.counts.record(log)
            return out

        blob = {
            "session_id": workbench_session_id,
            "messages": [{"role": "user", "content": text}],
        }
        try:
            from clawjournal.redaction.secrets import apply_findings_to_blob
            from clawjournal.workbench.index import _build_deterministic_redaction_log

            log = _build_deterministic_redaction_log(
                self.conn,
                blob,
                user_allowlist=self.user_allowlist,
            )
            redacted, count = apply_findings_to_blob(
                blob,
                self.conn,
                workbench_session_id,
                user_allowlist=self.user_allowlist,
            )
        except sqlite3.OperationalError:
            from clawjournal.redaction.secrets import redact_text

            out, _count, log = redact_text(
                text,
                user_allowlist=self.user_allowlist,
            )
            self.counts.record(log)
            return out

        self.counts.record(log)
        extra = count - len(log)
        if extra > 0:
            self.counts.add("deterministic_extra", extra)
        redacted_messages = redacted.get("messages") or []
        if not redacted_messages:
            return text
        redacted_text = redacted_messages[0].get("content")
        return redacted_text if isinstance(redacted_text, str) else text


def _redact_text_with(
    redactor: _BundleRedactor,
    text: str | None,
    *,
    session_key: str | None,
    field: str,
) -> str | None:
    return redactor.text(text, session_key=session_key, field=field)


def _redact_path_with(anonymizer: Anonymizer, path: str | None) -> str | None:
    if path is None:
        return None
    return anonymizer.path(path)


def _redacted_path_token(session_key: str, path: str) -> str:
    digest = hashlib.sha256(
        f"clawjournal:redacted-path:v1\0{session_key}\0{path}".encode("utf-8")
    ).hexdigest()[:16]
    return f"[REDACTED_PATH_{digest}]"


def _collect_source_paths(
    events: list[dict],
    token_usage: list[dict],
    cost_anomalies: list[dict],
    incidents: list[dict],
) -> list[tuple[str, str]]:
    paths: list[tuple[str, str]] = []
    for ev in events:
        if ev.get("source_path"):
            paths.append((ev["session_key"], ev["source_path"]))
    for row in token_usage:
        if row.get("source_path"):
            paths.append((row["session_key"], row["source_path"]))
    for row in cost_anomalies:
        if row.get("turn_source_path"):
            paths.append((row["session_key"], row["turn_source_path"]))
    for row in incidents:
        for key in ("first_source_path", "last_source_path"):
            if row.get(key):
                paths.append((row["session_key"], row[key]))
    return paths


def _build_redacted_path_map(
    anonymizer: Anonymizer,
    paths: list[tuple[str, str]],
) -> dict[tuple[str, str], str]:
    """Map original paths to bundle-safe identities.

    ``Anonymizer.path`` intentionally collapses home-directory paths to a
    single display sentinel. In an events bundle, ``raw_ref.source_path``
    is also part of the import identity, so collapsing distinct files to
    the same sentinel can drop events under the recorder UNIQUE key. Use a
    stable session/path discriminator for collapsed paths so independently
    exported bundles cannot all reuse the same ordinal token.
    """
    unique_paths = sorted({(session_key, path) for session_key, path in paths if path})
    redacted = {
        (session_key, path): _redact_path_with(anonymizer, path) or path
        for session_key, path in unique_paths
    }
    for session_key, path in unique_paths:
        if redacted[(session_key, path)] == _REDACTED_PATH_SENTINEL:
            redacted[(session_key, path)] = _redacted_path_token(session_key, path)
    return redacted


# --------------------------------------------------------------------------- #
# bundle assembly
# --------------------------------------------------------------------------- #


def _canonical_dump(obj: Any) -> str:
    """Deterministic JSON serialization for the manifest hash input.

    UTF-8, no BOM, sort_keys=True, separators=(",", ":"), no trailing
    newline. The caller wraps the bundle in a final pretty-printed
    serialization for disk; this helper is for the digest only.
    """
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _sha256_hex(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _build_session_block(row: dict, *, workbench_session_id: str | None) -> dict:
    block = {
        "session_key": row["session_key"],
        "client": row["client"],
        "client_version": row.get("client_version"),
        "started_at": row.get("started_at"),
        "ended_at": row.get("ended_at"),
        "status": row.get("status"),
        "parent_session_key": row.get("parent_session_key"),
    }
    if workbench_session_id is not None:
        block["workbench_session_id"] = workbench_session_id
    return block


def _build_event_record(
    event: dict,
    *,
    redacted_raw_json: str,
    redacted_source_path: str,
) -> dict:
    # `raw_ref` is the 4-element identity tuple matching events.UNIQUE
    # (source, source_path, source_offset, seq). Carrying `source` here
    # is necessary because two events from different sources can share
    # the same (source_path, source_offset, seq) — the importer's
    # raw_ref → events.id map would otherwise collide.
    return {
        "session_key": event["session_key"],
        "type": event["type"],
        "event_at": event.get("event_at"),
        "source": event["source"],
        "client": event["client"],
        "confidence": event["confidence"],
        "lossiness": event["lossiness"],
        "event_key": event.get("event_key"),
        "raw_ref": [
            event["source"],
            redacted_source_path,
            event["source_offset"],
            event["seq"],
        ],
        "raw_json": redacted_raw_json,
    }


def _build_override_record(override: dict, *, redacted_payload_json: str) -> dict:
    return {
        "session_key": override["session_key"],
        "event_key": override["event_key"],
        "type": override["type"],
        "source": override["source"],
        "confidence": override["confidence"],
        "lossiness": override["lossiness"],
        "event_at": override.get("event_at"),
        "payload_json": redacted_payload_json,
        "origin": override.get("origin"),
        "created_at": override.get("created_at"),
    }


def _build_token_usage_record(
    row: dict, *, redacted_source_path: str, source: str
) -> dict:
    return {
        "session_key": row["session_key"],
        "raw_ref": [source, redacted_source_path, row["source_offset"], row["seq"]],
        "model": row.get("model"),
        "model_family": row.get("model_family"),
        "model_tier": row.get("model_tier"),
        "model_provider": row.get("model_provider"),
        "input": row.get("input"),
        "output": row.get("output"),
        "cache_read": row.get("cache_read"),
        "cache_write": row.get("cache_write"),
        "reasoning": row.get("reasoning"),
        "service_tier": row.get("service_tier"),
        "data_source": row["data_source"],
        "cost_estimate": row.get("cost_estimate"),
        "pricing_table_version": row.get("pricing_table_version"),
        "event_at": row.get("event_at"),
    }


def _build_cost_anomaly_record(
    row: dict, *, redacted_source_path: str | None, turn_source: str | None
) -> dict:
    raw_ref = (
        [turn_source, redacted_source_path, row["turn_source_offset"], row["turn_seq"]]
        if turn_source is not None
        and redacted_source_path is not None
        and row.get("turn_source_offset") is not None
        else None
    )
    try:
        evidence = json.loads(row["evidence_json"])
    except (TypeError, json.JSONDecodeError):
        evidence = row.get("evidence_json")
    return {
        "session_key": row["session_key"],
        "kind": row["kind"],
        "confidence": row["confidence"],
        "turn_raw_ref": raw_ref,
        "evidence": evidence,
        "created_at": row.get("created_at"),
    }


def _build_incident_record(
    row: dict, *, first_path: str, last_path: str,
    first_source: str, last_source: str,
) -> dict:
    try:
        evidence = json.loads(row["evidence_json"])
    except (TypeError, json.JSONDecodeError):
        evidence = row.get("evidence_json")
    return {
        "session_key": row["session_key"],
        "kind": row["kind"],
        "confidence": row["confidence"],
        "count": row["count"],
        "first_raw_ref": [
            first_source, first_path,
            row["first_source_offset"], row["first_seq"],
        ],
        "last_raw_ref": [
            last_source, last_path,
            row["last_source_offset"], row["last_seq"],
        ],
        "evidence": evidence,
        "created_at": row.get("created_at"),
    }


def _generate_snippets(
    redactor: _BundleRedactor,
    events: list[dict],
    path_for: Callable[[str, str | None], str | None],
) -> tuple[dict[str, str], int]:
    """For each unique event identity tuple, fetch the vendor line and
    redact it. Returns (snippets, unavailable_count).

    Snippet keys are 4-segment strings ``<source>:<anon_path>:<offset>:<seq>``
    matching the events' raw_ref 4-tuple. Including ``source`` is
    necessary because two distinct real paths can both anonymize to
    ``[REDACTED_PATH]`` and share ``(offset, seq)`` (e.g. a parent and
    a subagent's first event both at offset 0); without ``source`` the
    snippet entries would silently overwrite each other.
    """
    snippets: dict[str, str] = {}
    seen: set[tuple[str, str, int, int]] = set()
    unavailable = 0
    for ev in events:
        source = ev["source"]
        original_path = ev["source_path"]
        offset = ev["source_offset"]
        seq = ev["seq"]
        identity = (source, original_path, offset, seq)
        if identity in seen:
            continue
        seen.add(identity)
        line = fetch_vendor_line(original_path, offset)
        anon_path = path_for(ev["session_key"], original_path)
        snippet_key = f"{source}:{anon_path}:{offset}:{seq}"
        if line is None:
            snippets[snippet_key] = _SNIPPET_UNAVAILABLE_SENTINEL
            unavailable += 1
            continue
        redacted = redactor.text(
            line,
            session_key=ev["session_key"],
            field="source_snippets",
        )
        snippets[snippet_key] = redacted
    return snippets, unavailable


# --------------------------------------------------------------------------- #
# atomic write + manifest
# --------------------------------------------------------------------------- #


def _resolve_output_path(
    output_path: Path | None, session_key: str
) -> Path:
    if output_path is None:
        from clawjournal.config import CONFIG_DIR

        export_dir = Path(CONFIG_DIR) / _DEFAULT_EXPORT_DIRNAME
        export_dir.mkdir(parents=True, exist_ok=True)
        suffix = hashlib.sha256(session_key.encode("utf-8")).hexdigest()[:8]
        return export_dir / f"{_BUNDLE_FILENAME_PREFIX}{suffix}.json"

    candidate = Path(output_path).expanduser().resolve()
    home = Path.home().resolve()
    safe_roots = [home, Path("/tmp").resolve()]
    # Also accept the platform tempdir (macOS uses
    # /var/folders/.../T/... — neither under /tmp nor $HOME).
    sys_tmp = Path(tempfile.gettempdir()).resolve()
    if sys_tmp not in safe_roots:
        safe_roots.append(sys_tmp)
    if not any(candidate.is_relative_to(root) for root in safe_roots):
        raise ExportError(
            f"Output path must resolve under $HOME, /tmp, or "
            f"the platform tempdir ({sys_tmp}): {candidate}"
        )
    candidate.parent.mkdir(parents=True, exist_ok=True)
    return candidate


def _atomic_write(path: Path, text: str) -> None:
    """Write `text` to `path` atomically (tmp + fsync + rename)."""
    fd, tmp_name = tempfile.mkstemp(
        prefix=path.name + ".", suffix=".tmp", dir=str(path.parent)
    )
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise


def _serialize_bundle(bundle: dict[str, Any], *, pretty: bool) -> str:
    indent = 2 if pretty else None
    separators = None if pretty else (",", ":")
    text = json.dumps(
        bundle, sort_keys=True, indent=indent, separators=separators, ensure_ascii=False
    )
    if pretty and not text.endswith("\n"):
        text += "\n"
    return text


# --------------------------------------------------------------------------- #
# entry point
# --------------------------------------------------------------------------- #


_DEFAULT_TEST_SETTINGS = {
    "custom_strings": [],
    "extra_usernames": [],
    "allowlist_entries": [],
    "excluded_projects": [],
    "blocked_domains": [],
}


def export_session_bundle(
    conn: sqlite3.Connection,
    session_key: str,
    *,
    output_path: Path | None = None,
    include_snippets: bool = True,
    include_children: bool = True,
    allow_no_workbench_row: bool = False,
    pretty: bool = True,
    config: dict[str, Any] | None = None,
    settings: dict[str, Any] | None = None,
    skip_global_gates: bool = False,
) -> ExportSummary:
    """Export a single session_key (plus children) to a bundle JSON file.

    Raises:
        ExportError — bad input (unknown session_key, invalid output path).
        ExportGateBlocked — share-time gate refused (hold-state, projects,
            findings, or source/projects-not-confirmed).

    On a TruffleHog block the function does NOT raise; it writes a
    manifest-only artifact and returns an ExportSummary with
    ``blocked=True`` so the CLI can map to exit code 2 the same way
    ``bundle-export`` does today.

    Test seam: ``settings`` and ``skip_global_gates`` let unit tests skip
    the workbench-schema lookups (``get_effective_share_settings`` reads
    the ``policies`` table) and the source/projects-confirmed gates.
    Production callers should always leave these at their defaults.
    """
    from clawjournal.config import load_config
    from clawjournal.workbench.index import get_effective_share_settings

    if config is None:
        config = load_config()

    if settings is None:
        try:
            settings = get_effective_share_settings(conn, config)
        except sqlite3.OperationalError:
            # Workbench schema not initialized — fall back to permissive
            # defaults. Real exports go through `open_index()` which
            # bootstraps the workbench schema, so this only fires in
            # ad-hoc tests / events-only DBs.
            settings = dict(_DEFAULT_TEST_SETTINGS)

    if not skip_global_gates:
        _enforce_global_config_gates(config)

    parent_row = _resolve_event_session(conn, session_key)
    if parent_row is None:
        raise ExportError(
            f"session_key not found: {session_key} "
            "(run `clawjournal events sessions` to list known keys)"
        )
    parent = dict(parent_row)

    workbench_row = _enforce_session_gates(
        conn,
        session_key=session_key,
        label="session",
        allow_no_workbench_row=allow_no_workbench_row,
        excluded_projects=settings["excluded_projects"],
    )

    children: list[sqlite3.Row] = []
    child_workbench_rows: dict[str, sqlite3.Row | None] = {}
    if include_children:
        children = _children_of(conn, session_key)
        for child in children:
            child_row = _enforce_session_gates(
                conn,
                session_key=child["session_key"],
                label="child session",
                allow_no_workbench_row=allow_no_workbench_row,
                excluded_projects=settings["excluded_projects"],
            )
            child_workbench_rows[child["session_key"]] = child_row

    all_session_ids = [parent["id"]] + [c["id"] for c in children]
    all_session_keys = [parent["session_key"]] + [c["session_key"] for c in children]

    events = _load_events(conn, all_session_ids)
    overrides = _load_overrides(conn, all_session_ids)
    token_usage = _load_token_usage(conn, all_session_ids)
    cost_anomalies = _load_cost_anomalies(conn, all_session_ids)
    incidents = _load_incidents(conn, all_session_ids)

    anonymizer = Anonymizer(extra_usernames=settings["extra_usernames"])
    counts = _RedactionCounts()
    custom_strings = settings.get("custom_strings", [])
    allowlist_entries = settings.get("allowlist_entries", [])
    blocked_domains = settings.get("blocked_domains", [])

    workbench_session_ids: dict[str, str] = {}
    if workbench_row is not None:
        workbench_session_ids[parent["session_key"]] = workbench_row["session_id"]
    for child_key, child_row in child_workbench_rows.items():
        if child_row is not None:
            workbench_session_ids[child_key] = child_row["session_id"]

    redactor = _BundleRedactor(
        conn=conn,
        anonymizer=anonymizer,
        custom_strings=custom_strings,
        user_allowlist=allowlist_entries,
        blocked_domains=blocked_domains,
        counts=counts,
        workbench_session_ids=workbench_session_ids,
    )

    redacted_paths = _build_redacted_path_map(
        anonymizer,
        _collect_source_paths(events, token_usage, cost_anomalies, incidents),
    )

    def _path(session_key: str, p: str | None) -> str | None:
        if p is None:
            return None
        key = (session_key, p)
        cached = redacted_paths.get(key)
        if cached is not None:
            return cached
        result = _redact_path_with(anonymizer, p)
        if result == _REDACTED_PATH_SENTINEL:
            result = _redacted_path_token(session_key, p)
        redacted_paths[key] = result
        return result

    redacted_events: list[dict] = []
    for ev in events:
        red_path = _path(ev["session_key"], ev["source_path"])
        red_raw = _redact_text_with(
            redactor,
            ev["raw_json"],
            session_key=ev["session_key"],
            field="raw_json",
        )
        redacted_events.append(
            _build_event_record(
                ev, redacted_raw_json=red_raw, redacted_source_path=red_path
            )
        )

    redacted_overrides: list[dict] = []
    for o in overrides:
        red_payload = _redact_text_with(
            redactor,
            o["payload_json"],
            session_key=o["session_key"],
            field="payload_json",
        )
        redacted_overrides.append(
            _build_override_record(o, redacted_payload_json=red_payload)
        )

    redacted_token_usage = [
        _build_token_usage_record(
            r,
            redacted_source_path=_path(r["session_key"], r["source_path"]),
            source=r["source"],
        )
        for r in token_usage
    ]

    redacted_cost_anomalies = [
        _build_cost_anomaly_record(
            r,
            redacted_source_path=_path(r["session_key"], r.get("turn_source_path")),
            turn_source=r.get("turn_source"),
        )
        for r in cost_anomalies
    ]

    redacted_incidents = [
        _build_incident_record(
            r,
            first_path=_path(r["session_key"], r["first_source_path"]),
            last_path=_path(r["session_key"], r["last_source_path"]),
            first_source=r["first_source"],
            last_source=r["last_source"],
        )
        for r in incidents
    ]

    snippets: dict[str, str] = {}
    snippet_unavailable = 0
    if include_snippets and events:
        snippets, snippet_unavailable = _generate_snippets(
            redactor,
            events,
            _path,
        )

    children_blocks = [
        _build_session_block(
            dict(c),
            workbench_session_id=(
                child_workbench_rows[c["session_key"]]["session_id"]
                if child_workbench_rows.get(c["session_key"]) is not None
                else None
            ),
        )
        for c in children
    ]

    bundle: dict[str, Any] = {
        "bundle_schema_version": BUNDLE_SCHEMA_VERSION,
        "recorder_schema_version": RECORDER_SCHEMA_VERSION,
        "bundle_created_at": _utc_now(),
        "client_version_observed": parent.get("client_version"),
        "session": _build_session_block(
            parent,
            workbench_session_id=workbench_row["session_id"] if workbench_row else None,
        ),
        "children": children_blocks,
        "events": redacted_events,
        "event_overrides": redacted_overrides,
        "token_usage": redacted_token_usage,
        "cost_anomalies": redacted_cost_anomalies,
        "incidents": redacted_incidents,
        "capabilities": capabilities_json(),
    }
    if include_snippets:
        bundle["source_snippets"] = snippets

    digest_input = {k: v for k, v in bundle.items()}  # exclude manifest below
    digest_text = _canonical_dump(digest_input)
    sha = _sha256_hex(digest_text)

    redaction_summary = {
        "total": counts.secrets,
        "by_type": dict(counts.by_type),
    }

    th_report = th.scan_text(digest_text)
    th_summary = th_report.summary()
    blocked = th_report.blocking
    block_reason = th_report.block_reason

    target = _resolve_output_path(output_path, session_key)

    if blocked:
        manifest_only_bundle = {
            "bundle_schema_version": BUNDLE_SCHEMA_VERSION,
            "recorder_schema_version": RECORDER_SCHEMA_VERSION,
            "bundle_created_at": bundle["bundle_created_at"],
            "session": bundle["session"],
            "manifest": {
                "sha256": sha,
                "blocked": True,
                "block_reason": block_reason,
                "trufflehog": th_summary,
                "redaction_summary": redaction_summary,
            },
        }
        text = _serialize_bundle(manifest_only_bundle, pretty=pretty)
        _atomic_write(target, text)
        return ExportSummary(
            bundle_path=target,
            sha256=sha,
            blocked=True,
            block_reason=block_reason,
            session_keys=all_session_keys,
            event_count=len(redacted_events),
            override_count=len(redacted_overrides),
            token_usage_count=len(redacted_token_usage),
            cost_anomaly_count=len(redacted_cost_anomalies),
            incident_count=len(redacted_incidents),
            snippet_count=len(snippets),
            snippet_unavailable_count=snippet_unavailable,
            redaction_summary=redaction_summary,
            trufflehog=th_summary,
            bundle_size_bytes=len(text.encode("utf-8")),
        )

    bundle["manifest"] = {
        "sha256": sha,
        "trufflehog": th_summary,
        "redaction_summary": redaction_summary,
    }
    text = _serialize_bundle(bundle, pretty=pretty)
    _atomic_write(target, text)

    return ExportSummary(
        bundle_path=target,
        sha256=sha,
        blocked=False,
        block_reason=None,
        session_keys=all_session_keys,
        event_count=len(redacted_events),
        override_count=len(redacted_overrides),
        token_usage_count=len(redacted_token_usage),
        cost_anomaly_count=len(redacted_cost_anomalies),
        incident_count=len(redacted_incidents),
        snippet_count=len(snippets),
        snippet_unavailable_count=snippet_unavailable,
        redaction_summary=redaction_summary,
        trufflehog=th_summary,
        bundle_size_bytes=len(text.encode("utf-8")),
    )
