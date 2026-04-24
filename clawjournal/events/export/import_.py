"""Replay-export bundle importer (phase-1 plan 07).

``import_session_bundle`` reads a JSON bundle written by
``export_session_bundle`` and rehydrates it into the local SQLite DB.

Key invariants (per plan 07):

- **No re-classification**: events are inserted with their bundle-recorded
  ``type`` / ``event_at`` / ``event_key`` verbatim. The local 02 classifier
  is never re-run on ``raw_json``. The bundle exists precisely to insulate
  against classifier drift between exporter and importer.
- **ID-modulo round-trip**: ``events.id`` is local autoincrement. The
  bundle identifies cross-row references via ``raw_ref =
  (source, source_path, source_offset, seq)``. The importer inserts events,
  builds a bundle-raw_ref → local-id map, then rewrites every
  ``token_usage.event_id`` / ``cost_anomalies.turn_event_id`` /
  ``incidents.first_event_id`` / ``incidents.last_event_id`` reference
  through that map.
- **No-snippets local inspectability**: bundles without
  ``source_snippets`` may restore this machine's ``sessions.raw_source_path``
  for events whose bundle path is ``[REDACTED_PATH]``. Cross-row references
  still bind through the original bundle raw_ref.
- **Idempotent re-import**: events use ``INSERT OR IGNORE`` against 02's
  unique index; overrides go through the rank-guarded
  ``write_hook_override`` upsert; cost_anomalies / incidents have UNIQUE
  indexes that no-op on re-insert; snippets use INSERT OR REPLACE on the
  storage triple PK.
- **Session-key resolution**: the importer upserts ``event_sessions`` rows
  for the parent and any children. Child sessions whose
  ``parent_session_key`` doesn't resolve (parent absent both in the
  bundle and locally) insert with ``parent_session_id=NULL`` per 02's
  NULL-and-backfill semantics.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import hashlib
import warnings

from clawjournal.events.export.schema import ensure_export_schema
from clawjournal.events.schema import ensure_schema as ensure_events_schema
from clawjournal.events.view import (
    _resolve_session_id,
    _write_hook_override_inner,
    ensure_view_schema,
)

_REDACTED_PATH_SENTINEL = "[REDACTED_PATH]"
_REDACTED_PATH_TOKEN_PREFIX = "[REDACTED_PATH_"


SUPPORTED_BUNDLE_MAJOR = "1"
SUPPORTED_BUNDLE_MINOR = 0  # warn on minor > this; reject on major mismatch
SUPPORTED_RECORDER_MAJOR = "1"


class ImportError_(Exception):
    """Generic import failure (validation, malformed bundle)."""


@dataclass
class ImportSummary:
    bundle_path: Path
    sha256: str | None
    session_keys: list[str] = field(default_factory=list)
    events_inserted: int = 0
    events_skipped_existing: int = 0
    overrides_inserted: int = 0
    overrides_rejected: int = 0
    token_usage_inserted: int = 0
    token_usage_skipped_unresolved: int = 0
    cost_anomalies_inserted: int = 0
    cost_anomalies_skipped_unresolved: int = 0
    incidents_inserted: int = 0
    incidents_skipped_unresolved: int = 0
    snippets_inserted: int = 0
    workbench_session_keys_backfilled: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "bundle_path": str(self.bundle_path),
            "sha256": self.sha256,
            "session_keys": list(self.session_keys),
            "events_inserted": self.events_inserted,
            "events_skipped_existing": self.events_skipped_existing,
            "overrides_inserted": self.overrides_inserted,
            "overrides_rejected": self.overrides_rejected,
            "token_usage_inserted": self.token_usage_inserted,
            "token_usage_skipped_unresolved": self.token_usage_skipped_unresolved,
            "cost_anomalies_inserted": self.cost_anomalies_inserted,
            "cost_anomalies_skipped_unresolved": self.cost_anomalies_skipped_unresolved,
            "incidents_inserted": self.incidents_inserted,
            "incidents_skipped_unresolved": self.incidents_skipped_unresolved,
            "snippets_inserted": self.snippets_inserted,
            "workbench_session_keys_backfilled": self.workbench_session_keys_backfilled,
        }


# --------------------------------------------------------------------------- #
# version checks
# --------------------------------------------------------------------------- #


def _check_bundle_version(bundle: dict[str, Any]) -> None:
    version = bundle.get("bundle_schema_version")
    if not isinstance(version, str) or "." not in version:
        raise ImportError_(
            f"missing or malformed bundle_schema_version: {version!r}"
        )
    major, _, minor_str = version.partition(".")
    if major != SUPPORTED_BUNDLE_MAJOR:
        raise ImportError_(
            f"unsupported bundle_schema_version major: {version!r} "
            f"(this clawjournal supports major {SUPPORTED_BUNDLE_MAJOR}.x)"
        )
    minor_head = minor_str.split(".", 1)[0]
    if not minor_head.isdigit():
        raise ImportError_(
            f"malformed bundle_schema_version minor: {version!r}"
        )
    minor = int(minor_head)
    if minor > SUPPORTED_BUNDLE_MINOR:
        warnings.warn(
            f"bundle_schema_version {version!r} is newer than this importer "
            f"knows ({SUPPORTED_BUNDLE_MAJOR}.{SUPPORTED_BUNDLE_MINOR}); "
            "additive fields will be ignored",
            stacklevel=3,
        )

    recorder_version = bundle.get("recorder_schema_version")
    if recorder_version is None:
        return  # absent on bundles older than this field
    if not isinstance(recorder_version, str) or "." not in recorder_version:
        raise ImportError_(
            f"malformed recorder_schema_version: {recorder_version!r}"
        )
    rec_major = recorder_version.split(".", 1)[0]
    if rec_major != SUPPORTED_RECORDER_MAJOR:
        raise ImportError_(
            f"unsupported recorder_schema_version major: {recorder_version!r} "
            f"(this clawjournal supports recorder major "
            f"{SUPPORTED_RECORDER_MAJOR}.x)"
        )


def _verify_manifest_sha256(bundle: dict[str, Any]) -> None:
    """Recompute the canonical sha256 of the bundle minus its manifest
    and compare against ``bundle.manifest.sha256``. Raises ImportError_
    on mismatch — closes the trust gap where a tampered-in-transit
    bundle would otherwise import silently.

    No-op when the bundle has no manifest sha256 (older bundles) or the
    sha256 field is malformed."""
    manifest = bundle.get("manifest") or {}
    expected = manifest.get("sha256")
    if not isinstance(expected, str) or len(expected) != 64:
        return
    digest_input = {k: v for k, v in bundle.items() if k != "manifest"}
    canonical = json.dumps(
        digest_input, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    actual = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    if actual != expected:
        raise ImportError_(
            "manifest.sha256 mismatch — bundle has been modified since "
            f"export (expected {expected[:12]}..., got {actual[:12]}...)"
        )


# --------------------------------------------------------------------------- #
# session upsert (mirrors 02's ingest path semantics)
# --------------------------------------------------------------------------- #


_SESSION_UPSERT_SQL = """
INSERT INTO event_sessions (
    session_key, parent_session_key, parent_session_id,
    client, client_version, started_at, ended_at, status
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(session_key) DO UPDATE SET
    parent_session_key = COALESCE(event_sessions.parent_session_key, excluded.parent_session_key),
    parent_session_id  = COALESCE(event_sessions.parent_session_id, excluded.parent_session_id),
    client_version     = COALESCE(event_sessions.client_version, excluded.client_version),
    started_at         = CASE
        WHEN event_sessions.started_at IS NULL THEN excluded.started_at
        WHEN excluded.started_at IS NULL       THEN event_sessions.started_at
        WHEN excluded.started_at < event_sessions.started_at THEN excluded.started_at
        ELSE event_sessions.started_at
    END,
    ended_at           = CASE
        WHEN event_sessions.ended_at IS NULL THEN excluded.ended_at
        WHEN excluded.ended_at IS NULL       THEN event_sessions.ended_at
        WHEN excluded.ended_at > event_sessions.ended_at THEN excluded.ended_at
        ELSE event_sessions.ended_at
    END,
    status             = CASE
        WHEN event_sessions.status = 'ended' THEN 'ended'
        ELSE excluded.status
    END
"""


def _upsert_session(conn: sqlite3.Connection, block: dict[str, Any]) -> int:
    parent_key = block.get("parent_session_key")
    parent_id: int | None = None
    if parent_key:
        parent_id = _resolve_session_id(conn, parent_key)
    conn.execute(
        _SESSION_UPSERT_SQL,
        (
            block["session_key"],
            parent_key,
            parent_id,
            block["client"],
            block.get("client_version"),
            block.get("started_at"),
            block.get("ended_at"),
            block.get("status") or "active",
        ),
    )
    sid = _resolve_session_id(conn, block["session_key"])
    if sid is None:
        raise ImportError_(
            f"failed to resolve session_id after upsert for "
            f"session_key={block['session_key']!r}"
        )
    return sid


# --------------------------------------------------------------------------- #
# event insert + raw_ref → local-id mapping
# --------------------------------------------------------------------------- #


_INSERT_EVENT_SQL = """
INSERT OR IGNORE INTO events (
    session_id, type, event_key, event_at, ingested_at,
    source, source_path, source_offset, seq, client,
    confidence, lossiness, raw_json
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


def _normalize_raw_ref(raw_ref: list) -> tuple[str, str, int, int]:
    """Coerce a bundle's raw_ref into the 4-tuple
    (source, source_path, source_offset, seq) matching events.UNIQUE.

    The bundle layout requires a 4-element raw_ref; the source field is
    necessary for cross-reference binding because two events from
    different sources can share (source_path, source_offset, seq).

    No legacy 3-tuple fallback: bundle_schema_version 1.0 is the first
    public schema and ships with the 4-tuple. Anything else is a
    malformed bundle.
    """
    if raw_ref is None:
        return None  # type: ignore[return-value]
    if len(raw_ref) == 4:
        return (raw_ref[0], raw_ref[1], int(raw_ref[2]), int(raw_ref[3]))
    raise ImportError_(f"malformed raw_ref (expected 4 elements): {raw_ref!r}")


def _local_source_path_from_workbench(
    conn: sqlite3.Connection,
    block: dict[str, Any],
) -> str | None:
    """Return this machine's raw source path for a bundle session, if known."""
    candidates: list[tuple[str, str]] = []
    workbench_session_id = block.get("workbench_session_id")
    if workbench_session_id:
        candidates.append(("session_id", str(workbench_session_id)))
    session_key = block.get("session_key")
    if session_key:
        candidates.append(("session_key", str(session_key)))

    for column, value in candidates:
        try:
            row = conn.execute(
                f"SELECT raw_source_path FROM sessions WHERE {column} = ?",
                (value,),
            ).fetchone()
        except sqlite3.OperationalError:
            continue
        if row is not None and row["raw_source_path"]:
            return str(row["raw_source_path"])
    return None


def _local_source_paths_by_session_key(
    conn: sqlite3.Connection,
    session_blocks: list[dict[str, Any]],
) -> dict[str, str]:
    paths: dict[str, str] = {}
    for block in session_blocks:
        session_key = block.get("session_key")
        if not session_key:
            continue
        path = _local_source_path_from_workbench(conn, block)
        if path:
            paths[str(session_key)] = path
    return paths


def _is_redacted_path_placeholder(path: str) -> bool:
    return (
        path == _REDACTED_PATH_SENTINEL
        or (
            path.startswith(_REDACTED_PATH_TOKEN_PREFIX)
            and path.endswith("]")
        )
    )


def _redacted_paths_by_session_key(events: list[dict]) -> dict[str, set[str]]:
    paths: dict[str, set[str]] = {}
    for ev in events:
        try:
            ref = _normalize_raw_ref(ev["raw_ref"])
        except (KeyError, TypeError):
            continue
        if _is_redacted_path_placeholder(ref[1]):
            paths.setdefault(str(ev["session_key"]), set()).add(ref[1])
    return paths


def _source_path_for_import(
    ref: tuple[str, str, int, int],
    *,
    session_key: str,
    restore_local_source_paths: bool,
    local_source_paths: dict[str, str],
) -> str:
    if not restore_local_source_paths:
        return ref[1]
    if not _is_redacted_path_placeholder(ref[1]):
        return ref[1]
    return local_source_paths.get(session_key) or ref[1]


def _insert_events_and_map(
    conn: sqlite3.Connection,
    events: list[dict],
    session_id_by_key: dict[str, int],
    *,
    restore_local_source_paths: bool = False,
    local_source_paths: dict[str, str] | None = None,
) -> tuple[dict[tuple[str, str, int, int], int], int, int]:
    """Insert events; return (bundle raw_ref → events.id map, inserted, skipped).

    The map is keyed on the bundle's full 4-tuple
    ``(source, source_path, source_offset, seq)`` so cross-reference
    sections can still bind when the importer restores a local source
    path for inspectability. The lookup is restricted to the sessions we
    just upserted so unrelated local events don't pollute the binding for
    token_usage / incidents / cost_anomalies.
    """
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    inserted = 0
    skipped = 0
    raw_ref_to_id: dict[tuple[str, str, int, int], int] = {}
    local_source_paths = local_source_paths or {}

    for ev in events:
        sid = session_id_by_key.get(ev["session_key"])
        if sid is None:
            raise ImportError_(
                f"event references unknown session_key {ev['session_key']!r}"
            )
        ref = _normalize_raw_ref(ev["raw_ref"])
        stored_source_path = _source_path_for_import(
            ref,
            session_key=ev["session_key"],
            restore_local_source_paths=restore_local_source_paths,
            local_source_paths=local_source_paths,
        )
        # `ev["source"]` is the bundle's per-event source; ref[0] should
        # match. Defend against bundle inconsistencies.
        if ref[0] != "__legacy__" and ref[0] != ev["source"]:
            raise ImportError_(
                f"event source mismatch: raw_ref carries {ref[0]!r} but "
                f"event.source is {ev['source']!r} (event_key={ev.get('event_key')!r})"
            )
        cur = conn.execute(
            _INSERT_EVENT_SQL,
            (
                sid,
                ev["type"],
                ev.get("event_key"),
                ev.get("event_at"),
                now,
                ev["source"],
                stored_source_path,
                ref[2],
                ref[3],
                ev["client"],
                ev["confidence"],
                ev["lossiness"],
                ev["raw_json"],
            ),
        )
        if cur.rowcount > 0:
            inserted += 1
            # Fresh insert: lastrowid is the new events.id — no SELECT needed.
            raw_ref_to_id[ref] = int(cur.lastrowid)
        else:
            skipped += 1
            # Insert was ignored because a row already exists with the same
            # (source, source_path, source_offset, seq). Resolve the local
            # id, but ONLY within the sessions we just upserted — otherwise
            # cross-references would bind to unrelated sessions' events.
            row = conn.execute(
                "SELECT id FROM events "
                "WHERE session_id = ? AND source = ? AND source_path = ? "
                "AND source_offset = ? AND seq = ?",
                (sid, ref[0], stored_source_path, ref[2], ref[3]),
            ).fetchone()
            if row is not None:
                raw_ref_to_id[ref] = row["id"]
    return raw_ref_to_id, inserted, skipped


# --------------------------------------------------------------------------- #
# token_usage / cost_anomalies / incidents
# --------------------------------------------------------------------------- #


_INSERT_TOKEN_USAGE_SQL = """
INSERT OR IGNORE INTO token_usage (
    event_id, session_id, model, model_family, model_tier, model_provider,
    input, output, cache_read, cache_write, reasoning,
    service_tier, data_source, cost_estimate, pricing_table_version, event_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


def _insert_token_usage(
    conn: sqlite3.Connection,
    rows: list[dict],
    session_id_by_key: dict[str, int],
    raw_ref_to_id: dict[tuple[str, str, int, int], int],
) -> tuple[int, int]:
    """Insert token_usage rows from the bundle.

    Uses INSERT OR IGNORE so re-importing a bundle does not overwrite
    locally-recosted values (cost_estimate, pricing_table_version).
    Returns (inserted, skipped_unresolved). ``skipped_unresolved``
    counts rows the importer dropped because the bundle's raw_ref
    couldn't be bound to a local events.id — typically because the
    pre-existing event lives under a different session (see
    events.UNIQUE's per-DB scope).
    """
    n = 0
    unresolved = 0
    for r in rows:
        sid = session_id_by_key.get(r["session_key"])
        if sid is None:
            unresolved += 1
            continue
        ref = _normalize_raw_ref(r["raw_ref"])
        eid = raw_ref_to_id.get(ref)
        if eid is None:
            unresolved += 1
            continue
        cur = conn.execute(
            _INSERT_TOKEN_USAGE_SQL,
            (
                eid,
                sid,
                r.get("model"),
                r.get("model_family"),
                r.get("model_tier"),
                r.get("model_provider"),
                r.get("input"),
                r.get("output"),
                r.get("cache_read"),
                r.get("cache_write"),
                r.get("reasoning"),
                r.get("service_tier"),
                r["data_source"],
                r.get("cost_estimate"),
                r.get("pricing_table_version"),
                r.get("event_at"),
            ),
        )
        if cur.rowcount > 0:
            n += 1
    return n, unresolved


_INSERT_COST_ANOMALY_SQL = """
INSERT OR IGNORE INTO cost_anomalies (
    session_id, turn_event_id, kind, confidence, evidence_json, created_at
) VALUES (?, ?, ?, ?, ?, ?)
"""


def _insert_cost_anomalies(
    conn: sqlite3.Connection,
    rows: list[dict],
    session_id_by_key: dict[str, int],
    raw_ref_to_id: dict[tuple[str, str, int, int], int],
) -> tuple[int, int]:
    """Returns (inserted, skipped_unresolved). See ``_insert_token_usage``."""
    n = 0
    unresolved = 0
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    for r in rows:
        sid = session_id_by_key.get(r["session_key"])
        if sid is None:
            unresolved += 1
            continue
        turn_ref = r.get("turn_raw_ref")
        if turn_ref:
            eid = raw_ref_to_id.get(_normalize_raw_ref(turn_ref))
            if eid is None:
                unresolved += 1
                continue
        else:
            eid = None
        cur = conn.execute(
            _INSERT_COST_ANOMALY_SQL,
            (
                sid,
                eid,
                r["kind"],
                r["confidence"],
                json.dumps(r.get("evidence", {}), sort_keys=True),
                r.get("created_at") or now,
            ),
        )
        if cur.rowcount > 0:
            n += 1
    return n, unresolved


_INSERT_INCIDENT_SQL = """
INSERT OR IGNORE INTO incidents (
    session_id, kind, first_event_id, last_event_id,
    evidence_json, count, confidence, created_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"""


def _insert_incidents(
    conn: sqlite3.Connection,
    rows: list[dict],
    session_id_by_key: dict[str, int],
    raw_ref_to_id: dict[tuple[str, str, int, int], int],
) -> tuple[int, int]:
    """Returns (inserted, skipped_unresolved). See ``_insert_token_usage``."""
    n = 0
    unresolved = 0
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    for r in rows:
        sid = session_id_by_key.get(r["session_key"])
        if sid is None:
            unresolved += 1
            continue
        first_ref = _normalize_raw_ref(r["first_raw_ref"])
        last_ref = _normalize_raw_ref(r["last_raw_ref"])
        first_id = raw_ref_to_id.get(first_ref)
        last_id = raw_ref_to_id.get(last_ref)
        if first_id is None or last_id is None:
            unresolved += 1
            continue
        cur = conn.execute(
            _INSERT_INCIDENT_SQL,
            (
                sid,
                r["kind"],
                first_id,
                last_id,
                json.dumps(r.get("evidence", {}), sort_keys=True),
                r["count"],
                r["confidence"],
                r.get("created_at") or now,
            ),
        )
        if cur.rowcount > 0:
            n += 1
    return n, unresolved


# --------------------------------------------------------------------------- #
# overrides
# --------------------------------------------------------------------------- #


def _insert_overrides(
    conn: sqlite3.Connection, rows: list[dict]
) -> tuple[int, int]:
    """Apply override rows from the bundle.

    Uses the no-transaction inner helper because the importer manages a
    single outer transaction around the whole import. ``created_at`` is
    passed through from the bundle so re-imports are idempotent against
    the override row's wall-clock timestamp (otherwise every re-import
    would mutate ``event_overrides.created_at`` even when the row
    counts don't change).
    """
    inserted = 0
    rejected = 0
    for r in rows:
        landed = _write_hook_override_inner(
            conn,
            session_key=r["session_key"],
            event_key=r["event_key"],
            event_type=r["type"],
            source=r["source"],
            confidence=r["confidence"],
            lossiness=r["lossiness"],
            event_at=r.get("event_at"),
            payload_json=r["payload_json"],
            origin=r.get("origin"),
            created_at=r.get("created_at"),
        )
        if landed:
            inserted += 1
        else:
            rejected += 1
    return inserted, rejected


# --------------------------------------------------------------------------- #
# snippets
# --------------------------------------------------------------------------- #


_INSERT_SNIPPET_SQL = """
INSERT OR REPLACE INTO event_source_snippets (
    source_path, source_offset, seq, text, imported_at
) VALUES (?, ?, ?, ?, ?)
"""


def _insert_snippets(conn: sqlite3.Connection, snippets: dict[str, str]) -> int:
    """Materialize bundle source_snippets into ``event_source_snippets``.

    Bundle key shape is ``<source>:<source_path>:<source_offset>:<seq>``
    (matches the 4-tuple raw_ref). We rsplit thrice to pull the
    integers + source off the tail, leaving the path (which may itself
    contain ':' or be the anonymized ``[REDACTED_PATH]`` literal) as
    the prefix.

    Note: 03's ``event_source_snippets`` table is keyed
    ``(source_path, source_offset, seq)`` — the snippet PK doesn't
    include ``source`` today. This means two events from different
    sources sharing ``(anon_path, offset, seq)`` still collide on
    insert; the second snippet overwrites the first. Tracked as a
    follow-up; the export-side fix above prevents the in-bundle
    collision so cross-source disambiguation lives in the bundle even
    if the local table can't represent it yet.
    """
    n = 0
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    for key, text in snippets.items():
        try:
            head, seq_str = key.rsplit(":", 1)
            head, offset_str = head.rsplit(":", 1)
            _source, _, path = head.partition(":")
            if not path:
                raise ImportError_(
                    f"malformed snippet key (expected source:path:offset:seq): {key!r}"
                )
            offset = int(offset_str)
            seq = int(seq_str)
        except (ValueError, AttributeError) as exc:
            raise ImportError_(f"malformed snippet key: {key!r}") from exc
        conn.execute(_INSERT_SNIPPET_SQL, (path, offset, seq, text, now))
        n += 1
    return n


# --------------------------------------------------------------------------- #
# workbench session_key backfill
# --------------------------------------------------------------------------- #


def _backfill_workbench_session_key(
    conn: sqlite3.Connection, workbench_session_id: str | None, session_key: str
) -> bool:
    """If a workbench `sessions` row exists with the bundle's
    workbench_session_id and its `session_key` is NULL, fill it.
    Never overwrites a non-null session_key. Returns True if updated."""
    if not workbench_session_id:
        return False
    try:
        cur = conn.execute(
            "UPDATE sessions SET session_key = ? "
            "WHERE session_id = ? AND session_key IS NULL",
            (session_key, workbench_session_id),
        )
        return cur.rowcount > 0
    except sqlite3.OperationalError:
        return False


# --------------------------------------------------------------------------- #
# entry point
# --------------------------------------------------------------------------- #


def import_session_bundle(
    conn: sqlite3.Connection,
    bundle_path: str | Path,
    *,
    rebuild_derived: bool = False,
) -> ImportSummary:
    """Import a bundle JSON file into the local DB.

    See module docstring for invariants. Returns an ImportSummary; raises
    ImportError_ on validation failure (unsupported version, malformed
    structure).

    `rebuild_derived` is accepted but not yet wired through to 04/05's
    rebuild paths — the v0.1 implementation always preserves the bundle's
    cost_anomalies / incidents verbatim. Pass-through for forward-compat.
    """
    ensure_events_schema(conn)
    ensure_view_schema(conn)
    try:
        from clawjournal.events.cost.schema import ensure_cost_schema

        ensure_cost_schema(conn)
    except Exception:
        pass
    try:
        from clawjournal.events.incidents.schema import ensure_incidents_schema

        ensure_incidents_schema(conn)
    except Exception:
        pass
    ensure_export_schema(conn)

    path = Path(bundle_path).expanduser().resolve()
    text = path.read_text(encoding="utf-8")
    try:
        bundle = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ImportError_(f"bundle is not valid JSON: {exc}") from exc

    _check_bundle_version(bundle)

    manifest = bundle.get("manifest") or {}
    if manifest.get("blocked"):
        raise ImportError_(
            f"bundle is a manifest-only blocked artifact "
            f"(reason={manifest.get('block_reason')!r}); "
            "no events to import"
        )

    _verify_manifest_sha256(bundle)

    sha256 = manifest.get("sha256")

    summary = ImportSummary(bundle_path=path, sha256=sha256)

    parent_block = bundle["session"]
    children_blocks = list(bundle.get("children") or [])
    events = bundle.get("events") or []
    session_blocks = [parent_block] + children_blocks
    restore_local_source_paths = "source_snippets" not in bundle
    redacted_paths_by_session = (
        _redacted_paths_by_session_key(events)
        if restore_local_source_paths
        else {}
    )
    local_source_paths = (
        _local_source_paths_by_session_key(conn, session_blocks)
        if restore_local_source_paths
        else {}
    )
    if restore_local_source_paths:
        local_source_paths = {
            session_key: source_path
            for session_key, source_path in local_source_paths.items()
            if len(redacted_paths_by_session.get(session_key, set())) <= 1
        }

    with conn:  # one transaction for the whole import
        session_id_by_key: dict[str, int] = {}

        parent_sid = _upsert_session(conn, parent_block)
        session_id_by_key[parent_block["session_key"]] = parent_sid
        if _backfill_workbench_session_key(
            conn,
            parent_block.get("workbench_session_id"),
            parent_block["session_key"],
        ):
            summary.workbench_session_keys_backfilled += 1

        for child_block in children_blocks:
            cid = _upsert_session(conn, child_block)
            session_id_by_key[child_block["session_key"]] = cid
            if _backfill_workbench_session_key(
                conn,
                child_block.get("workbench_session_id"),
                child_block["session_key"],
            ):
                summary.workbench_session_keys_backfilled += 1

        raw_ref_to_id, inserted, skipped = _insert_events_and_map(
            conn,
            events,
            session_id_by_key,
            restore_local_source_paths=restore_local_source_paths,
            local_source_paths=local_source_paths,
        )
        summary.events_inserted = inserted
        summary.events_skipped_existing = skipped

        overrides = bundle.get("event_overrides") or []
        ov_inserted, ov_rejected = _insert_overrides(conn, overrides)
        summary.overrides_inserted = ov_inserted
        summary.overrides_rejected = ov_rejected

        token_usage = bundle.get("token_usage") or []
        (
            summary.token_usage_inserted,
            summary.token_usage_skipped_unresolved,
        ) = _insert_token_usage(
            conn, token_usage, session_id_by_key, raw_ref_to_id
        )

        cost_anomalies = bundle.get("cost_anomalies") or []
        (
            summary.cost_anomalies_inserted,
            summary.cost_anomalies_skipped_unresolved,
        ) = _insert_cost_anomalies(
            conn, cost_anomalies, session_id_by_key, raw_ref_to_id
        )

        incidents = bundle.get("incidents") or []
        (
            summary.incidents_inserted,
            summary.incidents_skipped_unresolved,
        ) = _insert_incidents(
            conn, incidents, session_id_by_key, raw_ref_to_id
        )

        snippets = bundle.get("source_snippets") or {}
        summary.snippets_inserted = _insert_snippets(conn, snippets)

    summary.session_keys = [parent_block["session_key"]] + [
        c["session_key"] for c in children_blocks
    ]

    if rebuild_derived:
        # 04/05's existing --rebuild flags are global today; the plan calls
        # for a per-session-scoped rebuild, but plumbing that through both
        # subsystems is a larger change. For v0.1 we run the global rebuild
        # paths so the imported sessions get fresh derived state — at the
        # cost of also touching unrelated sessions. Tracked as follow-up.
        try:
            from clawjournal.events.cost import ingest_cost_pending
        except ImportError:
            warnings.warn(
                "rebuild_derived: cost ledger module not available; skipping",
                stacklevel=2,
            )
        else:
            try:
                ingest_cost_pending(conn, rebuild=True)
            except Exception as exc:
                warnings.warn(
                    f"rebuild_derived: cost-ledger rebuild failed: {exc!r}",
                    stacklevel=2,
                )

        try:
            from clawjournal.events.incidents import ingest_loop_incidents
        except ImportError:
            warnings.warn(
                "rebuild_derived: loop detector module not available; skipping",
                stacklevel=2,
            )
        else:
            try:
                ingest_loop_incidents(conn, rebuild=True)
            except Exception as exc:
                warnings.warn(
                    f"rebuild_derived: loop-detector rebuild failed: {exc!r}",
                    stacklevel=2,
                )

    return summary
