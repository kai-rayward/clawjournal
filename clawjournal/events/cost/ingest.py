"""Drive the cost ledger from already-ingested `events` rows.

Walks new `events` rows since the cost ledger's last successful run,
runs the per-client extractor against `raw_json`, and persists a row
(`data_source='api'`) when the vendor surfaced usage. Rows that carry
no usage block are still marked as examined via the cost-ingest cursor
so repeated runs do not rescan them forever.

For Codex — where the model id is carried on `turn_context` rather
than the `token_count` event itself — we backfill the most-recently-
seen model per session as we walk in canonical order, and seed from
earlier raw events when a token_count lands in a later batch than its
preceding turn_context.

This module deliberately does NOT perform content-length estimation.
The spec leaves the door open for `data_source='estimated'` rows but
the v0.1 ingest only records what the API actually emitted; the
extraction layer (`clawjournal/events/cost/extract/`) returns None
for lines without a usage block. Estimation is tracked as a
follow-up so that the data we do persist is unambiguously vendor
truth.

After token_usage rows land for the touched sessions, we run the
anomaly detectors on those sessions and replace that session's
`cost_anomalies` rows with the fresh hit set so stale anomalies do not
linger after late-arriving events change adjacency or baselines.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone

from clawjournal.events.cost.anomalies import detect_session_anomalies
from clawjournal.events.cost.extract import extract_tokens
from clawjournal.events.cost.pricing import (
    PRICING_TABLE_VERSION,
    estimate_cost,
    normalize_model,
)
from clawjournal.events.cost.schema import ensure_cost_schema
from clawjournal.events.schema import ensure_schema as ensure_events_schema

COST_CONSUMER_ID = "cost_ledger"


@dataclass
class CostIngestSummary:
    events_scanned: int = 0
    token_rows_written: int = 0
    anomalies_written: int = 0
    sessions_touched: set[int] = field(default_factory=set, repr=False)

    def to_dict(self) -> dict[str, int]:
        return {
            "events_scanned": self.events_scanned,
            "token_rows_written": self.token_rows_written,
            "anomalies_written": self.anomalies_written,
            "sessions_touched": len(self.sessions_touched),
        }


_INSERT_TOKEN_USAGE_SQL = """
INSERT OR REPLACE INTO token_usage (
    event_id, session_id, model, model_family, model_tier, model_provider,
    input, output, cache_read, cache_write, reasoning,
    service_tier, data_source, cost_estimate, pricing_table_version, event_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

_INSERT_ANOMALY_SQL = """
INSERT INTO cost_anomalies (
    session_id, turn_event_id, kind, confidence, evidence_json, created_at
) VALUES (?, ?, ?, ?, ?, ?)
"""

_SELECT_NEW_EVENTS_SQL = """
SELECT events.id           AS event_id,
       events.session_id   AS session_id,
       events.client       AS client,
       events.type         AS type,
       events.event_at     AS event_at,
       events.raw_json     AS raw_json
  FROM events
 WHERE events.id > ?
 ORDER BY events.session_id, events.event_at IS NULL, events.event_at, events.id
"""

_SELECT_LAST_EVENT_ID_SQL = """
SELECT last_event_id
  FROM cost_ingest_state
 WHERE consumer_id = ?
"""

_UPSERT_LAST_EVENT_ID_SQL = """
INSERT INTO cost_ingest_state (consumer_id, last_event_id)
VALUES (?, ?)
ON CONFLICT(consumer_id) DO UPDATE SET
    last_event_id = excluded.last_event_id
"""

_DELETE_INGEST_STATE_SQL = """
DELETE FROM cost_ingest_state
 WHERE consumer_id = ?
"""

_DELETE_ALL_TOKEN_USAGE_SQL = """
DELETE FROM token_usage
"""

_DELETE_SESSION_ANOMALIES_SQL = """
DELETE FROM cost_anomalies
 WHERE session_id = ?
"""

_DELETE_ALL_ANOMALIES_SQL = """
DELETE FROM cost_anomalies
"""


def ingest_cost_pending(
    conn: sqlite3.Connection,
    *,
    now: datetime | None = None,
    rebuild: bool = False,
) -> CostIngestSummary:
    """Scan new events, persist usage where the vendor emitted it, and
    (re)compute anomalies for touched sessions.

    The per-run cursor advances only after the token rows and anomaly
    refresh commit together, so a crash cannot skip unprocessed
    events. Re-running with no new events is a no-op.

    With `rebuild=True`, the cost-ledger tables are cleared first and
    then replayed from `events` id 1 onward. This is the supported path
    when extractor coverage expands or an operator wants to recompute
    historical rows from the raw event stream.
    """
    ensure_events_schema(conn)
    ensure_cost_schema(conn)

    summary = CostIngestSummary()
    created_at = _utc_now_iso(now)
    last_model_per_session: dict[int, str] = {}
    last_event_id = 0 if rebuild else _get_last_processed_event_id(conn)

    rows = conn.execute(_SELECT_NEW_EVENTS_SQL, (last_event_id,)).fetchall()
    pending: list[tuple] = []
    max_event_id = last_event_id
    for row in rows:
        summary.events_scanned += 1
        event_id = int(row["event_id"])
        session_id = int(row["session_id"])
        max_event_id = max(max_event_id, event_id)
        client = row["client"]
        try:
            line = json.loads(row["raw_json"])
        except (TypeError, json.JSONDecodeError):
            continue
        if not isinstance(line, dict):
            continue

        # For Codex, threads the latest model from turn_context onto
        # later token_count events in the same session.
        if client == "codex":
            model_from_turn = _codex_model_from_turn_context(line)
            if model_from_turn:
                last_model_per_session[session_id] = model_from_turn

        usage = extract_tokens(client, line)
        if usage is None:
            continue

        model = usage.model
        if model is None and client == "codex":
            model = last_model_per_session.get(session_id)
            if model is None:
                model = _latest_codex_model_before_event(
                    conn,
                    session_id=session_id,
                    event_at=row["event_at"],
                    event_id=event_id,
                )
                if model:
                    last_model_per_session[session_id] = model

        info = normalize_model(model)
        cost = estimate_cost(
            info,
            input_tokens=usage.input,
            output_tokens=usage.output,
            cache_read_tokens=usage.cache_read,
            cache_write_tokens=usage.cache_write,
            reasoning_tokens=usage.reasoning,
        )

        pending.append(
            (
                event_id,
                session_id,
                model,
                info.family,
                info.tier,
                info.provider,
                usage.input,
                usage.output,
                usage.cache_read,
                usage.cache_write,
                usage.reasoning,
                usage.service_tier,
                usage.data_source,
                cost,
                PRICING_TABLE_VERSION,
                row["event_at"],
            )
        )
        summary.sessions_touched.add(session_id)

    if summary.events_scanned or rebuild:
        with conn:
            if rebuild:
                _reset_cost_ledger(conn)
            if pending:
                conn.executemany(_INSERT_TOKEN_USAGE_SQL, pending)
                summary.token_rows_written = len(pending)

            # Anomaly detection runs against the post-insert state, so any
            # session that just received a new token_usage row gets fully
            # recomputed. Delete-then-insert keeps the table aligned with the
            # current session view and drops stale hits.
            for session_id in sorted(summary.sessions_touched):
                hits = detect_session_anomalies(conn, session_id)
                conn.execute(_DELETE_SESSION_ANOMALIES_SQL, (session_id,))
                if not hits:
                    continue
                conn.executemany(
                    _INSERT_ANOMALY_SQL,
                    [
                        (
                            hit.session_id,
                            hit.turn_event_id,
                            hit.kind,
                            hit.confidence,
                            json.dumps(hit.evidence, sort_keys=True),
                            created_at,
                        )
                        for hit in hits
                    ],
                )
                summary.anomalies_written += len(hits)

            conn.execute(
                _UPSERT_LAST_EVENT_ID_SQL,
                (COST_CONSUMER_ID, max_event_id),
            )

    return summary


def rebuild_cost_ledger(
    conn: sqlite3.Connection,
    *,
    now: datetime | None = None,
) -> CostIngestSummary:
    """Replay the full cost ledger from raw events."""
    return ingest_cost_pending(conn, now=now, rebuild=True)


def _get_last_processed_event_id(conn: sqlite3.Connection) -> int:
    row = conn.execute(_SELECT_LAST_EVENT_ID_SQL, (COST_CONSUMER_ID,)).fetchone()
    if row is None:
        return 0
    return int(row["last_event_id"])


def _reset_cost_ledger(conn: sqlite3.Connection) -> None:
    """Clear derived cost-ledger state so the next ingest replays all events."""
    conn.execute(_DELETE_ALL_ANOMALIES_SQL)
    conn.execute(_DELETE_ALL_TOKEN_USAGE_SQL)
    conn.execute(_DELETE_INGEST_STATE_SQL, (COST_CONSUMER_ID,))


def _latest_codex_model_before_event(
    conn: sqlite3.Connection,
    *,
    session_id: int,
    event_at: str | None,
    event_id: int,
) -> str | None:
    """Find the nearest earlier Codex turn_context model for a session.

    Incremental ingest only scans new events, so a token_count row may land in
    a later run than its preceding turn_context. Query the earlier raw events in
    canonical order to recover the model rather than depending on re-scans.
    """
    if event_at is not None:
        model = _latest_codex_model_from_rows(
            conn.execute(
                """
                SELECT raw_json
                  FROM events
                 WHERE session_id = ?
                   AND client = 'codex'
                   AND event_at IS NOT NULL
                   AND (event_at < ? OR (event_at = ? AND id < ?))
                 ORDER BY event_at DESC, id DESC
                """,
                (session_id, event_at, event_at, event_id),
            )
        )
        if model:
            return model

    # Fallback for earlier rows that carried no vendor timestamp. This keeps
    # replay robust when an older turn_context predates a later token_count by
    # source order / id but not by event_at presence.
    model = _latest_codex_model_from_rows(
        conn.execute(
            """
            SELECT raw_json
              FROM events
             WHERE session_id = ?
               AND client = 'codex'
               AND event_at IS NULL
               AND id < ?
             ORDER BY id DESC
            """,
            (session_id, event_id),
        )
    )
    if model:
        return model

    if event_at is None:
        model = _latest_codex_model_from_rows(
            conn.execute(
                """
                SELECT raw_json
                  FROM events
                 WHERE session_id = ?
                   AND client = 'codex'
                   AND event_at IS NOT NULL
                   AND id < ?
                 ORDER BY event_at DESC, id DESC
                """,
                (session_id, event_id),
            )
        )
        if model:
            return model
    return None


def _latest_codex_model_from_rows(rows) -> str | None:
    for row in rows:
        try:
            line = json.loads(row["raw_json"])
        except (TypeError, json.JSONDecodeError):
            continue
        if not isinstance(line, dict):
            continue
        model = _codex_model_from_turn_context(line)
        if model:
            return model
    return None


def _codex_model_from_turn_context(line: dict) -> str | None:
    if line.get("type") != "turn_context":
        return None
    payload = line.get("payload")
    if not isinstance(payload, dict):
        return None
    model = payload.get("model")
    if isinstance(model, str) and model.strip():
        return model
    return None


def _utc_now_iso(now: datetime | None) -> str:
    effective = now or datetime.now(timezone.utc)
    return effective.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
