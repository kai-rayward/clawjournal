"""Drive the loop detector incrementally from already-recorded events.

The driver mirrors the cost ledger pattern (`events/cost/ingest.py`):

- A `loop_ingest_state` cursor tracks the highest `events.id` we've
  evaluated, so re-running with no new events is a no-op.
- Every session that gained a new event since the last run gets its
  full event list re-evaluated by `detect_session_loops`. Recomputing
  the whole session is necessary because a new repeat can extend a
  pre-existing run — we have to update the prior `incidents` row's
  `count` / `last_event_id`.
- Per-session refresh deletes existing `loop_exact_repeat` rows for
  that session before re-inserting the current hit set, so runs that
  shrink (because of a re-classification or override) don't leave
  stale incident rows behind.
- `--rebuild` clears the entire `incidents` table for `kind =
  loop_exact_repeat` plus the cursor, then replays from `events.id =
  0`.

The schema's `UNIQUE (session_id, kind, first_event_id)` is the
spec's dedupe key; the per-session DELETE keeps it from ever
firing in normal operation but the constraint stays as defense in
depth.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone

from clawjournal.events.incidents.loop_detector import (
    DEFAULT_RULES,
    LoopRule,
    detect_session_loops,
)
from clawjournal.events.incidents.schema import ensure_incidents_schema
from clawjournal.events.incidents.types import LOOP_INCIDENT_KIND
from clawjournal.events.schema import ensure_schema as ensure_events_schema

LOOP_CONSUMER_ID = "loop_detector"


@dataclass
class LoopIngestSummary:
    events_scanned: int = 0
    sessions_evaluated: int = 0
    incidents_written: int = 0
    sessions_touched: set[int] = field(default_factory=set, repr=False)

    def to_dict(self) -> dict[str, int]:
        return {
            "events_scanned": self.events_scanned,
            "sessions_evaluated": self.sessions_evaluated,
            "incidents_written": self.incidents_written,
            "sessions_touched": len(self.sessions_touched),
        }


_SELECT_NEW_EVENT_SESSIONS_SQL = """
SELECT DISTINCT session_id, COUNT(*) AS new_event_count
  FROM events
 WHERE id > ?
 GROUP BY session_id
"""

_SELECT_MAX_EVENT_ID_SQL = """
SELECT COALESCE(MAX(id), 0) AS max_id FROM events
"""

_SELECT_LAST_EVENT_ID_SQL = """
SELECT last_event_id FROM loop_ingest_state WHERE consumer_id = ?
"""

_UPSERT_LAST_EVENT_ID_SQL = """
INSERT INTO loop_ingest_state (consumer_id, last_event_id)
VALUES (?, ?)
ON CONFLICT(consumer_id) DO UPDATE SET last_event_id = excluded.last_event_id
"""

_DELETE_LOOP_INCIDENTS_FOR_SESSION_SQL = """
DELETE FROM incidents WHERE session_id = ? AND kind = ?
"""

_DELETE_ALL_LOOP_INCIDENTS_SQL = """
DELETE FROM incidents WHERE kind = ?
"""

_DELETE_INGEST_STATE_SQL = """
DELETE FROM loop_ingest_state WHERE consumer_id = ?
"""

_INSERT_INCIDENT_SQL = """
INSERT INTO incidents (
    session_id, kind, first_event_id, last_event_id,
    evidence_json, count, confidence, created_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(session_id, kind, first_event_id) DO UPDATE SET
    last_event_id = excluded.last_event_id,
    evidence_json = excluded.evidence_json,
    count         = excluded.count,
    confidence    = excluded.confidence,
    created_at    = excluded.created_at
"""


def ingest_loop_incidents(
    conn: sqlite3.Connection,
    *,
    now: datetime | None = None,
    rebuild: bool = False,
    rules: tuple[LoopRule, ...] = DEFAULT_RULES,
) -> LoopIngestSummary:
    """Scan for new events, re-evaluate touched sessions, and refresh
    `incidents` rows of kind `loop_exact_repeat`.

    With `rebuild=True`, the cursor + all loop incidents are cleared
    first and every session with at least one event is re-evaluated.
    """
    ensure_events_schema(conn)
    ensure_incidents_schema(conn)

    summary = LoopIngestSummary()
    created_at = _utc_now_iso(now)
    last_event_id = 0 if rebuild else _get_last_processed_event_id(conn)

    new_session_rows = conn.execute(
        _SELECT_NEW_EVENT_SESSIONS_SQL, (last_event_id,)
    ).fetchall()
    sessions_to_evaluate: list[int] = [int(r["session_id"]) for r in new_session_rows]
    summary.events_scanned = sum(int(r["new_event_count"]) for r in new_session_rows)

    if rebuild:
        all_sessions = conn.execute(
            "SELECT id FROM event_sessions"
        ).fetchall()
        sessions_to_evaluate = [int(r["id"]) for r in all_sessions]

    max_event_id_row = conn.execute(_SELECT_MAX_EVENT_ID_SQL).fetchone()
    max_event_id = int(max_event_id_row["max_id"] or 0)

    if not sessions_to_evaluate and not rebuild:
        return summary

    with conn:
        if rebuild:
            _reset_loop_state(conn)

        for session_id in sorted(sessions_to_evaluate):
            summary.sessions_evaluated += 1
            hits = detect_session_loops(conn, session_id, rules=rules)
            conn.execute(
                _DELETE_LOOP_INCIDENTS_FOR_SESSION_SQL,
                (session_id, LOOP_INCIDENT_KIND),
            )
            if not hits:
                continue
            conn.executemany(
                _INSERT_INCIDENT_SQL,
                [
                    (
                        hit.session_id,
                        hit.kind,
                        hit.first_event_id,
                        hit.last_event_id,
                        json.dumps(hit.evidence, sort_keys=True),
                        hit.count,
                        hit.confidence,
                        created_at,
                    )
                    for hit in hits
                ],
            )
            summary.incidents_written += len(hits)
            summary.sessions_touched.add(session_id)

        if max_event_id > last_event_id or rebuild:
            conn.execute(
                _UPSERT_LAST_EVENT_ID_SQL,
                (LOOP_CONSUMER_ID, max_event_id),
            )

    return summary


def rebuild_loop_incidents(
    conn: sqlite3.Connection,
    *,
    now: datetime | None = None,
) -> LoopIngestSummary:
    """Clear loop incidents + cursor and re-evaluate every session."""
    return ingest_loop_incidents(conn, now=now, rebuild=True)


def _get_last_processed_event_id(conn: sqlite3.Connection) -> int:
    row = conn.execute(_SELECT_LAST_EVENT_ID_SQL, (LOOP_CONSUMER_ID,)).fetchone()
    if row is None:
        return 0
    return int(row["last_event_id"])


def _reset_loop_state(conn: sqlite3.Connection) -> None:
    conn.execute(_DELETE_ALL_LOOP_INCIDENTS_SQL, (LOOP_INCIDENT_KIND,))
    conn.execute(_DELETE_INGEST_STATE_SQL, (LOOP_CONSUMER_ID,))


def _utc_now_iso(now: datetime | None) -> str:
    effective = now or datetime.now(timezone.utc)
    return effective.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
