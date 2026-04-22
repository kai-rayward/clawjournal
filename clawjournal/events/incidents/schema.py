"""SQLite schema for the incidents pipeline (phase-1 plan 05).

Two tables, both alongside 02's `events` / `event_sessions` in
`~/.clawjournal/index.db`:

- `incidents` — one row per detected incident. Shared by all
  incident kinds (loop_exact_repeat today; later beats add more).
  `UNIQUE (session_id, kind, first_event_id)` is the spec's dedupe
  key — re-running ingest updates the same row's `count` /
  `last_event_id` / `evidence_json` rather than inserting again.
- `loop_ingest_state` — per-consumer cursor (consumer_id PK,
  last_event_id) so the loop detector can advance only after the
  events it needs to evaluate have actually been ingested by 02.
"""

from __future__ import annotations

import sqlite3

INCIDENTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS incidents (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id      INTEGER NOT NULL REFERENCES event_sessions(id) ON DELETE CASCADE,
    kind            TEXT    NOT NULL,
    first_event_id  INTEGER NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    last_event_id   INTEGER NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    evidence_json   TEXT    NOT NULL,
    count           INTEGER NOT NULL,
    confidence      TEXT    NOT NULL,
    created_at      TEXT    NOT NULL,
    UNIQUE (session_id, kind, first_event_id)
);
CREATE INDEX IF NOT EXISTS idx_incidents_session
    ON incidents(session_id, kind);
"""

LOOP_INGEST_STATE_SQL = """
CREATE TABLE IF NOT EXISTS loop_ingest_state (
    consumer_id   TEXT PRIMARY KEY,
    last_event_id INTEGER NOT NULL
);
"""


def ensure_incidents_schema(conn: sqlite3.Connection) -> None:
    """Create the incidents tables if absent. Safe to call repeatedly."""
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(INCIDENTS_TABLE_SQL)
    conn.executescript(LOOP_INGEST_STATE_SQL)
