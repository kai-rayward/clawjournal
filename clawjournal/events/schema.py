"""SQLite schema for the execution recorder."""

from __future__ import annotations

import sqlite3

EVENT_SESSIONS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS event_sessions (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    session_key        TEXT    NOT NULL UNIQUE,
    parent_session_key TEXT,
    parent_session_id  INTEGER REFERENCES event_sessions(id) ON DELETE SET NULL,
    client             TEXT    NOT NULL,
    client_version     TEXT,
    started_at         TEXT,
    ended_at           TEXT,
    status             TEXT    NOT NULL DEFAULT 'active'
);
"""

EVENTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS events (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id    INTEGER NOT NULL REFERENCES event_sessions(id) ON DELETE CASCADE,
    type          TEXT    NOT NULL,
    event_key     TEXT,
    event_at      TEXT,
    ingested_at   TEXT    NOT NULL,
    source        TEXT    NOT NULL,
    source_path   TEXT    NOT NULL,
    source_offset INTEGER NOT NULL,
    seq           INTEGER NOT NULL DEFAULT 0,
    client        TEXT    NOT NULL,
    confidence    TEXT    NOT NULL,
    lossiness     TEXT    NOT NULL,
    raw_json      TEXT    NOT NULL,
    UNIQUE (source, source_path, source_offset, seq)
);
CREATE INDEX IF NOT EXISTS idx_events_session_time
    ON events(session_id, event_at);
CREATE INDEX IF NOT EXISTS idx_events_session_source
    ON events(session_id, source, source_path, source_offset, seq);
CREATE INDEX IF NOT EXISTS idx_events_event_key
    ON events(session_id, event_key)
    WHERE event_key IS NOT NULL;
"""

EVENT_SESSIONS_PARENT_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_event_sessions_parent_key
    ON event_sessions(parent_session_key)
    WHERE parent_session_key IS NOT NULL;
"""


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute(EVENT_SESSIONS_TABLE_SQL)

    existing = {
        row[1] for row in conn.execute("PRAGMA table_info(event_sessions)")
    }
    if "parent_session_key" not in existing:
        conn.execute("ALTER TABLE event_sessions ADD COLUMN parent_session_key TEXT")
    if "parent_session_id" not in existing:
        conn.execute(
            "ALTER TABLE event_sessions "
            "ADD COLUMN parent_session_id INTEGER REFERENCES event_sessions(id) ON DELETE SET NULL"
        )

    conn.executescript(EVENT_SESSIONS_PARENT_INDEX_SQL)
    conn.executescript(EVENTS_TABLE_SQL)
