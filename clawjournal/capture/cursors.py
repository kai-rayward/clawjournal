"""Capture cursor persistence.

Cursors are per-consumer. Each downstream consumer (the workbench Scanner
adapter, the 02 normalized-event pipeline, ...) owns its own cursor rows.
The primary key is (consumer_id, source_path). A consumer advances its
cursor only after its own sink commit, so one consumer cannot cause
another to miss data and a crash between cursor advance and sink commit
cannot lose data — the retry re-reads the unadvanced range.

Stored in the clawjournal workbench index.db so capture and workbench
share one data root.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

CURSOR_SCHEMA = """
CREATE TABLE IF NOT EXISTS capture_cursors (
    consumer_id   TEXT NOT NULL,
    source_path   TEXT NOT NULL,
    inode         INTEGER NOT NULL,
    last_offset   INTEGER NOT NULL,
    last_modified REAL NOT NULL,
    client        TEXT NOT NULL,
    first_seen    TEXT NOT NULL,
    last_seen     TEXT NOT NULL,
    PRIMARY KEY (consumer_id, source_path)
);
CREATE INDEX IF NOT EXISTS idx_capture_cursors_client ON capture_cursors(client);
CREATE INDEX IF NOT EXISTS idx_capture_cursors_path ON capture_cursors(source_path);
"""


@dataclass(frozen=True)
class Cursor:
    consumer_id: str
    source_path: str
    inode: int
    last_offset: int
    last_modified: float
    client: str


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(CURSOR_SCHEMA)


def get_cursor(
    conn: sqlite3.Connection, consumer_id: str, source_path: Path | str
) -> Cursor | None:
    row = conn.execute(
        "SELECT consumer_id, source_path, inode, last_offset, last_modified, client "
        "FROM capture_cursors WHERE consumer_id = ? AND source_path = ?",
        (consumer_id, str(source_path)),
    ).fetchone()
    if row is None:
        return None
    return Cursor(*row)


def set_cursor(conn: sqlite3.Connection, cursor: Cursor) -> None:
    now_iso = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        INSERT INTO capture_cursors
            (consumer_id, source_path, inode, last_offset, last_modified, client, first_seen, last_seen)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(consumer_id, source_path) DO UPDATE SET
            inode = excluded.inode,
            last_offset = excluded.last_offset,
            last_modified = excluded.last_modified,
            client = excluded.client,
            last_seen = excluded.last_seen
        """,
        (
            cursor.consumer_id,
            cursor.source_path,
            cursor.inode,
            cursor.last_offset,
            cursor.last_modified,
            cursor.client,
            now_iso,
            now_iso,
        ),
    )


def list_cursors(
    conn: sqlite3.Connection,
    *,
    consumer_id: str | None = None,
    client: str | None = None,
) -> list[Cursor]:
    clauses: list[str] = []
    params: list[str] = []
    if consumer_id is not None:
        clauses.append("consumer_id = ?")
        params.append(consumer_id)
    if client is not None:
        clauses.append("client = ?")
        params.append(client)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    rows = conn.execute(
        "SELECT consumer_id, source_path, inode, last_offset, last_modified, client "
        f"FROM capture_cursors {where} ORDER BY consumer_id, source_path",
        params,
    ).fetchall()
    return [Cursor(*row) for row in rows]
