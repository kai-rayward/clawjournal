"""SQLite schema for the replay-export importer (phase-1 plan 07).

One table:

- ``event_source_snippets`` — materializes the bundle's ``source_snippets``
  section on the importing host so 03's ``clawjournal events inspect`` can
  fall back to the bundle's redacted vendor line when the original JSONL
  isn't on this machine.

The PK is the storage triple ``(source_path, source_offset, seq)`` rather
than an FK to ``events.id`` — snippets identify by raw_ref so they remain
addressable even if the local ``events`` row gets purged. Orphans are
intentionally lazy cleanup; a future ``events purge-snippets`` verb can
sweep them when storage growth justifies it.
"""

from __future__ import annotations

import sqlite3

EVENT_SOURCE_SNIPPETS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS event_source_snippets (
    source_path   TEXT    NOT NULL,
    source_offset INTEGER NOT NULL,
    seq           INTEGER NOT NULL,
    text          TEXT    NOT NULL,
    imported_at   TEXT    NOT NULL,
    PRIMARY KEY (source_path, source_offset, seq)
);
"""


def ensure_export_schema(conn: sqlite3.Connection) -> None:
    """Create the export-importer tables if absent. Safe to call repeatedly."""
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(EVENT_SOURCE_SNIPPETS_TABLE_SQL)
