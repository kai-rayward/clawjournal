"""SQLite FTS5 schema for cross-session search (phase-1 plan 11).

One virtual table (``events_fts``) in external-content mode pointing at
``events``, plus three triggers so inserts / updates / deletes on
``events`` keep the index in lockstep. External-content mode is the
right tradeoff for this workload:

- The ``raw_json`` column is already stored in ``events``; replicating
  it inside the FTS table would roughly double on-disk size for the
  one feature that wants the content. External-content keeps a single
  copy.
- ``snippet()`` and ``highlight()`` still work in external-content
  mode because FTS5 follows the rowid back to ``events.raw_json`` to
  fetch the actual text at query time.

Tokenizer: ``unicode61 remove_diacritics 2 tokenchars '-_'``.

- ``unicode61`` is FTS5's default; it normalizes case and handles
  unicode letter classes correctly.
- ``remove_diacritics 2`` strips combining marks even from precomposed
  characters (mode 1 misses some), so ``café`` and ``cafe`` both index
  as ``cafe``.
- ``tokenchars '-_'`` keeps hyphen and underscore inside tokens, so
  ``snake_case`` and ``kebab-case`` index as single tokens. Plan 11
  §Open questions notes the recall tradeoff (``rate-limit`` no longer
  matches a search for ``rate limit``); keep this conservative for
  now and revisit after real-world usage.

Porter stemming is intentionally NOT enabled — it stems ``authenticate``
and ``authority`` to the same token, which is wrong for code search.

Rebuild path: ``INSERT INTO events_fts(events_fts) VALUES('rebuild')``
is FTS5's documented one-shot reindex. ``rebuild_search_index`` wraps
that in a single transaction.
"""

from __future__ import annotations

import sqlite3

EVENTS_FTS_TABLE_SQL = """
CREATE VIRTUAL TABLE IF NOT EXISTS events_fts USING fts5(
    raw_json,
    content='events',
    content_rowid='id',
    tokenize="unicode61 remove_diacritics 2 tokenchars '-_'"
);
"""

# Triggers: insert / delete / update keep events_fts mirroring events.
# Using `events_fts(rowid, raw_json)` for inserts and the documented
# `events_fts(events_fts, rowid, raw_json)` form for deletes/updates so
# FTS5 records the negation rather than trying to scan the now-deleted
# row in the content table.
EVENTS_FTS_TRIGGERS_SQL = """
CREATE TRIGGER IF NOT EXISTS events_ai_fts AFTER INSERT ON events BEGIN
    INSERT INTO events_fts(rowid, raw_json) VALUES (new.id, new.raw_json);
END;
CREATE TRIGGER IF NOT EXISTS events_ad_fts AFTER DELETE ON events BEGIN
    INSERT INTO events_fts(events_fts, rowid, raw_json)
        VALUES('delete', old.id, old.raw_json);
END;
CREATE TRIGGER IF NOT EXISTS events_au_fts AFTER UPDATE ON events BEGIN
    INSERT INTO events_fts(events_fts, rowid, raw_json)
        VALUES('delete', old.id, old.raw_json);
    INSERT INTO events_fts(rowid, raw_json) VALUES (new.id, new.raw_json);
END;
"""


def ensure_search_schema(conn: sqlite3.Connection) -> None:
    """Create the FTS virtual table + triggers if absent.

    Idempotent — safe to call on every CLI invocation. Backfills the
    FTS index whenever it is empty but ``events`` is not, which
    catches both the fresh-install case AND a recovery from partial
    migration: if a previous call created ``events_fts`` but failed
    to create the triggers (because ``events`` did not yet exist),
    a later call after ``events ingest`` populated ``events`` would
    have left the FTS out of sync. Round-3 self-review fix — the
    earlier ``pre_existing_fts`` short-circuit was correct for the
    happy path but missed the partial-migration recovery.
    """

    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(EVENTS_FTS_TABLE_SQL)
    conn.executescript(EVENTS_FTS_TRIGGERS_SQL)

    # ``events_fts_docsize`` is FTS5's internal docsize-shadow table;
    # it has one row per indexed document. ``SELECT * FROM events_fts``
    # in external-content mode reads through to the events table, so
    # it cannot tell "indexed" from "not indexed" — counting docsize
    # is the only reliable way to detect the FTS-empty state. When
    # FTS-empty + events-non-empty, run a backfill so partial-
    # migration recovery (FTS table created without triggers, then
    # ingest happens) self-heals on the next search invocation.
    fts_empty = (
        conn.execute("SELECT count(*) FROM events_fts_docsize").fetchone()[0]
        == 0
    )
    has_events = bool(
        conn.execute("SELECT 1 FROM events LIMIT 1").fetchone()
    )
    if fts_empty and has_events:
        rebuild_search_index(conn)
        # Round-7 fix: must commit. sqlite3.connect() defaults to
        # isolation_level="" which auto-begins a transaction on DML
        # (the FTS5 'rebuild' insert qualifies). Without an explicit
        # commit, the rebuild lands in an open transaction; a later
        # conn.close() rolls it back, and the next ensure_search_schema
        # call re-detects FTS-empty and rebuilds again — an eternal
        # rebuild loop with rolled-back work. The earlier search-
        # recovery test masked the bug by querying before close.
        conn.commit()


def rebuild_search_index(conn: sqlite3.Connection) -> None:
    """One-shot reindex of every row in ``events`` into ``events_fts``.

    Safe to call after corruption (`DELETE FROM events_fts`) or after
    any surgery on ``events`` that bypassed the triggers. FTS5's
    ``rebuild`` command rebuilds the inverted index from the current
    content table state.
    """

    conn.execute("INSERT INTO events_fts(events_fts) VALUES('rebuild')")


__all__ = [
    "EVENTS_FTS_TABLE_SQL",
    "EVENTS_FTS_TRIGGERS_SQL",
    "ensure_search_schema",
    "rebuild_search_index",
]
