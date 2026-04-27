"""End-to-end query execution tests for `events search` (plan 11).

Each test seeds an in-memory SQLite fixture with the events schema +
search FTS schema, runs the search, and asserts on the hits / counts
/ metadata. Includes the SQL-injection regression for FTS5 MATCH
parameterization, the trigger-sync invariant, and the hold-state
exclusion default.
"""

from __future__ import annotations

import sqlite3

import pytest

from clawjournal.events.schema import ensure_schema as ensure_events_schema
from clawjournal.events.search import (
    SearchSpec,
    ensure_search_schema,
    parse_search_spec,
    rebuild_search_index,
    run,
)


SESSIONS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS sessions (
    session_id TEXT PRIMARY KEY,
    session_key TEXT,
    hold_state TEXT,
    embargo_until TEXT
);
"""


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    ensure_events_schema(c)
    c.executescript(SESSIONS_TABLE_SQL)
    ensure_search_schema(c)
    yield c
    c.close()


_event_seq = {"n": 0}


def _add_session(
    conn, session_key, *, client="claude", hold_state=None, embargo_until=None,
):
    cur = conn.execute(
        "INSERT INTO event_sessions (session_key, client, started_at, status) "
        "VALUES (?, ?, '2026-04-21T10:00:00Z', 'ended')",
        (session_key, client),
    )
    sid = cur.lastrowid
    if hold_state is not None:
        conn.execute(
            "INSERT INTO sessions (session_id, session_key, hold_state, "
            "embargo_until) VALUES (?, ?, ?, ?)",
            (session_key, session_key, hold_state, embargo_until),
        )
    return sid


def _add_event(
    conn,
    session_id,
    raw_json,
    *,
    type_="user_message",
    client="claude",
    source="claude-jsonl",
    source_path="/Users/synthetic-user/proj/file.jsonl",
    confidence="high",
    event_at="2026-04-21T10:00:00Z",
):
    seq = _event_seq["n"]
    _event_seq["n"] += 1
    conn.execute(
        "INSERT INTO events "
        "(session_id, type, event_at, ingested_at, source, source_path, "
        " source_offset, seq, client, confidence, lossiness, raw_json) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            session_id, type_, event_at, "2026-04-21T10:00:00Z",
            source, source_path, 0, seq, client, confidence, "none", raw_json,
        ),
    )


def test_basic_match_returns_hit(conn):
    sid = _add_session(conn, "claude:proj:s1")
    _add_event(conn, sid, '{"text": "401 Unauthorized: authentication failed"}')
    _add_event(conn, sid, '{"text": "all good"}')

    spec = SearchSpec(query="authentication")
    result = run(spec, conn)
    assert result.rows_matched == 1
    assert len(result.hits) == 1
    assert "authentication" in result.hits[0].snippet.lower()


def test_phrase_query_only_matches_full_phrase(conn):
    sid = _add_session(conn, "claude:proj:s1")
    _add_event(conn, sid, '{"text": "tool error: command failed"}')
    _add_event(conn, sid, '{"text": "tool succeeded; no error"}')

    spec = SearchSpec(query='"tool error"')
    result = run(spec, conn)
    assert result.rows_matched == 1


def test_match_is_parameterized_sql_injection_value_treated_as_literal(conn):
    """Plan 11 §Security #1: an injection-shaped query string is bound
    to the FTS5 MATCH predicate as a single parameter. FTS5 will
    either parse it as a literal MATCH expression (zero hits or all
    hits depending on tokens) or raise a syntax error — either way,
    no SQL composition. The parser layer above us turns the syntax
    error into a usage_error; here we just pin that the value never
    leaks out as SQL."""

    sid = _add_session(conn, "claude:proj:s1")
    _add_event(conn, sid, '{"text": "totally unrelated content"}')

    # FTS5 typically rejects this with a syntax error; a clean test
    # asserts EITHER zero hits OR a clean OperationalError raised
    # from the query layer (which the CLI handler maps to usage_error
    # code 2). The point is no rows leak.
    try:
        result = run(SearchSpec(query="' OR 1=1 --"), conn)
        assert result.hits == []
    except sqlite3.OperationalError as exc:
        assert "fts5" in str(exc).lower() or "syntax error" in str(exc).lower()


def test_triggers_keep_fts_in_sync_on_insert_and_delete(conn):
    sid = _add_session(conn, "claude:proj:s1")
    for i in range(5):
        _add_event(conn, sid, f'{{"text": "marker_token_{i}"}}')

    result = run(SearchSpec(query="marker_token_3"), conn)
    assert result.rows_matched == 1

    # Delete the matching event; FTS must follow.
    conn.execute(
        "DELETE FROM events WHERE raw_json LIKE '%marker_token_3%'"
    )
    result_after = run(SearchSpec(query="marker_token_3"), conn)
    assert result_after.rows_matched == 0


def test_partial_migration_rebuild_persists_across_close(tmp_path):
    """Round 7: the rebuild from ``ensure_search_schema``'s partial-
    migration recovery must commit. sqlite3.connect()'s default
    isolation level auto-begins a transaction on FTS5's 'rebuild'
    insert; without an explicit commit, conn.close() rolls the
    rebuild back. The earlier recovery test queried before close so
    it never noticed the rollback. This test reopens and verifies
    the FTS index actually persisted.
    """

    db_path = tmp_path / "rebuild_persist.db"

    # Phase 1: simulate partial migration (FTS table only, no triggers).
    c = sqlite3.connect(str(db_path))
    c.execute(
        "CREATE VIRTUAL TABLE events_fts USING fts5("
        "raw_json, content='events', content_rowid='id', "
        "tokenize=\"unicode61 remove_diacritics 2 tokenchars '-_'\")"
    )
    c.commit()
    c.close()

    # Phase 2: events ingest populates events without firing triggers.
    c = sqlite3.connect(str(db_path))
    ensure_events_schema(c)
    c.executescript(SESSIONS_TABLE_SQL)
    cur = c.execute(
        "INSERT INTO event_sessions (session_key, client, started_at, "
        "status) VALUES ('claude:proj:s1', 'claude', "
        "'2026-04-21T10:00:00Z', 'ended')"
    )
    sid = cur.lastrowid
    c.execute(
        "INSERT INTO events (session_id, type, event_at, ingested_at, "
        "source, source_path, source_offset, seq, client, confidence, "
        "lossiness, raw_json) VALUES (?, 'tool_result', "
        "'2026-04-21T10:00:00Z', '2026-04-21T10:00:00Z', 'claude-jsonl', "
        "'/x', 0, 0, 'claude', 'high', 'none', "
        "'{\"text\": \"persistence_marker_token\"}')",
        (sid,),
    )
    c.commit()
    c.close()

    # Phase 3: ensure_search_schema runs and (we hope) commits the
    # rebuild. Close without an explicit commit on this connection.
    c = sqlite3.connect(str(db_path))
    ensure_search_schema(c)
    c.close()

    # Phase 4: reopen with a fresh connection. If the rebuild was
    # rolled back by Phase 3's close, docsize is 0 and the next
    # ensure_search_schema call would have to rebuild again — an
    # eternal loop in production.
    c2 = sqlite3.connect(str(db_path))
    docs = c2.execute(
        "SELECT count(*) FROM events_fts_docsize"
    ).fetchone()[0]
    c2.close()
    assert docs == 1, (
        f"FTS rebuild was rolled back at close: docsize_count={docs}; "
        f"each subsequent search would re-rebuild and re-roll-back"
    )


def test_ensure_search_schema_recovers_from_partial_migration(tmp_path):
    """Round 3: if ``ensure_search_schema`` previously created
    ``events_fts`` but failed before installing the triggers (because
    ``events`` did not yet exist), a later call after ingest should
    backfill the FTS index instead of short-circuiting on
    ``pre_existing_fts``. The earlier check was correct for the
    happy path but missed this recovery case."""

    db_path = tmp_path / "partial.db"
    c = sqlite3.connect(str(db_path))
    # Phase 1: simulate a partial migration — create events_fts but
    # NOT the triggers, and NOT the events table. This is the state
    # we'd be in if the first ensure_search_schema call hit "no such
    # table: events" while creating the triggers.
    c.execute(
        "CREATE VIRTUAL TABLE events_fts USING fts5("
        "raw_json, content='events', content_rowid='id', "
        "tokenize=\"unicode61 remove_diacritics 2 tokenchars '-_'\")"
    )
    c.commit()
    c.close()

    # Phase 2: events ingest happens — events table created, rows
    # inserted. The triggers don't exist yet so events_fts stays empty.
    c = sqlite3.connect(str(db_path))
    ensure_events_schema(c)
    c.executescript(SESSIONS_TABLE_SQL)
    cur = c.execute(
        "INSERT INTO event_sessions (session_key, client, started_at, "
        "status) VALUES ('claude:proj:s1', 'claude', "
        "'2026-04-21T10:00:00Z', 'ended')"
    )
    sid = cur.lastrowid
    c.execute(
        "INSERT INTO events (session_id, type, event_at, ingested_at, "
        "source, source_path, source_offset, seq, client, confidence, "
        "lossiness, raw_json) VALUES (?, 'tool_result', "
        "'2026-04-21T10:00:00Z', '2026-04-21T10:00:00Z', 'claude-jsonl', "
        "'/x', 0, 0, 'claude', 'high', 'none', "
        "'{\"text\": \"recovery_marker_token\"}')",
        (sid,),
    )
    c.commit()
    c.close()

    # Phase 3: a fresh ensure_search_schema call should see
    # FTS-empty + events-non-empty and trigger a backfill, leaving
    # the search functional.
    c = sqlite3.connect(str(db_path))
    ensure_search_schema(c)
    result = run(SearchSpec(query="recovery_marker_token"), c)
    c.close()
    assert result.rows_matched == 1, (
        f"partial-migration recovery did not backfill FTS; "
        f"hits={[h.event_id for h in result.hits]}"
    )


def test_rebuild_index_recovers_after_truncation(conn):
    sid = _add_session(conn, "claude:proj:s1")
    _add_event(conn, sid, '{"text": "needle in the haystack"}')
    assert run(SearchSpec(query="needle"), conn).rows_matched == 1

    # Simulate FTS table corruption — delete its content.
    conn.execute("INSERT INTO events_fts(events_fts) VALUES('delete-all')")
    assert run(SearchSpec(query="needle"), conn).rows_matched == 0

    rebuild_search_index(conn)
    assert run(SearchSpec(query="needle"), conn).rows_matched == 1


def test_held_sessions_excluded_by_default(conn):
    sid_open = _add_session(conn, "claude:proj:s1")
    sid_held = _add_session(
        conn, "claude:proj:s2", hold_state="pending_review"
    )
    _add_event(conn, sid_open, '{"text": "shared_token open"}')
    _add_event(conn, sid_held, '{"text": "shared_token held"}')

    result = run(SearchSpec(query="shared_token"), conn)
    assert result.rows_matched == 1
    assert result.hits[0].session_key == "claude:proj:s1"


def test_held_sessions_surfaced_with_include_held(conn):
    sid_open = _add_session(conn, "claude:proj:s1")
    sid_held = _add_session(
        conn, "claude:proj:s2", hold_state="pending_review"
    )
    _add_event(conn, sid_open, '{"text": "shared_token open"}')
    _add_event(conn, sid_held, '{"text": "shared_token held"}')

    result = run(SearchSpec(query="shared_token", include_held=True), conn)
    keys = sorted(h.session_key for h in result.hits)
    assert keys == ["claude:proj:s1", "claude:proj:s2"]


def test_embargoed_sessions_also_excluded_by_default(conn):
    sid_open = _add_session(conn, "claude:proj:s1")
    # An active embargo (until 2099) should block.
    sid_emb = _add_session(
        conn, "claude:proj:s2",
        hold_state="embargoed", embargo_until="2099-01-01T00:00:00Z",
    )
    _add_event(conn, sid_open, '{"text": "shared_token open"}')
    _add_event(conn, sid_emb, '{"text": "shared_token embargoed"}')

    result = run(SearchSpec(query="shared_token"), conn)
    assert result.rows_matched == 1


def test_expired_embargo_passes_through_default_search(conn):
    """Round 2: an embargo whose ``embargo_until`` has already passed
    is operationally released — same semantics as
    ``workbench.index.effective_hold_state``. Without this rule,
    expired embargoes would silently linger as search-blocked even
    though every other code path treats them as released."""

    sid_open = _add_session(conn, "claude:proj:s1")
    sid_expired = _add_session(
        conn, "claude:proj:s2",
        hold_state="embargoed", embargo_until="2000-01-01T00:00:00Z",
    )
    _add_event(conn, sid_open, '{"text": "shared_token open"}')
    _add_event(conn, sid_expired, '{"text": "shared_token expired_embargo"}')

    result = run(SearchSpec(query="shared_token"), conn)
    assert result.rows_matched == 2, (
        f"expired embargo should pass through default search; got "
        f"{[h.session_key for h in result.hits]}"
    )


def test_session_with_no_workbench_row_passes_through(conn):
    """A session that has not been touched by the workbench (no
    `sessions` row at all) is NOT held — it should surface in default
    search just like any other session. The LEFT JOIN's NULL
    hold_state passes the filter."""

    sid = _add_session(conn, "claude:proj:s1")  # no hold_state arg → no row
    _add_event(conn, sid, '{"text": "untouched_session"}')

    result = run(SearchSpec(query="untouched_session"), conn)
    assert result.rows_matched == 1


def test_filter_client_narrows_results(conn):
    sid_a = _add_session(conn, "claude:proj:s1", client="claude")
    sid_b = _add_session(conn, "codex:/x", client="codex")
    _add_event(conn, sid_a, '{"text": "common_token"}', client="claude")
    _add_event(
        conn, sid_b, '{"text": "common_token"}',
        client="codex", source="codex-rollout",
    )

    spec = parse_search_spec(
        query="common_token",
        client=("claude",),
        limit=50,
        snippet_tokens=16,
        include_held=False,
    )
    result = run(spec, conn)
    assert {h.client for h in result.hits} == {"claude"}


def test_filter_in_clause_with_multiple_values(conn):
    sid_a = _add_session(conn, "claude:proj:s1", client="claude")
    sid_b = _add_session(conn, "codex:/x", client="codex")
    sid_c = _add_session(conn, "openclaw:/y", client="openclaw")
    for sid, client, source in [
        (sid_a, "claude", "claude-jsonl"),
        (sid_b, "codex", "codex-rollout"),
        (sid_c, "openclaw", "openclaw-jsonl"),
    ]:
        _add_event(
            conn, sid, '{"text": "shared_token"}',
            client=client, source=source,
        )

    spec = parse_search_spec(
        query="shared_token",
        client=("claude", "codex"),
        limit=50,
        snippet_tokens=16,
        include_held=False,
    )
    result = run(spec, conn)
    assert {h.client for h in result.hits} == {"claude", "codex"}


def test_since_filter_bounds_time_window(conn):
    sid = _add_session(conn, "claude:proj:s1")
    _add_event(conn, sid, '{"text": "old_token"}', event_at="2026-03-01T00:00:00Z")
    _add_event(conn, sid, '{"text": "new_token"}', event_at="2026-04-21T10:00:00Z")

    spec = SearchSpec(
        query="old_token OR new_token",
        since_iso="2026-04-01T00:00:00Z",
    )
    result = run(spec, conn)
    snippets = " ".join(h.snippet for h in result.hits)
    assert "new_token" in snippets
    assert "old_token" not in snippets


def test_limit_caps_returned_hits_but_not_rows_matched(conn):
    sid = _add_session(conn, "claude:proj:s1")
    for i in range(7):
        _add_event(conn, sid, f'{{"text": "common_term seq_{i}"}}')

    result = run(SearchSpec(query="common_term", limit=3), conn)
    assert len(result.hits) == 3
    assert result.rows_matched == 7


def test_bm25_orders_results_relevance_first(conn):
    sid = _add_session(conn, "claude:proj:s1")
    # Two events, one with the term twice and one with it once. FTS5
    # BM25 returns smaller-is-better; the doubled-term doc should
    # rank first.
    _add_event(conn, sid, '{"text": "alpha alpha beta gamma"}')
    _add_event(conn, sid, '{"text": "alpha beta gamma delta epsilon"}')

    result = run(SearchSpec(query="alpha"), conn)
    assert len(result.hits) == 2
    # Smaller bm25 = better match.
    assert result.hits[0].bm25 <= result.hits[1].bm25


def test_snapshot_isolation_against_concurrent_writes(tmp_path):
    """Both queries (hits + count) must run against the same snapshot
    or `rows_matched` would drift away from `len(hits)` under
    concurrent writes from `clawjournal serve`. Mirrors plan 10's
    snapshot-isolation pattern."""

    db_path = tmp_path / "search.db"
    writer = sqlite3.connect(str(db_path))
    writer.execute("PRAGMA journal_mode=WAL")
    ensure_events_schema(writer)
    writer.executescript(SESSIONS_TABLE_SQL)
    ensure_search_schema(writer)
    cur = writer.execute(
        "INSERT INTO event_sessions (session_key, client, started_at, status) "
        "VALUES ('claude:proj:s1', 'claude', '2026-04-21T10:00:00Z', 'ended')"
    )
    sid = cur.lastrowid
    for i in range(3):
        writer.execute(
            "INSERT INTO events (session_id, type, event_at, ingested_at, "
            "source, source_path, source_offset, seq, client, confidence, "
            "lossiness, raw_json) VALUES (?, 'user_message', "
            "'2026-04-21T10:00:00Z', '2026-04-21T10:00:00Z', 'claude-jsonl', "
            "'/x', 0, ?, 'claude', 'high', 'none', "
            "'{\"text\": \"shared_token\"}')",
            (sid, i),
        )
    writer.commit()

    raw_reader = sqlite3.connect(str(db_path))
    call_count = {"n": 0}

    class _ReaderProxy:
        def __init__(self, conn):
            self._conn = conn

        def execute(self, sql, *args, **kwargs):
            result = self._conn.execute(sql, *args, **kwargs)
            call_count["n"] += 1
            # Calls in order: BEGIN(1), hits SELECT(2), count SELECT(3),
            # COMMIT(4). Inject a writer commit between hits and count.
            if call_count["n"] == 2:
                writer.execute(
                    "INSERT INTO events (session_id, type, event_at, "
                    "ingested_at, source, source_path, source_offset, seq, "
                    "client, confidence, lossiness, raw_json) VALUES "
                    "(?, 'user_message', '2026-04-21T10:00:00Z', "
                    "'2026-04-21T10:00:00Z', 'claude-jsonl', '/x', 0, 99, "
                    "'claude', 'high', 'none', "
                    "'{\"text\": \"shared_token\"}')",
                    (sid,),
                )
                writer.commit()
            return result

        @property
        def in_transaction(self):
            return self._conn.in_transaction

        def __getattr__(self, name):
            return getattr(self._conn, name)

    reader = _ReaderProxy(raw_reader)
    try:
        result = run(SearchSpec(query="shared_token"), reader)  # type: ignore[arg-type]
    finally:
        raw_reader.close()
        writer.close()

    assert len(result.hits) == result.rows_matched, (
        f"snapshot isolation broken: hits={len(result.hits)} "
        f"matched={result.rows_matched}"
    )
    assert result.rows_matched == 3
