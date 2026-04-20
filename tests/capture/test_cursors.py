import sqlite3

from clawjournal.capture.cursors import (
    Cursor,
    ensure_schema,
    get_cursor,
    list_cursors,
    set_cursor,
)


def _open() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    ensure_schema(conn)
    return conn


def _cursor(
    *,
    consumer_id: str = "scanner",
    source_path: str = "/tmp/a.jsonl",
    inode: int = 1,
    last_offset: int = 0,
    last_modified: float = 0.0,
    client: str = "claude",
) -> Cursor:
    return Cursor(
        consumer_id=consumer_id,
        source_path=source_path,
        inode=inode,
        last_offset=last_offset,
        last_modified=last_modified,
        client=client,
    )


def test_ensure_schema_is_idempotent():
    conn = _open()
    ensure_schema(conn)
    cols = {row[1] for row in conn.execute("PRAGMA table_info(capture_cursors)")}
    assert {
        "consumer_id",
        "source_path",
        "inode",
        "last_offset",
        "last_modified",
        "client",
        "first_seen",
        "last_seen",
    } <= cols


def test_set_then_get_round_trip():
    conn = _open()
    cursor = _cursor(inode=42, last_offset=1024, last_modified=1700000000.0)
    set_cursor(conn, cursor)
    assert get_cursor(conn, "scanner", "/tmp/a.jsonl") == cursor


def test_set_cursor_upserts_on_conflict_same_consumer():
    conn = _open()
    set_cursor(conn, _cursor(inode=1, last_offset=0))
    set_cursor(conn, _cursor(inode=2, last_offset=100, last_modified=1.0))
    got = get_cursor(conn, "scanner", "/tmp/a.jsonl")
    assert got.inode == 2
    assert got.last_offset == 100
    assert got.last_modified == 1.0


def test_two_consumers_same_file_are_independent():
    conn = _open()
    set_cursor(conn, _cursor(consumer_id="scanner", last_offset=50))
    set_cursor(conn, _cursor(consumer_id="events", last_offset=10))
    scanner = get_cursor(conn, "scanner", "/tmp/a.jsonl")
    events = get_cursor(conn, "events", "/tmp/a.jsonl")
    assert scanner.last_offset == 50
    assert events.last_offset == 10
    # Advancing one does not advance the other
    set_cursor(conn, _cursor(consumer_id="events", last_offset=100))
    assert get_cursor(conn, "scanner", "/tmp/a.jsonl").last_offset == 50
    assert get_cursor(conn, "events", "/tmp/a.jsonl").last_offset == 100


def test_get_cursor_missing_returns_none():
    conn = _open()
    assert get_cursor(conn, "scanner", "/does/not/exist.jsonl") is None


def test_get_cursor_wrong_consumer_returns_none():
    conn = _open()
    set_cursor(conn, _cursor(consumer_id="scanner"))
    assert get_cursor(conn, "events", "/tmp/a.jsonl") is None


def test_set_cursor_preserves_first_seen_across_updates():
    """first_seen records when the cursor was initially created; updates
    should leave it unchanged while advancing last_seen. The ON CONFLICT
    DO UPDATE clause must not include first_seen in its SET list."""
    conn = _open()
    set_cursor(conn, _cursor(last_offset=0))
    first_row = conn.execute(
        "SELECT first_seen, last_seen FROM capture_cursors "
        "WHERE consumer_id = ? AND source_path = ?",
        ("scanner", "/tmp/a.jsonl"),
    ).fetchone()
    original_first_seen = first_row[0]
    # Advance the cursor; first_seen should stick, last_seen should refresh
    set_cursor(conn, _cursor(last_offset=100))
    second_row = conn.execute(
        "SELECT first_seen, last_seen FROM capture_cursors "
        "WHERE consumer_id = ? AND source_path = ?",
        ("scanner", "/tmp/a.jsonl"),
    ).fetchone()
    assert second_row[0] == original_first_seen
    assert second_row[1] >= first_row[1]


def test_list_cursors_filters_by_consumer_and_client():
    conn = _open()
    set_cursor(conn, _cursor(consumer_id="scanner", source_path="/tmp/a.jsonl", client="claude"))
    set_cursor(conn, _cursor(consumer_id="scanner", source_path="/tmp/b.jsonl", client="codex"))
    set_cursor(conn, _cursor(consumer_id="events", source_path="/tmp/a.jsonl", client="claude"))

    scanner_rows = list_cursors(conn, consumer_id="scanner")
    assert {(c.consumer_id, c.source_path) for c in scanner_rows} == {
        ("scanner", "/tmp/a.jsonl"),
        ("scanner", "/tmp/b.jsonl"),
    }
    claude_rows = list_cursors(conn, client="claude")
    assert {(c.consumer_id, c.source_path) for c in claude_rows} == {
        ("scanner", "/tmp/a.jsonl"),
        ("events", "/tmp/a.jsonl"),
    }
    scanner_claude = list_cursors(conn, consumer_id="scanner", client="claude")
    assert [c.source_path for c in scanner_claude] == ["/tmp/a.jsonl"]
    assert len(list_cursors(conn)) == 3
