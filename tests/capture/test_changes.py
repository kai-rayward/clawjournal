from pathlib import Path

from clawjournal.capture.changes import (
    cursor_after,
    cursor_to_eof,
    file_has_changed,
    iter_new_lines,
)
from clawjournal.capture.cursors import Cursor


def _append(path: Path, content: bytes) -> None:
    with path.open("ab") as f:
        f.write(content)


# ---------- line-level deltas ----------


def test_first_read_returns_all_complete_lines(tmp_path):
    p = tmp_path / "a.jsonl"
    p.write_bytes(b'{"a":1}\n{"b":2}\n')
    batch = iter_new_lines(p, None, client="claude")
    assert batch is not None
    assert batch.lines == ['{"a":1}', '{"b":2}']
    assert batch.start_offset == 0
    assert batch.end_offset == len(b'{"a":1}\n{"b":2}\n')
    assert batch.client == "claude"


def test_incremental_append_reads_only_new(tmp_path):
    p = tmp_path / "a.jsonl"
    p.write_bytes(b'{"a":1}\n')
    batch1 = iter_new_lines(p, None, client="claude")
    cur = cursor_after(batch1, p, consumer_id="events")
    _append(p, b'{"b":2}\n')
    batch2 = iter_new_lines(p, cur, client="claude")
    assert batch2 is not None
    assert batch2.lines == ['{"b":2}']
    assert batch2.start_offset == batch1.end_offset


def test_no_change_returns_none(tmp_path):
    p = tmp_path / "a.jsonl"
    p.write_bytes(b'{"a":1}\n')
    batch = iter_new_lines(p, None, client="claude")
    cur = cursor_after(batch, p, consumer_id="events")
    assert iter_new_lines(p, cur, client="claude") is None


def test_partial_trailing_line_is_not_consumed(tmp_path):
    p = tmp_path / "a.jsonl"
    p.write_bytes(b'{"a":1}\n{"incomple')
    batch = iter_new_lines(p, None, client="claude")
    assert batch is not None
    assert batch.lines == ['{"a":1}']
    assert batch.end_offset == len(b'{"a":1}\n')

    cur = cursor_after(batch, p, consumer_id="events")
    _append(p, b'te":true}\n{"c":3}\n')
    batch2 = iter_new_lines(p, cur, client="claude")
    assert batch2 is not None
    assert batch2.lines == ['{"incomplete":true}', '{"c":3}']


def test_rotation_resets_offset(tmp_path):
    p = tmp_path / "a.jsonl"
    p.write_bytes(b'{"a":1}\n')
    batch = iter_new_lines(p, None, client="claude")
    cur = cursor_after(batch, p, consumer_id="events")
    # Logrotate-style rotation: move the old file aside, then write a
    # fresh file at the original path. Guarantees a new inode on any
    # POSIX filesystem. Unlink-then-recreate on Linux often reuses the
    # inode immediately, making stat(2) unable to distinguish rotation
    # from a plain append — see the rotation note in changes.py.
    p.rename(tmp_path / "a.jsonl.1")
    p.write_bytes(b'{"new":1}\n')
    batch2 = iter_new_lines(p, cur, client="claude")
    assert batch2 is not None
    assert batch2.start_offset == 0
    assert batch2.lines == ['{"new":1}']


def test_truncation_resets_offset(tmp_path):
    p = tmp_path / "a.jsonl"
    p.write_bytes(b'{"a":1}\n{"b":2}\n')
    batch = iter_new_lines(p, None, client="claude")
    cur = cursor_after(batch, p, consumer_id="events")
    p.write_bytes(b'{"x":1}\n')
    batch2 = iter_new_lines(p, cur, client="claude")
    assert batch2 is not None
    assert batch2.start_offset == 0
    assert batch2.lines == ['{"x":1}']


def test_missing_file_returns_none(tmp_path):
    assert iter_new_lines(tmp_path / "nope.jsonl", None, client="claude") is None


def test_empty_file_returns_none(tmp_path):
    p = tmp_path / "empty.jsonl"
    p.write_bytes(b"")
    assert iter_new_lines(p, None, client="claude") is None


# ---------- file-level change detection ----------


def test_file_has_changed_no_cursor(tmp_path):
    p = tmp_path / "a.jsonl"
    p.write_bytes(b"x")
    assert file_has_changed(p, None) is True


def test_file_has_changed_no_cursor_empty_file(tmp_path):
    p = tmp_path / "empty.jsonl"
    p.write_bytes(b"")
    # Empty file, no cursor: nothing to do yet
    assert file_has_changed(p, None) is False


def test_file_has_changed_missing_file_is_false(tmp_path):
    assert file_has_changed(tmp_path / "nope.jsonl", None) is False


def test_file_has_changed_detects_append(tmp_path):
    p = tmp_path / "a.jsonl"
    p.write_bytes(b'{"a":1}\n')
    cur = cursor_to_eof(p, consumer_id="scanner", client="claude")
    assert file_has_changed(p, cur) is False
    _append(p, b'{"b":2}\n')
    assert file_has_changed(p, cur) is True


def test_file_has_changed_detects_rotation(tmp_path):
    p = tmp_path / "a.jsonl"
    p.write_bytes(b'{"a":1}\n')
    cur = cursor_to_eof(p, consumer_id="scanner", client="claude")
    # Rename-then-recreate so the test doesn't depend on inode-reuse
    # behavior of the underlying filesystem.
    p.rename(tmp_path / "a.jsonl.1")
    p.write_bytes(b'{"new":1}\n')
    assert file_has_changed(p, cur) is True


def test_file_has_changed_detects_truncation(tmp_path):
    p = tmp_path / "a.jsonl"
    p.write_bytes(b'{"a":1}\n{"b":2}\n')
    cur = cursor_to_eof(p, consumer_id="scanner", client="claude")
    p.write_bytes(b'{"x":1}\n')
    assert file_has_changed(p, cur) is True


def test_cursor_to_eof_points_at_file_end(tmp_path):
    p = tmp_path / "a.jsonl"
    p.write_bytes(b'{"a":1}\n{"b":2}\n')
    cur = cursor_to_eof(p, consumer_id="scanner", client="claude")
    assert cur.consumer_id == "scanner"
    assert cur.client == "claude"
    assert cur.last_offset == p.stat().st_size
    assert cur.inode == p.stat().st_ino
