import os
from pathlib import Path
from unittest.mock import patch

from clawjournal.capture.changes import (
    cursor_after,
    cursor_for_reparse,
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
    # LineBatch carries the stat snapshot captured at read time.
    st = p.stat()
    assert batch.inode == st.st_ino
    assert batch.last_modified == st.st_mtime


def test_incremental_append_reads_only_new(tmp_path):
    p = tmp_path / "a.jsonl"
    p.write_bytes(b'{"a":1}\n')
    batch1 = iter_new_lines(p, None, client="claude")
    cur = cursor_after(batch1, consumer_id="events")
    _append(p, b'{"b":2}\n')
    batch2 = iter_new_lines(p, cur, client="claude")
    assert batch2 is not None
    assert batch2.lines == ['{"b":2}']
    assert batch2.start_offset == batch1.end_offset


def test_no_change_returns_none(tmp_path):
    p = tmp_path / "a.jsonl"
    p.write_bytes(b'{"a":1}\n')
    batch = iter_new_lines(p, None, client="claude")
    cur = cursor_after(batch, consumer_id="events")
    assert iter_new_lines(p, cur, client="claude") is None


def test_partial_trailing_line_is_not_consumed(tmp_path):
    p = tmp_path / "a.jsonl"
    p.write_bytes(b'{"a":1}\n{"incomple')
    batch = iter_new_lines(p, None, client="claude")
    assert batch is not None
    assert batch.lines == ['{"a":1}']
    assert batch.end_offset == len(b'{"a":1}\n')

    cur = cursor_after(batch, consumer_id="events")
    _append(p, b'te":true}\n{"c":3}\n')
    batch2 = iter_new_lines(p, cur, client="claude")
    assert batch2 is not None
    assert batch2.lines == ['{"incomplete":true}', '{"c":3}']


def test_rotation_resets_offset(tmp_path):
    p = tmp_path / "a.jsonl"
    p.write_bytes(b'{"a":1}\n')
    batch = iter_new_lines(p, None, client="claude")
    cur = cursor_after(batch, consumer_id="events")
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
    cur = cursor_after(batch, consumer_id="events")
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


def test_cursor_after_is_immune_to_replacement_between_read_and_commit(tmp_path):
    """Regression for the TOCTOU where cursor_after used to re-stat the
    live path: if the file was replaced after iter_new_lines but before
    cursor_after, the cursor bound the NEW inode to the OLD end_offset
    and the next read silently resumed partway into the replacement.
    The fixed API captures the stat inside LineBatch at read time, so
    cursor_after uses the batch's snapshot and never re-stats."""
    p = tmp_path / "a.jsonl"
    p.write_bytes(b'{"a":1}\n')  # 8 bytes
    batch = iter_new_lines(p, None, client="claude")
    assert batch is not None
    old_inode = batch.inode
    # Simulate a concurrent replacement between read and commit
    p.rename(tmp_path / "a.jsonl.old")
    p.write_bytes(b'{"new":"much longer"}\n{"more":"stuff"}\n')
    cur = cursor_after(batch, consumer_id="events")
    assert cur.inode == old_inode
    assert cur.last_offset == 8
    # Next poll sees a different inode and rewinds to offset 0 —
    # no bytes from the replacement are skipped.
    batch2 = iter_new_lines(p, cur, client="claude")
    assert batch2 is not None
    assert batch2.start_offset == 0
    assert batch2.lines == ['{"new":"much longer"}', '{"more":"stuff"}']


def test_iter_new_lines_binds_batch_to_opened_file(tmp_path):
    """If the path is replaced immediately before open(), the batch must
    describe the replacement file we actually read, not the pre-open
    inode that used to live at the path.
    """
    p = tmp_path / "a.jsonl"
    p.write_bytes(b'{"old":1}\n')
    replacement = b'{"new":1}\n{"new":2}\n'
    original_open = Path.open
    swapped = False

    def fake_open(self, *args, **kwargs):
        nonlocal swapped
        if self == p and not swapped:
            os.rename(p, tmp_path / "a.jsonl.old")
            with open(p, "wb") as f:
                f.write(replacement)
            swapped = True
        return original_open(self, *args, **kwargs)

    with patch.object(Path, "open", fake_open):
        batch = iter_new_lines(p, None, client="claude")

    assert batch is not None
    assert batch.lines == ['{"new":1}', '{"new":2}']
    assert batch.inode == p.stat().st_ino
    cur = cursor_after(batch, consumer_id="events")
    assert iter_new_lines(p, cur, client="claude") is None


# ---------- file-level change detection ----------


def test_file_has_changed_no_cursor(tmp_path):
    p = tmp_path / "a.jsonl"
    p.write_bytes(b"x")
    assert file_has_changed(p, None) is True


def test_file_has_changed_no_cursor_empty_file(tmp_path):
    p = tmp_path / "empty.jsonl"
    p.write_bytes(b"")
    assert file_has_changed(p, None) is False


def test_file_has_changed_missing_file_is_false(tmp_path):
    assert file_has_changed(tmp_path / "nope.jsonl", None) is False


def test_file_has_changed_detects_append(tmp_path):
    p = tmp_path / "a.jsonl"
    p.write_bytes(b'{"a":1}\n')
    cur = cursor_for_reparse(p, consumer_id="scanner", client="claude")
    assert file_has_changed(p, cur) is False
    _append(p, b'{"b":2}\n')
    assert file_has_changed(p, cur) is True


def test_file_has_changed_detects_rotation(tmp_path):
    p = tmp_path / "a.jsonl"
    p.write_bytes(b'{"a":1}\n')
    cur = cursor_for_reparse(p, consumer_id="scanner", client="claude")
    p.rename(tmp_path / "a.jsonl.1")
    p.write_bytes(b'{"new":1}\n')
    assert file_has_changed(p, cur) is True


def test_file_has_changed_detects_truncation(tmp_path):
    p = tmp_path / "a.jsonl"
    p.write_bytes(b'{"a":1}\n{"b":2}\n')
    cur = cursor_for_reparse(p, consumer_id="scanner", client="claude")
    p.write_bytes(b'{"x":1}\n')
    assert file_has_changed(p, cur) is True


def test_cursor_for_reparse_points_at_current_state(tmp_path):
    p = tmp_path / "a.jsonl"
    p.write_bytes(b'{"a":1}\n{"b":2}\n')
    cur = cursor_for_reparse(p, consumer_id="scanner", client="claude")
    assert cur is not None
    assert cur.consumer_id == "scanner"
    assert cur.client == "claude"
    assert cur.last_offset == p.stat().st_size
    assert cur.inode == p.stat().st_ino


def test_cursor_for_reparse_returns_none_for_missing_file(tmp_path):
    assert (
        cursor_for_reparse(
            tmp_path / "nope.jsonl", consumer_id="scanner", client="claude"
        )
        is None
    )


def test_file_has_changed_returns_true_for_line_level_cursor_with_partial_tail(
    tmp_path,
):
    """Documents that file_has_changed is a whole-file-reparse gate. A
    line-level cursor (from cursor_after) can legitimately sit behind
    the live file size when the file ends in a partial trailing line;
    file_has_changed sees `last_offset < size` and returns True even
    though no new complete lines are available. Line-level consumers
    must use iter_new_lines directly to check for actual progress."""
    p = tmp_path / "a.jsonl"
    p.write_bytes(b'{"a":1}\n{"partial')  # partial trailing line after the first
    batch = iter_new_lines(p, None, client="claude")
    assert batch is not None
    assert batch.end_offset < p.stat().st_size  # cursor legitimately trails
    cur = cursor_after(batch, consumer_id="events")
    # No new writes; the file is in steady state from the consumer's
    # point of view — but file_has_changed still flags it because the
    # size does not equal last_offset.
    assert file_has_changed(p, cur) is True
    # The correct staleness check for a line-level consumer:
    assert iter_new_lines(p, cur, client="claude") is None


def test_cursor_for_reparse_snapshot_survives_concurrent_append(tmp_path):
    """TOCTOU-safe usage: snapshot BEFORE parse, persist AFTER sink
    commit. If the file grows during the parse, the cursor stays at
    the pre-parse size and the next poll's file_has_changed sees the
    growth. The sink's idempotency absorbs the replayed bytes."""
    p = tmp_path / "a.jsonl"
    p.write_bytes(b'{"a":1}\n')  # 8 bytes
    # Pre-parse snapshot
    cur = cursor_for_reparse(p, consumer_id="scanner", client="claude")
    assert cur is not None
    # Simulate an append that lands during the "parse" phase
    _append(p, b'{"b":2}\n')  # 16 bytes now
    # The cursor still points at the pre-parse size. Persisting it means
    # the next poll will see the growth rather than marking the file clean.
    assert cur.last_offset == 8
    assert file_has_changed(p, cur) is True
