"""Change detection for the capture adapter.

Two consumer models are supported:

- **Line-level deltas** (`iter_new_lines` / `cursor_after`) — streaming
  consumers (the 02 normalized-event pipeline) read appended lines since
  the cursor and advance one line-batch at a time. The cursor they
  persist is derived from the descriptor-backed stat snapshot captured
  for the file ACTUALLY opened for the batch, not a fresh path stat, so
  a file replacement before or after the read cannot move the cursor
  into an unrelated file's middle.

- **File-level change detection** (`file_has_changed` /
  `cursor_for_reparse`) — whole-file-reparse consumers (the workbench
  Scanner adapter in migration step 2). The required ordering is:

      snap = cursor_for_reparse(path, ...)   # stat snapshot BEFORE parse
      # parse the file (may read bytes appended during the parse)
      # write to the sink and commit
      set_cursor(conn, snap)                  # persist the pre-parse cursor

  If the file grows between the snapshot and the commit, the cursor
  stays at the pre-parse size and the next poll's `file_has_changed`
  sees the growth. The sink's idempotency (`upsert_sessions` keyed on
  session_id, the `events` UNIQUE index) absorbs any replayed bytes.

Each consumer holds its own cursor (see cursors.py) and advances only
after its own sink commit.

Rotation detection is best-effort at the stat level. Inode change,
size decrease, and mtime regression all signal rotation/truncation.
Unlink-then-recreate on Linux often reuses the inode immediately; if
the replacement file is also larger than the prior cursor offset, it
is indistinguishable from a plain append at stat(2). Vendor JSONLs
are append-only, so this is not a production concern — logrotate-style
rotation always produces a new inode via rename and is caught.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from clawjournal.capture.cursors import Cursor


@dataclass(frozen=True)
class LineBatch:
    path: Path
    client: str
    start_offset: int
    end_offset: int
    lines: list[str]
    inode: int
    last_modified: float


def iter_new_lines(
    path: Path, cursor: Cursor | None, *, client: str
) -> LineBatch | None:
    try:
        with path.open("rb") as f:
            st = os.fstat(f.fileno())
            start_offset = 0
            if (
                cursor is not None
                and cursor.inode == st.st_ino
                and cursor.last_offset <= st.st_size
            ):
                start_offset = cursor.last_offset

            if start_offset >= st.st_size:
                return None

            # Read against the same descriptor we snapshotted so batch
            # metadata matches the bytes we actually consumed.
            f.seek(start_offset)
            raw = f.read(st.st_size - start_offset)
    except FileNotFoundError:
        return None

    last_newline = raw.rfind(b"\n")
    if last_newline < 0:
        return None
    complete = raw[: last_newline + 1]
    text_lines = [
        line.decode("utf-8", errors="replace")
        for line in complete.splitlines()
    ]
    end_offset = start_offset + last_newline + 1

    return LineBatch(
        path=path,
        client=client,
        start_offset=start_offset,
        end_offset=end_offset,
        lines=text_lines,
        inode=st.st_ino,
        last_modified=st.st_mtime,
    )


def cursor_after(batch: LineBatch, *, consumer_id: str) -> Cursor:
    """Build the Cursor a line-level consumer should persist after its
    sink commit. Uses the stat snapshot captured when the batch was read
    (`batch.inode`, `batch.last_modified`), not a fresh stat, so a file
    replacement between read and commit cannot move the cursor into an
    unrelated file's middle.
    """
    return Cursor(
        consumer_id=consumer_id,
        source_path=str(batch.path),
        inode=batch.inode,
        last_offset=batch.end_offset,
        last_modified=batch.last_modified,
        client=batch.client,
    )


def file_has_changed(path: Path, cursor: Cursor | None) -> bool:
    """Cheap gate for whole-file-reparse consumers.

    Returns True if `path` differs from the cursor's recorded state in
    inode, size, or mtime. Missing files return False (nothing to do).

    Designed for `cursor_for_reparse` cursors, where `last_offset`
    equals the file size at snapshot time. A `cursor_after` cursor's
    `last_offset` can legitimately trail the file size (the file ended
    in a partial line at read time), so this function will return True
    in steady state on such cursors even when no new bytes have been
    written. Line-level consumers should use `iter_new_lines` directly
    rather than gating on `file_has_changed`.
    """
    try:
        st = path.stat()
    except FileNotFoundError:
        return False
    if cursor is None:
        return st.st_size > 0
    if cursor.inode != st.st_ino:
        return True
    if cursor.last_offset != st.st_size:
        return True
    return cursor.last_modified != st.st_mtime


def cursor_for_reparse(
    path: Path, *, consumer_id: str, client: str
) -> Cursor | None:
    """Snapshot the file's stat and return the cursor a whole-file-reparse
    consumer should persist AFTER its sink commit.

    Contract: call BEFORE parsing the file. The returned cursor captures
    the pre-parse `(inode, size, mtime)`. If the file grows during the
    parse, the cursor stays at the pre-parse size, and the next poll's
    `file_has_changed` returns True because the new size differs from
    the cursor's recorded size. The sink's idempotency absorbs the
    replayed bytes.

    Calling this AFTER the parse is the bug the rename guards against:
    the cursor would advance past any bytes appended during the parse,
    and those bytes would be lost.

    Returns None if the file is missing.
    """
    try:
        st = path.stat()
    except FileNotFoundError:
        return None
    return Cursor(
        consumer_id=consumer_id,
        source_path=str(path),
        inode=st.st_ino,
        last_offset=st.st_size,
        last_modified=st.st_mtime,
        client=client,
    )
