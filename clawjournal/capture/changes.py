"""Change detection for the capture adapter.

Two consumer models are supported:

- **Line-level deltas** (`iter_new_lines` / `cursor_after`) — streaming
  consumers (the 02 normalized-event pipeline) read appended lines since
  the cursor and advance one line-batch at a time.
- **File-level change detection** (`file_has_changed` / `cursor_to_eof`)
  — whole-file-reparse consumers (the workbench Scanner adapter in
  migration step 2) ask whether a file has changed, reparse it fully,
  then advance the cursor to EOF after their sink commit.

Each consumer holds its own cursor (see cursors.py) and advances only
after its own sink commit, so one consumer cannot cause another to miss
data and crashes replay cleanly.
"""

from __future__ import annotations

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


def iter_new_lines(
    path: Path, cursor: Cursor | None, *, client: str
) -> LineBatch | None:
    try:
        st = path.stat()
    except FileNotFoundError:
        return None

    start_offset = 0
    if cursor is not None and cursor.inode == st.st_ino and cursor.last_offset <= st.st_size:
        start_offset = cursor.last_offset

    if start_offset >= st.st_size:
        return None

    with path.open("rb") as f:
        f.seek(start_offset)
        raw = f.read(st.st_size - start_offset)

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
    )


def cursor_after(batch: LineBatch, path: Path, *, consumer_id: str) -> Cursor:
    """Cursor a line-level consumer should persist after its sink commit."""
    st = path.stat()
    return Cursor(
        consumer_id=consumer_id,
        source_path=str(path),
        inode=st.st_ino,
        last_offset=batch.end_offset,
        last_modified=st.st_mtime,
        client=batch.client,
    )


def file_has_changed(path: Path, cursor: Cursor | None) -> bool:
    """Cheap gate for whole-file-reparse consumers.

    Returns True if `path` differs from the cursor's recorded state in
    inode, size, or mtime. Missing files return False (nothing to do).
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


def cursor_to_eof(
    path: Path, *, consumer_id: str, client: str
) -> Cursor:
    """Cursor a file-level consumer should persist after reparsing to EOF."""
    st = path.stat()
    return Cursor(
        consumer_id=consumer_id,
        source_path=str(path),
        inode=st.st_ino,
        last_offset=st.st_size,
        last_modified=st.st_mtime,
        client=client,
    )
