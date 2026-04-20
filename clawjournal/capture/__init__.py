"""Capture adapter for coding-agent session logs.

Owns file discovery, per-consumer cursor tracking, and change detection
for supported client JSONL directories. Downstream consumers (the
workbench Scanner adapter and the upcoming normalized-event pipeline)
read from this module rather than rediscovering files themselves.
Contract: docs/plans/phase-1/01-capture-pipeline.md.
"""

from clawjournal.capture.changes import (
    LineBatch,
    cursor_after,
    cursor_for_reparse,
    file_has_changed,
    iter_new_lines,
)
from clawjournal.capture.cursors import (
    Cursor,
    ensure_schema,
    get_cursor,
    list_cursors,
    set_cursor,
)
from clawjournal.capture.discovery import ParseInput, SourceFile, iter_parse_inputs, iter_source_files

__all__ = [
    "Cursor",
    "LineBatch",
    "ParseInput",
    "SourceFile",
    "cursor_after",
    "cursor_for_reparse",
    "ensure_schema",
    "file_has_changed",
    "get_cursor",
    "iter_parse_inputs",
    "iter_new_lines",
    "iter_source_files",
    "list_cursors",
    "set_cursor",
]
