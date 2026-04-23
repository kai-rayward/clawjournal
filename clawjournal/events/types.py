"""Shared types and validation for the execution recorder.

``session_key`` grammar (per ADR-001, 2026-04-22)
-------------------------------------------------

``session_key`` is the canonical public session identifier for every phase-1
feature from 06 onward: timeline-viewer URLs, replay-export bundles, encrypted
HTML-share metadata, aggregation bucket keys, cross-session FTS results, and
any external tool (MCP responses, etc.) that needs to reference a session.
Workbench's legacy ``sessions.session_id`` remains for internal plumbing only.

Current grammar, by ``client``:

- ``claude:<project_dir_name>:<session_uuid>``
  Native Claude Code (``~/.claude/projects/<project_dir_name>/<uuid>.jsonl``)
  and the Claude-Desktop local-agent convergence case where the CLI session id
  matches the native uuid.
- ``claude:<workspace_key>:<cliSessionId>``
  Claude-Desktop local-agent path when the CLI session id differs from the
  native uuid. ``workspace_key`` is derived from the wrapper's
  ``userSelectedFolders[0]`` (path separators replaced with ``-``) or
  ``_cowork_<sessionId>`` when no user-selected folder is present.
- ``codex:<absolute_source_path>``
- ``openclaw:<absolute_source_path>``

Stability guarantees
--------------------

The grammar above is the public contract. Any change to the grammar itself
requires an ADR amendment or a superseding ADR. Do not edit the grammar in
isolation; the export, URL, and FTS consumers all assume the shapes above.

For a fixed grammar, ``session_key`` values are stable with respect to a
fixed set of inputs (``project_dir_name``, session UUID, wrapper metadata).
They are **not** guaranteed stable across input changes — renaming
``~/Projects/myapp`` → ``~/Projects/my-app`` today produces a new
``session_key`` because ``project_dir_name`` is part of the claude grammar.
This is ADR-001's Open Question #1 and is deferred to a follow-up ADR; a
future grammar may decouple ``project_dir_name`` from the key specifically to
survive renames.

Consumers that care about durability across renames (bundle export, URL
bookmarks, encrypted share metadata) should either (a) snapshot the
``session_key`` at share time and treat it as an opaque handle, or (b) carry
a bundle ``schema_version`` that can be converted when the grammar changes.

Within a single grammar version:

- Bundles exported on one ``schema_version`` remain resolvable by any later
  clawjournal version that honors the same ``schema_version``.
- URLs of the form ``clawjournal://session/<session_key>#event-<id>`` remain
  resolvable as long as the underlying inputs (path + wrapper) are unchanged.
- Workbench ``sessions.session_key`` re-derives on upsert — a rescan after an
  input change will overwrite the stored value with the new derivation. This
  is the intentional escape hatch for fixing derivation bugs; it is also why
  renames surface as an Open Question rather than a silent breakage.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import NamedTuple

EVENT_TYPES = (
    "user_message",
    "assistant_message",
    "tool_call",
    "tool_result",
    "file_read",
    "file_write",
    "patch",
    "command_start",
    "command_exit",
    "stdout_chunk",
    "stderr_chunk",
    "approval_request",
    "approval_decision",
    "compaction",
    "session_open",
    "session_close",
    "schema_unknown",
)
EVENT_TYPE_SET = set(EVENT_TYPES)

VALID_SOURCES = {
    "claude-jsonl",
    "codex-rollout",
    "openclaw-jsonl",
    "hook",
    "flightrec-derived",
}
VALID_CONFIDENCE = {"high", "medium", "low", "missing"}
VALID_LOSSINESS = {"none", "partial", "unknown", "compacted"}

# `missing` is a read-time presentation state (produced by capability_join)
# and never persisted. Included here with rank 0 so comparisons against
# hypothetical missing inputs stay well-defined; writers reject it upstream.
CONFIDENCE_RANK: dict[str, int] = {
    "high": 3,
    "medium": 2,
    "low": 1,
    "missing": 0,
}


class ClassifiedEvent(NamedTuple):
    type: str
    event_at: str | None
    event_key: str | None
    confidence: str
    lossiness: str


class SessionMeta(NamedTuple):
    client_version: str | None = None
    # Raw parent id from the vendor line. The ingest layer turns this into a
    # concrete session_key using the current SourceFile context.
    parent_session_id: str | None = None
    closure_seen: bool = False


def validate_classified_event(event: ClassifiedEvent) -> None:
    if event.type not in EVENT_TYPE_SET:
        raise ValueError(f"Unsupported event type: {event.type}")
    if event.confidence not in VALID_CONFIDENCE:
        raise ValueError(f"Unsupported event confidence: {event.confidence}")
    if event.lossiness not in VALID_LOSSINESS:
        raise ValueError(f"Unsupported event lossiness: {event.lossiness}")


def normalize_vendor_timestamp(
    value: object,
) -> tuple[str | None, bool]:
    """Return `(utc_iso_z, was_timezone_naive)` for a vendor timestamp."""

    if value is None:
        return None, False
    if isinstance(value, (int, float)):
        dt = datetime.fromtimestamp(value / 1000, tz=timezone.utc)
        return dt.isoformat().replace("+00:00", "Z"), False
    if not isinstance(value, str):
        return None, False

    raw = value.strip()
    if not raw:
        return None, False

    normalized = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        return None, False
    if dt.tzinfo is None:
        return None, True
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"), False
