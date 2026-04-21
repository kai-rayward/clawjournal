"""Shared types and validation for the execution recorder."""

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
