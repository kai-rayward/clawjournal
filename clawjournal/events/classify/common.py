"""Shared classifier helpers."""

from __future__ import annotations

from typing import Any

from clawjournal.events.types import ClassifiedEvent, normalize_vendor_timestamp

READ_TOOL_NAMES = {
    "read",
    "read_file",
    "readmanyfiles",
    "view",
    "view_image",
}
WRITE_TOOL_NAMES = {
    "write",
    "write_file",
    "edit",
    "edit_file",
    "multiedit",
    "notebookedit",
}
PATCH_TOOL_NAMES = {
    "apply_patch",
    "patch",
}
SHELL_TOOL_NAMES = {
    "bash",
    "exec",
    "exec_command",
    "shell",
    "shell_command",
}


def resolve_timestamp(*candidates: object) -> tuple[str | None, str, str]:
    for candidate in candidates:
        normalized, naive = normalize_vendor_timestamp(candidate)
        if normalized is not None:
            return normalized, "high", "none"
        if naive:
            return None, "low", "unknown"
    return None, "high", "none"


def event(
    event_type: str,
    *,
    event_at: str | None,
    event_key: str | None = None,
    confidence: str = "high",
    lossiness: str = "none",
) -> ClassifiedEvent:
    return ClassifiedEvent(
        type=event_type,
        event_at=event_at,
        event_key=event_key,
        confidence=confidence,
        lossiness=lossiness,
    )


def schema_unknown(event_at: str | None, *, lossiness: str = "none") -> ClassifiedEvent:
    return event(
        "schema_unknown",
        event_at=event_at,
        confidence="low",
        lossiness=lossiness,
    )


def tool_secondary_type(name: Any) -> str | None:
    normalized = _normalize_tool_name(name)
    if normalized in READ_TOOL_NAMES:
        return "file_read"
    if normalized in WRITE_TOOL_NAMES:
        return "file_write"
    if normalized in PATCH_TOOL_NAMES:
        return "patch"
    return None


def is_shell_tool(name: Any) -> bool:
    return _normalize_tool_name(name) in SHELL_TOOL_NAMES


def _normalize_tool_name(name: Any) -> str:
    if not isinstance(name, str):
        return ""
    return name.strip().lower().replace("-", "_")
