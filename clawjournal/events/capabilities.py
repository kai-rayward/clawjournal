"""Static client capability matrix for the execution recorder."""

from __future__ import annotations

from clawjournal.events.types import EVENT_TYPES

CAPABILITY_MATRIX: dict[tuple[str, str], tuple[bool, str]] = {}

for client in ("claude", "codex", "openclaw"):
    for event_type in EVENT_TYPES:
        CAPABILITY_MATRIX[(client, event_type)] = (
            False,
            "not emitted by this client",
        )


def _set(client: str, event_type: str, supported: bool, reason: str) -> None:
    CAPABILITY_MATRIX[(client, event_type)] = (supported, reason)


for event_type, reason in (
    ("user_message", "direct"),
    ("assistant_message", "direct"),
    ("tool_call", "direct"),
    ("tool_result", "direct"),
    ("file_read", "inferred from tool name"),
    ("file_write", "inferred from tool name"),
    ("patch", "inferred from tool name"),
    ("command_start", "direct for shell-like tools"),
    ("stdout_chunk", "best-effort from bashExecution output"),
    ("command_exit", "direct for bashExecution exit codes"),
    ("schema_unknown", "fallback for parseable but unsupported lines"),
):
    _set("claude", event_type, True, reason)

for event_type, reason in (
    ("session_open", "direct from session header"),
    ("user_message", "direct"),
    ("assistant_message", "direct"),
    ("tool_call", "direct"),
    ("tool_result", "direct"),
    ("file_read", "inferred from tool name"),
    ("file_write", "inferred from tool name"),
    ("patch", "inferred from tool name"),
    ("command_start", "inferred from shell-like tool names"),
    ("command_exit", "direct when output carries exit metadata"),
    ("schema_unknown", "fallback for parseable but unsupported lines"),
):
    _set("codex", event_type, True, reason)

for event_type, reason in (
    ("session_open", "direct from session header"),
    ("user_message", "direct"),
    ("assistant_message", "direct"),
    ("tool_call", "direct"),
    ("tool_result", "direct"),
    ("file_read", "inferred from tool name"),
    ("file_write", "inferred from tool name"),
    ("patch", "inferred from tool name"),
    ("command_start", "direct for shell-like tools"),
    ("stdout_chunk", "best-effort from bashExecution output"),
    ("command_exit", "direct for bashExecution exit codes"),
    ("compaction", "direct"),
    ("schema_unknown", "fallback for parseable but unsupported lines"),
):
    _set("openclaw", event_type, True, reason)


def capabilities_json() -> dict[str, dict[str, dict[str, object]]]:
    payload: dict[str, dict[str, dict[str, object]]] = {}
    for client in ("claude", "codex", "openclaw"):
        payload[client] = {}
        for event_type in EVENT_TYPES:
            supported, reason = CAPABILITY_MATRIX[(client, event_type)]
            payload[client][event_type] = {
                "supported": supported,
                "reason": reason,
            }
    return payload


def effective_matrix() -> dict[tuple[str, str], tuple[bool, str]]:
    """Overlay-aware view of ``CAPABILITY_MATRIX``.

    Lazy-imports the loader from ``events.doctor.overlay`` so PyYAML is
    not pulled in for non-events CLI invocations. Plan 08.
    """

    from clawjournal.events.doctor.overlay import effective_matrix as _impl

    return _impl()
