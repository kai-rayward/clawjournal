"""Claude JSONL line classifier."""

from __future__ import annotations

from typing import Any

from clawjournal.events.classify.common import (
    event,
    is_shell_tool,
    resolve_timestamp,
    schema_unknown,
    tool_secondary_type,
)
from clawjournal.events.types import ClassifiedEvent, SessionMeta


def classify(line: dict) -> list[ClassifiedEvent]:
    message = line.get("message", {})
    event_at, confidence, lossiness = resolve_timestamp(
        message.get("timestamp") if isinstance(message, dict) else None,
        line.get("timestamp"),
    )
    entry_type = line.get("type")
    role = message.get("role") if isinstance(message, dict) else None

    if entry_type == "compaction":
        return [
            event(
                "compaction",
                event_at=event_at,
                confidence=confidence,
                lossiness="compacted",
            )
        ]
    if entry_type in {"session_close", "session_end"}:
        return [
            event(
                "session_close",
                event_at=event_at,
                confidence=confidence,
                lossiness=lossiness,
            )
        ]
    if role == "bashExecution":
        return _classify_bash_execution(message, event_at)
    if entry_type == "user":
        return _classify_user_message(message, event_at, confidence, lossiness)
    if entry_type == "assistant":
        return _classify_assistant_message(
            message, event_at, confidence, lossiness
        )
    if entry_type in {"approval_request", "approval_decision"}:
        return [
            event(
                entry_type,
                event_at=event_at,
                confidence=confidence,
                lossiness=lossiness,
            )
        ]
    return [schema_unknown(event_at)]


def session_meta(line: dict) -> SessionMeta:
    message = line.get("message", {})
    parent_id = _first_string(
        line.get("parentSessionId"),
        line.get("parent_session_id"),
        message.get("parentSessionId") if isinstance(message, dict) else None,
        message.get("parent_session_id") if isinstance(message, dict) else None,
    )
    closure_seen = line.get("type") in {"session_close", "session_end", "closed"}
    client_version = line.get("version")
    if not isinstance(client_version, str) or not client_version.strip():
        client_version = None
    return SessionMeta(
        client_version=client_version,
        parent_session_id=parent_id,
        closure_seen=closure_seen,
    )


def _classify_user_message(
    message: Any,
    event_at: str | None,
    confidence: str,
    lossiness: str,
) -> list[ClassifiedEvent]:
    if not isinstance(message, dict):
        return [schema_unknown(event_at)]
    content = message.get("content")
    events: list[ClassifiedEvent] = []
    has_text = False

    if isinstance(content, str):
        has_text = bool(content.strip())
    elif isinstance(content, list):
        for block in content:
            if not isinstance(block, dict):
                continue
            if block.get("type") == "text" and _has_text(block.get("text")):
                has_text = True
            elif block.get("type") == "tool_result":
                tool_id = _first_string(
                    block.get("tool_use_id"),
                    block.get("toolUseId"),
                )
                events.append(
                    event(
                        "tool_result",
                        event_at=event_at,
                        event_key=(
                            f"tool_result:{tool_id}" if tool_id is not None else None
                        ),
                        confidence=confidence,
                        lossiness=lossiness,
                    )
                )

    if has_text:
        events.insert(
            0,
            event(
                "user_message",
                event_at=event_at,
                confidence=confidence,
                lossiness=lossiness,
            ),
        )
    return events or [schema_unknown(event_at)]


def _classify_assistant_message(
    message: Any,
    event_at: str | None,
    confidence: str,
    lossiness: str,
) -> list[ClassifiedEvent]:
    if not isinstance(message, dict):
        return [schema_unknown(event_at)]
    content = message.get("content")
    if not isinstance(content, list):
        return [schema_unknown(event_at)]

    events: list[ClassifiedEvent] = []
    has_assistant_text = False
    for block in content:
        if not isinstance(block, dict):
            continue
        block_type = block.get("type")
        if block_type in {"text", "thinking"}:
            if _has_text(block.get("text")) or _has_text(block.get("thinking")):
                has_assistant_text = True
        elif block_type in {"tool_use", "toolCall"}:
            tool_name = block.get("name")
            tool_id = _first_string(block.get("id"), block.get("toolUseId"))
            events.append(
                event(
                    "tool_call",
                    event_at=event_at,
                    event_key=f"tool_call:{tool_id}" if tool_id is not None else None,
                    confidence=confidence,
                    lossiness=lossiness,
                )
            )
            secondary = tool_secondary_type(tool_name)
            if secondary is not None:
                events.append(
                    event(
                        secondary,
                        event_at=event_at,
                        event_key=(
                            f"{secondary}:{tool_id}" if tool_id is not None else None
                        ),
                        confidence=confidence,
                        lossiness=lossiness,
                    )
                )
            if is_shell_tool(tool_name):
                events.append(
                    event(
                        "command_start",
                        event_at=event_at,
                        event_key=(
                            f"command_start:{tool_id}"
                            if tool_id is not None
                            else None
                        ),
                        confidence=confidence,
                        lossiness=lossiness,
                    )
                )

    if has_assistant_text:
        events.insert(
            0,
            event(
                "assistant_message",
                event_at=event_at,
                confidence=confidence,
                lossiness=lossiness,
            ),
        )
    return events or [schema_unknown(event_at)]


def _classify_bash_execution(
    message: Any,
    event_at: str | None,
) -> list[ClassifiedEvent]:
    if not isinstance(message, dict):
        return [schema_unknown(event_at)]

    events: list[ClassifiedEvent] = []
    command = message.get("command")
    output = message.get("output")
    exit_code = message.get("exitCode")
    if isinstance(command, str) and command.strip():
        events.append(event("command_start", event_at=event_at))
    if isinstance(output, str) and output.strip():
        events.append(
            event(
                "stdout_chunk",
                event_at=event_at,
                confidence="low",
                lossiness="unknown",
            )
        )
    if exit_code is not None:
        events.append(event("command_exit", event_at=event_at))
    return events or [schema_unknown(event_at)]


def _has_text(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _first_string(*values: object) -> str | None:
    for value in values:
        if isinstance(value, str) and value.strip():
            return value
    return None
