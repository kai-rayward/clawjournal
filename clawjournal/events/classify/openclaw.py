"""OpenClaw JSONL line classifier."""

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
    if entry_type == "session":
        return [
            event(
                "session_open",
                event_at=event_at,
                confidence=confidence,
                lossiness=lossiness,
            )
        ]
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
    if entry_type in {"approval_request", "approval_decision"}:
        return [
            event(
                entry_type,
                event_at=event_at,
                confidence=confidence,
                lossiness=lossiness,
            )
        ]
    if entry_type != "message":
        return [schema_unknown(event_at)]
    return _classify_message(message, event_at, confidence, lossiness)


def session_meta(line: dict) -> SessionMeta:
    client_version = line.get("version")
    if not isinstance(client_version, str) or not client_version.strip():
        client_version = None
    closure_seen = line.get("type") in {"session_close", "session_end", "closed"}
    return SessionMeta(client_version=client_version, closure_seen=closure_seen)


def _classify_message(
    message: Any,
    event_at: str | None,
    confidence: str,
    lossiness: str,
) -> list[ClassifiedEvent]:
    if not isinstance(message, dict):
        return [schema_unknown(event_at)]
    role = message.get("role")
    if role == "user":
        return _classify_user_message(message, event_at, confidence, lossiness)
    if role == "assistant":
        return _classify_assistant_message(message, event_at, confidence, lossiness)
    if role == "toolResult":
        tool_id = _as_string(message.get("toolCallId"))
        return [
            event(
                "tool_result",
                event_at=event_at,
                event_key=f"tool_result:{tool_id}" if tool_id is not None else None,
                confidence=confidence,
                lossiness=lossiness,
            )
        ]
    if role == "bashExecution":
        return _classify_bash_execution(message, event_at)
    if role in {"approval_request", "approval_decision"}:
        return [
            event(
                role,
                event_at=event_at,
                confidence=confidence,
                lossiness=lossiness,
            )
        ]
    return [schema_unknown(event_at)]


def _classify_user_message(
    message: Any,
    event_at: str | None,
    confidence: str,
    lossiness: str,
) -> list[ClassifiedEvent]:
    if not isinstance(message, dict):
        return [schema_unknown(event_at)]
    content = message.get("content")
    if isinstance(content, str) and content.strip():
        return [
            event(
                "user_message",
                event_at=event_at,
                confidence=confidence,
                lossiness=lossiness,
            )
        ]
    if isinstance(content, list):
        for block in content:
            if isinstance(block, dict) and _has_text(block.get("text")):
                return [
                    event(
                        "user_message",
                        event_at=event_at,
                        confidence=confidence,
                        lossiness=lossiness,
                    )
                ]
    return [schema_unknown(event_at)]


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
        elif block_type == "toolCall":
            tool_name = block.get("name")
            tool_id = _as_string(block.get("id"))
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
                        event_key=f"{secondary}:{tool_id}" if tool_id is not None else None,
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
                            f"command_start:{tool_id}" if tool_id is not None else None
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


def _as_string(value: object) -> str | None:
    if isinstance(value, str) and value.strip():
        return value
    return None
