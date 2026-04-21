"""Codex rollout line classifier."""

from __future__ import annotations

import json
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
    event_at, confidence, lossiness = resolve_timestamp(line.get("timestamp"))
    entry_type = line.get("type")

    if entry_type == "session_meta":
        return [
            event(
                "session_open",
                event_at=event_at,
                confidence=confidence,
                lossiness=lossiness,
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
    if entry_type == "turn_context":
        return [schema_unknown(event_at)]
    if entry_type == "response_item":
        return _classify_response_item(
            line.get("payload"), event_at, confidence, lossiness
        )
    if entry_type == "event_msg":
        return _classify_event_msg(
            line.get("payload"), event_at, confidence, lossiness
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
    payload = line.get("payload", {})
    if not isinstance(payload, dict):
        payload = {}
    client_version = payload.get("version")
    if not isinstance(client_version, str) or not client_version.strip():
        client_version = None
    closure_seen = line.get("type") in {"session_close", "session_end"}
    if line.get("type") == "event_msg":
        closure_seen = closure_seen or payload.get("type") in {
            "session_close",
            "session_end",
        }
    return SessionMeta(client_version=client_version, closure_seen=closure_seen)


def _classify_response_item(
    payload: Any,
    event_at: str | None,
    confidence: str,
    lossiness: str,
) -> list[ClassifiedEvent]:
    if not isinstance(payload, dict):
        return [schema_unknown(event_at)]
    payload_type = payload.get("type")
    if payload_type in {"function_call", "custom_tool_call"}:
        tool_name = payload.get("name")
        call_id = _as_string(payload.get("call_id"))
        events: list[ClassifiedEvent] = [
            event(
                "tool_call",
                event_at=event_at,
                event_key=f"tool_call:{call_id}" if call_id is not None else None,
                confidence=confidence,
                lossiness=lossiness,
            )
        ]
        secondary = tool_secondary_type(tool_name)
        if secondary is not None:
            events.append(
                event(
                    secondary,
                    event_at=event_at,
                    event_key=f"{secondary}:{call_id}" if call_id is not None else None,
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
                        f"command_start:{call_id}" if call_id is not None else None
                    ),
                    confidence=confidence,
                    lossiness=lossiness,
                )
            )
        return events
    if payload_type in {"function_call_output", "custom_tool_call_output"}:
        call_id = _as_string(payload.get("call_id"))
        events = [
            event(
                "tool_result",
                event_at=event_at,
                event_key=f"tool_result:{call_id}" if call_id is not None else None,
                confidence=confidence,
                lossiness=lossiness,
            )
        ]
        if _has_direct_exit_metadata(payload):
            events.append(
                event(
                    "command_exit",
                    event_at=event_at,
                    event_key=f"command_exit:{call_id}" if call_id is not None else None,
                    confidence=confidence,
                    lossiness=lossiness,
                )
            )
        return events
    if payload_type == "reasoning":
        return [
            event(
                "assistant_message",
                event_at=event_at,
                confidence="medium",
                lossiness="partial",
            )
        ]
    if payload_type in {"approval_request", "approval_decision"}:
        return [
            event(
                payload_type,
                event_at=event_at,
                confidence=confidence,
                lossiness=lossiness,
            )
        ]
    if payload_type in {"session_close", "session_end"}:
        return [
            event(
                "session_close",
                event_at=event_at,
                confidence=confidence,
                lossiness=lossiness,
            )
        ]
    return [schema_unknown(event_at)]


def _classify_event_msg(
    payload: Any,
    event_at: str | None,
    confidence: str,
    lossiness: str,
) -> list[ClassifiedEvent]:
    if not isinstance(payload, dict):
        return [schema_unknown(event_at)]
    payload_type = payload.get("type")
    if payload_type == "user_message":
        return [
            event(
                "user_message",
                event_at=event_at,
                confidence=confidence,
                lossiness=lossiness,
            )
        ]
    if payload_type == "agent_message":
        return [
            event(
                "assistant_message",
                event_at=event_at,
                confidence=confidence,
                lossiness=lossiness,
            )
        ]
    if payload_type == "agent_reasoning":
        return [
            event(
                "assistant_message",
                event_at=event_at,
                confidence="medium",
                lossiness="partial",
            )
        ]
    if payload_type in {"approval_request", "approval_decision"}:
        return [
            event(
                payload_type,
                event_at=event_at,
                confidence=confidence,
                lossiness=lossiness,
            )
        ]
    if payload_type in {"session_close", "session_end"}:
        return [
            event(
                "session_close",
                event_at=event_at,
                confidence=confidence,
                lossiness=lossiness,
            )
        ]
    return [schema_unknown(event_at)]


def _as_string(value: object) -> str | None:
    if isinstance(value, str) and value.strip():
        return value
    return None


def _has_direct_exit_metadata(payload: dict[str, Any]) -> bool:
    raw_output = payload.get("output")
    if isinstance(raw_output, str):
        if any(
            line.startswith("Exit code: ")
            for line in raw_output.splitlines()
        ):
            return True
        try:
            parsed = json.loads(raw_output)
        except json.JSONDecodeError:
            return False
        metadata = parsed.get("metadata", {}) if isinstance(parsed, dict) else {}
        return "exit_code" in metadata
    if not isinstance(raw_output, list):
        return False

    for block in raw_output:
        if not isinstance(block, dict):
            continue
        if block.get("type") != "input_text":
            continue
        text = block.get("text")
        if isinstance(text, str) and any(
            line.startswith("Exit code: ")
            for line in text.splitlines()
        ):
            return True
    return False
