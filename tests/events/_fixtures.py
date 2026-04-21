"""Shared classifier corpora for per-client pinning + matrix drift tests.

Each fixture declares the exact list of events the classifier should
emit for a realistic (or deliberately crafted) vendor line, as tuples
of `(type, event_key, event_at, confidence, lossiness)`.

`realistic=True` fixtures mirror shapes actually observed in vendor
JSONL and are used by both the per-client classifier tests and the
capability-matrix pinning test in test_invariants.py.

`realistic=False` fixtures exercise defensive / dead-branch code paths
that today's vendors don't emit. They stay in the per-client tests
(to pin behavior if those branches ever fire) but are filtered out of
matrix pinning so that known drift doesn't make the tripwire useless.
"""

from __future__ import annotations

from typing import NamedTuple

# Whole-second UTC timestamps so input == `normalize_vendor_timestamp` output.
TS = "2026-04-20T10:00:00Z"
TS_NAIVE = "2026-04-20T10:00:00"  # no tz → confidence=low, lossiness=unknown


class ExpectedEvent(NamedTuple):
    type: str
    event_key: str | None
    event_at: str | None
    confidence: str
    lossiness: str


class Fixture(NamedTuple):
    name: str
    line: dict
    expected: list[ExpectedEvent]
    realistic: bool = True


def _ev(
    type_: str,
    event_key: str | None = None,
    event_at: str | None = TS,
    confidence: str = "high",
    lossiness: str = "none",
) -> ExpectedEvent:
    return ExpectedEvent(type_, event_key, event_at, confidence, lossiness)


def _schema_unknown(event_at: str | None = TS) -> ExpectedEvent:
    return _ev("schema_unknown", event_at=event_at, confidence="low")


# --------------------------------------------------------------------------- #
# Claude
# --------------------------------------------------------------------------- #

CLAUDE_CORPUS: list[Fixture] = [
    # --- user messages ---
    Fixture(
        "user_plain_string",
        {"type": "user", "timestamp": TS, "message": {"content": "hi"}},
        [_ev("user_message")],
    ),
    Fixture(
        "user_empty_string_is_schema_unknown",
        {"type": "user", "timestamp": TS, "message": {"content": "   "}},
        [_schema_unknown()],
    ),
    Fixture(
        "user_content_none_is_schema_unknown",
        {"type": "user", "timestamp": TS, "message": {"content": None}},
        [_schema_unknown()],
    ),
    Fixture(
        "user_empty_list_is_schema_unknown",
        {"type": "user", "timestamp": TS, "message": {"content": []}},
        [_schema_unknown()],
    ),
    Fixture(
        "user_list_text_block",
        {
            "type": "user",
            "timestamp": TS,
            "message": {"content": [{"type": "text", "text": "hi"}]},
        },
        [_ev("user_message")],
    ),
    Fixture(
        "user_list_tool_result_with_id",
        {
            "type": "user",
            "timestamp": TS,
            "message": {
                "content": [
                    {"type": "tool_result", "tool_use_id": "tu-1", "content": "ok"}
                ]
            },
        },
        [_ev("tool_result", event_key="tool_result:tu-1")],
    ),
    Fixture(
        "user_list_tool_result_without_id",
        {
            "type": "user",
            "timestamp": TS,
            "message": {"content": [{"type": "tool_result", "content": "ok"}]},
        },
        [_ev("tool_result", event_key=None)],
    ),
    Fixture(
        "user_list_text_plus_tool_result",
        {
            "type": "user",
            "timestamp": TS,
            "message": {
                "content": [
                    {"type": "text", "text": "explain"},
                    {"type": "tool_result", "tool_use_id": "tu-2", "content": "ok"},
                ]
            },
        },
        [_ev("user_message"), _ev("tool_result", event_key="tool_result:tu-2")],
    ),
    Fixture(
        "user_list_with_only_unknown_blocks",
        {
            "type": "user",
            "timestamp": TS,
            "message": {"content": [{"type": "image", "source": {}}]},
        },
        [_schema_unknown()],
    ),
    Fixture(
        "user_message_not_dict_is_schema_unknown",
        {"type": "user", "timestamp": TS, "message": "string"},
        [_schema_unknown()],
    ),
    # --- assistant messages ---
    Fixture(
        "assistant_text_only",
        {
            "type": "assistant",
            "timestamp": TS,
            "message": {"content": [{"type": "text", "text": "answer"}]},
        },
        [_ev("assistant_message")],
    ),
    Fixture(
        "assistant_thinking_only",
        {
            "type": "assistant",
            "timestamp": TS,
            "message": {"content": [{"type": "thinking", "thinking": "reasoning"}]},
        },
        [_ev("assistant_message")],
    ),
    Fixture(
        "assistant_tool_use_read_emits_file_read",
        {
            "type": "assistant",
            "timestamp": TS,
            "message": {
                "content": [
                    {"type": "tool_use", "id": "tu-3", "name": "Read", "input": {}}
                ]
            },
        },
        [
            _ev("tool_call", event_key="tool_call:tu-3"),
            _ev("file_read", event_key="file_read:tu-3"),
        ],
    ),
    Fixture(
        "assistant_tool_use_write_emits_file_write",
        {
            "type": "assistant",
            "timestamp": TS,
            "message": {
                "content": [
                    {"type": "tool_use", "id": "tu-4", "name": "Write", "input": {}}
                ]
            },
        },
        [
            _ev("tool_call", event_key="tool_call:tu-4"),
            _ev("file_write", event_key="file_write:tu-4"),
        ],
    ),
    Fixture(
        "assistant_tool_use_edit_emits_file_write",
        {
            "type": "assistant",
            "timestamp": TS,
            "message": {
                "content": [
                    {"type": "tool_use", "id": "tu-5", "name": "Edit", "input": {}}
                ]
            },
        },
        [
            _ev("tool_call", event_key="tool_call:tu-5"),
            _ev("file_write", event_key="file_write:tu-5"),
        ],
    ),
    Fixture(
        "assistant_tool_use_multiedit_emits_file_write",
        {
            "type": "assistant",
            "timestamp": TS,
            "message": {
                "content": [
                    {"type": "tool_use", "id": "tu-6", "name": "MultiEdit", "input": {}}
                ]
            },
        },
        [
            _ev("tool_call", event_key="tool_call:tu-6"),
            _ev("file_write", event_key="file_write:tu-6"),
        ],
    ),
    Fixture(
        "assistant_tool_use_bash_emits_command_start",
        {
            "type": "assistant",
            "timestamp": TS,
            "message": {
                "content": [
                    {"type": "tool_use", "id": "tu-7", "name": "Bash", "input": {}}
                ]
            },
        },
        [
            _ev("tool_call", event_key="tool_call:tu-7"),
            _ev("command_start", event_key="command_start:tu-7"),
        ],
    ),
    Fixture(
        "assistant_tool_use_apply_patch_emits_patch",
        {
            "type": "assistant",
            "timestamp": TS,
            "message": {
                "content": [
                    {
                        "type": "tool_use",
                        "id": "tu-8",
                        "name": "apply_patch",
                        "input": {},
                    }
                ]
            },
        },
        [
            _ev("tool_call", event_key="tool_call:tu-8"),
            _ev("patch", event_key="patch:tu-8"),
        ],
    ),
    Fixture(
        "assistant_tool_use_unknown_name_tool_call_only",
        {
            "type": "assistant",
            "timestamp": TS,
            "message": {
                "content": [
                    {
                        "type": "tool_use",
                        "id": "tu-9",
                        "name": "Glob",  # not in READ/WRITE/PATCH/SHELL sets
                        "input": {},
                    }
                ]
            },
        },
        [_ev("tool_call", event_key="tool_call:tu-9")],
    ),
    Fixture(
        "assistant_tool_use_missing_id_has_no_event_key",
        {
            "type": "assistant",
            "timestamp": TS,
            "message": {
                "content": [
                    {"type": "tool_use", "name": "Read", "input": {}}
                ]
            },
        },
        [
            _ev("tool_call", event_key=None),
            _ev("file_read", event_key=None),
        ],
    ),
    Fixture(
        "assistant_toolCall_alternate_key_works",
        {
            "type": "assistant",
            "timestamp": TS,
            "message": {
                "content": [
                    {"type": "toolCall", "id": "tu-10", "name": "Read", "input": {}}
                ]
            },
        },
        [
            _ev("tool_call", event_key="tool_call:tu-10"),
            _ev("file_read", event_key="file_read:tu-10"),
        ],
    ),
    Fixture(
        "assistant_text_plus_tool_use",
        {
            "type": "assistant",
            "timestamp": TS,
            "message": {
                "content": [
                    {"type": "text", "text": "reading"},
                    {"type": "tool_use", "id": "tu-11", "name": "Read", "input": {}},
                ]
            },
        },
        [
            _ev("assistant_message"),
            _ev("tool_call", event_key="tool_call:tu-11"),
            _ev("file_read", event_key="file_read:tu-11"),
        ],
    ),
    Fixture(
        "assistant_content_not_list_is_schema_unknown",
        {
            "type": "assistant",
            "timestamp": TS,
            "message": {"content": "oops"},
        },
        [_schema_unknown()],
    ),
    # --- misc ---
    Fixture(
        "unknown_entry_type_is_schema_unknown",
        {"type": "summary", "timestamp": TS},
        [_schema_unknown()],
    ),
    Fixture(
        "missing_timestamp_defaults_confidence_high",
        {"type": "user", "message": {"content": "hi"}},
        [_ev("user_message", event_at=None)],
    ),
    Fixture(
        "naive_timestamp_downgrades_confidence",
        {"type": "user", "timestamp": TS_NAIVE, "message": {"content": "hi"}},
        [_ev("user_message", event_at=None, confidence="low", lossiness="unknown")],
    ),
    # --- defensive / dead branches (not observed in real Claude JSONL) ---
    Fixture(
        "compaction_entry_is_defensive",
        {"type": "compaction", "timestamp": TS, "summary": "compacted"},
        [_ev("compaction", lossiness="compacted")],
        realistic=False,
    ),
    Fixture(
        "session_close_entry_is_defensive",
        {"type": "session_close", "timestamp": TS, "message": {}},
        [_ev("session_close")],
        realistic=False,
    ),
    Fixture(
        "approval_request_is_defensive",
        {"type": "approval_request", "timestamp": TS, "message": {}},
        [_ev("approval_request")],
        realistic=False,
    ),
    Fixture(
        "approval_decision_is_defensive",
        {"type": "approval_decision", "timestamp": TS, "message": {}},
        [_ev("approval_decision")],
        realistic=False,
    ),
]

# --------------------------------------------------------------------------- #
# Codex
# --------------------------------------------------------------------------- #

CODEX_CORPUS: list[Fixture] = [
    Fixture(
        "session_meta_emits_session_open_and_records_version",
        {
            "type": "session_meta",
            "timestamp": TS,
            "payload": {"version": "0.7.1"},
        },
        [_ev("session_open")],
    ),
    Fixture(
        "response_item_function_call_shell",
        {
            "type": "response_item",
            "timestamp": TS,
            "payload": {
                "type": "function_call",
                "name": "shell",
                "call_id": "c-1",
                "arguments": "{}",
            },
        },
        [
            _ev("tool_call", event_key="tool_call:c-1"),
            _ev("command_start", event_key="command_start:c-1"),
        ],
    ),
    Fixture(
        "response_item_function_call_read_file",
        {
            "type": "response_item",
            "timestamp": TS,
            "payload": {
                "type": "function_call",
                "name": "read_file",
                "call_id": "c-2",
            },
        },
        [
            _ev("tool_call", event_key="tool_call:c-2"),
            _ev("file_read", event_key="file_read:c-2"),
        ],
    ),
    Fixture(
        "response_item_function_call_unknown_tool",
        {
            "type": "response_item",
            "timestamp": TS,
            "payload": {
                "type": "function_call",
                "name": "mystery_tool",
                "call_id": "c-3",
            },
        },
        [_ev("tool_call", event_key="tool_call:c-3")],
    ),
    Fixture(
        "response_item_custom_tool_call",
        {
            "type": "response_item",
            "timestamp": TS,
            "payload": {
                "type": "custom_tool_call",
                "name": "read_file",
                "call_id": "c-4",
            },
        },
        [
            _ev("tool_call", event_key="tool_call:c-4"),
            _ev("file_read", event_key="file_read:c-4"),
        ],
    ),
    Fixture(
        "response_item_function_call_output",
        {
            "type": "response_item",
            "timestamp": TS,
            "payload": {
                "type": "function_call_output",
                "call_id": "c-5",
                "output": "ok",
            },
        },
        [_ev("tool_result", event_key="tool_result:c-5")],
    ),
    Fixture(
        "response_item_custom_tool_call_output",
        {
            "type": "response_item",
            "timestamp": TS,
            "payload": {
                "type": "custom_tool_call_output",
                "call_id": "c-6",
                "output": "ok",
            },
        },
        [_ev("tool_result", event_key="tool_result:c-6")],
    ),
    Fixture(
        "response_item_reasoning_is_low_fidelity_assistant_message",
        {
            "type": "response_item",
            "timestamp": TS,
            "payload": {"type": "reasoning", "summary": "…"},
        },
        [_ev("assistant_message", confidence="medium", lossiness="partial")],
    ),
    Fixture(
        "response_item_unknown_payload_is_schema_unknown",
        {
            "type": "response_item",
            "timestamp": TS,
            "payload": {"type": "made_up_kind"},
        },
        [_schema_unknown()],
    ),
    Fixture(
        "event_msg_user_message",
        {
            "type": "event_msg",
            "timestamp": TS,
            "payload": {"type": "user_message", "message": "hi"},
        },
        [_ev("user_message")],
    ),
    Fixture(
        "event_msg_agent_message",
        {
            "type": "event_msg",
            "timestamp": TS,
            "payload": {"type": "agent_message", "message": "hello"},
        },
        [_ev("assistant_message")],
    ),
    Fixture(
        "event_msg_agent_reasoning_is_low_fidelity",
        {
            "type": "event_msg",
            "timestamp": TS,
            "payload": {"type": "agent_reasoning", "text": "…"},
        },
        [_ev("assistant_message", confidence="medium", lossiness="partial")],
    ),
    Fixture(
        "event_msg_token_count_is_schema_unknown",
        {
            "type": "event_msg",
            "timestamp": TS,
            "payload": {"type": "token_count", "info": {"total": 1}},
        },
        [_schema_unknown()],
    ),
    Fixture(
        "turn_context_is_schema_unknown",
        {"type": "turn_context", "timestamp": TS, "payload": {}},
        [_schema_unknown()],
    ),
    Fixture(
        "session_close_entry_is_defensive",
        {"type": "session_close", "timestamp": TS},
        [_ev("session_close")],
        realistic=False,
    ),
    Fixture(
        "approval_request_is_defensive",
        {"type": "approval_request", "timestamp": TS, "payload": {}},
        [_ev("approval_request")],
        realistic=False,
    ),
    Fixture(
        "approval_decision_is_defensive",
        {"type": "approval_decision", "timestamp": TS, "payload": {}},
        [_ev("approval_decision")],
        realistic=False,
    ),
]

# --------------------------------------------------------------------------- #
# OpenClaw
# --------------------------------------------------------------------------- #

OPENCLAW_CORPUS: list[Fixture] = [
    Fixture(
        "session_header_emits_session_open",
        {"type": "session", "timestamp": TS, "cwd": "/x"},
        [_ev("session_open")],
    ),
    Fixture(
        "compaction_entry",
        {"type": "compaction", "timestamp": TS},
        [_ev("compaction", lossiness="compacted")],
    ),
    Fixture(
        "message_user_string_content",
        {
            "type": "message",
            "timestamp": TS,
            "message": {"role": "user", "content": "hi"},
        },
        [_ev("user_message")],
    ),
    Fixture(
        "message_user_list_with_text",
        {
            "type": "message",
            "timestamp": TS,
            "message": {
                "role": "user",
                "content": [{"type": "text", "text": "hi"}],
            },
        },
        [_ev("user_message")],
    ),
    Fixture(
        "message_user_list_without_text_is_schema_unknown",
        {
            "type": "message",
            "timestamp": TS,
            "message": {"role": "user", "content": [{"type": "image"}]},
        },
        [_schema_unknown()],
    ),
    Fixture(
        "message_assistant_text",
        {
            "type": "message",
            "timestamp": TS,
            "message": {
                "role": "assistant",
                "content": [{"type": "text", "text": "answer"}],
            },
        },
        [_ev("assistant_message")],
    ),
    Fixture(
        "message_assistant_thinking",
        {
            "type": "message",
            "timestamp": TS,
            "message": {
                "role": "assistant",
                "content": [{"type": "thinking", "thinking": "reasoning"}],
            },
        },
        [_ev("assistant_message")],
    ),
    Fixture(
        "message_assistant_toolcall_read",
        {
            "type": "message",
            "timestamp": TS,
            "message": {
                "role": "assistant",
                "content": [
                    {"type": "toolCall", "id": "tc-1", "name": "read_file"}
                ],
            },
        },
        [
            _ev("tool_call", event_key="tool_call:tc-1"),
            _ev("file_read", event_key="file_read:tc-1"),
        ],
    ),
    Fixture(
        "message_assistant_toolcall_write",
        {
            "type": "message",
            "timestamp": TS,
            "message": {
                "role": "assistant",
                "content": [{"type": "toolCall", "id": "tc-2", "name": "write_file"}],
            },
        },
        [
            _ev("tool_call", event_key="tool_call:tc-2"),
            _ev("file_write", event_key="file_write:tc-2"),
        ],
    ),
    Fixture(
        "message_assistant_toolcall_shell",
        {
            "type": "message",
            "timestamp": TS,
            "message": {
                "role": "assistant",
                "content": [{"type": "toolCall", "id": "tc-3", "name": "bash"}],
            },
        },
        [
            _ev("tool_call", event_key="tool_call:tc-3"),
            _ev("command_start", event_key="command_start:tc-3"),
        ],
    ),
    Fixture(
        "message_assistant_toolcall_unknown_tool",
        {
            "type": "message",
            "timestamp": TS,
            "message": {
                "role": "assistant",
                "content": [{"type": "toolCall", "id": "tc-4", "name": "glob"}],
            },
        },
        [_ev("tool_call", event_key="tool_call:tc-4")],
    ),
    Fixture(
        "message_assistant_text_plus_toolcall",
        {
            "type": "message",
            "timestamp": TS,
            "message": {
                "role": "assistant",
                "content": [
                    {"type": "text", "text": "reading"},
                    {"type": "toolCall", "id": "tc-5", "name": "read_file"},
                ],
            },
        },
        [
            _ev("assistant_message"),
            _ev("tool_call", event_key="tool_call:tc-5"),
            _ev("file_read", event_key="file_read:tc-5"),
        ],
    ),
    Fixture(
        "message_toolresult_with_id",
        {
            "type": "message",
            "timestamp": TS,
            "message": {"role": "toolResult", "toolCallId": "tc-1"},
        },
        [_ev("tool_result", event_key="tool_result:tc-1")],
    ),
    Fixture(
        "message_toolresult_without_id",
        {
            "type": "message",
            "timestamp": TS,
            "message": {"role": "toolResult"},
        },
        [_ev("tool_result", event_key=None)],
    ),
    Fixture(
        "message_bashexecution_full",
        {
            "type": "message",
            "timestamp": TS,
            "message": {
                "role": "bashExecution",
                "command": "ls",
                "output": "a\nb\n",
                "exitCode": 0,
            },
        },
        [
            _ev("command_start"),
            _ev("stdout_chunk", confidence="low", lossiness="unknown"),
            _ev("command_exit"),
        ],
    ),
    Fixture(
        "message_bashexecution_no_output",
        {
            "type": "message",
            "timestamp": TS,
            "message": {"role": "bashExecution", "command": "ls", "exitCode": 0},
        },
        [_ev("command_start"), _ev("command_exit")],
    ),
    Fixture(
        "message_bashexecution_exit_only",
        {
            "type": "message",
            "timestamp": TS,
            "message": {"role": "bashExecution", "exitCode": 1},
        },
        [_ev("command_exit")],
    ),
    Fixture(
        "message_unknown_role_is_schema_unknown",
        {
            "type": "message",
            "timestamp": TS,
            "message": {"role": "system", "content": "x"},
        },
        [_schema_unknown()],
    ),
    Fixture(
        "unknown_entry_type_is_schema_unknown",
        {"type": "telemetry", "timestamp": TS},
        [_schema_unknown()],
    ),
    Fixture(
        "session_close_entry_is_defensive",
        {"type": "session_close", "timestamp": TS},
        [_ev("session_close")],
        realistic=False,
    ),
    Fixture(
        "approval_request_is_defensive",
        {"type": "approval_request", "timestamp": TS},
        [_ev("approval_request")],
        realistic=False,
    ),
    Fixture(
        "approval_decision_is_defensive",
        {"type": "approval_decision", "timestamp": TS},
        [_ev("approval_decision")],
        realistic=False,
    ),
]


ALL_CORPORA: dict[str, list[Fixture]] = {
    "claude": CLAUDE_CORPUS,
    "codex": CODEX_CORPUS,
    "openclaw": OPENCLAW_CORPUS,
}
