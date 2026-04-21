import pytest

from clawjournal.events.classify import classify_line
from clawjournal.events.types import (
    EVENT_TYPE_SET,
    ClassifiedEvent,
    validate_classified_event,
)


def test_claude_assistant_tool_use_emits_assistant_and_tool_events():
    line = {
        "type": "assistant",
        "timestamp": "2026-04-20T10:00:00.000Z",
        "message": {
            "content": [
                {"type": "text", "text": "Reading the file."},
                {
                    "type": "tool_use",
                    "id": "tu-1",
                    "name": "Read",
                    "input": {"file_path": "/tmp/demo.py"},
                },
            ]
        },
    }

    events = classify_line("claude", line)
    assert [event.type for event in events] == [
        "assistant_message",
        "tool_call",
        "file_read",
    ]
    assert all(event.type in EVENT_TYPE_SET for event in events)


def test_codex_unknown_payload_becomes_schema_unknown():
    line = {
        "type": "event_msg",
        "timestamp": "2026-04-20T10:00:00.000Z",
        "payload": {"type": "token_count", "info": {"total": 1}},
    }

    events = classify_line("codex", line)
    assert len(events) == 1
    assert events[0].type == "schema_unknown"


def test_codex_function_call_output_with_exit_code_emits_command_exit():
    line = {
        "type": "response_item",
        "timestamp": "2026-04-20T10:00:00.000Z",
        "payload": {
            "type": "function_call_output",
            "call_id": "call-x",
            "output": "Exit code: 0\nWall time: 0 seconds\nOutput:\nfoo.py\n",
        },
    }

    events = classify_line("codex", line)
    assert [event.type for event in events] == ["tool_result", "command_exit"]
    assert [event.event_key for event in events] == [
        "tool_result:call-x",
        "command_exit:call-x",
    ]


def test_openclaw_session_header_is_session_open():
    line = {
        "type": "session",
        "timestamp": "2026-04-20T10:00:00.000Z",
        "cwd": "/Users/test/repo",
    }

    events = classify_line("openclaw", line)
    assert len(events) == 1
    assert events[0].type == "session_open"


def test_validate_classified_event_rejects_unlisted_type():
    bogus = ClassifiedEvent(
        type="totally_made_up",
        event_at=None,
        event_key=None,
        confidence="high",
        lossiness="none",
    )
    with pytest.raises(ValueError, match="Unsupported event type"):
        validate_classified_event(bogus)


def test_validate_classified_event_rejects_unlisted_confidence_and_lossiness():
    bad_confidence = ClassifiedEvent(
        type="user_message",
        event_at=None,
        event_key=None,
        confidence="certain",
        lossiness="none",
    )
    with pytest.raises(ValueError, match="confidence"):
        validate_classified_event(bad_confidence)

    bad_lossiness = ClassifiedEvent(
        type="user_message",
        event_at=None,
        event_key=None,
        confidence="high",
        lossiness="mangled",
    )
    with pytest.raises(ValueError, match="lossiness"):
        validate_classified_event(bad_lossiness)
