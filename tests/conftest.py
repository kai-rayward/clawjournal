"""Shared fixtures for clawjournal tests."""

import pytest

from clawjournal.redaction.anonymizer import Anonymizer


@pytest.fixture
def sample_user_entry():
    """Realistic JSONL user entry dict."""
    return {
        "type": "user",
        "timestamp": 1706000000000,
        "cwd": "/Users/testuser/Documents/myproject",
        "gitBranch": "main",
        "version": "1.0.0",
        "sessionId": "abc-123",
        "message": {
            "content": "Fix the login bug in src/auth.py",
        },
    }


@pytest.fixture
def sample_assistant_entry():
    """Realistic JSONL assistant entry dict."""
    return {
        "type": "assistant",
        "timestamp": 1706000001000,
        "message": {
            "model": "claude-sonnet-4-20250514",
            "content": [
                {"type": "thinking", "thinking": "Let me look at the auth file."},
                {"type": "text", "text": "I'll fix the login bug."},
                {
                    "type": "tool_use",
                    "name": "Read",
                    "input": {"file_path": "/Users/testuser/Documents/myproject/src/auth.py"},
                },
            ],
            "usage": {
                "input_tokens": 500,
                "output_tokens": 100,
                "cache_read_input_tokens": 200,
            },
        },
    }


@pytest.fixture
def mock_anonymizer(monkeypatch):
    """Anonymizer with patched _detect_home_dir returning deterministic values."""
    monkeypatch.setattr(
        "clawjournal.redaction.anonymizer._detect_home_dir",
        lambda: ("/Users/testuser", "testuser"),
    )
    return Anonymizer()


@pytest.fixture
def tmp_config(tmp_path, monkeypatch):
    """Monkeypatch CONFIG_FILE and CONFIG_DIR to tmp_path."""
    config_dir = tmp_path / ".clawjournal"
    config_file = config_dir / "config.json"
    monkeypatch.setattr("clawjournal.config.CONFIG_DIR", config_dir)
    monkeypatch.setattr("clawjournal.config.CONFIG_FILE", config_file)
    return config_file
