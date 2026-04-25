"""End-to-end CLI tests for the three new commands."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest


@pytest.fixture
def isolated_home(monkeypatch, tmp_path):
    """Run CLI in a subprocess with HOME pointing at a tmp dir."""

    env = {
        "HOME": str(tmp_path),
        "PATH": "/usr/bin:/bin",
        "PYTHONPATH": str(Path(__file__).parent.parent.parent.parent),
        "CLAWJOURNAL_SKIP_TRUFFLEHOG": "1",
    }
    return env


def _run(args: list[str], env: dict) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", "clawjournal.cli", *args],
        capture_output=True,
        text=True,
        env=env,
        timeout=30,
    )


def test_doctor_fresh_install_returns_zero(isolated_home):
    result = _run(["events", "doctor", "--json"], isolated_home)
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["events_doctor_schema_version"] == "1.0"
    assert payload["install_state"] == "fresh"


def test_doctor_request_id_echoed(isolated_home):
    result = _run(
        ["events", "doctor", "--json", "--request-id", "rq-cli-123"],
        isolated_home,
    )
    assert result.returncode == 0
    payload = json.loads(result.stdout)
    assert payload["_meta"]["request_id"] == "rq-cli-123"


def test_features_returns_zero(isolated_home):
    result = _run(["events", "features", "--json"], isolated_home)
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["events_features_schema_version"] == "1.0"


def test_docs_missing_topic_returns_two(isolated_home):
    result = _run(["events", "docs", "--json"], isolated_home)
    assert result.returncode == 2
    # Error envelope is written to stderr per envelope.emit_error default.
    payload = json.loads(result.stderr)
    assert payload["error"]["kind"] == "usage_error"


def test_docs_unknown_topic_returns_nine(isolated_home):
    result = _run(["events", "docs", "bogus", "--json"], isolated_home)
    assert result.returncode == 9
    payload = json.loads(result.stderr)
    assert payload["error"]["kind"] == "topic_unknown"


def test_docs_known_topic_markdown(isolated_home):
    result = _run(["events", "docs", "guide"], isolated_home)
    assert result.returncode == 0, result.stderr
    assert result.stdout.startswith("# ")


def test_docs_known_topic_json(isolated_home):
    result = _run(["events", "docs", "schemas", "--json"], isolated_home)
    assert result.returncode == 0
    payload = json.loads(result.stdout)
    assert payload["topic"] == "schemas"
    assert isinstance(payload["schemas"], list)
    assert len(payload["schemas"]) > 0
