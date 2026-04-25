"""Structured-error-envelope tests + closed kind enum + getpass-absence."""

from __future__ import annotations

import io
import json
from pathlib import Path

import pytest

from clawjournal.events.doctor import envelope
from clawjournal.events.doctor.envelope import (
    KIND_INDEX_MISSING,
    KIND_TOPIC_UNKNOWN,
    KIND_UNSPECIFIED,
    KIND_USAGE_ERROR,
    KIND_VERSION_INCOMPATIBLE,
    VALID_KINDS,
    emit_error,
)


def test_kind_enum_is_closed():
    assert VALID_KINDS == frozenset(
        {
            KIND_INDEX_MISSING,
            KIND_VERSION_INCOMPATIBLE,
            KIND_USAGE_ERROR,
            KIND_TOPIC_UNKNOWN,
            KIND_UNSPECIFIED,
        }
    )


def test_emit_error_returns_code():
    buf = io.StringIO()
    code = emit_error(
        code=3,
        kind=KIND_INDEX_MISSING,
        message="m",
        hint="h",
        json_mode=True,
        stream=buf,
    )
    assert code == 3


def test_emit_error_json_envelope_shape():
    buf = io.StringIO()
    emit_error(
        code=3,
        kind=KIND_INDEX_MISSING,
        message="Index missing",
        hint="run scan",
        request_id="rq-1",
        json_mode=True,
        stream=buf,
    )
    payload = json.loads(buf.getvalue())
    err = payload["error"]
    assert err["code"] == 3
    assert err["kind"] == KIND_INDEX_MISSING
    assert err["message"] == "Index missing"
    assert err["hint"] == "run scan"
    assert err["retryable"] is False
    assert err["_meta"]["request_id"] == "rq-1"


def test_emit_error_anonymizes_path(monkeypatch):
    monkeypatch.setattr(
        "clawjournal.redaction.anonymizer._detect_home_dir",
        lambda: ("/Users/synthetic-user", "synthetic-user"),
    )
    buf = io.StringIO()
    emit_error(
        code=3,
        kind=KIND_INDEX_MISSING,
        message="Cannot read /Users/synthetic-user/.clawjournal/index.db",
        hint="run scan",
        json_mode=True,
        stream=buf,
    )
    text = buf.getvalue()
    assert "synthetic-user" not in text
    assert "[REDACTED_PATH]" in text


def test_emit_error_unknown_kind_falls_back_to_unspecified():
    buf = io.StringIO()
    emit_error(
        code=9,
        kind="bogus_kind_not_in_enum",
        message="x",
        json_mode=True,
        stream=buf,
    )
    payload = json.loads(buf.getvalue())
    assert payload["error"]["kind"] == KIND_UNSPECIFIED


def test_doctor_module_does_not_call_getpass():
    """Privacy contract: doctor's success path must never call
    ``getpass.getuser()``. The username only enters via the anonymizer
    (which doctor doesn't run on success output)."""

    pkg = Path(__file__).parent.parent.parent.parent / "clawjournal" / "events" / "doctor"
    offenders: list[str] = []
    for py in pkg.glob("*.py"):
        text = py.read_text(encoding="utf-8")
        if "getpass" in text:
            offenders.append(py.name)
    # ``envelope.py`` and ``probes.py`` are doctor's success-path code;
    # both must be clean. Anonymizer (in clawjournal/redaction/) calls
    # _detect_home_dir which itself does not import getpass — that's
    # fine; the rule is doctor-package-local.
    assert offenders == [], f"getpass found in doctor modules: {offenders}"
