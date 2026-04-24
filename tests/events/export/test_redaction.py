"""Redaction surface coverage for the bundle exporter.

The plan promises: anonymizer covers raw_ref source paths, secrets in
event payloads get redacted, source_snippets keys use the post-
anonymization path so they line up with raw_ref triples, and the
get_effective_share_settings knobs (custom_strings, extra_usernames,
allowlist) flow through.
"""

from __future__ import annotations

import json

import pytest

from clawjournal.events.export import export_session_bundle

from ._helpers import (
    PERMISSIVE_CONFIG,
    insert_workbench_session,
    insert_event,
    insert_event_session,
    make_conn,
)


@pytest.fixture
def patched_anonymizer(monkeypatch):
    """Force a known username so anonymizer behavior is deterministic."""
    monkeypatch.setattr(
        "clawjournal.redaction.anonymizer._detect_home_dir",
        lambda: ("/Users/testuser", "testuser"),
    )


def _bundle(conn, key, tmp_path, monkeypatch, **kwargs):
    monkeypatch.setattr("clawjournal.config.CONFIG_DIR", tmp_path / ".clawjournal")
    summary = export_session_bundle(
        conn,
        key,
        config=PERMISSIVE_CONFIG,
        allow_no_workbench_row=True,
        skip_global_gates=True,
        **kwargs,
    )
    return json.loads(summary.bundle_path.read_text(encoding="utf-8")), summary


def test_anonymizer_strips_home_paths_from_raw_ref(
    tmp_path, monkeypatch, patched_anonymizer
):
    conn = make_conn()
    sid = insert_event_session(conn, session_key="claude:p:s")
    insert_event(
        conn,
        session_id=sid,
        event_type="user_message",
        source_path="/Users/testuser/.claude/projects/-p/s.jsonl",
        source_offset=0,
        seq=0,
        raw_json={"text": "hi"},
    )

    bundle, _ = _bundle(conn, "claude:p:s", tmp_path, monkeypatch)

    raw_ref = bundle["events"][0]["raw_ref"]
    # raw_ref is [source, source_path, source_offset, seq]
    assert "/Users/testuser" not in raw_ref[1], (
        f"raw_ref source_path leaks home dir: {raw_ref[1]!r}"
    )
    assert raw_ref[1].startswith("[REDACTED_PATH_")
    assert raw_ref[1].endswith("]")


def test_anonymizer_strips_home_paths_from_raw_json(
    tmp_path, monkeypatch, patched_anonymizer
):
    conn = make_conn()
    sid = insert_event_session(conn, session_key="claude:p:s")
    insert_event(
        conn,
        session_id=sid,
        event_type="tool_call",
        source_path="/tmp/src.jsonl",
        raw_json={
            "tool": "Read",
            "input": {"file_path": "/Users/testuser/secret/file.py"},
        },
    )

    bundle, _ = _bundle(conn, "claude:p:s", tmp_path, monkeypatch)

    raw_json_text = bundle["events"][0]["raw_json"]
    assert "/Users/testuser" not in raw_json_text
    assert "[REDACTED_PATH]" in raw_json_text


def test_snippet_keys_use_anonymized_source_path(
    tmp_path, monkeypatch, patched_anonymizer
):
    """A snippet's lookup key has to line up with the events' raw_ref triples
    after anonymization, otherwise off-machine inspect can't find them."""
    conn = make_conn()
    sid = insert_event_session(conn, session_key="claude:p:s")

    real_jsonl = tmp_path / "real.jsonl"
    real_jsonl.write_text('{"hello": "world"}\n', encoding="utf-8")

    insert_event(
        conn,
        session_id=sid,
        event_type="user_message",
        source_path=str(real_jsonl),
        source_offset=0,
        seq=0,
        raw_json={"text": "hi"},
    )

    bundle, _ = _bundle(conn, "claude:p:s", tmp_path, monkeypatch)

    snippets = bundle["source_snippets"]
    raw_ref = bundle["events"][0]["raw_ref"]
    # snippet key is "<source>:<source_path>:<offset>:<seq>" — full raw_ref
    expected_key = f"{raw_ref[0]}:{raw_ref[1]}:{raw_ref[2]}:{raw_ref[3]}"
    assert expected_key in snippets, (
        f"snippet key {expected_key!r} not in {list(snippets.keys())!r}"
    )


def test_secrets_in_raw_json_are_redacted(tmp_path, monkeypatch, patched_anonymizer):
    """A real secret in raw_json is replaced before the bundle hits disk."""
    conn = make_conn()
    sid = insert_event_session(conn, session_key="claude:p:s")
    fake_jwt = (
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
        "eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIn0_"
        "abc123abc123abc123"
    )
    insert_event(
        conn,
        session_id=sid,
        event_type="user_message",
        source_path="/tmp/x.jsonl",
        raw_json={"text": f"Bearer {fake_jwt}"},
    )

    bundle, _ = _bundle(conn, "claude:p:s", tmp_path, monkeypatch)

    raw_json_text = bundle["events"][0]["raw_json"]
    assert fake_jwt not in raw_json_text


def test_custom_strings_setting_flows_through(
    tmp_path, monkeypatch, patched_anonymizer
):
    conn = make_conn()
    sid = insert_event_session(conn, session_key="claude:p:s")
    insert_event(
        conn,
        session_id=sid,
        event_type="user_message",
        source_path="/tmp/x.jsonl",
        raw_json={"text": "ACME-INTERNAL-PROJECT-NAME-X"},
    )

    monkeypatch.setattr("clawjournal.config.CONFIG_DIR", tmp_path / ".clawjournal")
    summary = export_session_bundle(
        conn,
        "claude:p:s",
        config=PERMISSIVE_CONFIG,
        settings={
            "custom_strings": ["ACME-INTERNAL-PROJECT-NAME-X"],
            "extra_usernames": [],
            "allowlist_entries": [],
            "excluded_projects": [],
            "blocked_domains": [],
        },
        allow_no_workbench_row=True,
        skip_global_gates=True,
    )
    bundle = json.loads(summary.bundle_path.read_text(encoding="utf-8"))
    assert "ACME-INTERNAL-PROJECT-NAME-X" not in bundle["events"][0]["raw_json"]


def _insert_finding_decision(
    conn,
    *,
    session_id: str,
    entity_text: str,
    status: str,
    entity_type: str = "email",
) -> None:
    from clawjournal.findings import hash_entity

    conn.execute(
        "INSERT INTO findings ("
        "finding_id, session_id, engine, rule, entity_type, entity_hash, "
        "entity_length, field, message_index, tool_field, offset, length, "
        "confidence, status, decided_by, decision_source_id, decided_at, "
        "decision_reason, revision, created_at"
        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            f"finding-{status}-{entity_text}",
            session_id,
            "regex_pii",
            entity_type,
            entity_type,
            hash_entity(entity_text),
            len(entity_text),
            "content",
            0,
            None,
            0,
            len(entity_text),
            0.95,
            status,
            "user",
            None,
            "2026-04-22T09:00:00Z",
            None,
            "v1:test",
            "2026-04-22T09:00:00Z",
        ),
    )
    conn.commit()


def test_findings_decisions_are_honored_in_raw_json(
    tmp_path, monkeypatch, patched_anonymizer
):
    from clawjournal.findings import reset_salt_cache

    monkeypatch.setattr(
        "clawjournal.workbench.index.INDEX_DB",
        tmp_path / ".clawjournal" / "index.db",
    )
    reset_salt_cache()

    conn = make_conn()
    sid = insert_event_session(conn, session_key="claude:p:s")
    insert_workbench_session(
        conn,
        session_id="wb-1",
        session_key="claude:p:s",
    )
    accepted = "alice@example.com"
    ignored = "bob@example.com"
    insert_event(
        conn,
        session_id=sid,
        event_type="user_message",
        source_path="/tmp/x.jsonl",
        raw_json={"text": f"Contact {accepted} and {ignored}"},
    )
    _insert_finding_decision(
        conn,
        session_id="wb-1",
        entity_text=accepted,
        status="accepted",
    )
    _insert_finding_decision(
        conn,
        session_id="wb-1",
        entity_text=ignored,
        status="ignored",
    )

    bundle, _ = _bundle(conn, "claude:p:s", tmp_path, monkeypatch)
    raw_json_text = bundle["events"][0]["raw_json"]

    assert accepted not in raw_json_text
    assert "[REDACTED_EMAIL]" in raw_json_text
    assert ignored in raw_json_text


def test_blocked_domains_redact_raw_json_and_snippets(
    tmp_path, monkeypatch, patched_anonymizer
):
    conn = make_conn()
    sid = insert_event_session(conn, session_key="claude:p:s")
    source_file = tmp_path / "domain.jsonl"
    source_file.write_text(
        '{"url": "https://api.internal.test/v1"}\n',
        encoding="utf-8",
    )
    insert_event(
        conn,
        session_id=sid,
        event_type="tool_call",
        source_path=str(source_file),
        source_offset=0,
        seq=0,
        raw_json={"url": "https://api.internal.test/v1"},
    )

    monkeypatch.setattr("clawjournal.config.CONFIG_DIR", tmp_path / ".clawjournal")
    summary = export_session_bundle(
        conn,
        "claude:p:s",
        config=PERMISSIVE_CONFIG,
        settings={
            "custom_strings": [],
            "extra_usernames": [],
            "allowlist_entries": [],
            "excluded_projects": [],
            "blocked_domains": ["*.internal.test"],
        },
        allow_no_workbench_row=True,
        skip_global_gates=True,
    )
    bundle = json.loads(summary.bundle_path.read_text(encoding="utf-8"))

    raw_json_text = bundle["events"][0]["raw_json"]
    snippet_text = next(iter(bundle["source_snippets"].values()))
    assert "api.internal.test" not in raw_json_text
    assert "api.internal.test" not in snippet_text
    assert "[REDACTED_DOMAIN]" in raw_json_text
    assert "[REDACTED_DOMAIN]" in snippet_text


def test_manifest_contains_redaction_summary(
    tmp_path, monkeypatch, patched_anonymizer
):
    conn = make_conn()
    sid = insert_event_session(conn, session_key="claude:p:s")
    fake_jwt = (
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
        "eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIn0_"
        "xyz789xyz789xyz789"
    )
    insert_event(
        conn,
        session_id=sid,
        event_type="user_message",
        source_path="/Users/testuser/x.jsonl",
        raw_json={"text": f"hello {fake_jwt}"},
    )

    bundle, summary = _bundle(conn, "claude:p:s", tmp_path, monkeypatch)

    summary_dict = bundle["manifest"]["redaction_summary"]
    assert summary_dict["total"] >= 1
    assert any(k in summary_dict["by_type"] for k in ("jwt", "jwt_partial"))
