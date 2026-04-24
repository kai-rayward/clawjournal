"""Subagent children round-trip with their parent.

Per ADR-001 amendment + plan 07: subagent sessions travel with the parent
(default `--no-children` is False). Children's events / overrides /
token_usage / cost_anomalies / incidents are interleaved into the same
top-level arrays, distinguished by `session_key` per row.
"""

from __future__ import annotations

import json

import pytest

from clawjournal.events.export import (
    export_session_bundle,
    import_session_bundle,
)

from ._helpers import (
    PERMISSIVE_CONFIG,
    insert_event,
    insert_event_session,
    make_conn,
)


@pytest.fixture
def parent_with_child():
    conn = make_conn()
    insert_event_session(conn, session_key="claude:p:parent")
    insert_event_session(
        conn,
        session_key="claude:p:child",
        parent_session_key="claude:p:parent",
        started_at="2026-04-22T09:30:00Z",
        ended_at="2026-04-22T09:45:00Z",
    )

    parent_sid = conn.execute(
        "SELECT id FROM event_sessions WHERE session_key = 'claude:p:parent'"
    ).fetchone()["id"]
    child_sid = conn.execute(
        "SELECT id FROM event_sessions WHERE session_key = 'claude:p:child'"
    ).fetchone()["id"]

    insert_event(
        conn,
        session_id=parent_sid,
        event_type="user_message",
        source_path="/tmp/parent.jsonl",
        source_offset=0,
        seq=0,
        raw_json={"text": "parent says hi"},
    )
    insert_event(
        conn,
        session_id=child_sid,
        event_type="tool_call",
        event_key="tool_call:c1",
        source_path="/tmp/child.jsonl",
        source_offset=0,
        seq=0,
        event_at="2026-04-22T09:31:00Z",
        raw_json={"text": "child works"},
    )
    return conn


def test_children_round_trip(parent_with_child, tmp_path, monkeypatch):
    src_conn = parent_with_child
    monkeypatch.setattr("clawjournal.config.CONFIG_DIR", tmp_path / ".clawjournal")

    summary = export_session_bundle(
        src_conn,
        "claude:p:parent",
        config=PERMISSIVE_CONFIG,
        allow_no_workbench_row=True,
        skip_global_gates=True,
    )

    bundle = json.loads(summary.bundle_path.read_text(encoding="utf-8"))
    assert len(bundle["children"]) == 1
    assert bundle["children"][0]["session_key"] == "claude:p:child"
    assert bundle["children"][0]["parent_session_key"] == "claude:p:parent"

    session_keys_in_events = {e["session_key"] for e in bundle["events"]}
    assert session_keys_in_events == {"claude:p:parent", "claude:p:child"}
    assert summary.event_count == 2

    # Round-trip into a fresh DB
    dst_conn = make_conn()
    import_summary = import_session_bundle(dst_conn, summary.bundle_path)
    assert import_summary.events_inserted == 2
    assert set(import_summary.session_keys) == {"claude:p:parent", "claude:p:child"}

    # Both sessions exist on the importing side
    rows = dst_conn.execute(
        "SELECT session_key, parent_session_key, parent_session_id "
        "FROM event_sessions ORDER BY session_key"
    ).fetchall()
    assert len(rows) == 2
    by_key = {r["session_key"]: r for r in rows}
    parent_id = by_key["claude:p:parent"]["session_id"] if False else None
    parent_row = dst_conn.execute(
        "SELECT id FROM event_sessions WHERE session_key = 'claude:p:parent'"
    ).fetchone()
    assert by_key["claude:p:child"]["parent_session_key"] == "claude:p:parent"
    # parent_session_id should resolve since the parent was upserted first
    assert by_key["claude:p:child"]["parent_session_id"] == parent_row["id"]


def test_no_children_excludes_subagents(parent_with_child, tmp_path, monkeypatch):
    src_conn = parent_with_child
    monkeypatch.setattr("clawjournal.config.CONFIG_DIR", tmp_path / ".clawjournal")

    summary = export_session_bundle(
        src_conn,
        "claude:p:parent",
        config=PERMISSIVE_CONFIG,
        allow_no_workbench_row=True,
        include_children=False,
        skip_global_gates=True,
    )
    bundle = json.loads(summary.bundle_path.read_text(encoding="utf-8"))
    assert bundle["children"] == []
    assert all(e["session_key"] == "claude:p:parent" for e in bundle["events"])
