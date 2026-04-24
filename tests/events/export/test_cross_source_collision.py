"""Regression: two events with the same (source_path, offset, seq) but
different `source` must round-trip without cross-binding.

This was a critical bug pre-fix: the importer's raw_ref → events.id map
was keyed on the 3-tuple (path, offset, seq), so when two sources had
colliding triples the second overwrote the first. token_usage,
incidents, and cost_anomalies referencing one would silently bind to
the other.

Fixed by extending raw_ref to a 4-tuple (source, path, offset, seq)
matching events.UNIQUE.
"""

from __future__ import annotations

from clawjournal.events.export import (
    export_session_bundle,
    import_session_bundle,
)

from ._helpers import (
    PERMISSIVE_CONFIG,
    insert_cost_anomaly,
    insert_event,
    insert_event_session,
    insert_token_usage,
    make_conn,
)


def test_colliding_raw_ref_across_sources_does_not_mis_bind(tmp_path, monkeypatch):
    src = make_conn()
    sid = insert_event_session(src, session_key="claude:p:s")

    # Two events with identical (source_path, source_offset, seq) but
    # different `source`. This is a valid state per events.UNIQUE.
    e1 = insert_event(
        src,
        session_id=sid,
        event_type="user_message",
        source="claude-jsonl",
        source_path="/tmp/shared.jsonl",
        source_offset=0,
        seq=0,
        raw_json={"id": 1, "via": "native"},
    )
    e2 = insert_event(
        src,
        session_id=sid,
        event_type="user_message",
        source="codex-rollout",
        source_path="/tmp/shared.jsonl",
        source_offset=0,
        seq=0,
        raw_json={"id": 2, "via": "codex"},
    )

    # Token usage on e1 only — if the importer mis-binds, the row would
    # land on e2 instead and we'd see different model fields, etc.
    insert_token_usage(src, event_id=e1, session_id=sid, model="claude-sonnet-4")

    monkeypatch.setattr("clawjournal.config.CONFIG_DIR", tmp_path / ".clawjournal")
    summary = export_session_bundle(
        src,
        "claude:p:s",
        config=PERMISSIVE_CONFIG,
        allow_no_workbench_row=True,
        skip_global_gates=True,
    )

    dst = make_conn()
    import_session_bundle(dst, summary.bundle_path)

    # Both events should land on the importing side, distinguishable by source.
    rows = list(
        dst.execute(
            "SELECT id, source, raw_json FROM events ORDER BY source"
        )
    )
    assert len(rows) == 2
    sources = [r["source"] for r in rows]
    assert sources == ["claude-jsonl", "codex-rollout"]

    # token_usage row should reference the claude-jsonl event, not the codex one.
    tu_rows = list(
        dst.execute(
            "SELECT tu.event_id, e.source FROM token_usage tu "
            "JOIN events e ON e.id = tu.event_id"
        )
    )
    assert len(tu_rows) == 1
    assert tu_rows[0]["source"] == "claude-jsonl", (
        f"token_usage mis-bound to {tu_rows[0]['source']!r} instead of claude-jsonl"
    )


def test_redacted_home_paths_keep_per_file_identity(tmp_path, monkeypatch):
    """Distinct home-directory source files must not collapse to one
    bundle raw_ref identity after path redaction."""
    monkeypatch.setattr(
        "clawjournal.redaction.anonymizer._detect_home_dir",
        lambda: ("/Users/testuser", "testuser"),
    )

    src = make_conn()
    parent_sid = insert_event_session(src, session_key="claude:p:parent")
    child_sid = insert_event_session(
        src,
        session_key="claude:p:child",
        parent_session_key="claude:p:parent",
    )
    e1 = insert_event(
        src,
        session_id=parent_sid,
        event_type="user_message",
        source="claude-jsonl",
        source_path="/Users/testuser/.claude/projects/-p/parent.jsonl",
        source_offset=0,
        seq=0,
        raw_json={"id": "parent"},
    )
    e2 = insert_event(
        src,
        session_id=child_sid,
        event_type="user_message",
        source="claude-jsonl",
        source_path="/Users/testuser/.claude/projects/-p/child.jsonl",
        source_offset=0,
        seq=0,
        raw_json={"id": "child"},
    )
    insert_token_usage(src, event_id=e1, session_id=parent_sid, model="parent-model")
    insert_token_usage(src, event_id=e2, session_id=child_sid, model="child-model")

    monkeypatch.setattr("clawjournal.config.CONFIG_DIR", tmp_path / ".clawjournal")
    summary = export_session_bundle(
        src,
        "claude:p:parent",
        config=PERMISSIVE_CONFIG,
        allow_no_workbench_row=True,
        skip_global_gates=True,
    )

    import json

    bundle = json.loads(summary.bundle_path.read_text(encoding="utf-8"))
    raw_paths = [event["raw_ref"][1] for event in bundle["events"]]
    assert len(raw_paths) == 2
    assert len(set(raw_paths)) == 2
    assert all(path.startswith("[REDACTED_PATH_") for path in raw_paths)
    assert all("/Users/testuser" not in path for path in raw_paths)

    dst = make_conn()
    import_session_bundle(dst, summary.bundle_path)

    assert dst.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 2
    assert dst.execute("SELECT COUNT(*) FROM token_usage").fetchone()[0] == 2


def test_pre_existing_local_event_does_not_pollute_bind(tmp_path, monkeypatch):
    """If the importing DB has a pre-existing events row with the same
    raw_ref under a DIFFERENT session, the importer's map must not see
    that row — otherwise cross-references could bind to unrelated events.

    Note: events.UNIQUE is global (not per-session), so the imported
    event itself gets skipped by INSERT OR IGNORE in this scenario — a
    known-edge limitation of the v0.1 schema (two unrelated sessions
    sharing the same vendor file path + byte offset is unusual but
    possible). The test below pins the desired property: the importer's
    scoped map prevents wrong-session binding even when no imported
    event lands.
    """
    src = make_conn()
    sid = insert_event_session(src, session_key="claude:p:s1")
    e1 = insert_event(
        src,
        session_id=sid,
        event_type="user_message",
        source="claude-jsonl",
        source_path="/tmp/shared.jsonl",
        source_offset=0,
        seq=0,
        raw_json={"v": "src"},
    )
    insert_token_usage(src, event_id=e1, session_id=sid, model="claude-sonnet-4")
    insert_cost_anomaly(src, session_id=sid, turn_event_id=e1)

    monkeypatch.setattr("clawjournal.config.CONFIG_DIR", tmp_path / ".clawjournal")
    summary = export_session_bundle(
        src,
        "claude:p:s1",
        config=PERMISSIVE_CONFIG,
        allow_no_workbench_row=True,
        skip_global_gates=True,
    )

    dst = make_conn()
    other_sid = insert_event_session(dst, session_key="other:session:x")
    insert_event(
        dst,
        session_id=other_sid,
        event_type="user_message",
        source="claude-jsonl",
        source_path="/tmp/shared.jsonl",
        source_offset=0,
        seq=0,
        raw_json={"v": "preexisting"},
    )

    import_summary = import_session_bundle(dst, summary.bundle_path)

    # The pre-existing event still belongs to its original session.
    other_event = dst.execute(
        "SELECT session_id FROM events WHERE source_path = '/tmp/shared.jsonl'"
    ).fetchone()
    assert other_event["session_id"] == other_sid

    # No token_usage row should be bound to the pre-existing event —
    # the scoped map prevents wrong-session pollution. (The bundle's
    # token_usage / cost_anomaly rows are dropped because the imported
    # event was skipped by INSERT OR IGNORE; the importer must surface
    # that as an unresolved-reference count rather than dropping silently.)
    polluted = dst.execute(
        "SELECT COUNT(*) FROM token_usage WHERE session_id = ?",
        (other_sid,),
    ).fetchone()[0]
    assert polluted == 0, (
        f"token_usage leaked into other session_id={other_sid}; "
        "the importer's raw_ref → events.id map must scope to imported sessions"
    )

    assert dst.execute("SELECT COUNT(*) FROM cost_anomalies").fetchone()[0] == 0

    assert import_summary.token_usage_skipped_unresolved == 1
    assert import_summary.cost_anomalies_skipped_unresolved == 1

    import_session_bundle(dst, summary.bundle_path)
    assert dst.execute("SELECT COUNT(*) FROM cost_anomalies").fetchone()[0] == 0
