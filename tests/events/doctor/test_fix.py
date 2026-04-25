"""``events doctor --fix`` round-trip + structural-drift refusal."""

from __future__ import annotations

from pathlib import Path

import pytest

from clawjournal.events.capabilities import effective_matrix
from clawjournal.events.doctor import overlay as overlay_mod
from clawjournal.events.doctor import probes
from clawjournal.events.doctor.overlay import fix_additive_drift
from clawjournal.events.schema import ensure_schema as ensure_events_schema
from clawjournal.workbench.index import open_index


@pytest.fixture(autouse=True)
def _isolated_home(monkeypatch, tmp_path):
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path))
    monkeypatch.setattr(
        "clawjournal.workbench.index.INDEX_DB", tmp_path / ".clawjournal" / "index.db"
    )
    monkeypatch.setattr(
        "clawjournal.workbench.index.CONFIG_DIR", tmp_path / ".clawjournal"
    )
    monkeypatch.setattr("clawjournal.config.CONFIG_DIR", tmp_path / ".clawjournal")
    overlay_mod.reset_cache()
    yield
    overlay_mod.reset_cache()


def _seed_event(client: str, event_type: str, *, client_version: str = "1.0.0") -> None:
    conn = open_index()
    try:
        ensure_events_schema(conn)
        conn.execute(
            "INSERT INTO event_sessions (session_key, client, client_version, "
            "started_at, status) VALUES (?, ?, ?, ?, ?)",
            (
                f"{client}:test:{event_type}",
                client,
                client_version,
                "2026-01-01T00:00:00Z",
                "ended",
            ),
        )
        sid = conn.execute(
            "SELECT id FROM event_sessions WHERE session_key=?",
            (f"{client}:test:{event_type}",),
        ).fetchone()[0]
        conn.execute(
            "INSERT INTO events (session_id, ingested_at, source, source_path, "
            "source_offset, seq, type, raw_json, event_at, confidence, lossiness, client) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                sid,
                "2026-01-01T00:00:00Z",
                f"{client}-jsonl",
                f"/tmp/{event_type}.jsonl",
                0,
                0,
                event_type,
                "{}",
                "2026-01-01T00:00:00Z",
                "high",
                "none",
                client,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def test_fix_writes_overlay_for_additive_drift(monkeypatch, tmp_path):
    # `claude` emitting `compaction` is additive drift: the type is in
    # EVENT_TYPES, but the shipped matrix has supported:false for claude.
    _seed_event("claude", "compaction")
    report = probes.collect()

    result = fix_additive_drift(report)
    assert result["no_op"] is False
    assert result["skipped_structural"] == []
    assert len(result["added"]) == 1
    entry = result["added"][0]
    assert entry["client"] == "claude"
    assert entry["event_type"] == "compaction"
    assert entry["supported"] is True

    # Overlay file exists at the canonical path.
    assert overlay_mod.overlay_path().exists()

    # After write, effective_matrix() reflects the change.
    overlay_mod.reset_cache()
    matrix = effective_matrix()
    assert matrix[("claude", "compaction")][0] is True


def test_fix_no_op_on_clean_install(monkeypatch, tmp_path):
    cfg = tmp_path / ".clawjournal"
    cfg.mkdir(parents=True)
    conn = open_index()
    try:
        ensure_events_schema(conn)
    finally:
        conn.close()

    report = probes.collect()
    result = fix_additive_drift(report)
    assert result["no_op"] is True
    assert result["added"] == []
    # Overlay file is NOT created when there's nothing to write.
    assert not overlay_mod.overlay_path().exists()


def test_fix_refuses_structural_drift(monkeypatch, tmp_path):
    # `tool_call_v2` is not in EVENT_TYPES — structural drift.
    _seed_event("claude", "tool_call_v2")
    report = probes.collect()

    result = fix_additive_drift(report)
    # Structural drift is reported but NOT auto-patched.
    assert result["no_op"] is True
    assert result["added"] == []
    assert len(result["skipped_structural"]) == 1
    skipped = result["skipped_structural"][0]
    assert skipped["client"] == "claude"
    assert skipped["event_type"] == "tool_call_v2"


def test_fix_idempotent(monkeypatch, tmp_path):
    _seed_event("claude", "compaction")
    report = probes.collect()

    first = fix_additive_drift(report)
    overlay_mod.reset_cache()
    second_report = probes.collect()
    second = fix_additive_drift(second_report)

    assert len(first["added"]) == 1
    # After the first --fix, the matrix shows supported:true for the
    # entry, so the second pass detects no additive drift.
    assert second["no_op"] is True
    assert second["added"] == []
