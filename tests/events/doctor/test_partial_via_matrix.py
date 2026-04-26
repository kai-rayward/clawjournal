"""Partial-compatibility verdict via matrix `supported: false` (not via
schema_unknown rows). Covers the second branch of probes._verdict that
test_probes only exercises through schema_unknown.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from clawjournal.events.doctor import overlay as overlay_mod
from clawjournal.events.doctor import probes
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


def test_partial_verdict_lists_unsupported_event_types(monkeypatch, tmp_path):
    # `claude` emitting `compaction` is in EVENT_TYPES but the shipped
    # matrix has supported:false for claude. Verdict: partial.
    cfg = tmp_path / ".clawjournal"
    cfg.mkdir(parents=True)
    conn = open_index()
    try:
        ensure_events_schema(conn)
        conn.execute(
            "INSERT INTO event_sessions (session_key, client, client_version, "
            "started_at, status) VALUES (?, ?, ?, ?, ?)",
            ("claude:test:abc", "claude", "1.43.0", "2026-01-01T00:00:00Z", "ended"),
        )
        sid = conn.execute("SELECT id FROM event_sessions").fetchone()[0]
        conn.execute(
            "INSERT INTO events (session_id, ingested_at, source, source_path, "
            "source_offset, seq, type, raw_json, event_at, confidence, lossiness, client) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                sid,
                "2026-01-01T00:00:00Z",
                "claude-jsonl",
                "/tmp/x.jsonl",
                0,
                0,
                "compaction",
                "{}",
                "2026-01-01T00:00:00Z",
                "high",
                "none",
                "claude",
            ),
        )
        conn.commit()
    finally:
        conn.close()

    report = probes.collect()
    assert len(report.clients) == 1
    obs = report.clients[0]
    assert obs.verdict == probes.VERDICT_PARTIAL
    assert "compaction" in obs.unsupported_event_types
    assert obs.schema_unknown_rows == 0
    assert probes.exit_code_for(report) == 1


def test_render_human_lists_unsupported_types(monkeypatch, tmp_path):
    """The render layer surfaces unsupported_event_types in human output —
    fixing the regression where _unsupported_event_types only reported
    schema_unknown."""

    from clawjournal.events.doctor.render import render_human
    from clawjournal.events.doctor.probes import (
        ClientObservation,
        DoctorReport,
        INSTALL_HEALTHY,
        TruffleHogStatus,
        VERDICT_PARTIAL,
    )

    obs = ClientObservation(
        client="claude",
        client_version="1.43.0",
        sessions_count=1,
        event_types_observed=["user_message", "compaction"],
        unknown_event_types=[],
        unsupported_event_types=["compaction"],
        schema_unknown_rows=0,
        matrix_supported_count=11,
        verdict=VERDICT_PARTIAL,
    )
    report = DoctorReport(
        install_state=INSTALL_HEALTHY,
        install_hint="ok",
        clawjournal_version="0.0.2",
        bundle_schema_version="1.0",
        recorder_schema_version="1.0",
        security_schema_version=2,
        config_dir="/x/.clawjournal",
        index_db_path="/x/.clawjournal/index.db",
        events_count=1,
        sessions_count=1,
        trufflehog=TruffleHogStatus(state="missing", version=None),
        clients=[obs],
    )
    text = render_human(report)
    assert "compaction" in text
    assert "not supported in matrix" in text
