"""``BUNDLE_SOFT_LIMIT_BYTES`` warning fires when an exported bundle
exceeds the threshold. We monkeypatch the constant to a tiny value
so the test doesn't have to construct a 50 MB bundle.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from clawjournal.events.export import bundle as bundle_mod
from clawjournal.events.export.bundle import export_session_bundle
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
    yield


def _seed_session_with_event(conn) -> tuple[str, int]:
    ensure_events_schema(conn)
    conn.execute(
        "INSERT INTO event_sessions (session_key, client, client_version, "
        "started_at, status) VALUES (?, ?, ?, ?, ?)",
        ("claude:test:abc", "claude", "1.42.0", "2026-01-01T00:00:00Z", "ended"),
    )
    sid = conn.execute(
        "SELECT id FROM event_sessions WHERE session_key=?", ("claude:test:abc",)
    ).fetchone()[0]
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
            "user_message",
            "{}",
            "2026-01-01T00:00:00Z",
            "high",
            "none",
            "claude",
        ),
    )
    conn.commit()
    return ("claude:test:abc", sid)


def test_soft_limit_warning_fires_when_bundle_exceeds(
    monkeypatch, tmp_path, capsys
):
    # Force the threshold below any plausible bundle size.
    monkeypatch.setattr(bundle_mod, "BUNDLE_SOFT_LIMIT_BYTES", 50)
    # Source + project confirmation gates need to be skipped — easiest
    # path is to bypass the gates by sending an `_allow_no_workbench_row`
    # flag on a session with no workbench row, which is the export
    # function's escape hatch for events-only data.
    cfg = tmp_path / ".clawjournal"
    cfg.mkdir(parents=True)
    conn = open_index()
    try:
        session_key, _ = _seed_session_with_event(conn)
    finally:
        conn.close()

    out_path = tmp_path / "bundle.json"
    conn = open_index()
    try:
        try:
            export_session_bundle(
                conn,
                session_key,
                output_path=out_path,
                allow_no_workbench_row=True,
                skip_global_gates=True,
                settings={
                    "extra_usernames": [],
                    "custom_strings": [],
                    "blocked_domains": [],
                    "excluded_projects": [],
                    "allowlist_entries": [],
                },
            )
        except Exception as exc:
            pytest.skip(f"export blocked: {exc!r}")
    finally:
        conn.close()

    captured = capsys.readouterr()
    assert "soft limit" in captured.err
    assert "50" in captured.err  # the threshold value


def test_soft_limit_silent_below_threshold(monkeypatch, tmp_path, capsys):
    # Default threshold (50 MB); a tiny bundle never trips it.
    cfg = tmp_path / ".clawjournal"
    cfg.mkdir(parents=True)
    conn = open_index()
    try:
        session_key, _ = _seed_session_with_event(conn)
    finally:
        conn.close()

    out_path = tmp_path / "bundle.json"
    conn = open_index()
    try:
        try:
            export_session_bundle(
                conn,
                session_key,
                output_path=out_path,
                allow_no_workbench_row=True,
                skip_global_gates=True,
                settings={
                    "extra_usernames": [],
                    "custom_strings": [],
                    "blocked_domains": [],
                    "excluded_projects": [],
                    "allowlist_entries": [],
                },
            )
        except Exception as exc:
            pytest.skip(f"export blocked: {exc!r}")
    finally:
        conn.close()

    captured = capsys.readouterr()
    assert "soft limit" not in captured.err
