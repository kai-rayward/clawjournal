"""Tier-1 invariant tests for the execution recorder.

These tests pin the plan's core guarantees independently of classifier
branch coverage: raw-JSON round-trip, classifier ↔ capability-matrix
agreement, summary accuracy vs. the DB, monotonic session upsert,
multi-batch append idempotency, and byte-accurate per-line offsets
under multibyte UTF-8.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

from clawjournal.events.capabilities import CAPABILITY_MATRIX
from clawjournal.events.classify import classify_line
from clawjournal.events.ingest import ingest_pending
from clawjournal.workbench.index import open_index

from ._fixtures import ALL_CORPORA


# Timestamps chosen so input == normalized output (no trailing zero
# fractions to be stripped by datetime.isoformat).
TS0 = "2026-04-20T10:00:00Z"
TS1 = "2026-04-20T10:00:01Z"
TS2 = "2026-04-20T10:00:02Z"
TS3 = "2026-04-20T10:00:03Z"


def _patch_db(monkeypatch, tmp_path):
    monkeypatch.setattr("clawjournal.workbench.index.INDEX_DB", tmp_path / "index.db")
    monkeypatch.setattr("clawjournal.workbench.index.CONFIG_DIR", tmp_path / "config")
    monkeypatch.setattr("clawjournal.config.CONFIG_DIR", tmp_path / "config")


def _patch_sources(monkeypatch, tmp_path):
    from clawjournal.parsing import parser

    monkeypatch.setattr(parser, "PROJECTS_DIR", tmp_path / "claude" / "projects")
    monkeypatch.setattr(parser, "CODEX_SESSIONS_DIR", tmp_path / "codex" / "sessions")
    monkeypatch.setattr(
        parser, "CODEX_ARCHIVED_DIR", tmp_path / "codex" / "archived_sessions"
    )
    monkeypatch.setattr(parser, "LOCAL_AGENT_DIR", tmp_path / "local_agent")
    monkeypatch.setattr(parser, "OPENCLAW_AGENTS_DIR", tmp_path / "openclaw" / "agents")


def _write_claude_session(tmp_path, project, name, lines):
    path = tmp_path / "claude" / "projects" / project / f"{name}.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(
        ("\n".join(json.dumps(line) for line in lines) + "\n").encode("utf-8")
    )
    return path


def _append_claude_session(path, lines):
    with path.open("ab") as f:
        f.write(
            ("\n".join(json.dumps(line) for line in lines) + "\n").encode("utf-8")
        )


def _now_from(path):
    return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)


# --------------------------------------------------------------------------- #
# Raw-JSON round-trip
# --------------------------------------------------------------------------- #


def test_raw_json_roundtrip_preserves_vendor_content(monkeypatch, tmp_path):
    """For every parseable line, json.loads(raw_json) must equal the
    original parsed dict — 02 never filters vendor content."""
    _patch_db(monkeypatch, tmp_path)
    _patch_sources(monkeypatch, tmp_path)

    fixtures = [
        {
            "type": "assistant",
            "timestamp": TS0,
            "version": "1.2.3",
            "message": {
                "content": [
                    {"type": "text", "text": "hello"},
                    {
                        "type": "tool_use",
                        "id": "t-1",
                        "name": "Read",
                        "input": {
                            "path": "/a/b/c",
                            "meta": {"deep": {"nested": [1, 2, 3], "flag": True}},
                        },
                    },
                ],
                "extras": {"null_value": None, "false_value": False, "float": 0.5},
            },
        },
        {
            "type": "user",
            "timestamp": TS1,
            "message": {"content": "你好 🙂 مرحبا שלום"},
        },
        {
            "type": "user",
            "timestamp": TS2,
            "message": {"content": [], "tags": {}},
        },
        {
            "type": "user",
            "timestamp": TS3,
            "message": {
                "content": "numbers",
                "big_int": 2**53,
                "neg_float": -1.5e-10,
                "unicode_key_é": "ok",
            },
        },
    ]

    path = _write_claude_session(tmp_path, "demo-project", "roundtrip", fixtures)

    conn = open_index()
    try:
        ingest_pending(conn, source_filter="claude", now=_now_from(path))
        rows = conn.execute(
            """
            SELECT DISTINCT source_offset, raw_json
              FROM events
             ORDER BY source_offset
            """
        ).fetchall()
        assert len(rows) == len(fixtures)
        for row, fixture in zip(rows, fixtures):
            assert json.loads(row["raw_json"]) == fixture
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# Classifier ↔ capability-matrix agreement
# --------------------------------------------------------------------------- #


def test_classifier_never_emits_type_that_matrix_marks_unsupported():
    """Every (client, type) a classifier emits for a realistic vendor
    line must appear in CAPABILITY_MATRIX with supported=True. Pulls
    the full per-client corpora from _fixtures so every new realistic
    fixture automatically widens the drift tripwire. Fixtures tagged
    realistic=False (defensive branches for shapes no vendor emits
    today) are skipped — if future work starts emitting those types,
    flip them to realistic=True and the matrix will need an entry."""
    drift: list[tuple[str, str, str]] = []
    for client, corpus in ALL_CORPORA.items():
        for fixture in corpus:
            if not fixture.realistic:
                continue
            events = classify_line(client, fixture.line)
            for event in events:
                supported, _reason = CAPABILITY_MATRIX.get(
                    (client, event.type), (False, "missing")
                )
                if not supported:
                    drift.append((client, event.type, fixture.name))

    assert not drift, (
        "Classifier emitted types the capability matrix marks unsupported: "
        f"{drift}"
    )


# --------------------------------------------------------------------------- #
# Summary accuracy
# --------------------------------------------------------------------------- #


def test_summary_event_rows_equals_db_count_on_first_ingest(monkeypatch, tmp_path):
    """summary.event_rows must match COUNT(*) from events after a fresh
    ingest — if the two ever diverge, observability into the recorder is
    broken."""
    _patch_db(monkeypatch, tmp_path)
    _patch_sources(monkeypatch, tmp_path)

    fixtures = [
        {
            "type": "assistant",
            "timestamp": TS0,
            "message": {
                "content": [
                    {"type": "text", "text": "calling"},
                    {"type": "tool_use", "id": "t", "name": "Read", "input": {}},
                ]
            },
        },
        {"type": "user", "timestamp": TS1, "message": {"content": "hi"}},
    ]
    path = _write_claude_session(tmp_path, "demo-project", "summary", fixtures)

    conn = open_index()
    try:
        summary = ingest_pending(conn, source_filter="claude", now=_now_from(path))
        db_count = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        assert summary.to_dict()["event_rows"] == db_count
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# Monotonic event_sessions upsert
# --------------------------------------------------------------------------- #


def _select_session(conn, session_key):
    return conn.execute(
        """
        SELECT session_key, client_version, started_at, ended_at, status
          FROM event_sessions WHERE session_key = ?
        """,
        (session_key,),
    ).fetchone()


def test_started_at_only_decreases_across_batches(monkeypatch, tmp_path):
    _patch_db(monkeypatch, tmp_path)
    _patch_sources(monkeypatch, tmp_path)

    path = _write_claude_session(
        tmp_path,
        "demo-project",
        "starts",
        [{"type": "user", "timestamp": TS3, "message": {"content": "late"}}],
    )

    conn = open_index()
    try:
        ingest_pending(conn, source_filter="claude", now=_now_from(path))
        first = _select_session(conn, "claude:demo-project:starts")
        assert first["started_at"] == TS3

        _append_claude_session(
            path,
            [{"type": "user", "timestamp": TS1, "message": {"content": "earlier"}}],
        )
        ingest_pending(conn, source_filter="claude", now=_now_from(path))
        second = _select_session(conn, "claude:demo-project:starts")
        assert second["started_at"] == TS1
        assert second["ended_at"] == TS3  # unchanged — TS1 < TS3
    finally:
        conn.close()


def test_ended_at_only_increases_across_batches(monkeypatch, tmp_path):
    _patch_db(monkeypatch, tmp_path)
    _patch_sources(monkeypatch, tmp_path)

    path = _write_claude_session(
        tmp_path,
        "demo-project",
        "ends",
        [{"type": "user", "timestamp": TS1, "message": {"content": "early"}}],
    )

    conn = open_index()
    try:
        ingest_pending(conn, source_filter="claude", now=_now_from(path))
        first = _select_session(conn, "claude:demo-project:ends")
        assert first["ended_at"] == TS1

        _append_claude_session(
            path,
            [{"type": "user", "timestamp": TS3, "message": {"content": "later"}}],
        )
        ingest_pending(conn, source_filter="claude", now=_now_from(path))
        second = _select_session(conn, "claude:demo-project:ends")
        assert second["ended_at"] == TS3
        assert second["started_at"] == TS1  # unchanged — TS3 > TS1
    finally:
        conn.close()


def test_status_never_downgrades_from_ended(monkeypatch, tmp_path):
    _patch_db(monkeypatch, tmp_path)
    _patch_sources(monkeypatch, tmp_path)

    path = _write_claude_session(
        tmp_path,
        "demo-project",
        "lifecycle",
        [
            {"type": "user", "timestamp": TS0, "message": {"content": "hi"}},
            {"type": "session_close", "timestamp": TS1, "message": {}},
        ],
    )

    conn = open_index()
    try:
        ingest_pending(conn, source_filter="claude", now=_now_from(path))
        assert _select_session(conn, "claude:demo-project:lifecycle")["status"] == "ended"

        _append_claude_session(
            path,
            [{"type": "user", "timestamp": TS2, "message": {"content": "post-close"}}],
        )
        ingest_pending(conn, source_filter="claude", now=_now_from(path))
        assert (
            _select_session(conn, "claude:demo-project:lifecycle")["status"] == "ended"
        )
    finally:
        conn.close()


def test_client_version_first_non_null_wins(monkeypatch, tmp_path):
    _patch_db(monkeypatch, tmp_path)
    _patch_sources(monkeypatch, tmp_path)

    # Batch 1: no version
    path = _write_claude_session(
        tmp_path,
        "demo-project",
        "version",
        [{"type": "user", "timestamp": TS0, "message": {"content": "hi"}}],
    )
    conn = open_index()
    try:
        ingest_pending(conn, source_filter="claude", now=_now_from(path))
        assert (
            _select_session(conn, "claude:demo-project:version")["client_version"]
            is None
        )

        # Batch 2: version appears
        _append_claude_session(
            path,
            [
                {
                    "type": "user",
                    "timestamp": TS1,
                    "version": "1.0.0",
                    "message": {"content": "v1"},
                }
            ],
        )
        ingest_pending(conn, source_filter="claude", now=_now_from(path))
        assert (
            _select_session(conn, "claude:demo-project:version")["client_version"]
            == "1.0.0"
        )

        # Batch 3: different version — must NOT overwrite
        _append_claude_session(
            path,
            [
                {
                    "type": "user",
                    "timestamp": TS2,
                    "version": "2.0.0",
                    "message": {"content": "v2"},
                }
            ],
        )
        ingest_pending(conn, source_filter="claude", now=_now_from(path))
        assert (
            _select_session(conn, "claude:demo-project:version")["client_version"]
            == "1.0.0"
        )
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# Multi-batch append
# --------------------------------------------------------------------------- #


def test_multi_batch_append_produces_no_duplicates(monkeypatch, tmp_path):
    _patch_db(monkeypatch, tmp_path)
    _patch_sources(monkeypatch, tmp_path)

    first_lines = [
        {"type": "user", "timestamp": TS0, "message": {"content": "one"}},
        {"type": "user", "timestamp": TS1, "message": {"content": "two"}},
    ]
    path = _write_claude_session(tmp_path, "demo-project", "append", first_lines)

    conn = open_index()
    try:
        first = ingest_pending(conn, source_filter="claude", now=_now_from(path))
        assert first.to_dict()["batches"] == 1
        assert conn.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 2

        second_lines = [
            {"type": "user", "timestamp": TS2, "message": {"content": "three"}},
            {"type": "user", "timestamp": TS3, "message": {"content": "four"}},
        ]
        _append_claude_session(path, second_lines)

        second = ingest_pending(conn, source_filter="claude", now=_now_from(path))
        assert second.to_dict()["batches"] == 1
        assert conn.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 4

        # Cursor landed at EOF for the events consumer.
        end_offset, inode_now = path.stat().st_size, path.stat().st_ino
        cursor_row = conn.execute(
            "SELECT inode, last_offset FROM capture_cursors "
            "WHERE consumer_id = 'events' AND source_path = ?",
            (str(path),),
        ).fetchone()
        assert cursor_row["last_offset"] == end_offset
        assert cursor_row["inode"] == inode_now

        # Offsets form a strictly increasing sequence, one per line.
        offsets = [
            row[0]
            for row in conn.execute(
                "SELECT DISTINCT source_offset FROM events "
                "WHERE source_path = ? ORDER BY source_offset",
                (str(path),),
            )
        ]
        assert offsets == sorted(offsets)
        assert len(offsets) == 4

        # A third poll with no changes is a no-op.
        third = ingest_pending(conn, source_filter="claude", now=_now_from(path))
        assert third.to_dict()["batches"] == 0
        assert conn.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 4
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# Multibyte UTF-8 offsets
# --------------------------------------------------------------------------- #


def test_source_offsets_are_byte_accurate_under_multibyte_utf8(
    monkeypatch, tmp_path
):
    """Per-line source_offset is a byte offset, not a char offset — a
    line containing multibyte UTF-8 (CJK + emoji + Arabic) must not
    throw the following line's offset off."""
    _patch_db(monkeypatch, tmp_path)
    _patch_sources(monkeypatch, tmp_path)

    lines = [
        {"type": "user", "timestamp": TS0, "message": {"content": "你好 🙂 مرحبا שלום"}},
        {"type": "user", "timestamp": TS1, "message": {"content": "next line"}},
        {"type": "user", "timestamp": TS2, "message": {"content": "three"}},
    ]
    path = _write_claude_session(tmp_path, "demo-project", "utf8", lines)
    raw = path.read_bytes()
    # Expected byte offsets = cumulative sum of "<json>\n" byte lengths.
    expected_offsets: list[int] = []
    running = 0
    for line in lines:
        expected_offsets.append(running)
        running += len(json.dumps(line).encode("utf-8")) + 1  # +1 for \n
    assert running == len(raw)  # fixture sanity

    conn = open_index()
    try:
        ingest_pending(conn, source_filter="claude", now=_now_from(path))
        rows = conn.execute(
            "SELECT DISTINCT source_offset FROM events "
            "WHERE source_path = ? ORDER BY source_offset",
            (str(path),),
        ).fetchall()
        actual_offsets = [row["source_offset"] for row in rows]
        assert actual_offsets == expected_offsets

        # Also round-trip raw_json for the multibyte line — proves the
        # offset slice aligns with the actual JSON boundary, not a
        # garbled UTF-8 midpoint.
        raw_json = conn.execute(
            "SELECT raw_json FROM events WHERE source_offset = ? LIMIT 1",
            (expected_offsets[0],),
        ).fetchone()["raw_json"]
        assert json.loads(raw_json) == lines[0]
    finally:
        conn.close()
