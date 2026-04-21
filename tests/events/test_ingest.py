import json
import sqlite3
from datetime import datetime, timedelta, timezone

from clawjournal.capture.cursors import Cursor, ensure_schema, list_cursors, set_cursor
from clawjournal.events.ingest import EVENT_CONSUMER_ID, ingest_pending
from clawjournal.events.schema import ensure_schema as ensure_event_schema
from clawjournal.workbench.index import open_index


def _patch_db(monkeypatch, tmp_path):
    monkeypatch.setattr("clawjournal.workbench.index.INDEX_DB", tmp_path / "index.db")
    monkeypatch.setattr("clawjournal.workbench.index.CONFIG_DIR", tmp_path / "config")
    monkeypatch.setattr("clawjournal.config.CONFIG_DIR", tmp_path / "config")


def _patch_sources(monkeypatch, tmp_path):
    from clawjournal.parsing import parser

    monkeypatch.setattr(parser, "PROJECTS_DIR", tmp_path / "claude" / "projects")
    monkeypatch.setattr(parser, "CODEX_SESSIONS_DIR", tmp_path / "codex" / "sessions")
    monkeypatch.setattr(parser, "CODEX_ARCHIVED_DIR", tmp_path / "codex" / "archived_sessions")
    monkeypatch.setattr(parser, "LOCAL_AGENT_DIR", tmp_path / "local_agent")
    monkeypatch.setattr(parser, "OPENCLAW_AGENTS_DIR", tmp_path / "openclaw" / "agents")


def test_ingest_records_offsets_and_raw_json(monkeypatch, tmp_path):
    _patch_db(monkeypatch, tmp_path)
    _patch_sources(monkeypatch, tmp_path)

    session_file = tmp_path / "claude" / "projects" / "demo-project" / "sess-1.jsonl"
    session_file.parent.mkdir(parents=True)
    line_1 = {
        "type": "assistant",
        "timestamp": "2026-04-20T10:00:00.000Z",
        "version": "1.2.3",
        "message": {
            "content": [
                {"type": "text", "text": "Reading the file."},
                {
                    "type": "tool_use",
                    "id": "tu-1",
                    "name": "Read",
                    "input": {"file_path": "/tmp/demo.py"},
                },
            ]
        },
    }
    line_2 = {
        "type": "user",
        "timestamp": "2026-04-20T10:00:01.000Z",
        "message": {
            "content": [
                {
                    "type": "tool_result",
                    "tool_use_id": "tu-1",
                    "content": "print('demo')",
                }
            ]
        },
    }
    session_file.write_text(
        json.dumps(line_1) + "\n" + json.dumps(line_2) + "\n",
        encoding="utf-8",
    )

    conn = open_index()
    try:
        now = datetime.fromtimestamp(session_file.stat().st_mtime, tz=timezone.utc)
        summary = ingest_pending(conn, source_filter="claude", now=now)
        assert summary.to_dict()["event_rows"] == 4

        rows = conn.execute(
            """
            SELECT type, source_offset, seq, raw_json
              FROM events
             ORDER BY source_offset, seq
            """
        ).fetchall()
        assert [row["type"] for row in rows] == [
            "assistant_message",
            "tool_call",
            "file_read",
            "tool_result",
        ]
        first_line_offset = 0
        second_line_offset = len(json.dumps(line_1).encode("utf-8")) + 1
        assert [row["source_offset"] for row in rows] == [
            first_line_offset,
            first_line_offset,
            first_line_offset,
            second_line_offset,
        ]
        assert [row["seq"] for row in rows] == [0, 1, 2, 0]
        assert json.loads(rows[0]["raw_json"]) == line_1

        session_row = conn.execute(
            "SELECT session_key, client, client_version, status FROM event_sessions"
        ).fetchone()
        assert session_row["session_key"] == "claude:demo-project:sess-1"
        assert session_row["client"] == "claude"
        assert session_row["client_version"] == "1.2.3"
        assert session_row["status"] == "active"
    finally:
        conn.close()


def test_ingest_is_idempotent_and_keeps_scanner_cursor(monkeypatch, tmp_path):
    _patch_db(monkeypatch, tmp_path)
    _patch_sources(monkeypatch, tmp_path)

    session_file = tmp_path / "claude" / "projects" / "demo-project" / "sess-2.jsonl"
    session_file.parent.mkdir(parents=True)
    session_file.write_text(
        json.dumps(
            {
                "type": "user",
                "timestamp": "2026-04-20T10:00:00.000Z",
                "message": {"content": "Hello"},
            }
        )
        + "\n",
        encoding="utf-8",
    )

    conn = open_index()
    try:
        ensure_schema(conn)
        set_cursor(
            conn,
            Cursor(
                consumer_id="scanner",
                source_path=str(session_file),
                inode=1,
                last_offset=0,
                last_modified=0.0,
                client="claude",
            ),
        )
        conn.commit()

        now = datetime.fromtimestamp(session_file.stat().st_mtime, tz=timezone.utc)
        ingest_pending(conn, source_filter="claude", now=now)
        first_count = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        assert first_count == 1

        ingest_pending(conn, source_filter="claude", now=now)
        second_count = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        assert second_count == first_count

        cursors = list_cursors(conn)
        assert {(cursor.consumer_id, cursor.source_path) for cursor in cursors} == {
            ("scanner", str(session_file)),
            (EVENT_CONSUMER_ID, str(session_file)),
        }
    finally:
        conn.close()


def test_idle_session_is_marked_ended_on_later_unchanged_poll(monkeypatch, tmp_path):
    _patch_db(monkeypatch, tmp_path)
    _patch_sources(monkeypatch, tmp_path)

    session_file = tmp_path / "claude" / "projects" / "demo-project" / "sess-idle.jsonl"
    session_file.parent.mkdir(parents=True)
    session_file.write_text(
        json.dumps(
            {
                "type": "user",
                "timestamp": "2026-04-20T10:00:00.000Z",
                "message": {"content": "Hello"},
            }
        )
        + "\n",
        encoding="utf-8",
    )

    conn = open_index()
    try:
        fresh_now = datetime.fromtimestamp(session_file.stat().st_mtime, tz=timezone.utc)
        ingest_pending(conn, source_filter="claude", now=fresh_now)
        status = conn.execute(
            "SELECT status FROM event_sessions WHERE session_key = 'claude:demo-project:sess-idle'"
        ).fetchone()[0]
        assert status == "active"

        stale_now = fresh_now + timedelta(hours=2)
        ingest_pending(conn, source_filter="claude", now=stale_now)
        status = conn.execute(
            "SELECT status FROM event_sessions WHERE session_key = 'claude:demo-project:sess-idle'"
        ).fetchone()[0]
        assert status == "ended"
    finally:
        conn.close()


def test_native_and_local_agent_converge_on_one_event_session(monkeypatch, tmp_path):
    _patch_db(monkeypatch, tmp_path)
    _patch_sources(monkeypatch, tmp_path)

    native_file = tmp_path / "claude" / "projects" / "-Users-me-ws" / "dup.jsonl"
    native_file.parent.mkdir(parents=True)
    native_file.write_text(
        json.dumps(
            {
                "type": "user",
                "timestamp": "2026-04-20T10:00:00.000Z",
                "message": {"content": "From native"},
            }
        )
        + "\n",
        encoding="utf-8",
    )

    workspace = (
        tmp_path
        / "local_agent"
        / "11111111-2222-3333-4444-555555555555"
        / "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    )
    workspace.mkdir(parents=True)
    wrapper = workspace / "local_dup.json"
    wrapper.write_text(
        json.dumps(
            {
                "cliSessionId": "dup",
                "sessionId": "sess-dup",
                "processName": "demo",
                "userSelectedFolders": ["/Users/me/ws"],
            }
        ),
        encoding="utf-8",
    )
    transcript_dir = wrapper.with_suffix("") / ".claude" / "projects" / "-sessions-demo"
    transcript_dir.mkdir(parents=True)
    (transcript_dir / "dup.jsonl").write_text(
        json.dumps(
            {
                "type": "user",
                "timestamp": "2026-04-20T10:00:01.000Z",
                "message": {"content": "From local agent"},
            }
        )
        + "\n",
        encoding="utf-8",
    )

    conn = open_index()
    try:
        now = datetime.now(timezone.utc)
        summary = ingest_pending(conn, source_filter="claude", now=now)
        assert summary.to_dict()["event_rows"] == 2

        session_rows = conn.execute(
            "SELECT session_key FROM event_sessions ORDER BY session_key"
        ).fetchall()
        assert [row["session_key"] for row in session_rows] == [
            "claude:-Users-me-ws:dup"
        ]
        assert conn.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 2
    finally:
        conn.close()


def test_unparseable_line_stored_with_wrapper(monkeypatch, tmp_path):
    _patch_db(monkeypatch, tmp_path)
    _patch_sources(monkeypatch, tmp_path)

    session_file = tmp_path / "claude" / "projects" / "demo-project" / "junk.jsonl"
    session_file.parent.mkdir(parents=True)
    good_line = {
        "type": "user",
        "timestamp": "2026-04-20T10:00:00.000Z",
        "message": {"content": "Hi"},
    }
    bad_line = "this is not json at all {{{"
    session_file.write_bytes(
        (json.dumps(good_line) + "\n" + bad_line + "\n").encode("utf-8")
    )

    conn = open_index()
    try:
        now = datetime.fromtimestamp(session_file.stat().st_mtime, tz=timezone.utc)
        ingest_pending(conn, source_filter="claude", now=now)

        rows = conn.execute(
            """
            SELECT type, confidence, lossiness, raw_json, seq
              FROM events
             ORDER BY source_offset, seq
            """
        ).fetchall()
        assert len(rows) == 2
        assert rows[0]["type"] == "user_message"

        unparseable = rows[1]
        assert unparseable["type"] == "schema_unknown"
        assert unparseable["confidence"] == "low"
        assert unparseable["lossiness"] == "unknown"
        assert unparseable["seq"] == 0
        wrapper = json.loads(unparseable["raw_json"])
        assert wrapper == {"_unparseable": True, "raw_text": bad_line}
    finally:
        conn.close()


def test_rooted_subagent_tool_call_lands_on_root_session(monkeypatch, tmp_path):
    _patch_db(monkeypatch, tmp_path)
    _patch_sources(monkeypatch, tmp_path)

    project_dir = tmp_path / "claude" / "projects" / "demo-project"
    project_dir.mkdir(parents=True)

    root_uuid = "11111111-2222-3333-4444-555555555555"
    root_file = project_dir / f"{root_uuid}.jsonl"
    rooted_subagent_tool_call = {
        "type": "assistant",
        "timestamp": "2026-04-20T10:00:00.000Z",
        "message": {
            "content": [
                {
                    "type": "tool_use",
                    "id": "tu-rooted",
                    "name": "Read",
                    "input": {"file_path": "/tmp/rooted.py"},
                }
            ]
        },
    }
    root_file.write_text(json.dumps(rooted_subagent_tool_call) + "\n", encoding="utf-8")

    sidecar = project_dir / root_uuid / "subagents"
    sidecar.mkdir(parents=True)
    (sidecar / "agent-1.jsonl").write_text(
        json.dumps(
            {
                "type": "assistant",
                "timestamp": "2026-04-20T10:00:01.000Z",
                "message": {
                    "content": [{"type": "text", "text": "subagent-only ghost"}]
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )

    conn = open_index()
    try:
        now = datetime.fromtimestamp(root_file.stat().st_mtime, tz=timezone.utc)
        ingest_pending(conn, source_filter="claude", now=now)

        session_rows = conn.execute(
            "SELECT session_key FROM event_sessions ORDER BY session_key"
        ).fetchall()
        assert [row["session_key"] for row in session_rows] == [
            f"claude:demo-project:{root_uuid}"
        ]

        event_paths = {
            row["source_path"]
            for row in conn.execute("SELECT DISTINCT source_path FROM events").fetchall()
        }
        assert event_paths == {str(root_file)}

        tool_call_rows = conn.execute(
            """
            SELECT events.event_key, event_sessions.session_key
              FROM events
              JOIN event_sessions ON event_sessions.id = events.session_id
             WHERE events.type = 'tool_call'
            """
        ).fetchall()
        assert len(tool_call_rows) == 1
        assert tool_call_rows[0]["event_key"] == "tool_call:tu-rooted"
        assert tool_call_rows[0]["session_key"] == f"claude:demo-project:{root_uuid}"
    finally:
        conn.close()


def test_parent_link_backfills_when_parent_arrives_later(monkeypatch, tmp_path):
    _patch_db(monkeypatch, tmp_path)
    _patch_sources(monkeypatch, tmp_path)

    child_file = (
        tmp_path / "claude" / "projects" / "demo-project" / "child" / "subagents" / "agent-1.jsonl"
    )
    child_file.parent.mkdir(parents=True)
    child_file.write_text(
        json.dumps(
            {
                "type": "assistant",
                "timestamp": "2026-04-20T10:00:00.000Z",
                "parentSessionId": "parent",
                "message": {"content": [{"type": "text", "text": "Child trace"}]},
            }
        )
        + "\n",
        encoding="utf-8",
    )

    conn = open_index()
    try:
        child_now = datetime.fromtimestamp(child_file.stat().st_mtime, tz=timezone.utc)
        ingest_pending(conn, source_filter="claude", now=child_now)
        child_row = conn.execute(
            """
            SELECT parent_session_key, parent_session_id
              FROM event_sessions
             WHERE session_key = 'claude:demo-project:child'
            """
        ).fetchone()
        assert child_row["parent_session_key"] == "claude:demo-project:parent"
        assert child_row["parent_session_id"] is None

        parent_file = tmp_path / "claude" / "projects" / "demo-project" / "parent.jsonl"
        parent_file.write_text(
            json.dumps(
                {
                    "type": "user",
                    "timestamp": "2026-04-20T10:00:01.000Z",
                    "message": {"content": "Parent trace"},
                }
            )
            + "\n",
            encoding="utf-8",
        )
        parent_now = datetime.fromtimestamp(parent_file.stat().st_mtime, tz=timezone.utc)
        ingest_pending(conn, source_filter="claude", now=parent_now)

        child_parent_id = conn.execute(
            """
            SELECT child.parent_session_id
              FROM event_sessions AS child
             WHERE child.session_key = 'claude:demo-project:child'
            """
        ).fetchone()[0]
        parent_id = conn.execute(
            """
            SELECT id
              FROM event_sessions
             WHERE session_key = 'claude:demo-project:parent'
            """
        ).fetchone()[0]
        assert child_parent_id == parent_id
    finally:
        conn.close()


def test_event_schema_migrates_older_event_sessions_table(tmp_path):
    db_path = tmp_path / "index.db"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE event_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_key TEXT NOT NULL UNIQUE,
                parent_session_id INTEGER,
                client TEXT NOT NULL,
                client_version TEXT,
                started_at TEXT,
                ended_at TEXT,
                status TEXT NOT NULL DEFAULT 'active'
            )
            """
        )
        conn.commit()

        ensure_event_schema(conn)

        columns = {
            row[1] for row in conn.execute("PRAGMA table_info(event_sessions)")
        }
        assert "parent_session_key" in columns
        indexes = {
            row[1] for row in conn.execute("PRAGMA index_list(event_sessions)")
        }
        assert "idx_event_sessions_parent_key" in indexes
    finally:
        conn.close()
