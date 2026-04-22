"""End-to-end tests for the loop detector lite.

Covers the spec's three acceptance criteria:

1. Three identical npm test failures → one incident with count=3.
2. Re-running ingestion does not duplicate incidents (dedupe key is
   (session_id, kind, first_event_id)).
3. Per-rule thresholds: 3 for shell, 5 for tool calls; sub-threshold
   runs do not emit.

Plus per-client fingerprint coverage for claude / codex / openclaw
and the "non-eligible event breaks the run" semantics.
"""

from __future__ import annotations

import json
import sqlite3

import pytest

from clawjournal.events.incidents import (
    DEFAULT_SHELL_THRESHOLD,
    DEFAULT_TOOL_CALL_THRESHOLD,
    LOOP_INCIDENT_KIND,
    detect_session_loops,
    ensure_incidents_schema,
    ingest_loop_incidents,
    rebuild_loop_incidents,
)
from clawjournal.events.schema import ensure_schema as ensure_events_schema
from clawjournal.events.view import ensure_view_schema


TS = "2026-04-21T10:00:00Z"


# --------------------------------------------------------------------------- #
# fixture helpers
# --------------------------------------------------------------------------- #


@pytest.fixture
def conn() -> sqlite3.Connection:
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    ensure_events_schema(c)
    ensure_incidents_schema(c)
    ensure_view_schema(c)
    return c


def _insert_session(conn, *, session_key: str, client: str = "claude") -> int:
    cur = conn.execute(
        "INSERT INTO event_sessions (session_key, client, status) VALUES (?, ?, 'active')",
        (session_key, client),
    )
    conn.commit()
    return int(cur.lastrowid)


def _insert_event(
    conn,
    *,
    session_id: int,
    client: str,
    event_type: str,
    raw: dict,
    event_key: str | None = None,
    event_at: str | None = TS,
    source: str | None = None,
    source_path: str = "/tmp/x.jsonl",
) -> int:
    if source is None:
        source = {"claude": "claude-jsonl", "codex": "codex-rollout", "openclaw": "openclaw-jsonl"}[client]
    offset = int(
        conn.execute(
            "SELECT COALESCE(MAX(source_offset), -1) + 1 FROM events"
        ).fetchone()[0]
    )
    cur = conn.execute(
        """
        INSERT INTO events (
            session_id, type, event_key, event_at, ingested_at, source,
            source_path, source_offset, seq, client, confidence, lossiness,
            raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, 'high', 'none', ?)
        """,
        (
            session_id, event_type, event_key, event_at, TS, source, source_path,
            offset, client, json.dumps(raw, sort_keys=True),
        ),
    )
    conn.commit()
    return int(cur.lastrowid)


def _claude_bash_pair(conn, *, session_id: int, tool_id: str, command: str, output: str):
    """Insert a paired Claude (assistant tool_call + user tool_result)
    pair so the loop detector can compute a complete fingerprint."""
    _insert_event(
        conn,
        session_id=session_id,
        client="claude",
        event_type="command_start",
        event_key=f"command_start:{tool_id}",
        raw={
            "type": "assistant",
            "message": {
                "content": [
                    {
                        "type": "tool_use",
                        "id": tool_id,
                        "name": "Bash",
                        "input": {"command": command},
                    }
                ]
            },
        },
    )
    _insert_event(
        conn,
        session_id=session_id,
        client="claude",
        event_type="tool_result",
        event_key=f"tool_result:{tool_id}",
        raw={
            "type": "user",
            "message": {
                "content": [
                    {
                        "type": "tool_result",
                        "tool_use_id": tool_id,
                        "content": [{"type": "text", "text": output}],
                    }
                ]
            },
        },
    )


def _claude_tool_call(conn, *, session_id: int, tool_id: str, name: str, args: dict, result: str):
    _insert_event(
        conn,
        session_id=session_id,
        client="claude",
        event_type="tool_call",
        event_key=f"tool_call:{tool_id}",
        raw={
            "type": "assistant",
            "message": {
                "content": [
                    {"type": "tool_use", "id": tool_id, "name": name, "input": args}
                ]
            },
        },
    )
    _insert_event(
        conn,
        session_id=session_id,
        client="claude",
        event_type="tool_result",
        event_key=f"tool_result:{tool_id}",
        raw={
            "type": "user",
            "message": {
                "content": [
                    {
                        "type": "tool_result",
                        "tool_use_id": tool_id,
                        "content": [{"type": "text", "text": result}],
                    }
                ]
            },
        },
    )


# --------------------------------------------------------------------------- #
# acceptance criteria
# --------------------------------------------------------------------------- #


def test_three_identical_npm_test_failures_produce_one_incident_with_count_3(conn):
    sid = _insert_session(conn, session_key="s:npm", client="claude")
    failure_text = (
        "FAIL src/auth.test.ts\n"
        "  ✕ login redirects on success (12 ms)\n"
        "Tests:       1 failed, 0 passed\n"
    )
    for i in range(3):
        _claude_bash_pair(
            conn,
            session_id=sid,
            tool_id=f"tu-npm-{i}",
            command="npm test",
            # Different timestamp / pid each run; normalization makes them equal.
            output=f"[{1000 + i}] start at 2026-04-21T10:00:0{i}Z\n{failure_text}",
        )

    summary = ingest_loop_incidents(conn)
    incidents = conn.execute(
        "SELECT * FROM incidents WHERE session_id = ?", (sid,)
    ).fetchall()

    assert summary.incidents_written == 1
    assert len(incidents) == 1
    incident = incidents[0]
    assert incident["kind"] == LOOP_INCIDENT_KIND
    assert incident["count"] == 3
    assert incident["confidence"] == "high"
    evidence = json.loads(incident["evidence_json"])
    assert evidence["event_type"] == "command_start"
    assert evidence["threshold"] == DEFAULT_SHELL_THRESHOLD
    assert len(evidence["event_ids"]) == 3


def test_two_failures_do_not_meet_shell_threshold(conn):
    sid = _insert_session(conn, session_key="s:two", client="claude")
    for i in range(2):
        _claude_bash_pair(
            conn,
            session_id=sid,
            tool_id=f"tu-{i}",
            command="npm test",
            output="FAIL same output every time",
        )
    ingest_loop_incidents(conn)
    n = conn.execute(
        "SELECT COUNT(*) FROM incidents WHERE session_id = ?", (sid,)
    ).fetchone()[0]
    assert n == 0


def test_re_running_does_not_duplicate_incidents(conn):
    sid = _insert_session(conn, session_key="s:idem", client="claude")
    for i in range(3):
        _claude_bash_pair(
            conn,
            session_id=sid,
            tool_id=f"tu-{i}",
            command="npm test",
            output="FAIL identical",
        )

    first = ingest_loop_incidents(conn)
    second = ingest_loop_incidents(conn)
    assert first.incidents_written == 1
    # Re-running with no new events is a true no-op: nothing scanned,
    # nothing evaluated, nothing rewritten.
    assert second.events_scanned == 0
    assert second.sessions_evaluated == 0
    assert second.incidents_written == 0
    assert conn.execute("SELECT COUNT(*) FROM incidents").fetchone()[0] == 1


def test_growing_run_updates_count_without_duplicating(conn):
    """When a run grows past its first incarnation, the session
    still has exactly one incident row — the count is the new
    length. The driver achieves this by deleting + re-inserting
    the session's loop rows; the spec requires "no duplicates"
    keyed by (session_id, kind, first_event_id), not that the
    integer id is stable across runs."""
    sid = _insert_session(conn, session_key="s:grow", client="claude")
    # First batch: three identical failures.
    for i in range(3):
        _claude_bash_pair(
            conn, session_id=sid, tool_id=f"a-{i}",
            command="npm test", output="FAIL same",
        )
    ingest_loop_incidents(conn)
    first_first_event_id = conn.execute(
        "SELECT first_event_id FROM incidents WHERE session_id = ?", (sid,)
    ).fetchone()[0]

    # Append two more identical failures. Run grows to 5.
    for i in range(2):
        _claude_bash_pair(
            conn, session_id=sid, tool_id=f"b-{i}",
            command="npm test", output="FAIL same",
        )
    summary = ingest_loop_incidents(conn)

    rows = conn.execute(
        "SELECT first_event_id, count FROM incidents WHERE session_id = ?", (sid,)
    ).fetchall()
    assert len(rows) == 1
    assert rows[0]["first_event_id"] == first_first_event_id  # same logical run
    assert rows[0]["count"] == 5
    assert summary.sessions_evaluated == 1


def test_unrelated_event_between_repeats_breaks_the_run(conn):
    """A non-eligible event in the middle prevents grouping."""
    sid = _insert_session(conn, session_key="s:break", client="claude")
    _claude_bash_pair(
        conn, session_id=sid, tool_id="a-0",
        command="npm test", output="FAIL same",
    )
    _claude_bash_pair(
        conn, session_id=sid, tool_id="a-1",
        command="npm test", output="FAIL same",
    )
    # Inject a user message between the second and third failure.
    _insert_event(
        conn, session_id=sid, client="claude", event_type="user_message",
        raw={"type": "user", "message": {"content": "hmm let me think"}},
    )
    _claude_bash_pair(
        conn, session_id=sid, tool_id="a-2",
        command="npm test", output="FAIL same",
    )
    ingest_loop_incidents(conn)
    n = conn.execute("SELECT COUNT(*) FROM incidents").fetchone()[0]
    assert n == 0


def test_transparent_event_between_repeats_does_not_break_the_run(conn):
    """Only `user_message` / `compaction` reset adjacency. An
    `assistant_message` between two `command_start`s is bookkeeping,
    not new external input, and must NOT break the run."""
    sid = _insert_session(conn, session_key="s:transparent", client="claude")
    for i in range(2):
        _claude_bash_pair(
            conn, session_id=sid, tool_id=f"a-{i}",
            command="npm test", output="FAIL same",
        )
    _insert_event(
        conn, session_id=sid, client="claude", event_type="assistant_message",
        raw={"type": "assistant", "message": {"content": "let me try again"}},
    )
    _claude_bash_pair(
        conn, session_id=sid, tool_id="a-2",
        command="npm test", output="FAIL same",
    )
    ingest_loop_incidents(conn)
    rows = conn.execute("SELECT count FROM incidents").fetchall()
    assert len(rows) == 1
    assert rows[0]["count"] == 3


def test_compaction_event_breaks_the_run(conn):
    """A `compaction` event means the conversation was summarized and
    the next attempt is against refreshed context — it must break
    adjacency just like a `user_message` does."""
    sid = _insert_session(conn, session_key="s:compaction", client="claude")
    for i in range(2):
        _claude_bash_pair(
            conn, session_id=sid, tool_id=f"a-{i}",
            command="npm test", output="FAIL same",
        )
    _insert_event(
        conn, session_id=sid, client="claude", event_type="compaction",
        raw={"type": "system", "subtype": "compact_boundary"},
    )
    _claude_bash_pair(
        conn, session_id=sid, tool_id="a-2",
        command="npm test", output="FAIL same",
    )
    ingest_loop_incidents(conn)
    assert conn.execute("SELECT COUNT(*) FROM incidents").fetchone()[0] == 0


def test_distinct_outputs_do_not_count_as_a_loop(conn):
    """Same command with materially different output is NOT a loop —
    progress is being made."""
    sid = _insert_session(conn, session_key="s:progress", client="claude")
    for i, outcome in enumerate(("first", "second", "third")):
        _claude_bash_pair(
            conn, session_id=sid, tool_id=f"tu-{i}",
            command="npm test", output=f"FAIL on iteration {outcome}",
        )
    ingest_loop_incidents(conn)
    n = conn.execute("SELECT COUNT(*) FROM incidents").fetchone()[0]
    assert n == 0


def test_cross_source_duplicates_do_not_false_positive_a_shell_loop(conn):
    sid = _insert_session(conn, session_key="s:xsrc-loop", client="claude")
    for tool_id, path in (("tu-0", "/a.jsonl"), ("tu-1", "/c.jsonl")):
        command_raw = {
            "type": "assistant",
            "message": {
                "content": [
                    {
                        "type": "tool_use",
                        "id": tool_id,
                        "name": "Bash",
                        "input": {"command": "npm test"},
                    }
                ]
            },
        }
        result_raw = {
            "type": "user",
            "message": {
                "content": [
                    {
                        "type": "tool_result",
                        "tool_use_id": tool_id,
                        "content": [{"type": "text", "text": "FAIL identical"}],
                    }
                ]
            },
        }
        for source, source_path in (
            ("claude-jsonl", path),
            ("hook", path.replace(".jsonl", "-dup.jsonl")),
        ):
            _insert_event(
                conn,
                session_id=sid,
                client="claude",
                event_type="command_start",
                event_key=f"command_start:{tool_id}",
                raw=command_raw,
                source=source,
                source_path=source_path,
            )
            _insert_event(
                conn,
                session_id=sid,
                client="claude",
                event_type="tool_result",
                event_key=f"tool_result:{tool_id}",
                raw=result_raw,
                source=source,
                source_path=source_path,
            )

    ingest_loop_incidents(conn)
    assert conn.execute("SELECT COUNT(*) FROM incidents").fetchone()[0] == 0


def test_tool_call_threshold_is_five(conn):
    sid = _insert_session(conn, session_key="s:tools", client="claude")
    # 5 identical Read tool calls.
    for i in range(DEFAULT_TOOL_CALL_THRESHOLD):
        _claude_tool_call(
            conn, session_id=sid, tool_id=f"tc-{i}",
            name="Read", args={"file_path": "/etc/hosts"},
            result="127.0.0.1 localhost",
        )

    ingest_loop_incidents(conn)
    rows = conn.execute("SELECT count, evidence_json FROM incidents").fetchall()
    assert len(rows) == 1
    assert rows[0]["count"] == 5
    assert json.loads(rows[0]["evidence_json"])["event_type"] == "tool_call"


def test_tool_call_below_threshold_does_not_emit(conn):
    sid = _insert_session(conn, session_key="s:tool-sub", client="claude")
    for i in range(DEFAULT_TOOL_CALL_THRESHOLD - 1):
        _claude_tool_call(
            conn, session_id=sid, tool_id=f"tc-{i}",
            name="Read", args={"file_path": "/etc/hosts"},
            result="127.0.0.1 localhost",
        )
    ingest_loop_incidents(conn)
    assert conn.execute("SELECT COUNT(*) FROM incidents").fetchone()[0] == 0


def test_shell_tool_call_events_do_not_double_emit(conn):
    """A shell `tool_call` event (e.g. name=`Bash`) has a paired
    `command_start` event carrying the more meaningful (command,
    outcome) fingerprint. If the `tool_call` rule also matched, 5
    identical Bash calls would produce TWO incidents — one under the
    command-threshold=3 rule, one under the tool-call-threshold=5 rule.
    `_fingerprint_for` drops shell-named tool_calls for exactly this
    reason."""
    sid = _insert_session(conn, session_key="s:shell-tc", client="claude")
    for i in range(DEFAULT_TOOL_CALL_THRESHOLD):
        tool_id = f"tc-{i}"
        # Emit BOTH a command_start (what the shell rule sees) and a
        # tool_call with name=Bash (what the tool-call rule would see
        # if the dedup guard regressed).
        _claude_bash_pair(
            conn, session_id=sid, tool_id=tool_id,
            command="npm test", output="FAIL",
        )
        _insert_event(
            conn,
            session_id=sid,
            client="claude",
            event_type="tool_call",
            event_key=f"tool_call:{tool_id}-tc",
            raw={
                "type": "assistant",
                "message": {
                    "content": [
                        {
                            "type": "tool_use",
                            "id": f"{tool_id}-tc",
                            "name": "Bash",
                            "input": {"command": "npm test"},
                        }
                    ]
                },
            },
        )

    ingest_loop_incidents(conn)
    rows = conn.execute("SELECT count, evidence_json FROM incidents").fetchall()
    assert len(rows) == 1  # ONE incident, not two
    assert json.loads(rows[0]["evidence_json"])["event_type"] == "command_start"


# --------------------------------------------------------------------------- #
# rebuild + cursor
# --------------------------------------------------------------------------- #


def test_rebuild_clears_and_replays(conn):
    sid = _insert_session(conn, session_key="s:rebuild", client="claude")
    for i in range(3):
        _claude_bash_pair(
            conn, session_id=sid, tool_id=f"tu-{i}",
            command="npm test", output="FAIL",
        )
    ingest_loop_incidents(conn)
    n_first = conn.execute("SELECT COUNT(*) FROM incidents").fetchone()[0]
    assert n_first == 1

    # Forge a stale row so we can prove rebuild deletes-and-replaces.
    conn.execute(
        "UPDATE incidents SET count = 999 WHERE session_id = ?", (sid,)
    )
    conn.commit()

    summary = rebuild_loop_incidents(conn)
    assert summary.sessions_evaluated == 1
    row = conn.execute("SELECT count FROM incidents WHERE session_id = ?", (sid,)).fetchone()
    assert row["count"] == 3  # forged value gone, real value back


def test_cursor_advances_only_after_evaluation(conn):
    sid = _insert_session(conn, session_key="s:cursor", client="claude")
    for i in range(3):
        _claude_bash_pair(
            conn, session_id=sid, tool_id=f"a-{i}",
            command="npm test", output="FAIL",
        )
    ingest_loop_incidents(conn)
    first_cursor = conn.execute(
        "SELECT last_event_id FROM loop_ingest_state WHERE consumer_id = 'loop_detector'"
    ).fetchone()["last_event_id"]
    assert first_cursor > 0

    # Append two more events — cursor should advance again.
    _claude_bash_pair(
        conn, session_id=sid, tool_id="b-0",
        command="npm test", output="FAIL",
    )
    ingest_loop_incidents(conn)
    second_cursor = conn.execute(
        "SELECT last_event_id FROM loop_ingest_state WHERE consumer_id = 'loop_detector'"
    ).fetchone()["last_event_id"]
    assert second_cursor > first_cursor


# --------------------------------------------------------------------------- #
# per-client fingerprint coverage
# --------------------------------------------------------------------------- #


def test_codex_shell_loop_detected(conn):
    sid = _insert_session(conn, session_key="s:codex-shell", client="codex")
    for i in range(3):
        tool_id = f"call_{i}"
        _insert_event(
            conn, session_id=sid, client="codex",
            event_type="command_start",
            event_key=f"command_start:{tool_id}",
            raw={
                "type": "response_item",
                "payload": {
                    "type": "function_call",
                    "name": "shell",
                    "call_id": tool_id,
                    "arguments": json.dumps(
                        {"command": ["bash", "-lc", "npm test"], "workdir": "/x"}
                    ),
                },
            },
        )
        _insert_event(
            conn, session_id=sid, client="codex",
            event_type="tool_result",
            event_key=f"tool_result:{tool_id}",
            raw={
                "type": "response_item",
                "payload": {
                    "type": "function_call_output",
                    "call_id": tool_id,
                    "output": json.dumps(
                        {
                            "output": "FAIL identical",
                            "metadata": {"exit_code": 1, "duration_seconds": 0.5 + i * 0.01},
                        }
                    ),
                },
            },
        )

    ingest_loop_incidents(conn)
    rows = conn.execute("SELECT count FROM incidents").fetchall()
    assert len(rows) == 1
    assert rows[0]["count"] == 3


def test_codex_shell_command_loop_detected(conn):
    sid = _insert_session(conn, session_key="s:codex-shell-command", client="codex")
    for i in range(3):
        tool_id = f"call_{i}"
        _insert_event(
            conn,
            session_id=sid,
            client="codex",
            event_type="command_start",
            event_key=f"command_start:{tool_id}",
            raw={
                "type": "response_item",
                "payload": {
                    "type": "function_call",
                    "name": "shell_command",
                    "call_id": tool_id,
                    "arguments": json.dumps(
                        {"command": "npm test", "workdir": "/repo"}
                    ),
                },
            },
        )
        _insert_event(
            conn,
            session_id=sid,
            client="codex",
            event_type="tool_result",
            event_key=f"tool_result:{tool_id}",
            raw={
                "type": "response_item",
                "payload": {
                    "type": "function_call_output",
                    "call_id": tool_id,
                    "output": json.dumps({"output": "FAIL identical"}),
                },
            },
        )

    ingest_loop_incidents(conn)
    rows = conn.execute("SELECT count FROM incidents").fetchall()
    assert len(rows) == 1
    assert rows[0]["count"] == 3


def test_codex_shell_args_are_part_of_the_fingerprint(conn):
    sid = _insert_session(conn, session_key="s:codex-shell-args", client="codex")
    for i in range(3):
        tool_id = f"call_{i}"
        _insert_event(
            conn,
            session_id=sid,
            client="codex",
            event_type="command_start",
            event_key=f"command_start:{tool_id}",
            raw={
                "type": "response_item",
                "payload": {
                    "type": "function_call",
                    "name": "shell_command",
                    "call_id": tool_id,
                    "arguments": json.dumps(
                        {"command": "npm test", "workdir": f"/repo/{i}"}
                    ),
                },
            },
        )
        _insert_event(
            conn,
            session_id=sid,
            client="codex",
            event_type="tool_result",
            event_key=f"tool_result:{tool_id}",
            raw={
                "type": "response_item",
                "payload": {
                    "type": "function_call_output",
                    "call_id": tool_id,
                    "output": json.dumps({"output": "FAIL identical"}),
                },
            },
        )

    ingest_loop_incidents(conn)
    assert conn.execute("SELECT COUNT(*) FROM incidents").fetchone()[0] == 0


def test_openclaw_tool_loop_detected(conn):
    sid = _insert_session(conn, session_key="s:opc", client="openclaw")
    for i in range(DEFAULT_TOOL_CALL_THRESHOLD):
        tool_id = f"oc-{i}"
        _insert_event(
            conn, session_id=sid, client="openclaw",
            event_type="tool_call",
            event_key=f"tool_call:{tool_id}",
            raw={
                "type": "message",
                "message": {
                    "role": "assistant",
                    "content": [
                        {"type": "toolCall", "id": tool_id, "name": "read_file",
                         "input": {"path": "/etc/hosts"}}
                    ],
                },
            },
        )
        _insert_event(
            conn, session_id=sid, client="openclaw",
            event_type="tool_result",
            event_key=f"tool_result:{tool_id}",
            raw={
                "type": "message",
                "message": {
                    "role": "toolResult",
                    "toolCallId": tool_id,
                    "content": [{"type": "text", "text": "127.0.0.1 localhost"}],
                },
            },
        )

    ingest_loop_incidents(conn)
    assert conn.execute("SELECT COUNT(*) FROM incidents").fetchone()[0] == 1


def test_openclaw_distinct_results_do_not_count_as_a_loop(conn):
    sid = _insert_session(conn, session_key="s:opc-distinct", client="openclaw")
    for i in range(DEFAULT_TOOL_CALL_THRESHOLD):
        tool_id = f"oc-{i}"
        _insert_event(
            conn,
            session_id=sid,
            client="openclaw",
            event_type="tool_call",
            event_key=f"tool_call:{tool_id}",
            raw={
                "type": "message",
                "message": {
                    "role": "assistant",
                    "content": [
                        {
                            "type": "toolCall",
                            "id": tool_id,
                            "name": "read_file",
                            "input": {"path": "/etc/hosts"},
                        }
                    ],
                },
            },
        )
        _insert_event(
            conn,
            session_id=sid,
            client="openclaw",
            event_type="tool_result",
            event_key=f"tool_result:{tool_id}",
            raw={
                "type": "message",
                "message": {
                    "role": "toolResult",
                    "toolCallId": tool_id,
                    "content": [{"type": "text", "text": f"result {i}"}],
                },
            },
        )

    ingest_loop_incidents(conn)
    assert conn.execute("SELECT COUNT(*) FROM incidents").fetchone()[0] == 0


def test_pure_read_helper_does_not_write(conn):
    sid = _insert_session(conn, session_key="s:pure", client="claude")
    for i in range(3):
        _claude_bash_pair(
            conn, session_id=sid, tool_id=f"tu-{i}",
            command="npm test", output="FAIL",
        )
    hits = detect_session_loops(conn, sid)
    assert len(hits) == 1
    assert hits[0].count == 3
    # detect_session_loops must not write anything.
    assert conn.execute("SELECT COUNT(*) FROM incidents").fetchone()[0] == 0


def test_pure_read_helper_works_without_view_schema():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    ensure_events_schema(conn)
    ensure_incidents_schema(conn)
    sid = _insert_session(conn, session_key="s:no-view", client="claude")
    for i in range(3):
        _claude_bash_pair(
            conn,
            session_id=sid,
            tool_id=f"tu-{i}",
            command="npm test",
            output="FAIL",
        )

    hits = detect_session_loops(conn, sid)

    assert len(hits) == 1
    assert hits[0].count == 3


def test_unparseable_raw_json_breaks_run_safely(conn):
    sid = _insert_session(conn, session_key="s:bad", client="claude")
    _claude_bash_pair(
        conn, session_id=sid, tool_id="a-0",
        command="npm test", output="FAIL",
    )
    # Manually inject a row whose raw_json is broken — must not crash.
    conn.execute(
        """
        INSERT INTO events (
            session_id, type, event_key, event_at, ingested_at, source,
            source_path, source_offset, seq, client, confidence, lossiness,
            raw_json
        ) VALUES (?, 'command_start', ?, ?, ?, 'claude-jsonl',
                  '/tmp/x.jsonl', 9999, 0, 'claude', 'low', 'unknown', ?)
        """,
        (sid, "command_start:bad", TS, TS, "{not json"),
    )
    conn.commit()
    _claude_bash_pair(
        conn, session_id=sid, tool_id="a-2",
        command="npm test", output="FAIL",
    )
    _claude_bash_pair(
        conn, session_id=sid, tool_id="a-3",
        command="npm test", output="FAIL",
    )
    summary = ingest_loop_incidents(conn)
    # The bad row breaks the run; we only have 2 and then 2 → no loop.
    assert summary.incidents_written == 0


def test_evidence_json_hashes_outcome_text_instead_of_storing_it(conn):
    """`incidents.evidence_json` must NOT carry the paired tool_result
    text verbatim — otherwise secrets that survived the normalizer
    (anything outside the five fingerprint-normalization rules) would
    be written into the DB and surface through any consumer that
    doesn't re-run redaction. The fingerprint's outcome slot is stored
    as a `sha256:<prefix>` token; the real text stays reachable via
    `first_event_id` / `last_event_id` through the normal paths."""
    sid = _insert_session(conn, session_key="s:secret-outcome", client="claude")
    # Construct an output whose "secret" portion survives the
    # normalizer (no timestamps / home paths / PIDs / whitespace).
    secret_output = "ERROR: invalid token AKIA1234567890ABCDEF please check config"
    for i in range(3):
        _claude_bash_pair(
            conn, session_id=sid, tool_id=f"tu-{i}",
            command="deploy", output=secret_output,
        )
    ingest_loop_incidents(conn)
    row = conn.execute(
        "SELECT evidence_json FROM incidents WHERE session_id = ?", (sid,)
    ).fetchone()
    evidence_text = row["evidence_json"]
    assert "AKIA1234567890ABCDEF" not in evidence_text
    evidence = json.loads(evidence_text)
    outcome_token = evidence["fingerprint"][-1]
    assert outcome_token.startswith("sha256:")
    assert len(outcome_token) == len("sha256:") + 16


def test_claude_bash_description_and_timeout_drift_still_count_as_loop(conn):
    """Claude's Bash `input` carries `description`, `timeout`, and
    `run_in_background` alongside `command`. Those fields drift between
    retries (model-narrated text, ergonomic knobs) and must NOT hide an
    otherwise-identical loop."""
    sid = _insert_session(conn, session_key="s:bash-drift", client="claude")
    for i, description in enumerate(("running tests", "retrying after fix", "one more time")):
        tool_id = f"tu-{i}"
        _insert_event(
            conn,
            session_id=sid,
            client="claude",
            event_type="command_start",
            event_key=f"command_start:{tool_id}",
            raw={
                "type": "assistant",
                "message": {
                    "content": [
                        {
                            "type": "tool_use",
                            "id": tool_id,
                            "name": "Bash",
                            "input": {
                                "command": "npm test",
                                "description": description,
                                "timeout": 5000 + i * 100,
                                "run_in_background": False,
                            },
                        }
                    ]
                },
            },
        )
        _insert_event(
            conn,
            session_id=sid,
            client="claude",
            event_type="tool_result",
            event_key=f"tool_result:{tool_id}",
            raw={
                "type": "user",
                "message": {
                    "content": [
                        {
                            "type": "tool_result",
                            "tool_use_id": tool_id,
                            "content": [{"type": "text", "text": "FAIL identical"}],
                        }
                    ]
                },
            },
        )

    ingest_loop_incidents(conn)
    rows = conn.execute("SELECT count FROM incidents").fetchall()
    assert len(rows) == 1
    assert rows[0]["count"] == 3


def test_cursor_advances_correctly_across_mid_run_boundary(conn):
    """Cursor stops mid-run, a later ingest picks up the rest — the
    session ends up with one incident whose count reflects the whole
    run, not two separate rows."""
    sid = _insert_session(conn, session_key="s:mid-run", client="claude")
    # First ingest sees only the first 3 of an eventual 5-run.
    for i in range(3):
        _claude_bash_pair(
            conn, session_id=sid, tool_id=f"a-{i}",
            command="npm test", output="FAIL same",
        )
    first = ingest_loop_incidents(conn)
    assert first.incidents_written == 1
    assert conn.execute(
        "SELECT count FROM incidents WHERE session_id = ?", (sid,)
    ).fetchone()["count"] == 3

    # Two more identical failures arrive — the run extends to 5.
    for i in range(2):
        _claude_bash_pair(
            conn, session_id=sid, tool_id=f"b-{i}",
            command="npm test", output="FAIL same",
        )
    second = ingest_loop_incidents(conn)

    rows = conn.execute(
        "SELECT count FROM incidents WHERE session_id = ?", (sid,)
    ).fetchall()
    assert len(rows) == 1
    assert rows[0]["count"] == 5
    assert second.sessions_evaluated == 1


def test_rebuild_preserves_non_loop_incident_kinds(conn):
    """The incidents table is shared across kinds; `--rebuild` must
    only touch `loop_exact_repeat` rows."""
    sid = _insert_session(conn, session_key="s:shared-kinds", client="claude")
    for i in range(3):
        _claude_bash_pair(
            conn, session_id=sid, tool_id=f"tu-{i}",
            command="npm test", output="FAIL",
        )
    ingest_loop_incidents(conn)
    # Inject a row of a different kind (as some future detector would).
    conn.execute(
        """
        INSERT INTO incidents (
            session_id, kind, first_event_id, last_event_id,
            evidence_json, count, confidence, created_at
        ) VALUES (?, 'other_kind', 1, 1, '{}', 1, 'high', ?)
        """,
        (sid, TS),
    )
    conn.commit()

    rebuild_loop_incidents(conn)

    kinds = {
        row["kind"]
        for row in conn.execute("SELECT kind FROM incidents WHERE session_id = ?", (sid,))
    }
    assert "other_kind" in kinds
    assert LOOP_INCIDENT_KIND in kinds
