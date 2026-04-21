"""Unit tests for clawjournal.events.view — 03 signal/confidence layer.

Covers:
- ensure_view_schema idempotency + CHECK constraints
- write_hook_override validation, rank guards, session lookup
- canonical_events base-only / override-wins / hook-only / cross-source
- capability_join present / missing / supported_but_absent + split counts
- fetch_vendor_line existing / missing / past-EOF / partial-line / CRLF
"""

from __future__ import annotations

import json
import sqlite3

import pytest

from clawjournal.events.schema import ensure_schema as ensure_events_schema
from clawjournal.events.view import (
    CanonicalEvent,
    canonical_events,
    capability_join,
    ensure_view_schema,
    fetch_vendor_line,
    write_hook_override,
)


TS0 = "2026-04-20T10:00:00Z"
TS1 = "2026-04-20T10:00:01Z"
TS2 = "2026-04-20T10:00:02Z"


# --------------------------------------------------------------------------- #
# Fixture helpers
# --------------------------------------------------------------------------- #


@pytest.fixture
def conn() -> sqlite3.Connection:
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    ensure_events_schema(c)
    ensure_view_schema(c)
    return c


def _insert_event_session(
    conn: sqlite3.Connection,
    *,
    session_key: str,
    client: str,
    status: str = "active",
) -> int:
    cursor = conn.execute(
        "INSERT INTO event_sessions (session_key, client, status) VALUES (?, ?, ?)",
        (session_key, client, status),
    )
    conn.commit()
    return int(cursor.lastrowid)


def _insert_event(
    conn: sqlite3.Connection,
    *,
    session_id: int,
    type: str = "user_message",
    event_key: str | None = None,
    event_at: str | None = TS0,
    source: str = "claude-jsonl",
    source_path: str = "/tmp/demo.jsonl",
    source_offset: int = 0,
    seq: int = 0,
    client: str = "claude",
    confidence: str = "high",
    lossiness: str = "none",
    raw_json: str = "{}",
) -> int:
    cursor = conn.execute(
        """
        INSERT INTO events (
            session_id, type, event_key, event_at, ingested_at, source,
            source_path, source_offset, seq, client, confidence, lossiness,
            raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            session_id, type, event_key, event_at, TS0, source,
            source_path, source_offset, seq, client, confidence, lossiness,
            raw_json,
        ),
    )
    conn.commit()
    return int(cursor.lastrowid)


# --------------------------------------------------------------------------- #
# ensure_view_schema
# --------------------------------------------------------------------------- #


def test_ensure_view_schema_is_idempotent(conn):
    ensure_view_schema(conn)  # second call must not raise
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    )}
    assert "event_overrides" in tables

    indexes = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index'"
    )}
    assert "idx_event_overrides_session" in indexes


def test_ensure_view_schema_rejects_invalid_source_via_check(conn):
    sid = _insert_event_session(conn, session_key="s:1", client="claude")
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            """
            INSERT INTO event_overrides
              (session_id, event_key, type, source, confidence, lossiness,
               event_at, payload_json, origin, created_at)
            VALUES (?, 'tool_call:x', 'tool_call', 'bogus-source', 'high',
                    'none', NULL, '{}', NULL, ?)
            """,
            (sid, TS0),
        )


def test_ensure_view_schema_rejects_missing_confidence_via_check(conn):
    sid = _insert_event_session(conn, session_key="s:2", client="claude")
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            """
            INSERT INTO event_overrides
              (session_id, event_key, type, source, confidence, lossiness,
               event_at, payload_json, origin, created_at)
            VALUES (?, 'tool_call:x', 'tool_call', 'hook', 'missing',
                    'none', NULL, '{}', NULL, ?)
            """,
            (sid, TS0),
        )


def test_ensure_view_schema_cascades_on_session_delete(conn):
    sid = _insert_event_session(conn, session_key="s:cascade", client="claude")
    assert write_hook_override(
        conn,
        session_key="s:cascade",
        event_key="tool_call:x",
        event_type="tool_call",
        source="hook",
        confidence="high",
        lossiness="none",
        event_at=TS0,
        payload_json="{}",
        origin=None,
    )
    assert conn.execute("SELECT COUNT(*) FROM event_overrides").fetchone()[0] == 1

    conn.execute("DELETE FROM event_sessions WHERE id = ?", (sid,))
    conn.commit()
    assert conn.execute("SELECT COUNT(*) FROM event_overrides").fetchone()[0] == 0


# --------------------------------------------------------------------------- #
# write_hook_override
# --------------------------------------------------------------------------- #


def test_write_hook_override_first_write_lands(conn):
    _insert_event_session(conn, session_key="s:hook-1", client="claude")
    result = write_hook_override(
        conn,
        session_key="s:hook-1",
        event_key="tool_call:tu-1",
        event_type="tool_call",
        source="hook",
        confidence="high",
        lossiness="none",
        event_at=TS0,
        payload_json='{"precise": true}',
        origin="hook:pre-tool-use:v1",
    )
    assert result is True
    row = conn.execute("SELECT * FROM event_overrides").fetchone()
    assert row["event_key"] == "tool_call:tu-1"
    assert row["origin"] == "hook:pre-tool-use:v1"


def test_write_hook_override_strict_greater_guard_rejects_equal_confidence(conn):
    _insert_event_session(conn, session_key="s:hook-2", client="claude")
    common = dict(
        session_key="s:hook-2",
        event_key="tool_call:tu-2",
        event_type="tool_call",
        source="hook",
        lossiness="none",
        event_at=TS0,
        payload_json="{}",
        origin=None,
    )
    assert write_hook_override(conn, confidence="high", **common) is True
    assert write_hook_override(conn, confidence="high", **common) is False
    assert conn.execute("SELECT COUNT(*) FROM event_overrides").fetchone()[0] == 1


def test_write_hook_override_lower_rank_rejected(conn):
    _insert_event_session(conn, session_key="s:hook-3", client="claude")
    common = dict(
        session_key="s:hook-3",
        event_key="tool_call:tu-3",
        event_type="tool_call",
        source="hook",
        lossiness="none",
        event_at=TS0,
        payload_json="{}",
        origin=None,
    )
    assert write_hook_override(conn, confidence="high", **common) is True
    assert write_hook_override(conn, confidence="low", **common) is False
    stored_confidence = conn.execute(
        "SELECT confidence FROM event_overrides"
    ).fetchone()[0]
    assert stored_confidence == "high"


def test_write_hook_override_higher_rank_replaces(conn):
    _insert_event_session(conn, session_key="s:hook-4", client="claude")
    common = dict(
        session_key="s:hook-4",
        event_key="tool_call:tu-4",
        event_type="tool_call",
        source="hook",
        lossiness="none",
        event_at=TS0,
        origin=None,
    )
    assert write_hook_override(
        conn, confidence="low", payload_json='{"v": 1}', **common
    ) is True
    assert write_hook_override(
        conn, confidence="high", payload_json='{"v": 2}', **common
    ) is True
    row = conn.execute(
        "SELECT confidence, payload_json FROM event_overrides"
    ).fetchone()
    assert row["confidence"] == "high"
    assert row["payload_json"] == '{"v": 2}'


def test_write_hook_override_raises_on_unknown_session(conn):
    with pytest.raises(KeyError):
        write_hook_override(
            conn,
            session_key="s:nonexistent",
            event_key="tool_call:x",
            event_type="tool_call",
            source="hook",
            confidence="high",
            lossiness="none",
            event_at=TS0,
            payload_json="{}",
            origin=None,
        )


@pytest.mark.parametrize(
    "kwargs,match",
    [
        (dict(event_type="not_a_type"), "event type"),
        (dict(source="bogus"), "source"),
        (dict(confidence="missing"), "confidence"),
        (dict(confidence="medium-high"), "confidence"),
        (dict(lossiness="weird"), "lossiness"),
        (dict(event_key=""), "event_key"),
        (dict(payload_json="not json at all"), "payload_json"),
        (dict(payload_json=""), "payload_json"),
        (dict(payload_json=None), "payload_json must be a JSON string"),
    ],
)
def test_write_hook_override_validation(conn, kwargs, match):
    _insert_event_session(conn, session_key="s:validate", client="claude")
    base = dict(
        session_key="s:validate",
        event_key="tool_call:x",
        event_type="tool_call",
        source="hook",
        confidence="high",
        lossiness="none",
        event_at=TS0,
        payload_json="{}",
        origin=None,
    )
    base.update(kwargs)
    with pytest.raises(ValueError, match=match):
        write_hook_override(conn, **base)


# --------------------------------------------------------------------------- #
# canonical_events
# --------------------------------------------------------------------------- #


def test_canonical_events_base_only_session(conn):
    sid = _insert_event_session(conn, session_key="s:base", client="claude")
    _insert_event(
        conn,
        session_id=sid,
        type="user_message",
        event_key=None,
        event_at=TS0,
        source_offset=0,
        raw_json='{"type":"user"}',
    )
    _insert_event(
        conn,
        session_id=sid,
        type="tool_call",
        event_key="tool_call:tu-1",
        event_at=TS1,
        source_offset=50,
        raw_json='{"tool":"read"}',
    )

    events = list(canonical_events(conn, sid))
    assert [e.type for e in events] == ["user_message", "tool_call"]
    assert all(e.payload_json is None for e in events)
    assert all(e.origin is None for e in events)
    assert events[0].raw_ref == ("/tmp/demo.jsonl", 0, 0)
    assert events[1].raw_ref == ("/tmp/demo.jsonl", 50, 0)


def test_canonical_events_override_wins_preserves_base_raw_json(conn):
    sid = _insert_event_session(conn, session_key="s:override", client="claude")
    _insert_event(
        conn,
        session_id=sid,
        type="tool_call",
        event_key="tool_call:tu-2",
        confidence="low",
        raw_json='{"base":"low"}',
        source_offset=100,
    )
    write_hook_override(
        conn,
        session_key="s:override",
        event_key="tool_call:tu-2",
        event_type="tool_call",
        source="hook",
        confidence="high",
        lossiness="none",
        event_at=TS0,
        payload_json='{"override":"high"}',
        origin="hook:test",
    )

    [event] = list(canonical_events(conn, sid))
    assert event.confidence == "high"
    assert event.source == "hook"
    assert event.origin == "hook:test"
    assert event.payload_json == '{"override":"high"}'
    assert event.raw_json == '{"base":"low"}'  # base content preserved
    assert event.raw_ref == ("/tmp/demo.jsonl", 100, 0)


def test_canonical_events_override_rejected_when_lower_confidence(conn):
    sid = _insert_event_session(conn, session_key="s:reject", client="claude")
    _insert_event(
        conn,
        session_id=sid,
        type="tool_call",
        event_key="tool_call:tu-3",
        confidence="high",
        raw_json='{"base":"high"}',
    )
    write_hook_override(
        conn,
        session_key="s:reject",
        event_key="tool_call:tu-3",
        event_type="tool_call",
        source="hook",
        confidence="low",
        lossiness="none",
        event_at=TS0,
        payload_json='{"override":"low"}',
        origin="hook:test",
    )

    [event] = list(canonical_events(conn, sid))
    assert event.confidence == "high"
    assert event.source == "claude-jsonl"  # base
    assert event.payload_json is None
    assert event.origin is None
    assert event.raw_json == '{"base":"high"}'


def test_canonical_events_equal_rank_override_wins(conn):
    sid = _insert_event_session(conn, session_key="s:equal", client="claude")
    _insert_event(
        conn,
        session_id=sid,
        type="tool_call",
        event_key="tool_call:tu-4",
        confidence="high",
        raw_json='{"base":true}',
    )
    write_hook_override(
        conn,
        session_key="s:equal",
        event_key="tool_call:tu-4",
        event_type="tool_call",
        source="hook",
        confidence="high",
        lossiness="none",
        event_at=TS0,
        payload_json='{"override":true}',
        origin="hook:test",
    )

    [event] = list(canonical_events(conn, sid))
    assert event.source == "hook"
    assert event.payload_json == '{"override":true}'


def test_canonical_events_hook_only_has_no_raw_json(conn):
    sid = _insert_event_session(conn, session_key="s:hookonly", client="claude")
    write_hook_override(
        conn,
        session_key="s:hookonly",
        event_key="tool_call:novel",
        event_type="tool_call",
        source="hook",
        confidence="high",
        lossiness="none",
        event_at=TS0,
        payload_json='{"hook":true}',
        origin="hook:test",
    )

    [event] = list(canonical_events(conn, sid))
    assert event.type == "tool_call"
    assert event.event_key == "tool_call:novel"
    assert event.raw_json is None
    assert event.raw_ref is None
    assert event.payload_json == '{"hook":true}'
    assert event.origin == "hook:test"


def test_canonical_events_cross_source_dedup(conn):
    sid = _insert_event_session(conn, session_key="s:xsrc", client="claude")
    # Two base rows on different paths sharing an event_key. Canonical
    # order is (event_at, source_path, source_offset, seq) — so the path
    # that sorts first lexically wins. Paths chosen so the expected
    # winner is visible in the assertion.
    _insert_event(
        conn,
        session_id=sid,
        type="tool_call",
        event_key="tool_call:shared",
        event_at=TS0,
        source="claude-jsonl",
        source_path="/a_first.jsonl",
        raw_json='{"from":"first"}',
    )
    _insert_event(
        conn,
        session_id=sid,
        type="tool_call",
        event_key="tool_call:shared",
        event_at=TS0,
        source="claude-jsonl",
        source_path="/b_second.jsonl",
        raw_json='{"from":"second"}',
    )

    events = list(canonical_events(conn, sid))
    assert len(events) == 1
    assert events[0].raw_ref[0] == "/a_first.jsonl"


def test_canonical_events_prefer_source_drives_cross_source_winner(conn):
    sid = _insert_event_session(conn, session_key="s:xsrc2", client="claude")
    _insert_event(
        conn,
        session_id=sid,
        type="tool_call",
        event_key="tool_call:shared",
        event_at=TS0,
        source="claude-jsonl",
        source_path="/x.jsonl",
        raw_json="{}",
    )
    _insert_event(
        conn,
        session_id=sid,
        type="tool_call",
        event_key="tool_call:shared",
        event_at=TS0,
        source="codex-rollout",
        source_path="/y.jsonl",
        raw_json="{}",
    )
    # Filter to codex-rollout: the claude-jsonl row disappears before dedup.
    [event] = list(canonical_events(conn, sid, prefer_source="codex-rollout"))
    assert event.source == "codex-rollout"
    assert event.raw_ref[0] == "/y.jsonl"


def test_canonical_events_prefer_source_filters_base_rows(conn):
    sid = _insert_event_session(conn, session_key="s:prefer", client="claude")
    _insert_event(
        conn,
        session_id=sid,
        type="tool_call",
        event_key="tool_call:A",
        source_path="/native.jsonl",
        source="claude-jsonl",
        raw_json='{"from":"native"}',
    )
    # A "codex-rollout" row to exercise filtering — contrived but valid.
    _insert_event(
        conn,
        session_id=sid,
        type="tool_call",
        event_key="tool_call:B",
        source_path="/c.jsonl",
        source="codex-rollout",
        raw_json='{"from":"codex"}',
    )

    [event] = list(canonical_events(conn, sid, prefer_source="claude-jsonl"))
    assert event.event_key == "tool_call:A"


def test_canonical_events_null_event_key_passes_through_every_row(conn):
    sid = _insert_event_session(conn, session_key="s:null", client="claude")
    for offset in (0, 20, 40):
        _insert_event(
            conn,
            session_id=sid,
            type="schema_unknown",
            event_key=None,
            source_offset=offset,
            raw_json=f'{{"offset":{offset}}}',
        )
    events = list(canonical_events(conn, sid))
    assert [e.raw_ref[1] for e in events] == [0, 20, 40]


def test_canonical_events_ordering_mixes_base_then_hook_only(conn):
    sid = _insert_event_session(conn, session_key="s:order", client="claude")
    _insert_event(
        conn,
        session_id=sid,
        type="tool_call",
        event_key="tool_call:B1",
        event_at=TS1,
        raw_json="{}",
    )
    write_hook_override(
        conn,
        session_key="s:order",
        event_key="tool_call:novel",
        event_type="tool_call",
        source="hook",
        confidence="high",
        lossiness="none",
        event_at=TS0,
        payload_json="{}",
        origin=None,
    )

    events = list(canonical_events(conn, sid))
    # Base rows emit first (in event_at order), then hook-only overrides.
    assert [e.event_key for e in events] == ["tool_call:B1", "tool_call:novel"]


# --------------------------------------------------------------------------- #
# capability_join
# --------------------------------------------------------------------------- #


def test_capability_join_present_from_base(conn):
    sid = _insert_event_session(conn, session_key="s:cap-present", client="claude")
    _insert_event(
        conn,
        session_id=sid,
        type="tool_call",
        event_key="tool_call:x",
        raw_json="{}",
    )
    states = {s.event_type: s for s in capability_join(conn, sid)}
    assert states["tool_call"].state == "present"
    assert states["tool_call"].observed_base_count == 1
    assert states["tool_call"].observed_override_count == 0


def test_capability_join_missing_for_unsupported_type(conn):
    sid = _insert_event_session(conn, session_key="s:cap-missing", client="codex")
    states = {s.event_type: s for s in capability_join(conn, sid)}
    # codex matrix: stdout_chunk / stderr_chunk / compaction are all False
    for t in ("stdout_chunk", "stderr_chunk", "compaction"):
        assert states[t].state == "missing"
        assert states[t].observed_base_count == 0
        assert states[t].observed_override_count == 0


def test_capability_join_supported_but_absent(conn):
    sid = _insert_event_session(
        conn, session_key="s:cap-absent", client="codex"
    )
    states = {s.event_type: s for s in capability_join(conn, sid)}
    # codex matrix: tool_call is supported but we've not inserted any events.
    assert states["tool_call"].state == "supported_but_absent"
    assert states["tool_call"].observed_base_count == 0


def test_capability_join_override_only_flips_to_present(conn):
    sid = _insert_event_session(
        conn, session_key="s:cap-override", client="codex"
    )
    # codex doesn't emit stdout_chunk — if a hook writes an override, it flips.
    write_hook_override(
        conn,
        session_key="s:cap-override",
        event_key="stdout_chunk:c1",
        event_type="stdout_chunk",
        source="hook",
        confidence="high",
        lossiness="none",
        event_at=TS0,
        payload_json="{}",
        origin=None,
    )
    states = {s.event_type: s for s in capability_join(conn, sid)}
    assert states["stdout_chunk"].state == "present"
    assert states["stdout_chunk"].observed_base_count == 0
    assert states["stdout_chunk"].observed_override_count == 1


def test_capability_join_raises_on_unknown_session(conn):
    with pytest.raises(KeyError):
        capability_join(conn, 999_999)


# --------------------------------------------------------------------------- #
# fetch_vendor_line
# --------------------------------------------------------------------------- #


def test_fetch_vendor_line_existing_line(tmp_path):
    path = tmp_path / "t.jsonl"
    line1 = b'{"a":1}\n'
    line2 = b'{"b":2}\n'
    path.write_bytes(line1 + line2)
    assert fetch_vendor_line(path, 0) == '{"a":1}'
    assert fetch_vendor_line(path, len(line1)) == '{"b":2}'


def test_fetch_vendor_line_missing_file(tmp_path):
    assert fetch_vendor_line(tmp_path / "nonexistent.jsonl", 0) is None


def test_fetch_vendor_line_past_eof_returns_none(tmp_path):
    path = tmp_path / "t.jsonl"
    path.write_bytes(b'{"a":1}\n')
    assert fetch_vendor_line(path, 9999) is None


def test_fetch_vendor_line_partial_trailing_line_returns_none(tmp_path):
    path = tmp_path / "t.jsonl"
    path.write_bytes(b'{"a":1}\n{"incomplete":true')
    # First line still works.
    assert fetch_vendor_line(path, 0) == '{"a":1}'
    # Partial line (no newline after offset) → None.
    assert fetch_vendor_line(path, len(b'{"a":1}\n')) is None


def test_fetch_vendor_line_strips_crlf(tmp_path):
    path = tmp_path / "t.jsonl"
    path.write_bytes(b'{"a":1}\r\n')
    assert fetch_vendor_line(path, 0) == '{"a":1}'


def test_fetch_vendor_line_handles_utf8(tmp_path):
    payload = {"msg": "你好 🙂"}
    line = (json.dumps(payload) + "\n").encode("utf-8")
    path = tmp_path / "t.jsonl"
    path.write_bytes(line)
    result = fetch_vendor_line(path, 0)
    assert result is not None
    assert json.loads(result) == payload


def test_fetch_vendor_line_enforces_safety_cap(tmp_path, monkeypatch):
    """A file with no newline within the safety cap returns None rather
    than reading the whole blob into memory."""
    from clawjournal.events import view as view_module

    monkeypatch.setattr(view_module, "_MAX_LINE_BYTES", 8)
    path = tmp_path / "huge.jsonl"
    # 32 bytes with no newline — well past the 8-byte cap.
    path.write_bytes(b"x" * 32)
    assert fetch_vendor_line(path, 0) is None


def test_fetch_vendor_line_rejects_newline_beyond_safety_cap(tmp_path, monkeypatch):
    from clawjournal.events import view as view_module

    monkeypatch.setattr(view_module, "_MAX_LINE_BYTES", 8)
    monkeypatch.setattr(view_module, "_FETCH_CHUNK", 32)
    path = tmp_path / "beyond-cap.jsonl"
    path.write_bytes(b"123456789\n")
    assert fetch_vendor_line(path, 0) is None


def test_fetch_vendor_line_short_line_under_cap_still_works(tmp_path, monkeypatch):
    from clawjournal.events import view as view_module

    monkeypatch.setattr(view_module, "_MAX_LINE_BYTES", 1024)
    path = tmp_path / "small.jsonl"
    path.write_bytes(b'{"ok":1}\n')
    assert fetch_vendor_line(path, 0) == '{"ok":1}'
