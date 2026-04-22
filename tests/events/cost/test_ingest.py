"""End-to-end tests for ingest_cost_pending + anomaly detection."""

from __future__ import annotations

import json
import sqlite3

import pytest

from clawjournal.events.cost import (
    ANOMALY_KINDS,
    PRICING_TABLE_VERSION,
    ensure_cost_schema,
    ingest_cost_pending,
)
from clawjournal.events.schema import ensure_schema as ensure_events_schema


TS = "2026-04-20T10:00:00Z"


# --------------------------------------------------------------------------- #
# fixture helpers
# --------------------------------------------------------------------------- #


@pytest.fixture
def conn() -> sqlite3.Connection:
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    ensure_events_schema(c)
    ensure_cost_schema(c)
    return c


def _insert_session(conn, *, session_key: str, client: str) -> int:
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
    event_at: str | None = TS,
    source: str | None = None,
    source_path: str = "/tmp/x.jsonl",
) -> int:
    if source is None:
        source = {"claude": "claude-jsonl", "codex": "codex-rollout", "openclaw": "openclaw-jsonl"}[client]
    # monotonically increasing offset so ORDER BY source_offset is stable.
    cur = conn.execute("SELECT COALESCE(MAX(source_offset), -1) + 1 FROM events").fetchone()
    offset = int(cur[0])
    cursor = conn.execute(
        """
        INSERT INTO events (
            session_id, type, event_key, event_at, ingested_at, source,
            source_path, source_offset, seq, client, confidence, lossiness,
            raw_json
        ) VALUES (?, ?, NULL, ?, ?, ?, ?, ?, 0, ?, 'high', 'none', ?)
        """,
        (
            session_id, event_type, event_at, TS, source, source_path,
            offset, client, json.dumps(raw, sort_keys=True),
        ),
    )
    conn.commit()
    return int(cursor.lastrowid)


def _claude_assistant(
    *,
    model: str = "claude-opus-4-6",
    input_tokens: int = 100,
    output_tokens: int = 50,
    cache_read: int | None = None,
    cache_write: int | None = None,
    service_tier: str | None = None,
) -> dict:
    usage: dict = {"input_tokens": input_tokens, "output_tokens": output_tokens}
    if cache_read is not None:
        usage["cache_read_input_tokens"] = cache_read
    if cache_write is not None:
        usage["cache_creation_input_tokens"] = cache_write
    if service_tier is not None:
        usage["service_tier"] = service_tier
    return {
        "type": "assistant",
        "message": {"model": model, "usage": usage},
    }


# --------------------------------------------------------------------------- #
# basic ingest
# --------------------------------------------------------------------------- #


def test_ingest_writes_token_usage_for_claude_assistant(conn):
    sid = _insert_session(conn, session_key="s:claude", client="claude")
    eid = _insert_event(
        conn,
        session_id=sid,
        client="claude",
        event_type="assistant_message",
        raw=_claude_assistant(input_tokens=10, output_tokens=5, cache_read=200),
    )

    summary = ingest_cost_pending(conn)
    assert summary.token_rows_written == 1
    assert summary.sessions_touched == {sid}

    row = conn.execute("SELECT * FROM token_usage WHERE event_id = ?", (eid,)).fetchone()
    assert row is not None
    assert row["model"] == "claude-opus-4-6"
    assert row["model_family"] == "claude"
    assert row["model_tier"] == "opus"
    assert row["input"] == 10
    assert row["output"] == 5
    assert row["cache_read"] == 200
    assert row["cache_write"] is None  # never emitted → stays NULL, not zero
    assert row["data_source"] == "api"
    assert row["pricing_table_version"] == PRICING_TABLE_VERSION
    assert row["cost_estimate"] is not None and row["cost_estimate"] > 0


def test_ingest_is_idempotent(conn):
    sid = _insert_session(conn, session_key="s:idem", client="claude")
    _insert_event(
        conn, session_id=sid, client="claude", event_type="assistant_message",
        raw=_claude_assistant(),
    )

    first = ingest_cost_pending(conn)
    second = ingest_cost_pending(conn)

    assert first.token_rows_written == 1
    # Re-running picks no new events because token_usage already covers them.
    assert second.events_scanned == 0
    assert second.token_rows_written == 0
    assert second.anomalies_written == 0


def test_ingest_skips_unparseable_raw_json(conn):
    sid = _insert_session(conn, session_key="s:bad", client="claude")
    # Manually insert a row whose raw_json isn't valid JSON.
    conn.execute(
        """
        INSERT INTO events (
            session_id, type, event_key, event_at, ingested_at, source,
            source_path, source_offset, seq, client, confidence, lossiness,
            raw_json
        ) VALUES (?, 'schema_unknown', NULL, ?, ?, 'claude-jsonl',
                  '/tmp/x.jsonl', 0, 0, 'claude', 'low', 'unknown', ?)
        """,
        (sid, TS, TS, "{not json"),
    )
    conn.commit()

    summary = ingest_cost_pending(conn)
    assert summary.token_rows_written == 0


def test_ingest_does_not_record_estimated_rows_today(conn):
    """v0.1 ingest only persists vendor-emitted (api) usage. A turn
    with no usage block produces no token_usage row."""
    sid = _insert_session(conn, session_key="s:noapi", client="claude")
    _insert_event(
        conn, session_id=sid, client="claude", event_type="assistant_message",
        raw={"type": "assistant", "message": {"model": "claude-opus-4-6"}},  # no usage
    )
    summary = ingest_cost_pending(conn)
    assert summary.token_rows_written == 0


def test_ingest_advances_past_examined_non_usage_rows(conn):
    sid = _insert_session(conn, session_key="s:examined", client="claude")
    eid = _insert_event(
        conn, session_id=sid, client="claude", event_type="assistant_message",
        raw={"type": "assistant", "message": {"model": "claude-opus-4-6"}},
    )

    first = ingest_cost_pending(conn)
    second = ingest_cost_pending(conn)

    assert first.events_scanned == 1
    assert first.token_rows_written == 0
    assert second.events_scanned == 0
    assert second.token_rows_written == 0

    # Historical backfill is still possible via explicit rebuild.
    conn.execute(
        "UPDATE events SET raw_json = ? WHERE id = ?",
        (json.dumps(_claude_assistant(input_tokens=10, output_tokens=5), sort_keys=True), eid),
    )
    conn.commit()

    third = ingest_cost_pending(conn)
    rebuilt = ingest_cost_pending(conn, rebuild=True)

    assert third.events_scanned == 0
    assert rebuilt.events_scanned == 1
    assert rebuilt.token_rows_written == 1
    row = conn.execute("SELECT * FROM token_usage WHERE event_id = ?", (eid,)).fetchone()
    assert row is not None
    assert row["input"] == 10
    assert row["output"] == 5


# --------------------------------------------------------------------------- #
# Codex model threading
# --------------------------------------------------------------------------- #


def _codex_token_count(
    *,
    input_tokens: int,
    output_tokens: int = 10,
    cached: int | None = None,
    reasoning: int | None = None,
) -> dict:
    last: dict = {"input_tokens": input_tokens, "output_tokens": output_tokens}
    if cached is not None:
        last["cached_input_tokens"] = cached
    if reasoning is not None:
        last["reasoning_output_tokens"] = reasoning
    return {
        "type": "event_msg",
        "payload": {
            "type": "token_count",
            "info": {"last_token_usage": last},
        },
    }


def test_ingest_threads_codex_model_from_turn_context(conn):
    sid = _insert_session(conn, session_key="s:codex", client="codex")
    _insert_event(
        conn, session_id=sid, client="codex", event_type="schema_unknown",
        raw={"type": "turn_context", "payload": {"model": "gpt-5-codex"}},
        event_at="2026-04-20T10:00:00Z",
    )
    eid = _insert_event(
        conn, session_id=sid, client="codex", event_type="schema_unknown",
        raw=_codex_token_count(input_tokens=3655, cached=2048, reasoning=64),
        event_at="2026-04-20T10:00:01Z",
    )

    ingest_cost_pending(conn)
    row = conn.execute("SELECT * FROM token_usage WHERE event_id = ?", (eid,)).fetchone()
    assert row is not None
    assert row["model"] == "gpt-5-codex"
    assert row["input"] == 3655
    assert row["cache_read"] == 2048
    assert row["reasoning"] == 64
    assert row["data_source"] == "api"
    assert row["cost_estimate"] is not None and row["cost_estimate"] > 0


def test_ingest_threads_codex_model_from_prior_run_turn_context(conn):
    sid = _insert_session(conn, session_key="s:codex-prior", client="codex")
    _insert_event(
        conn, session_id=sid, client="codex", event_type="schema_unknown",
        raw={"type": "turn_context", "payload": {"model": "gpt-5.3-codex"}},
        event_at="2026-04-20T10:00:00Z",
    )
    first = ingest_cost_pending(conn)
    assert first.events_scanned == 1
    assert first.token_rows_written == 0

    eid = _insert_event(
        conn, session_id=sid, client="codex", event_type="schema_unknown",
        raw=_codex_token_count(input_tokens=3655, cached=2048, reasoning=64),
        event_at="2026-04-20T10:00:01Z",
    )

    second = ingest_cost_pending(conn)
    row = conn.execute("SELECT * FROM token_usage WHERE event_id = ?", (eid,)).fetchone()
    assert second.events_scanned == 1
    assert second.token_rows_written == 1
    assert row is not None
    assert row["model"] == "gpt-5.3-codex"
    assert row["model_tier"] == "5.3-codex"
    assert row["cost_estimate"] is not None and row["cost_estimate"] > 0


def test_ingest_threads_codex_model_from_null_timestamp_turn_context(conn):
    sid = _insert_session(conn, session_key="s:codex-null-ts", client="codex")
    _insert_event(
        conn, session_id=sid, client="codex", event_type="schema_unknown",
        raw={"type": "turn_context", "payload": {"model": "gpt-5-codex"}},
        event_at=None,
    )
    eid = _insert_event(
        conn, session_id=sid, client="codex", event_type="schema_unknown",
        raw=_codex_token_count(input_tokens=3655, cached=2048, reasoning=64),
        event_at="2026-04-20T10:00:01Z",
    )

    ingest_cost_pending(conn)
    row = conn.execute("SELECT * FROM token_usage WHERE event_id = ?", (eid,)).fetchone()
    assert row is not None
    assert row["model"] == "gpt-5-codex"
    assert row["cost_estimate"] is not None and row["cost_estimate"] > 0


# --------------------------------------------------------------------------- #
# anomaly detectors — acceptance criteria from 04-cost-ledger.md
# --------------------------------------------------------------------------- #


def test_cache_read_collapse_flagged_on_warm_to_cold_transition(conn):
    sid = _insert_session(conn, session_key="s:collapse", client="claude")
    _insert_event(
        conn, session_id=sid, client="claude", event_type="assistant_message",
        raw=_claude_assistant(input_tokens=1000, cache_read=10_000),
        event_at="2026-04-20T10:00:00Z",
    )
    _insert_event(
        conn, session_id=sid, client="claude", event_type="assistant_message",
        raw=_claude_assistant(input_tokens=1000, cache_read=100),
        event_at="2026-04-20T10:00:01Z",
    )

    ingest_cost_pending(conn)
    anomalies = conn.execute(
        "SELECT kind, evidence_json FROM cost_anomalies WHERE session_id = ?",
        (sid,),
    ).fetchall()
    kinds = [a["kind"] for a in anomalies]
    assert "cache_read_collapse" in kinds
    evidence = json.loads(
        next(a["evidence_json"] for a in anomalies if a["kind"] == "cache_read_collapse")
    )
    assert evidence["previous_cache_read"] == 10_000
    assert evidence["current_cache_read"] == 100
    assert evidence["drop_ratio"] >= 0.5


def test_cache_read_collapse_skipped_when_estimated_row_present(conn):
    """Anomaly detectors must not compare api against estimated.

    We simulate this by manually marking one row as estimated.
    """
    sid = _insert_session(conn, session_key="s:est", client="claude")
    _insert_event(
        conn, session_id=sid, client="claude", event_type="assistant_message",
        raw=_claude_assistant(input_tokens=1000, cache_read=10_000),
        event_at="2026-04-20T10:00:00Z",
    )
    _insert_event(
        conn, session_id=sid, client="claude", event_type="assistant_message",
        raw=_claude_assistant(input_tokens=1000, cache_read=100),
        event_at="2026-04-20T10:00:01Z",
    )

    ingest_cost_pending(conn)
    # Demote the second row to estimated and re-run anomaly detection.
    conn.execute(
        "UPDATE token_usage SET data_source = 'estimated' "
        "WHERE event_at = '2026-04-20T10:00:01Z'"
    )
    conn.execute("DELETE FROM cost_anomalies")
    conn.commit()

    from clawjournal.events.cost.anomalies import detect_session_anomalies

    hits = detect_session_anomalies(conn, sid)
    cache_hits = [h for h in hits if h.kind == "cache_read_collapse"]
    assert cache_hits == []  # estimated row breaks the comparison chain


def test_input_spike_flagged_against_rolling_baseline(conn):
    sid = _insert_session(conn, session_key="s:spike", client="claude")
    # Five baseline turns at 100 input tokens, then one at 1000.
    for i in range(5):
        _insert_event(
            conn, session_id=sid, client="claude", event_type="assistant_message",
            raw=_claude_assistant(input_tokens=100, output_tokens=10),
            event_at=f"2026-04-20T10:00:0{i}Z",
        )
    _insert_event(
        conn, session_id=sid, client="claude", event_type="assistant_message",
        raw=_claude_assistant(input_tokens=1000, output_tokens=10),
        event_at="2026-04-20T10:00:05Z",
    )

    ingest_cost_pending(conn)
    spikes = [
        a for a in conn.execute(
            "SELECT kind, evidence_json FROM cost_anomalies WHERE session_id = ?",
            (sid,),
        )
        if a["kind"] == "input_spike"
    ]
    assert len(spikes) == 1
    evidence = json.loads(spikes[0]["evidence_json"])
    assert evidence["current_input"] == 1000
    assert evidence["baseline_mean"] == pytest.approx(100)
    assert evidence["ratio"] >= 3.0


def test_input_spike_requires_full_baseline_window(conn):
    sid = _insert_session(conn, session_key="s:spike-short", client="claude")
    for i, input_tokens in enumerate((100, 400)):
        _insert_event(
            conn, session_id=sid, client="claude", event_type="assistant_message",
            raw=_claude_assistant(input_tokens=input_tokens, output_tokens=10),
            event_at=f"2026-04-20T10:00:0{i}Z",
        )

    ingest_cost_pending(conn)
    kinds = [
        a["kind"]
        for a in conn.execute(
            "SELECT kind FROM cost_anomalies WHERE session_id = ?",
            (sid,),
        )
    ]
    assert "input_spike" not in kinds


def test_model_shift_detected_within_session(conn):
    sid = _insert_session(conn, session_key="s:shift", client="claude")
    _insert_event(
        conn, session_id=sid, client="claude", event_type="assistant_message",
        raw=_claude_assistant(model="claude-opus-4-6", input_tokens=10),
        event_at="2026-04-20T10:00:00Z",
    )
    _insert_event(
        conn, session_id=sid, client="claude", event_type="assistant_message",
        raw=_claude_assistant(model="claude-sonnet-4-6", input_tokens=10),
        event_at="2026-04-20T10:00:01Z",
    )

    ingest_cost_pending(conn)
    kinds = {
        a["kind"]
        for a in conn.execute(
            "SELECT kind FROM cost_anomalies WHERE session_id = ?", (sid,)
        )
    }
    assert "model_shift" in kinds


def test_service_tier_shift_distinct_from_model_shift(conn):
    sid = _insert_session(conn, session_key="s:tier", client="claude")
    _insert_event(
        conn, session_id=sid, client="claude", event_type="assistant_message",
        raw=_claude_assistant(input_tokens=10, service_tier="standard"),
        event_at="2026-04-20T10:00:00Z",
    )
    _insert_event(
        conn, session_id=sid, client="claude", event_type="assistant_message",
        raw=_claude_assistant(input_tokens=10, service_tier="priority"),
        event_at="2026-04-20T10:00:01Z",
    )

    ingest_cost_pending(conn)
    rows = list(conn.execute(
        "SELECT kind FROM cost_anomalies WHERE session_id = ?", (sid,)
    ))
    kinds = [r["kind"] for r in rows]
    assert "service_tier_shift" in kinds
    # Model didn't change → no model_shift recorded.
    assert "model_shift" not in kinds


def test_anomaly_recompute_drops_stale_rows(conn):
    sid = _insert_session(conn, session_key="s:stale", client="claude")
    _insert_event(
        conn, session_id=sid, client="claude", event_type="assistant_message",
        raw=_claude_assistant(input_tokens=1000, cache_read=10_000),
        event_at="2026-04-20T10:00:00Z",
    )
    _insert_event(
        conn, session_id=sid, client="claude", event_type="assistant_message",
        raw=_claude_assistant(input_tokens=1000, cache_read=100),
        event_at="2026-04-20T10:00:02Z",
    )

    first = ingest_cost_pending(conn)
    assert first.anomalies_written == 1

    _insert_event(
        conn, session_id=sid, client="claude", event_type="assistant_message",
        raw=_claude_assistant(input_tokens=10, cache_read=50),
        event_at="2026-04-20T10:00:01Z",
    )
    second = ingest_cost_pending(conn)

    assert second.anomalies_written == 0
    rows = list(
        conn.execute(
            "SELECT kind FROM cost_anomalies WHERE session_id = ?",
            (sid,),
        )
    )
    assert rows == []


def test_anomalies_unique_constraint_idempotent(conn):
    sid = _insert_session(conn, session_key="s:dup", client="claude")
    _insert_event(
        conn, session_id=sid, client="claude", event_type="assistant_message",
        raw=_claude_assistant(model="claude-opus-4-6", input_tokens=10),
        event_at="2026-04-20T10:00:00Z",
    )
    _insert_event(
        conn, session_id=sid, client="claude", event_type="assistant_message",
        raw=_claude_assistant(model="claude-sonnet-4-6", input_tokens=10),
        event_at="2026-04-20T10:00:01Z",
    )
    ingest_cost_pending(conn)
    n1 = conn.execute("SELECT COUNT(*) FROM cost_anomalies").fetchone()[0]

    # Force the anomaly detectors to run again by clearing the
    # token_usage cache hint and re-ingesting; rows already covered
    # by a token_usage entry are skipped, but anomaly INSERT OR IGNORE
    # should suppress duplicates either way.
    from clawjournal.events.cost.anomalies import detect_session_anomalies
    hits = detect_session_anomalies(conn, sid)
    with conn:
        conn.executemany(
            "INSERT OR IGNORE INTO cost_anomalies "
            "(session_id, turn_event_id, kind, confidence, evidence_json, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            [
                (h.session_id, h.turn_event_id, h.kind, h.confidence,
                 json.dumps(h.evidence, sort_keys=True), TS)
                for h in hits
            ],
        )
    n2 = conn.execute("SELECT COUNT(*) FROM cost_anomalies").fetchone()[0]
    assert n1 == n2  # dedup held


# --------------------------------------------------------------------------- #
# constants / api surface
# --------------------------------------------------------------------------- #


def test_anomaly_kinds_set_matches_spec():
    assert set(ANOMALY_KINDS) == {
        "cache_read_collapse",
        "input_spike",
        "model_shift",
        "service_tier_shift",
    }
