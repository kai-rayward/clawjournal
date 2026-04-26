"""End-to-end query execution tests (plan 10).

Each test seeds a small SQLite fixture in-memory, runs the
aggregator, and asserts the bucket shape, counts, ordering, and
metadata. Includes the SQL injection regression and the
cost-domain auto-partitioning invariant.
"""

from __future__ import annotations

import sqlite3

import pytest

from clawjournal.events.aggregate import (
    AggregationSpec,
    Metric,
    Predicate,
    run,
)
from clawjournal.events.cost.schema import ensure_cost_schema
from clawjournal.events.incidents.schema import ensure_incidents_schema
from clawjournal.events.schema import ensure_schema as ensure_events_schema


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    ensure_events_schema(c)
    ensure_cost_schema(c)
    ensure_incidents_schema(c)
    yield c
    c.close()


def _add_session(conn, session_key, client="claude"):
    cur = conn.execute(
        "INSERT INTO event_sessions (session_key, client, started_at, status) "
        "VALUES (?, ?, '2026-04-21T10:00:00Z', 'ended')",
        (session_key, client),
    )
    return cur.lastrowid


def _add_event(
    conn,
    session_id,
    *,
    type_="user_message",
    client="claude",
    source="claude-jsonl",
    confidence="high",
    event_at="2026-04-21T10:00:00Z",
    seq=None,
    source_path="/x",
):
    if seq is None:
        seq = _add_event.counter
        _add_event.counter += 1
    conn.execute(
        "INSERT INTO events "
        "(session_id, type, event_at, ingested_at, source, source_path, "
        " source_offset, seq, client, confidence, lossiness, raw_json) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            session_id, type_, event_at, "2026-04-21T10:00:00Z",
            source, source_path, 0, seq, client, confidence, "none", "{}",
        ),
    )


_add_event.counter = 0


def test_events_count_by_client_type(conn):
    sid_a = _add_session(conn, "claude:proj_a:s1")
    sid_b = _add_session(conn, "codex:/path/to/proj_b", client="codex")
    for _ in range(3):
        _add_event(conn, sid_a, type_="user_message", client="claude")
    for _ in range(5):
        _add_event(conn, sid_a, type_="tool_call", client="claude")
    for _ in range(2):
        _add_event(
            conn, sid_b, type_="user_message", client="codex",
            source="codex-rollout",
        )
    spec = AggregationSpec(
        domain="events",
        dimensions=("client", "type"),
        metrics=(Metric(kind="count"),),
        limit=10,
    )
    result = run(spec, conn)
    assert result.total == 10
    assert result.rows_scanned == 10
    assert result.other_count == 0
    assert [b["count"] for b in result.buckets] == [5, 3, 2]
    assert result.buckets[0]["key"] == {"client": "claude", "type": "tool_call"}


def test_events_filter_and_since(conn):
    sid = _add_session(conn, "claude:proj:s1")
    _add_event(conn, sid, event_at="2026-04-15T10:00:00Z", type_="tool_call")
    _add_event(conn, sid, event_at="2026-04-21T10:00:00Z", type_="tool_call")
    _add_event(conn, sid, event_at="2026-04-21T10:00:00Z", type_="user_message")

    spec = AggregationSpec(
        domain="events",
        dimensions=("type",),
        metrics=(Metric(kind="count"),),
        filters=(Predicate(field="client", op="=", value="claude"),),
        since_iso="2026-04-20T00:00:00Z",
        limit=10,
    )
    result = run(spec, conn)
    types = {b["key"]["type"]: b["count"] for b in result.buckets}
    assert types == {"tool_call": 1, "user_message": 1}
    assert result.rows_scanned == 2


def test_events_in_filter(conn):
    sid_a = _add_session(conn, "claude:proj:s1")
    sid_b = _add_session(conn, "codex:/path/proj", client="codex")
    sid_c = _add_session(conn, "openclaw:/path/proj", client="openclaw")
    _add_event(conn, sid_a, client="claude")
    _add_event(conn, sid_b, client="codex", source="codex-rollout")
    _add_event(conn, sid_c, client="openclaw", source="openclaw-jsonl")

    spec = AggregationSpec(
        domain="events",
        dimensions=("client",),
        metrics=(Metric(kind="count"),),
        filters=(
            Predicate(field="client", op="in", value=("claude", "codex")),
        ),
        limit=10,
    )
    result = run(spec, conn)
    keys = {b["key"]["client"] for b in result.buckets}
    assert keys == {"claude", "codex"}


def test_events_other_count_when_truncated(conn):
    sid = _add_session(conn, "claude:proj:s1")
    # 6 distinct sources, descending count.
    for type_, n in [("a", 5), ("b", 4), ("c", 3), ("d", 2), ("e", 1), ("f", 1)]:
        for _ in range(n):
            _add_event(conn, sid, type_=type_)
    spec = AggregationSpec(
        domain="events",
        dimensions=("type",),
        metrics=(Metric(kind="count"),),
        limit=2,
    )
    result = run(spec, conn)
    assert len(result.buckets) == 2
    assert [b["count"] for b in result.buckets] == [5, 4]
    # total is 16; top 2 = 9; other_count = 7.
    assert result.total == 16
    assert result.other_count == 7


def test_sql_injection_in_value_is_parameterized(conn):
    """Plan 10 §Security #1: malicious --where value must not return
    extra rows. With ``client="' OR 1=1 --"``, the predicate becomes
    a literal string compare against client column → zero matches."""

    sid = _add_session(conn, "claude:proj:s1")
    _add_event(conn, sid, client="claude")
    _add_event(conn, sid, client="claude")

    spec = AggregationSpec(
        domain="events",
        dimensions=("client",),
        metrics=(Metric(kind="count"),),
        filters=(
            Predicate(field="client", op="=", value="' OR 1=1 --"),
        ),
        limit=10,
    )
    result = run(spec, conn)
    # The injection is treated as a literal; matches no rows.
    assert result.buckets == []
    assert result.total == 0


def test_workspace_dimension_extracts_segment(conn):
    sid_claude = _add_session(conn, "claude:proj_a:s1")
    sid_codex = _add_session(
        conn, "codex:/Users/synthetic-user/workspace_b", client="codex"
    )
    _add_event(conn, sid_claude, client="claude")
    _add_event(conn, sid_codex, client="codex", source="codex-rollout")

    spec = AggregationSpec(
        domain="events",
        dimensions=("workspace",),
        metrics=(Metric(kind="count"),),
        limit=10,
    )
    result = run(spec, conn)
    workspaces = {b["key"]["workspace"] for b in result.buckets}
    assert "proj_a" in workspaces
    assert "/Users/synthetic-user/workspace_b" in workspaces


def test_date_dimension_buckets_by_day(conn):
    sid = _add_session(conn, "claude:proj:s1")
    _add_event(conn, sid, event_at="2026-04-21T10:00:00Z")
    _add_event(conn, sid, event_at="2026-04-21T15:00:00Z")
    _add_event(conn, sid, event_at="2026-04-22T03:00:00Z")
    spec = AggregationSpec(
        domain="events",
        dimensions=("date",),
        metrics=(Metric(kind="count"),),
        limit=10,
    )
    result = run(spec, conn)
    counts = {b["key"]["date"]: b["count"] for b in result.buckets}
    assert counts == {"2026-04-21": 2, "2026-04-22": 1}


def test_multi_metric(conn):
    """plan 10: ``--metric count,sum:input_tokens`` returns both per
    bucket. cost domain has the numeric metric_fields."""

    sid = _add_session(conn, "claude:proj:s1")
    _add_event(conn, sid)
    _add_event(conn, sid)
    eid_rows = [r[0] for r in conn.execute("SELECT id FROM events").fetchall()]
    for eid in eid_rows:
        conn.execute(
            "INSERT INTO token_usage "
            "(event_id, session_id, model, input, output, data_source, event_at) "
            "VALUES (?, ?, 'claude-3-5-sonnet', 100, 50, 'api', '2026-04-21T10:00:00Z')",
            (eid, sid),
        )
    spec = AggregationSpec(
        domain="cost",
        dimensions=("model",),
        metrics=(
            Metric(kind="count"),
            Metric(kind="sum", field="input_tokens"),
        ),
        # ``data_source=api`` filter so auto-partition doesn't fire and
        # alter the dimension order.
        filters=(Predicate(field="data_source", op="=", value="api"),),
        limit=10,
    )
    result = run(spec, conn)
    assert len(result.buckets) == 1
    bucket = result.buckets[0]
    assert bucket["count"] == 2
    assert bucket["sum_input_tokens"] == 200


def test_cost_auto_partitions_by_data_source(conn):
    """plan 10 §Security #5: cost queries without data_source in
    --by or --where get auto-partitioned so API + estimates never
    silently mix."""

    sid = _add_session(conn, "claude:proj:s1")
    _add_event(conn, sid)
    _add_event(conn, sid)
    eids = [r[0] for r in conn.execute("SELECT id FROM events").fetchall()]
    conn.execute(
        "INSERT INTO token_usage "
        "(event_id, session_id, model, input, data_source, event_at) "
        "VALUES (?, ?, 'claude-3-5-sonnet', 100, 'api', '2026-04-21T10:00:00Z')",
        (eids[0], sid),
    )
    conn.execute(
        "INSERT INTO token_usage "
        "(event_id, session_id, model, input, data_source, event_at) "
        "VALUES (?, ?, 'claude-3-5-sonnet', 200, 'estimated', '2026-04-21T10:00:00Z')",
        (eids[1], sid),
    )

    spec = AggregationSpec(
        domain="cost",
        dimensions=("model",),
        metrics=(Metric(kind="sum", field="input_tokens"),),
        limit=10,
    )
    result = run(spec, conn)
    assert result.auto_partitioned is True
    assert "data_source" in result.spec.dimensions
    # Two buckets — one per data_source — never silently merged.
    sums = {b["key"]["data_source"]: b["sum_input_tokens"] for b in result.buckets}
    assert sums == {"api": 100, "estimated": 200}


def test_cost_explicit_data_source_filter_disables_partition(conn):
    sid = _add_session(conn, "claude:proj:s1")
    _add_event(conn, sid)
    eid = conn.execute("SELECT id FROM events").fetchone()[0]
    conn.execute(
        "INSERT INTO token_usage "
        "(event_id, session_id, model, input, data_source, event_at) "
        "VALUES (?, ?, 'claude-3-5-sonnet', 100, 'api', '2026-04-21T10:00:00Z')",
        (eid, sid),
    )
    spec = AggregationSpec(
        domain="cost",
        dimensions=("model",),
        metrics=(Metric(kind="sum", field="input_tokens"),),
        filters=(Predicate(field="data_source", op="=", value="api"),),
        limit=10,
    )
    result = run(spec, conn)
    assert result.auto_partitioned is False
    assert result.spec.dimensions == ("model",)


def test_incidents_count_by_kind(conn):
    sid = _add_session(conn, "claude:proj:s1")
    eid = 0
    for _ in range(2):
        _add_event(conn, sid)
        eid = conn.execute("SELECT MAX(id) FROM events").fetchone()[0]
    conn.execute(
        "INSERT INTO incidents "
        "(session_id, kind, first_event_id, last_event_id, evidence_json, "
        " count, confidence, created_at) "
        "VALUES (?, 'loop_exact_repeat', ?, ?, '{}', 3, 'high', "
        "'2026-04-21T10:00:00Z')",
        (sid, eid, eid),
    )

    spec = AggregationSpec(
        domain="incidents",
        dimensions=("kind",),
        metrics=(Metric(kind="count"),),
        limit=10,
    )
    result = run(spec, conn)
    assert result.buckets == [
        {"key": {"kind": "loop_exact_repeat"}, "count": 1}
    ]


def test_metric_field_must_be_numeric(conn):
    """Refusing avg/sum on a non-numeric registered field surfaces a
    clear error rather than producing nonsense SQL."""

    sid = _add_session(conn, "claude:proj:s1")
    _add_event(conn, sid)
    spec = AggregationSpec(
        domain="cost",
        dimensions=("model",),
        metrics=(Metric(kind="sum", field="model"),),
        filters=(Predicate(field="data_source", op="=", value="api"),),
        limit=10,
    )
    with pytest.raises(ValueError):
        run(spec, conn)


def test_sum_of_real_column_preserves_float_type(conn):
    """Round 5: SUM over a REAL column (token_usage.cost_estimate)
    must always emit float in the bucket, even when the total
    happens to be a whole number. JSON consumers that parse
    ``sum_cost_estimate`` as a float-typed value should never get
    an int back depending on data luck."""

    sid = _add_session(conn, "claude:proj:s1")
    _add_event(conn, sid)
    _add_event(conn, sid)
    eids = [r[0] for r in conn.execute("SELECT id FROM events").fetchall()]
    # Two cost_estimates that sum to a whole number (3.0).
    for eid, est in zip(eids, (1.5, 1.5)):
        conn.execute(
            "INSERT INTO token_usage "
            "(event_id, session_id, model, input, data_source, "
            " cost_estimate, event_at) "
            "VALUES (?, ?, 'm', 100, 'api', ?, '2026-04-21T10:00:00Z')",
            (eid, sid, est),
        )
    spec = AggregationSpec(
        domain="cost",
        dimensions=("model",),
        metrics=(Metric(kind="sum", field="cost_estimate"),),
        filters=(Predicate(field="data_source", op="=", value="api"),),
        limit=10,
    )
    result = run(spec, conn)
    bucket_value = result.buckets[0]["sum_cost_estimate"]
    assert isinstance(bucket_value, float), (
        f"sum over REAL column must stay float; got {bucket_value!r} "
        f"({type(bucket_value).__name__})"
    )
    assert bucket_value == 3.0


def test_three_predicate_AND_intersection(conn):
    """Plan 10 §Acceptance: three repeated ``--where`` clauses must
    AND together (intersection), not OR (union). With
    ``session=A AND session=B AND session=A`` no row can match
    because a row has exactly one session, and A != B."""

    sid_a = _add_session(conn, "claude:proj:sA")
    sid_b = _add_session(conn, "claude:proj:sB")
    _add_event(conn, sid_a)
    _add_event(conn, sid_b)

    spec = AggregationSpec(
        domain="events",
        dimensions=("client",),
        metrics=(Metric(kind="count"),),
        filters=(
            Predicate(field="session", op="=", value="claude:proj:sA"),
            Predicate(field="session", op="=", value="claude:proj:sB"),
            Predicate(field="session", op="=", value="claude:proj:sA"),
        ),
        limit=10,
    )
    result = run(spec, conn)
    assert result.total == 0  # AND of contradictory bounds — empty
    assert result.buckets == []


def test_three_dim_by_emits_object_keys(conn):
    """Plan 10 §Acceptance: ``--by client,type,date`` bucket keys are
    objects (dict), not concatenated strings. The dict has one entry
    per ``--by`` dimension."""

    sid = _add_session(conn, "claude:proj:s1")
    _add_event(conn, sid, type_="user_message", event_at="2026-04-21T10:00:00Z")
    _add_event(conn, sid, type_="tool_call", event_at="2026-04-21T10:00:00Z")
    _add_event(conn, sid, type_="user_message", event_at="2026-04-22T10:00:00Z")

    spec = AggregationSpec(
        domain="events",
        dimensions=("client", "type", "date"),
        metrics=(Metric(kind="count"),),
        limit=10,
    )
    result = run(spec, conn)
    for bucket in result.buckets:
        assert isinstance(bucket["key"], dict), (
            f"bucket key must be a dict; got {bucket['key']!r}"
        )
        assert set(bucket["key"]) == {"client", "type", "date"}
    # The 2026-04-21 day has two distinct (type) buckets; the
    # 2026-04-22 day has one. Three buckets total.
    assert len(result.buckets) == 3


def test_returns_meta_elapsed_and_rows_scanned(conn):
    sid = _add_session(conn, "claude:proj:s1")
    _add_event(conn, sid)
    spec = AggregationSpec(
        domain="events",
        dimensions=("client",),
        metrics=(Metric(kind="count"),),
        limit=10,
    )
    result = run(spec, conn)
    assert result.elapsed_ms >= 0
    assert result.rows_scanned == 1
