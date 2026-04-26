"""Render-layer tests (plan 10) — JSON shape + workspace anonymization."""

from __future__ import annotations

import json
import os
import sqlite3

import pytest

from clawjournal.events.aggregate import (
    AggregationSpec,
    EVENTS_AGGREGATE_SCHEMA_VERSION,
    Metric,
    render_human,
    render_json,
    run,
)
from clawjournal.events.schema import ensure_schema as ensure_events_schema


@pytest.fixture
def conn():
    from clawjournal.events.cost.schema import ensure_cost_schema

    c = sqlite3.connect(":memory:")
    ensure_events_schema(c)
    ensure_cost_schema(c)
    yield c
    c.close()


def _seed(conn, *, session_keys: list[tuple[str, str]]):
    """``session_keys`` is a list of (key, client) pairs."""
    seq = 0
    for key, client in session_keys:
        conn.execute(
            "INSERT INTO event_sessions (session_key, client, started_at, "
            "status) VALUES (?, ?, '2026-04-21T10:00:00Z', 'ended')",
            (key, client),
        )
        sid = conn.execute(
            "SELECT id FROM event_sessions WHERE session_key=?", (key,)
        ).fetchone()[0]
        seq += 1
        src = "claude-jsonl" if client == "claude" else f"{client}-rollout"
        conn.execute(
            "INSERT INTO events "
            "(session_id, type, event_at, ingested_at, source, source_path, "
            " source_offset, seq, client, confidence, lossiness, raw_json) "
            "VALUES (?, 'user_message', '2026-04-21T10:00:00Z', "
            "'2026-04-21T10:00:00Z', ?, '/x', 0, ?, ?, 'high', 'none', '{}')",
            (sid, src, seq, client),
        )


def test_render_json_shape_and_schema_version(conn):
    _seed(conn, session_keys=[("claude:proj_a:s1", "claude")])
    spec = AggregationSpec(
        domain="events",
        dimensions=("client",),
        metrics=(Metric(kind="count"),),
        limit=5,
    )
    result = run(spec, conn)
    payload = json.loads(render_json(result, request_id="rq-7"))
    assert payload["events_aggregate_schema_version"] == EVENTS_AGGREGATE_SCHEMA_VERSION
    assert payload["domain"] == "events"
    assert payload["aggregation"]["by"] == ["client"]
    assert payload["aggregation"]["metric"] == ["count"]
    assert payload["aggregation"]["buckets"][0]["count"] == 1
    assert payload["_meta"]["request_id"] == "rq-7"
    assert "elapsed_ms" in payload["_meta"]
    assert "rows_scanned" in payload["_meta"]


def test_render_json_omits_request_id_when_unset(conn):
    _seed(conn, session_keys=[("claude:proj:s1", "claude")])
    spec = AggregationSpec(
        domain="events",
        dimensions=("client",),
        metrics=(Metric(kind="count"),),
    )
    result = run(spec, conn)
    payload = json.loads(render_json(result))
    assert "request_id" not in payload["_meta"]


def test_workspace_bucket_keys_are_anonymized(conn, monkeypatch):
    """Plan 10 §Acceptance: ``workspace`` bucket keys for home-rooted
    paths must not leak the home username. Tests use a literal
    ``/Users/synthetic-user/...`` path because the Anonymizer's
    string-match path is tuned for ``/Users/<u>/`` and ``/home/<u>/``
    patterns specifically — production session_keys contain those
    shapes (codex's session_key embeds the working directory)."""

    monkeypatch.setenv("HOME", "/Users/synthetic-user")

    workspace_abs = "/Users/synthetic-user/important-repo"
    _seed(
        conn,
        session_keys=[
            (f"codex:{workspace_abs}", "codex"),
            ("claude:plain_workspace:abc", "claude"),
        ],
    )
    spec = AggregationSpec(
        domain="events",
        dimensions=("workspace",),
        metrics=(Metric(kind="count"),),
        limit=10,
    )
    result = run(spec, conn)
    payload = json.loads(render_json(result))
    rendered = json.dumps(payload)

    # Acceptance: home username must not appear anywhere.
    assert "synthetic-user" not in rendered, (
        f"home username leaked into rendered output: {rendered!r}"
    )

    # Acceptance: the absolute workspace path must have been transformed.
    assert workspace_abs not in rendered, (
        f"unanonymized absolute path leaked: {workspace_abs!r}"
    )

    workspaces = {
        b["key"]["workspace"] for b in payload["aggregation"]["buckets"]
    }
    # ``Anonymizer().text()`` redacts home-rooted absolute paths to the
    # `[REDACTED_PATH]` placeholder (consistent with every other
    # share-time anonymized field). Plan 10's "~/…" prose was
    # aspirational; we use the placeholder shape that the rest of the
    # codebase emits.
    assert "[REDACTED_PATH]" in workspaces, (
        f"expected [REDACTED_PATH] for the home-rooted workspace, "
        f"got {workspaces!r}"
    )
    # Non-path workspace stays untouched.
    assert "plain_workspace" in workspaces


def test_session_bucket_keys_anonymize_embedded_paths(conn, monkeypatch):
    """Round 2: codex session_keys are formatted ``codex:<absolute-path>``,
    so grouping by ``session`` would otherwise emit
    ``codex:/Users/<currentuser>/myproj`` as a bucket key — leaking
    the home username. Render must run session keys through the
    anonymizer; ``.text()`` preserves the ``codex:`` prefix while
    redacting the embedded home path."""

    monkeypatch.setenv("HOME", "/Users/synthetic-user")

    _seed(
        conn,
        session_keys=[
            ("codex:/Users/synthetic-user/myproj", "codex"),
            ("claude:plain_proj:s1", "claude"),
        ],
    )
    spec = AggregationSpec(
        domain="events",
        dimensions=("session",),
        metrics=(Metric(kind="count"),),
        limit=10,
    )
    result = run(spec, conn)
    payload = json.loads(render_json(result))
    rendered = json.dumps(payload)

    assert "synthetic-user" not in rendered, (
        f"home username leaked via session bucket key: {rendered!r}"
    )
    assert "/Users/synthetic-user/myproj" not in rendered, (
        "unanonymized absolute path leaked via session bucket key"
    )

    sessions = {
        b["key"]["session"] for b in payload["aggregation"]["buckets"]
    }
    # Codex session: prefix preserved, path redacted to placeholder.
    assert any(
        s and s.startswith("codex:") and "REDACTED" in s for s in sessions
    ), f"expected codex:[REDACTED_PATH] form, got {sessions!r}"
    # Claude session: nothing to anonymize, passes through.
    assert "claude:plain_proj:s1" in sessions


def test_total_for_sum_metric_matches_pretruncation_sum(conn):
    """Round 2: ``total`` for a sum metric should equal the SUM of the
    metric column over all matching rows pre-truncation, regardless
    of how many buckets ``--limit`` cuts off."""

    sid = _add_session = lambda key, client: conn.execute(
        "INSERT INTO event_sessions (session_key, client, started_at, status) "
        "VALUES (?, ?, '2026-04-21T10:00:00Z', 'ended') RETURNING id",
        (key, client),
    ).fetchone()[0]
    sid = _add_session("claude:proj:s1", "claude")
    seq = 0
    for _ in range(5):
        seq += 1
        conn.execute(
            "INSERT INTO events (session_id, type, event_at, ingested_at, "
            "source, source_path, source_offset, seq, client, confidence, "
            "lossiness, raw_json) VALUES (?, 'user_message', "
            "'2026-04-21T10:00:00Z', '2026-04-21T10:00:00Z', 'claude-jsonl', "
            "'/x', 0, ?, 'claude', 'high', 'none', '{}')",
            (sid, seq),
        )
    eids = [r[0] for r in conn.execute("SELECT id FROM events ORDER BY id")]
    # Five token_usage rows with model/input pairs; --limit 2 should cut
    # off three.
    rows = [
        ("model_a", 100), ("model_a", 200),
        ("model_b", 50), ("model_c", 30), ("model_d", 5),
    ]
    for eid, (model, inp) in zip(eids, rows):
        conn.execute(
            "INSERT INTO token_usage "
            "(event_id, session_id, model, input, data_source, event_at) "
            "VALUES (?, ?, ?, ?, 'api', '2026-04-21T10:00:00Z')",
            (eid, sid, model, inp),
        )
    conn.commit()

    spec = AggregationSpec(
        domain="cost",
        dimensions=("model",),
        metrics=(Metric(kind="sum", field="input_tokens"),),
        filters=(
            __import__(
                "clawjournal.events.aggregate", fromlist=["Predicate"]
            ).Predicate(field="data_source", op="=", value="api"),
        ),
        limit=2,
    )
    result = run(spec, conn)
    assert result.total == 100 + 200 + 50 + 30 + 5  # full pre-truncation sum
    bucket_sum = sum(b["sum_input_tokens"] for b in result.buckets)
    assert bucket_sum == 100 + 200 + 50  # top 2 models: model_a (300), model_b (50)
    assert result.other_count == result.total - bucket_sum


def test_render_human_includes_meta_footer(conn):
    _seed(conn, session_keys=[("claude:proj:s1", "claude")])
    spec = AggregationSpec(
        domain="events",
        dimensions=("client",),
        metrics=(Metric(kind="count"),),
    )
    result = run(spec, conn)
    text = render_human(result)
    assert "client" in text
    assert "count" in text
    assert "rows scanned" in text
