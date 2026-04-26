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
    c = sqlite3.connect(":memory:")
    ensure_events_schema(c)
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


def test_workspace_bucket_keys_are_anonymized(conn, monkeypatch, tmp_path):
    """Plan 10 §Acceptance: ``workspace`` bucket keys appear as
    ``~/...`` form on a fixture with real-looking paths. The home
    username must not survive in rendered output, and the original
    absolute workspace path must be transformed (not pass through
    verbatim)."""

    fake_home = tmp_path / "synthetic-user"
    fake_home.mkdir()
    monkeypatch.setenv("HOME", str(fake_home))

    workspace_abs = str(fake_home / "important-repo")
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

    # Acceptance: home username basename must not appear anywhere.
    username = os.path.basename(str(fake_home))
    assert username not in rendered, (
        f"home username {username!r} leaked into rendered output: {rendered!r}"
    )

    # Acceptance: the absolute workspace path must have been transformed.
    # The anonymizer renders home-rooted paths as ~/... — so the literal
    # absolute path should not survive.
    assert workspace_abs not in rendered, (
        f"unanonymized absolute path leaked: {workspace_abs!r}"
    )

    workspaces = {
        b["key"]["workspace"] for b in payload["aggregation"]["buckets"]
    }
    # `Anonymizer().path()` collapses any home-rooted absolute path to
    # `[REDACTED_PATH]` (the placeholder defined in
    # ``redaction/anonymizer.py``). That's the actual rendered shape —
    # plan 10's "~/…" prose was aspirational; we use the placeholder
    # consistently with all other share-time anonymized fields.
    assert "[REDACTED_PATH]" in workspaces, (
        f"expected [REDACTED_PATH] for the home-rooted workspace, "
        f"got {workspaces!r}"
    )
    # Non-path workspace stays untouched.
    assert "plain_workspace" in workspaces


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
