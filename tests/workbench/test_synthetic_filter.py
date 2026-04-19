"""Regression tests for the `<synthetic>` filter across by_model queries.

`<synthetic>` is a parser-fallback model name. Export (cli.py:479) filters
it at the source; the in-DB aggregations used by the Dashboard and
Insights tabs must also filter it, otherwise synthetic sessions (which
usually have $0 cost) trivially win "Most Efficient" and pollute the
Model Effectiveness / Cost / Models charts.

Five filter points landed across these tests:
- scoring/insights.py:57 (collect_advisor_stats.by_model)
- scoring/insights.py:178 (collect_advisor_stats.interrupt_patterns)
- workbench/index.py:2004 (get_dashboard_analytics.by_model)
- workbench/index.py:2409 (get_insights.model_effectiveness)
- workbench/index.py:2462 (get_insights.cost_by_model)

Each test inserts a `<synthetic>` session alongside at least one real-model
session and asserts the query output excludes synthetic.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from clawjournal.scoring.insights import (
    collect_advisor_stats,
    generate_recommendations,
)
from clawjournal.workbench.index import (
    get_dashboard_analytics,
    get_insights,
    open_index,
    update_session,
)


@pytest.fixture
def index_conn(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "clawjournal.workbench.index.INDEX_DB", tmp_path / "index.db"
    )
    monkeypatch.setattr(
        "clawjournal.workbench.index.BLOBS_DIR", tmp_path / "blobs"
    )
    conn = open_index()
    yield conn
    conn.close()


def _insert(
    conn,
    *,
    session_id,
    model,
    started_days_ago=1,
    duration=600,
    quality=4,
    outcome="resolved",
    cost=1.50,
    user_interrupts=0,
):
    end = datetime.now(timezone.utc) - timedelta(days=started_days_ago)
    start = end - timedelta(seconds=duration)
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """INSERT INTO sessions (
            session_id, project, source, model,
            start_time, end_time, duration_seconds, git_branch,
            user_messages, assistant_messages, tool_uses,
            input_tokens, output_tokens, estimated_cost_usd,
            display_title, outcome_badge, value_badges, risk_badges,
            review_status, blob_path, raw_source_path,
            indexed_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            session_id, "proj", "claude", model,
            start.isoformat(), end.isoformat(), duration, "main",
            1, 1, 0, 500, 300, cost,
            f"Title for {session_id}", outcome, "[]", "[]",
            "new", f"blobs/{session_id}.json", f"/fake/{session_id}",
            now, now,
        ),
    )
    conn.commit()
    kwargs = {
        "ai_quality_score": quality,
        "ai_summary": f"Summary for {session_id}",
        "ai_outcome_badge": outcome,
    }
    # user_interrupts is on the sessions table but not in update_session's
    # API; write directly so interrupt_patterns has something to aggregate.
    update_session(conn, session_id, **kwargs)
    if user_interrupts:
        conn.execute(
            "UPDATE sessions SET user_interrupts = ? WHERE session_id = ?",
            (user_interrupts, session_id),
        )
        conn.commit()


class TestAdvisorByModel:
    def test_synthetic_excluded_from_advisor_by_model(self, index_conn):
        _insert(index_conn, session_id="real", model="claude-opus-4-7",
                cost=5.0, quality=4)
        _insert(index_conn, session_id="synth", model="<synthetic>",
                cost=0.0, quality=5)

        stats = collect_advisor_stats(index_conn, days=7)
        models = [m["model"] for m in stats["by_model"]]

        assert "claude-opus-4-7" in models
        assert "<synthetic>" not in models

    def test_synthetic_does_not_win_most_efficient(self, index_conn):
        # Without the filter, synthetic (cost=0) would trivially win
        # min(cost/sessions) on the generated recommendations payload.
        # With the filter it cannot.
        _insert(index_conn, session_id="real", model="claude-opus-4-7",
                cost=5.0, quality=4)
        _insert(index_conn, session_id="synth", model="<synthetic>",
                cost=0.0, quality=4)

        stats = collect_advisor_stats(index_conn, days=7)
        recs = generate_recommendations(stats)

        assert recs["summary_stats"]["most_efficient_model"] != "<synthetic>"
        assert recs["summary_stats"]["highest_quality_model"] != "<synthetic>"


class TestAdvisorInterruptPatterns:
    def test_synthetic_excluded_from_interrupt_patterns(self, index_conn):
        _insert(index_conn, session_id="real", model="claude-opus-4-7",
                user_interrupts=3)
        _insert(index_conn, session_id="synth", model="<synthetic>",
                user_interrupts=5)

        stats = collect_advisor_stats(index_conn, days=7)
        models = [p["model"] for p in stats.get("interrupt_patterns", [])]

        assert "<synthetic>" not in models


class TestDashboardByModel:
    def test_synthetic_excluded_from_dashboard_by_model(self, index_conn):
        _insert(index_conn, session_id="real", model="claude-opus-4-7")
        _insert(index_conn, session_id="synth", model="<synthetic>")

        data = get_dashboard_analytics(index_conn)
        models = [m["model"] for m in data["by_model"]]

        assert "claude-opus-4-7" in models
        assert "<synthetic>" not in models


class TestInsightsModelEffectiveness:
    def test_synthetic_excluded_from_model_effectiveness(self, index_conn):
        _insert(index_conn, session_id="real", model="claude-opus-4-7",
                cost=5.0, quality=4)
        _insert(index_conn, session_id="synth", model="<synthetic>",
                cost=0.0, quality=4)

        data = get_insights(index_conn)
        models = [m["model"] for m in data["model_effectiveness"]]

        assert "claude-opus-4-7" in models
        assert "<synthetic>" not in models


class TestInsightsCostByModel:
    def test_synthetic_excluded_from_cost_by_model(self, index_conn):
        _insert(index_conn, session_id="real", model="claude-opus-4-7",
                cost=5.0)
        _insert(index_conn, session_id="synth", model="<synthetic>",
                cost=10.0)

        data = get_insights(index_conn)
        models = [m["model"] for m in data.get("cost_by_model", [])]

        assert "claude-opus-4-7" in models
        assert "<synthetic>" not in models
