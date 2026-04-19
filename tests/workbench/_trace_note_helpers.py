"""Shared helpers for trace-note integration tests."""

from __future__ import annotations

from datetime import datetime, timezone

from clawjournal.workbench.index import update_session


def insert_sample_session(
    conn,
    session_id: str = "sess-abc",
    project: str = "clawjournal",
    display_title: str = "Redesign Share queue flow",
    ai_summary: str = "Split the Share page into Queue/Privacy/Done.",
    ai_quality_score: int = 5,
    reviewer_notes: str | None = None,
) -> None:
    """Insert a minimal but realistic sessions row for rendering tests."""
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        INSERT INTO sessions (
            session_id, project, source, model,
            start_time, end_time, duration_seconds, git_branch,
            user_messages, assistant_messages, tool_uses,
            input_tokens, output_tokens,
            display_title, outcome_badge, value_badges, risk_badges,
            review_status, reviewer_notes,
            blob_path, raw_source_path,
            indexed_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?,
                  ?, ?, ?, ?, ?,
                  ?, ?, ?, ?,
                  ?, ?, ?, ?, ?, ?)
        """,
        (
            session_id, project, "claude", "claude-opus-4-7",
            "2026-04-17T10:34:21+00:00", "2026-04-17T11:02:44+00:00",
            1703, "main",
            1, 1, 0,
            100, 200,
            display_title, "shipped", "[]", "[]",
            "new", reviewer_notes,
            f"blobs/{session_id}.json", f"/fake/{session_id}",
            now, now,
        ),
    )
    conn.commit()
    # Populate the AI fields via update_session so updated_at bumps correctly.
    update_session(
        conn, session_id,
        ai_summary=ai_summary,
        ai_quality_score=ai_quality_score,
        ai_outcome_badge="shipped",
        ai_value_badges='["feature"]',
        ai_risk_badges="[]",
        ai_effort_estimate=0.82,
    )


def read_session(conn, session_id: str) -> dict:
    row = conn.execute(
        "SELECT * FROM sessions WHERE session_id = ?", (session_id,)
    ).fetchone()
    assert row is not None, f"session {session_id} not found in test DB"
    return dict(row)
