"""Tests for clawjournal.scoring.insights."""

from datetime import datetime, timedelta, timezone

import pytest

from clawjournal.workbench.index import open_index, upsert_sessions
from clawjournal.scoring.insights import collect_advisor_stats


@pytest.fixture
def index_conn(tmp_path, monkeypatch):
    """Open an index DB in a temp directory."""
    monkeypatch.setattr("clawjournal.workbench.index.INDEX_DB", tmp_path / "index.db")
    monkeypatch.setattr("clawjournal.workbench.index.BLOBS_DIR", tmp_path / "blobs")
    conn = open_index()
    yield conn
    conn.close()


def _make_session(session_id: str = "sess-1") -> dict:
    now = datetime.now(timezone.utc)
    later = now + timedelta(minutes=10)
    return {
        "session_id": session_id,
        "project": "test-project",
        "source": "claude",
        "model": "claude-sonnet-4",
        "start_time": now.isoformat(),
        "end_time": later.isoformat(),
        "git_branch": "main",
        "messages": [
            {"role": "user", "content": "Document the config changes", "tool_uses": []},
            {"role": "assistant", "content": "Done.", "tool_uses": []},
        ],
        "stats": {
            "user_messages": 1,
            "assistant_messages": 1,
            "tool_uses": 0,
            "input_tokens": 400_000,
            "output_tokens": 100_000,
        },
    }


class TestCollectAdvisorStats:
    @pytest.mark.parametrize("task_type", ["documentation", "configuration"])
    def test_model_downgrade_candidates_include_new_task_labels(self, index_conn, task_type):
        upsert_sessions(index_conn, [_make_session()])
        index_conn.execute(
            "UPDATE sessions SET ai_quality_score = ?, ai_task_type = ? WHERE session_id = ?",
            (2, task_type, "sess-1"),
        )
        index_conn.commit()

        stats = collect_advisor_stats(index_conn, days=30)

        assert any(
            candidate["session_id"] == "sess-1"
            for candidate in stats["model_downgrade_candidates"]
        )
