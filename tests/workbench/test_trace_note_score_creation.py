"""Tests for the scoring-path auto-create hook.

Covers:
- `create_note_if_missing` writes a file for a session with an ai_summary
- Second call on the same session is a no-op (does not overwrite)
- Existing file with user content survives a "rescore" (still no-op)
"""

from __future__ import annotations

import pytest

from clawjournal.workbench.index import open_index, update_session
from clawjournal.workbench.trace_note import (
    create_note_if_missing,
    extract_trace_note_notes,
    trace_note_path,
)

from tests.workbench._trace_note_helpers import (
    insert_sample_session,
    read_session,
)


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "clawjournal.workbench.index.INDEX_DB", tmp_path / "index.db"
    )
    monkeypatch.setattr(
        "clawjournal.workbench.index.BLOBS_DIR", tmp_path / "blobs"
    )
    notes_dir = tmp_path / "notes"
    conn = open_index()
    yield conn, notes_dir
    conn.close()


class TestCreateOnFirstScore:
    def test_creates_note_when_missing(self, env):
        conn, notes_dir = env
        insert_sample_session(conn, session_id="s1")

        session = read_session(conn, "s1")
        result = create_note_if_missing(session)

        assert result is not None
        assert result == trace_note_path("s1")
        assert result.exists()
        text = result.read_text(encoding="utf-8")
        assert "# Redesign Share queue flow" in text
        assert "## Summary" in text
        assert "## Notes" in text

    def test_empty_summary_still_creates_note(self, env):
        # First-score hook should fire even if summary is unusually short.
        conn, _ = env
        insert_sample_session(conn, session_id="s1", ai_summary="")

        session = read_session(conn, "s1")
        result = create_note_if_missing(session)
        # Design choice: create_note_if_missing does not gate on ai_summary.
        # The daemon only calls it after a successful score, which already
        # implies a non-trivial session.
        assert result is not None


class TestNoOverwriteOnRescore:
    def test_second_call_is_noop(self, env):
        conn, _ = env
        insert_sample_session(conn, session_id="s1")

        session = read_session(conn, "s1")
        path = create_note_if_missing(session)
        assert path is not None
        first_content = path.read_text(encoding="utf-8")

        # Simulate rescore: ai_summary changes
        update_session(conn, "s1", ai_summary="totally new summary")

        session = read_session(conn, "s1")
        result = create_note_if_missing(session)
        # File already exists → function returns None, does NOT overwrite.
        assert result is None
        second_content = path.read_text(encoding="utf-8")
        assert first_content == second_content
        # Old summary still in the file — refresh is an explicit action.
        assert "totally new summary" not in second_content

    def test_user_edits_survive_rescore_hook(self, env):
        conn, _ = env
        insert_sample_session(conn, session_id="s1")

        session = read_session(conn, "s1")
        path = create_note_if_missing(session)
        assert path is not None

        # User edits `## Notes` section directly in their editor.
        text = path.read_text(encoding="utf-8")
        edited = text.replace(
            "## Notes\n", "## Notes\n\nhand-written reminder to self\n"
        )
        path.write_text(edited, encoding="utf-8")

        # Rescore; hook fires again.
        update_session(conn, "s1", ai_summary="post-rescore summary")
        session = read_session(conn, "s1")
        result = create_note_if_missing(session)
        assert result is None  # no-op, did not overwrite

        # User's edit is preserved.
        final = path.read_text(encoding="utf-8")
        block = extract_trace_note_notes(final)
        assert block == "hand-written reminder to self"


class TestMissingSessionId:
    def test_no_session_id_returns_none(self, env):
        # Defensive: create_note_if_missing should not crash on a malformed
        # dict. It returns None and writes nothing.
        _, notes_dir = env
        result = create_note_if_missing({})
        assert result is None
        # No files created in the notes dir
        if notes_dir.exists():
            assert list(notes_dir.glob("*.md")) == []
