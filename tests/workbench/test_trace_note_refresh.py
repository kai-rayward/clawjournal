"""Refresh-path tests for trace notes.

Covers:
- `note render` refuses to overwrite a file with unsynced `## Notes` edits
- `--force` bypasses the unsynced-edit guard
- `--stale` picks up three distinct change sources (rescore, UI comment edit,
  ingest-style updated_at bump)
- `--stale` skips files with unsynced edits unless `--force`
- Normalization false-positive guards: NULL/'' parity, CRLF, trailing
  newlines must NOT trigger unsynced-edit refusal
- Normalization true-positive checks: internal blank line / changed word
  MUST trigger refusal
"""

from __future__ import annotations

import pytest

from clawjournal.workbench.index import open_index, update_session
from clawjournal.workbench.trace_note import (
    _normalize_notes,
    extract_rendered_updated_at,
    extract_trace_note_notes,
    render_trace_note,
    trace_note_path,
    write_note_atomically,
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


def _render(conn, session_id: str):
    session = read_session(conn, session_id)
    text = render_trace_note(session, session.get("reviewer_notes"))
    path = trace_note_path(session_id)
    write_note_atomically(path, text)
    return path


def _is_unsynced(path, reviewer_notes) -> bool:
    """Local re-implementation of the CLI's guard (matches cli._has_unsynced_edits)."""
    if not path.exists():
        return False
    text = path.read_text(encoding="utf-8")
    block = extract_trace_note_notes(text)
    if block is None:
        return True
    return _normalize_notes(block) != _normalize_notes(reviewer_notes)


class TestForceDiscipline:
    def test_unsynced_edit_blocks_without_force(self, env):
        conn, _ = env
        insert_sample_session(conn, session_id="s1", reviewer_notes="")
        path = _render(conn, "s1")

        # User edits the file directly without syncing.
        text = path.read_text(encoding="utf-8")
        edited = text.replace("## Notes\n", "## Notes\n\nunsaved edit\n")
        path.write_text(edited, encoding="utf-8")

        row = read_session(conn, "s1")
        assert _is_unsynced(path, row["reviewer_notes"]) is True

    def test_force_overwrites_unsynced_edit(self, env):
        conn, _ = env
        insert_sample_session(conn, session_id="s1", reviewer_notes="db note")
        path = _render(conn, "s1")

        # Simulate local edit (different from db note)
        edited = path.read_text(encoding="utf-8").replace(
            "db note", "unsaved edit"
        )
        path.write_text(edited, encoding="utf-8")

        # With --force, re-render is allowed; file should now match DB again.
        session = read_session(conn, "s1")
        new_text = render_trace_note(session, session.get("reviewer_notes"))
        write_note_atomically(path, new_text)

        final = path.read_text(encoding="utf-8")
        assert "db note" in final
        assert "unsaved edit" not in final


class TestStaleDetectionAcrossChangeSources:
    def test_stale_after_rescore(self, env):
        """Rescore bumps updated_at via update_session."""
        conn, _ = env
        insert_sample_session(conn, session_id="s1")
        path = _render(conn, "s1")
        old_stamp = extract_rendered_updated_at(
            path.read_text(encoding="utf-8")
        )

        # Simulate rescore: ai_summary changes.
        update_session(conn, "s1", ai_summary="new summary from rescore")
        row = read_session(conn, "s1")
        assert row["updated_at"] > old_stamp

    def test_stale_after_ui_comment_edit(self, env):
        """UI comment edit → update_session(notes=...) bumps updated_at."""
        conn, _ = env
        insert_sample_session(conn, session_id="s1")
        path = _render(conn, "s1")
        old_stamp = extract_rendered_updated_at(
            path.read_text(encoding="utf-8")
        )

        update_session(conn, "s1", notes="edited via UI")
        row = read_session(conn, "s1")
        assert row["updated_at"] > old_stamp

    def test_stale_after_hold_state_transition(self, env):
        """Hold transitions bump updated_at too (conservative signal)."""
        from clawjournal.workbench.index import set_hold_state
        conn, _ = env
        insert_sample_session(conn, session_id="s1")
        path = _render(conn, "s1")
        old_stamp = extract_rendered_updated_at(
            path.read_text(encoding="utf-8")
        )

        set_hold_state(conn, "s1", "released", changed_by="test")
        row = read_session(conn, "s1")
        assert row["updated_at"] > old_stamp
        # Conservative signal: note would re-render even though hold_state
        # isn't surfaced in the body. That's the documented trade-off.


class TestStaleRefusesUnsyncedEdits:
    def test_unsynced_file_skipped_by_stale(self, env):
        conn, _ = env
        insert_sample_session(conn, session_id="s1", reviewer_notes="")
        path = _render(conn, "s1")

        # User edits locally, no sync; then DB bumps updated_at via rescore.
        edited = path.read_text(encoding="utf-8").replace(
            "## Notes\n", "## Notes\n\nlocal edit pending\n"
        )
        path.write_text(edited, encoding="utf-8")
        update_session(conn, "s1", ai_summary="rescore summary")

        # Guard says unsynced; stale refresh should refuse without --force.
        row = read_session(conn, "s1")
        assert _is_unsynced(path, row["reviewer_notes"]) is True


class TestNormalizationFalsePositiveGuards:
    """These must NOT count as unsynced edits."""

    def test_db_null_and_empty_file_block(self, env):
        conn, _ = env
        insert_sample_session(conn, session_id="s1", reviewer_notes=None)
        path = _render(conn, "s1")

        row = read_session(conn, "s1")
        assert row["reviewer_notes"] is None
        assert _is_unsynced(path, row["reviewer_notes"]) is False

    def test_db_empty_string_and_empty_file_block(self, env):
        conn, _ = env
        insert_sample_session(conn, session_id="s1", reviewer_notes="")
        path = _render(conn, "s1")

        row = read_session(conn, "s1")
        assert row["reviewer_notes"] == ""
        assert _is_unsynced(path, row["reviewer_notes"]) is False

    def test_crlf_edited_file_matches_lf_db(self, env):
        conn, _ = env
        insert_sample_session(conn, session_id="s1", reviewer_notes="a\nb\nc")
        path = _render(conn, "s1")

        # Simulate a Windows editor rewrite — all newlines become \r\n
        text = path.read_text(encoding="utf-8").replace("\n", "\r\n")
        path.write_bytes(text.encode("utf-8"))

        row = read_session(conn, "s1")
        assert _is_unsynced(path, row["reviewer_notes"]) is False

    def test_extra_trailing_blank_lines_ignored(self, env):
        conn, _ = env
        insert_sample_session(conn, session_id="s1", reviewer_notes="my note")
        path = _render(conn, "s1")

        # Append extra trailing blank lines to the file.
        with open(path, "a", encoding="utf-8") as f:
            f.write("\n\n\n")

        row = read_session(conn, "s1")
        assert _is_unsynced(path, row["reviewer_notes"]) is False

    def test_trailing_newline_vs_none_ignored(self, env):
        conn, _ = env
        # DB has no trailing newline; file will have one (render emits `\n`).
        insert_sample_session(conn, session_id="s1", reviewer_notes="tight")
        path = _render(conn, "s1")

        row = read_session(conn, "s1")
        assert _is_unsynced(path, row["reviewer_notes"]) is False


class TestNormalizationTruePositives:
    """These MUST count as unsynced edits."""

    def test_internal_blank_line_added(self, env):
        conn, _ = env
        insert_sample_session(
            conn, session_id="s1", reviewer_notes="line a\nline b"
        )
        path = _render(conn, "s1")

        # User adds a blank line between the paragraphs.
        text = path.read_text(encoding="utf-8").replace(
            "line a\nline b", "line a\n\nline b"
        )
        path.write_text(text, encoding="utf-8")

        row = read_session(conn, "s1")
        assert _is_unsynced(path, row["reviewer_notes"]) is True

    def test_single_word_changed(self, env):
        conn, _ = env
        insert_sample_session(
            conn, session_id="s1", reviewer_notes="fix the bug"
        )
        path = _render(conn, "s1")

        text = path.read_text(encoding="utf-8").replace(
            "fix the bug", "patch the bug"
        )
        path.write_text(text, encoding="utf-8")

        row = read_session(conn, "s1")
        assert _is_unsynced(path, row["reviewer_notes"]) is True

    def test_leading_whitespace_is_not_normalized(self, env):
        """Per §Canonical normalization: leading whitespace is content."""
        conn, _ = env
        insert_sample_session(
            conn, session_id="s1", reviewer_notes="hello"
        )
        path = _render(conn, "s1")

        # The rendered file already strips leading whitespace from notes body
        # (because the render puts the body right after a blank line).
        # If the user prepends indentation, that's a real edit.
        text = path.read_text(encoding="utf-8").replace(
            "\nhello\n", "\n    hello\n"
        )
        path.write_text(text, encoding="utf-8")

        row = read_session(conn, "s1")
        assert _is_unsynced(path, row["reviewer_notes"]) is True
