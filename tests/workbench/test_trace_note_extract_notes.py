"""Tests for extract_trace_note_notes — parses the `## Notes` block."""

from clawjournal.workbench.trace_note import extract_trace_note_notes


class TestExtractNotes:
    def test_populated_block(self):
        text = (
            "# title\n\n"
            "## Summary\n\nsome summary.\n\n"
            "## Notes\n\nmy user notes here\n"
        )
        assert extract_trace_note_notes(text) == "my user notes here"

    def test_empty_block_returns_empty_string(self):
        text = "# title\n\n## Summary\n\nx\n\n## Notes\n\n"
        # Empty block is a valid user choice to clear the note.
        assert extract_trace_note_notes(text) == ""

    def test_empty_block_only_blank_lines(self):
        text = "# title\n\n## Notes\n\n\n\n"
        assert extract_trace_note_notes(text) == ""

    def test_missing_heading_returns_none(self):
        text = "# title\n\n## Summary\n\njust a summary, no notes section.\n"
        assert extract_trace_note_notes(text) is None

    def test_multiline_notes_preserved(self):
        text = (
            "## Notes\n\n"
            "first paragraph.\n\n"
            "second paragraph with\n"
            "wrapped line.\n"
        )
        result = extract_trace_note_notes(text)
        assert result is not None
        assert "first paragraph." in result
        assert "second paragraph with\nwrapped line." in result

    def test_stray_heading_after_notes_ends_the_block(self):
        # Per §Render / parse rules: block ends at next `## ` heading.
        text = (
            "## Notes\n\n"
            "my note content\n\n"
            "## User Custom Heading\n\n"
            "other stuff the user added\n"
        )
        result = extract_trace_note_notes(text)
        assert result == "my note content"
        assert "other stuff" not in result

    def test_notes_before_summary_still_finds_notes(self):
        # Order-tolerant: even if file is malformed and Notes comes first.
        text = (
            "## Notes\n\n"
            "leading notes\n\n"
            "## Summary\n\n"
            "summary after\n"
        )
        # Notes block ends at `## Summary`.
        assert extract_trace_note_notes(text) == "leading notes"

    def test_heading_at_end_no_body(self):
        text = "# title\n\n## Notes"
        assert extract_trace_note_notes(text) == ""

    def test_heading_with_trailing_spaces(self):
        text = "## Notes   \n\nmy notes\n"
        assert extract_trace_note_notes(text) == "my notes"

    def test_three_hash_not_matched(self):
        # `### Notes` is not `## Notes`
        text = "### Notes\n\nnot a top-level section\n"
        assert extract_trace_note_notes(text) is None
