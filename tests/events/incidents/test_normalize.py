"""Per-rule unit tests for the loop-detector outcome-text normalizer.

Each rule from `clawjournal.events.incidents.normalize` gets its own
test so a regression in one rule isn't masked by the others firing.
"""

from __future__ import annotations

import pytest

from clawjournal.events.incidents.normalize import normalize_outcome_text


def test_normalize_handles_none_and_non_string():
    assert normalize_outcome_text(None) == ""
    assert normalize_outcome_text(42) == ""  # type: ignore[arg-type]
    assert normalize_outcome_text({"oops": True}) == ""  # type: ignore[arg-type]


def test_strips_iso8601_with_z():
    out = normalize_outcome_text("Failed at 2026-04-21T10:00:00Z please retry")
    assert "2026-04-21T10:00:00Z" not in out
    assert "<TS>" in out


def test_strips_iso8601_with_fractional_seconds_and_offset():
    out = normalize_outcome_text(
        "Failed at 2026-04-21T10:00:00.123456-08:00 please retry"
    )
    assert "<TS>" in out
    assert "2026" not in out


def test_strips_user_rooted_path():
    out = normalize_outcome_text("ENOENT: no such file '/Users/kai/llm/foo.py'")
    assert "/Users/kai/llm/foo.py" not in out
    assert "<PATH>" in out


def test_strips_linux_home_and_tmp_paths():
    out = normalize_outcome_text(
        "wrote /home/alice/x.log and /tmp/scratch/output"
    )
    assert "/home/alice/x.log" not in out
    assert "/tmp/scratch/output" not in out
    assert out.count("<PATH>") == 2


def test_does_not_strip_system_binary_paths():
    """A literal /usr/bin/foo isn't user-rooted; leave it alone so we
    can still tell different binaries apart."""
    out = normalize_outcome_text("ran /usr/bin/python3 ok")
    assert "/usr/bin/python3" in out


def test_strips_pid_inline_variants():
    for raw in ("pid 12345", "PID: 67890", "process 4242"):
        out = normalize_outcome_text(f"crashed: {raw}")
        assert "12345" not in out and "67890" not in out and "4242" not in out
        assert "<PID>" in out


def test_strips_bracketed_pid():
    out = normalize_outcome_text("[12345] error: oh no")
    assert "[<PID>]" in out
    assert "12345" not in out


def test_does_not_eat_long_digit_strings_as_pids():
    """[123456789012345] is a GUID-shaped number; the bracketed-PID
    rule caps at 7 digits to avoid swallowing those."""
    out = normalize_outcome_text("trace_id=[123456789012345]")
    assert "123456789012345" in out


def test_collapses_whitespace_after_substitutions():
    out = normalize_outcome_text(
        "ts=2026-04-21T10:00:00Z   pid 1   in   /Users/kai/x.py"
    )
    assert "  " not in out  # no double spaces survive
    assert out.startswith("ts=<TS>")


def test_strips_leading_and_trailing_whitespace():
    assert normalize_outcome_text("  hello  ") == "hello"


def test_collapses_newline_only_reflow():
    assert normalize_outcome_text("foo bar") == normalize_outcome_text("foo\nbar")


@pytest.mark.parametrize(
    "first,second",
    [
        # Two failures of the same command at different times produce
        # the same normalized form — that's the whole point.
        (
            "[12345] ENOENT at 2026-04-21T10:00:00Z in /Users/kai/x.py",
            "[67890] ENOENT at 2026-04-21T10:01:00Z in /Users/kai/x.py",
        ),
        (
            "Connection refused; pid 4242 in /Users/kai/proj/x.py",
            "Connection refused; pid 9999 in /Users/kai/proj/x.py",
        ),
    ],
)
def test_two_distinct_failures_normalize_to_same_text(first, second):
    assert normalize_outcome_text(first) == normalize_outcome_text(second)
