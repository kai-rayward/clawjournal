"""--where clause parser tests (plan 10)."""

from __future__ import annotations

import pytest

from clawjournal.events.aggregate import get_registry, parse_where_clauses


def test_simple_equals():
    preds = parse_where_clauses(["client=claude"], get_registry("events"))
    assert len(preds) == 1
    assert preds[0].field == "client"
    assert preds[0].op == "="
    assert preds[0].value == "claude"


def test_longest_op_wins_ge_not_split():
    preds = parse_where_clauses(["event_at>=2026-04-01"], get_registry("events"))
    assert preds[0].op == ">="
    assert preds[0].value == "2026-04-01"


def test_in_operator_splits_on_pipe():
    preds = parse_where_clauses(
        ["client" + "in:claude|codex|openclaw"], get_registry("events")
    )
    assert preds[0].op == "in"
    assert preds[0].value == ("claude", "codex", "openclaw")


def test_in_requires_value():
    with pytest.raises(ValueError):
        parse_where_clauses(["client" + "in:"], get_registry("events"))


def test_unknown_field_rejected():
    with pytest.raises(ValueError) as exc:
        parse_where_clauses(["raw_json=secret"], get_registry("events"))
    assert "raw_json" in str(exc.value)
    assert "allowed" in str(exc.value)


def test_empty_field_rejected():
    with pytest.raises(ValueError):
        parse_where_clauses(["=claude"], get_registry("events"))


def test_repeated_clauses_AND_ed():
    preds = parse_where_clauses(
        ["client=claude", "type=tool_call"], get_registry("events")
    )
    assert len(preds) == 2


def test_no_op_rejected():
    with pytest.raises(ValueError):
        parse_where_clauses(["just_a_field"], get_registry("events"))
