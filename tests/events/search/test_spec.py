"""Validation tests for `SearchSpec` (plan 11)."""

from __future__ import annotations

import pytest

from clawjournal.events.aggregate.spec import Predicate
from clawjournal.events.search import (
    DEFAULT_LIMIT,
    DEFAULT_SNIPPET_TOKENS,
    HARD_LIMIT_CEILING,
    MAX_QUERY_BYTES,
    MAX_SNIPPET_TOKENS,
    MIN_SNIPPET_TOKENS,
    SearchSpec,
)


def test_defaults():
    spec = SearchSpec(query="hello")
    assert spec.query == "hello"
    assert spec.limit == DEFAULT_LIMIT
    assert spec.snippet_tokens == DEFAULT_SNIPPET_TOKENS
    assert spec.include_held is False


def test_empty_query_rejected():
    with pytest.raises(ValueError):
        SearchSpec(query="")
    with pytest.raises(ValueError):
        SearchSpec(query="   ")


def test_oversize_query_rejected():
    with pytest.raises(ValueError) as exc:
        SearchSpec(query="a" * (MAX_QUERY_BYTES + 1))
    assert "byte cap" in str(exc.value)


def test_zero_limit_rejected():
    with pytest.raises(ValueError):
        SearchSpec(query="x", limit=0)


def test_limit_above_ceiling_rejected():
    with pytest.raises(ValueError):
        SearchSpec(query="x", limit=HARD_LIMIT_CEILING + 1)


def test_snippet_tokens_bounds():
    """FTS5 caps ``snippet()`` at 64 tokens; SearchSpec enforces the
    cap so the SQL builder never relies on SQLite's silent clamp."""

    with pytest.raises(ValueError):
        SearchSpec(query="x", snippet_tokens=MIN_SNIPPET_TOKENS - 1)
    with pytest.raises(ValueError):
        SearchSpec(query="x", snippet_tokens=MAX_SNIPPET_TOKENS + 1)
    # Boundaries are inclusive.
    SearchSpec(query="x", snippet_tokens=MIN_SNIPPET_TOKENS)
    SearchSpec(query="x", snippet_tokens=MAX_SNIPPET_TOKENS)


def test_unknown_filter_field_rejected():
    with pytest.raises(ValueError) as exc:
        SearchSpec(
            query="x",
            filters=(Predicate(field="raw_json", op="=", value="secret"),),
        )
    assert "raw_json" in str(exc.value)
    assert "allowed" in str(exc.value)


def test_known_filter_fields_accepted():
    SearchSpec(
        query="x",
        filters=(
            Predicate(field="client", op="=", value="claude"),
            Predicate(
                field="type", op="in", value=("user_message", "tool_call")
            ),
        ),
    )
