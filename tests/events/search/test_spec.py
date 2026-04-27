"""Validation tests for `SearchSpec` (plan 11)."""

from __future__ import annotations

import pytest

from clawjournal.events.aggregate.spec import Predicate
from clawjournal.events.search import (
    DEFAULT_LIMIT,
    DEFAULT_SNIPPET_LENGTH,
    HARD_LIMIT_CEILING,
    MAX_QUERY_BYTES,
    SearchSpec,
)


def test_defaults():
    spec = SearchSpec(query="hello")
    assert spec.query == "hello"
    assert spec.limit == DEFAULT_LIMIT
    assert spec.snippet_length == DEFAULT_SNIPPET_LENGTH
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


def test_snippet_length_bounds():
    with pytest.raises(ValueError):
        SearchSpec(query="x", snippet_length=4)
    with pytest.raises(ValueError):
        SearchSpec(query="x", snippet_length=99999)


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
