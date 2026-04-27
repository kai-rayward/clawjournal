"""Validation tests for AggregationSpec / Metric / Predicate (plan 10)."""

from __future__ import annotations

import pytest

from clawjournal.events.aggregate import (
    AggregationSpec,
    HARD_LIMIT_CEILING,
    MAX_DIMENSIONS,
    Metric,
    Predicate,
)


def test_metric_count_rejects_field():
    with pytest.raises(ValueError):
        Metric(kind="count", field="x")


def test_metric_sum_requires_field():
    with pytest.raises(ValueError):
        Metric(kind="sum")


def test_metric_unknown_kind_rejected():
    with pytest.raises(ValueError):
        Metric(kind="median")


def test_metric_output_key():
    assert Metric(kind="count").output_key == "count"
    assert Metric(kind="sum", field="input").output_key == "sum_input"
    assert Metric(kind="avg", field="input").output_key == "avg_input"


def test_predicate_unknown_op_rejected():
    with pytest.raises(ValueError):
        Predicate(field="client", op="like", value="x")


def test_predicate_in_requires_tuple():
    with pytest.raises(ValueError):
        Predicate(field="client", op="in", value="claude")
    Predicate(field="client", op="in", value=("claude", "codex"))


def test_spec_requires_dimensions():
    with pytest.raises(ValueError):
        AggregationSpec(domain="events", dimensions=(), metrics=(Metric(kind="count"),))


def test_spec_caps_dimensions():
    too_many = ("client", "type", "confidence", "source")
    assert len(too_many) > MAX_DIMENSIONS
    with pytest.raises(ValueError):
        AggregationSpec(
            domain="events",
            dimensions=too_many,
            metrics=(Metric(kind="count"),),
        )


def test_spec_rejects_duplicate_dimensions():
    with pytest.raises(ValueError):
        AggregationSpec(
            domain="events",
            dimensions=("client", "client"),
            metrics=(Metric(kind="count"),),
        )


def test_spec_requires_metrics():
    with pytest.raises(ValueError):
        AggregationSpec(domain="events", dimensions=("client",), metrics=())


def test_spec_limit_ceiling():
    AggregationSpec(
        domain="events",
        dimensions=("client",),
        metrics=(Metric(kind="count"),),
        limit=HARD_LIMIT_CEILING,
    )
    with pytest.raises(ValueError):
        AggregationSpec(
            domain="events",
            dimensions=("client",),
            metrics=(Metric(kind="count"),),
            limit=HARD_LIMIT_CEILING + 1,
        )


def test_spec_limit_must_be_positive():
    with pytest.raises(ValueError):
        AggregationSpec(
            domain="events",
            dimensions=("client",),
            metrics=(Metric(kind="count"),),
            limit=0,
        )
