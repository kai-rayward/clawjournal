"""Aggregation request shape (phase-1 plan 10).

`AggregationSpec` is the parsed, validated form of one
``clawjournal events aggregate`` / ``incidents aggregate`` /
``cost aggregate`` invocation. CLI handlers build it; the query
executor consumes it. Nothing here touches SQLite; that lives in
``query.py``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

VALID_OPS: frozenset[str] = frozenset({"=", "!=", ">", ">=", "<", "<=", "in"})
VALID_METRIC_KINDS: frozenset[str] = frozenset({"count", "sum", "avg"})

DEFAULT_LIMIT = 10
MAX_DIMENSIONS = 3
HARD_LIMIT_CEILING = 1000


@dataclass(frozen=True)
class Predicate:
    """One ``--where field<op>value`` clause, parameterized at execute time.

    ``field`` must already be validated against the domain's filter
    allowlist before construction. ``op`` must be in ``VALID_OPS``.
    For ``op == "in"``, ``value`` is a tuple of strings.
    """

    field: str
    op: str
    value: Any

    def __post_init__(self) -> None:
        if self.op not in VALID_OPS:
            raise ValueError(f"unsupported operator: {self.op!r}")
        if self.op == "in" and not isinstance(self.value, tuple):
            raise ValueError("`in` operator requires a tuple of values")


@dataclass(frozen=True)
class Metric:
    """One aggregation metric.

    - ``kind="count"`` ignores ``field``.
    - ``kind="sum"`` / ``"avg"`` require ``field`` to be in the
      domain's numeric-field allowlist.
    """

    kind: str
    field: str | None = None

    def __post_init__(self) -> None:
        if self.kind not in VALID_METRIC_KINDS:
            raise ValueError(f"unsupported metric kind: {self.kind!r}")
        if self.kind in ("sum", "avg") and not self.field:
            raise ValueError(f"{self.kind} requires a field")
        if self.kind == "count" and self.field is not None:
            raise ValueError("count does not take a field")

    @property
    def output_key(self) -> str:
        """Stable JSON key for this metric's value in a bucket."""

        if self.kind == "count":
            return "count"
        return f"{self.kind}_{self.field}"


@dataclass(frozen=True)
class AggregationSpec:
    """Validated aggregation request.

    Construction is the validation step — invariants below are
    enforced in ``__post_init__`` so any spec returned by a CLI
    handler is safe to feed straight into ``query.run``.
    """

    domain: str
    dimensions: tuple[str, ...]
    metrics: tuple[Metric, ...]
    filters: tuple[Predicate, ...] = ()
    since_iso: str | None = None
    limit: int = DEFAULT_LIMIT
    canonical: bool = False

    def __post_init__(self) -> None:
        if not self.dimensions:
            raise ValueError("at least one dimension is required")
        if len(self.dimensions) > MAX_DIMENSIONS:
            raise ValueError(
                f"--by accepts at most {MAX_DIMENSIONS} dimensions "
                f"(got {len(self.dimensions)})"
            )
        if len(set(self.dimensions)) != len(self.dimensions):
            raise ValueError(f"--by has duplicate dimension(s): {self.dimensions}")
        if not self.metrics:
            raise ValueError("at least one metric is required")
        if self.limit <= 0:
            raise ValueError("--limit must be positive")
        if self.limit > HARD_LIMIT_CEILING:
            raise ValueError(
                f"--limit ceiling is {HARD_LIMIT_CEILING} (got {self.limit})"
            )


__all__ = [
    "AggregationSpec",
    "DEFAULT_LIMIT",
    "HARD_LIMIT_CEILING",
    "MAX_DIMENSIONS",
    "Metric",
    "Predicate",
    "VALID_METRIC_KINDS",
    "VALID_OPS",
]
