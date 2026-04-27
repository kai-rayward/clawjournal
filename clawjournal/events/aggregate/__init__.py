"""Aggregation queries (phase-1 plan 10).

Public API:

- ``AggregationSpec`` / ``Metric`` / ``Predicate`` — the parsed,
  validated request shape. CLI handlers build them; ``query.run``
  consumes them.
- ``parse_where_clauses`` / ``parse_since`` — input parsers used by
  CLI handlers and tests.
- ``query.run(spec, conn) -> AggregationResult`` — execute the
  aggregation. Caller manages the connection.
- ``render_json`` / ``render_human`` — output renderers.

Module placement matches plan 10 §Module placement (implicit;
mirrors the doctor package layout from plan 08).
"""

from __future__ import annotations

from clawjournal.events.aggregate.filters import parse_where_clauses
from clawjournal.events.aggregate.query import AggregationResult, run
from clawjournal.events.aggregate.registry import (
    DomainRegistry,
    REGISTRIES,
    get_registry,
)
from clawjournal.events.aggregate.render import (
    EVENTS_AGGREGATE_SCHEMA_VERSION,
    render_human,
    render_json,
    result_to_payload,
)
from clawjournal.events.aggregate.spec import (
    AggregationSpec,
    DEFAULT_LIMIT,
    HARD_LIMIT_CEILING,
    MAX_DIMENSIONS,
    Metric,
    Predicate,
    VALID_METRIC_KINDS,
    VALID_OPS,
)
from clawjournal.events.aggregate.windows import parse_since

__all__ = [
    "AggregationResult",
    "AggregationSpec",
    "DEFAULT_LIMIT",
    "DomainRegistry",
    "EVENTS_AGGREGATE_SCHEMA_VERSION",
    "HARD_LIMIT_CEILING",
    "MAX_DIMENSIONS",
    "Metric",
    "Predicate",
    "REGISTRIES",
    "VALID_METRIC_KINDS",
    "VALID_OPS",
    "get_registry",
    "parse_since",
    "parse_where_clauses",
    "render_human",
    "render_json",
    "result_to_payload",
    "run",
]
