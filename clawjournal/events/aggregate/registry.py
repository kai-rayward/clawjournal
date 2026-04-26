"""Per-domain registry: which dimensions, filters, metrics, and base
table apply to each aggregation subcommand (phase-1 plan 10).

Registries are the single source of truth for "what's allowed."
The CLI parser asks the registry whether a `--by` value or a
`--where` field is valid. The SQL builder asks the registry for
the SQL projection of a dimension and the table/JOIN clause.

Adding a new dimension or filterable field starts here; the SQL
builder picks it up automatically.

Workspace dimension: derived in SQL from `event_sessions.session_key`
(format: `<client>:<workspace>:<session_id>` or `<client>:<path>`).
The render layer passes the value through the path anonymizer so
absolute paths render as `~/...`.
"""

from __future__ import annotations

from dataclasses import dataclass

# Shared SQL fragment that extracts the workspace segment from a
# session_key. Handles both `<client>:<ws>:<session>` (claude) and
# `<client>:<path>` (codex / openclaw) layouts. Returns the raw
# segment; the render layer anonymizes paths.
_WORKSPACE_SQL = (
    "CASE "
    "WHEN instr(substr(session_key, instr(session_key, ':') + 1), ':') > 0 THEN "
    "substr("
    "session_key, "
    "instr(session_key, ':') + 1, "
    "instr(substr(session_key, instr(session_key, ':') + 1), ':') - 1"
    ") "
    "ELSE substr(session_key, instr(session_key, ':') + 1) "
    "END"
)


@dataclass(frozen=True)
class FieldSpec:
    """Mapping from a logical name (used in ``--by`` / ``--where``)
    to the SQL expression that renders it.

    ``sql`` is fully qualified with table aliases used by the domain
    builder (events: ``e`` events, ``s`` event_sessions; cost:
    ``t`` token_usage, ``s`` event_sessions; incidents: ``i``
    incidents, ``s`` event_sessions). ``numeric=True`` means the
    field is a valid argument to ``sum:`` / ``avg:`` metrics.
    """

    sql: str
    numeric: bool = False
    anonymize_in_output: bool = False


@dataclass(frozen=True)
class DomainRegistry:
    """All metadata for one aggregation subcommand.

    ``base_sql`` is the FROM/JOIN portion (without WHERE). ``time_field``
    is the column the ``--since`` window filters on.
    """

    name: str
    base_sql: str
    time_field: str
    dimensions: dict[str, FieldSpec]
    filters: dict[str, FieldSpec]
    metric_fields: dict[str, FieldSpec]


_EVENTS_BASE = (
    "FROM events AS e "
    "JOIN event_sessions AS s ON s.id = e.session_id"
)

_EVENTS_DIMENSIONS: dict[str, FieldSpec] = {
    "client": FieldSpec(sql="e.client"),
    "type": FieldSpec(sql="e.type"),
    "confidence": FieldSpec(sql="e.confidence"),
    "source": FieldSpec(sql="e.source"),
    "lossiness": FieldSpec(sql="e.lossiness"),
    "session": FieldSpec(sql="s.session_key"),
    "workspace": FieldSpec(sql=_WORKSPACE_SQL, anonymize_in_output=True),
    "date": FieldSpec(sql="substr(e.event_at, 1, 10)"),
    "hour": FieldSpec(sql="substr(e.event_at, 1, 13)"),
}

_EVENTS_FILTERS: dict[str, FieldSpec] = {
    "client": FieldSpec(sql="e.client"),
    "type": FieldSpec(sql="e.type"),
    "confidence": FieldSpec(sql="e.confidence"),
    "source": FieldSpec(sql="e.source"),
    "session": FieldSpec(sql="s.session_key"),
    "workspace": FieldSpec(sql=_WORKSPACE_SQL),
    "event_at": FieldSpec(sql="e.event_at"),
}

_INCIDENTS_BASE = (
    "FROM incidents AS i "
    "JOIN event_sessions AS s ON s.id = i.session_id"
)

_INCIDENTS_DIMENSIONS: dict[str, FieldSpec] = {
    "kind": FieldSpec(sql="i.kind"),
    "confidence": FieldSpec(sql="i.confidence"),
    "session": FieldSpec(sql="s.session_key"),
    "workspace": FieldSpec(sql=_WORKSPACE_SQL, anonymize_in_output=True),
    "date": FieldSpec(sql="substr(i.created_at, 1, 10)"),
}

_INCIDENTS_FILTERS: dict[str, FieldSpec] = {
    "kind": FieldSpec(sql="i.kind"),
    "confidence": FieldSpec(sql="i.confidence"),
    "session": FieldSpec(sql="s.session_key"),
    "workspace": FieldSpec(sql=_WORKSPACE_SQL),
    "created_at": FieldSpec(sql="i.created_at"),
}

_INCIDENTS_METRIC_FIELDS: dict[str, FieldSpec] = {
    "count": FieldSpec(sql="i.count", numeric=True),
}

_COST_BASE = (
    "FROM token_usage AS t "
    "JOIN event_sessions AS s ON s.id = t.session_id"
)

_COST_DIMENSIONS: dict[str, FieldSpec] = {
    "model": FieldSpec(sql="t.model"),
    "provider": FieldSpec(sql="t.model_provider"),
    "data_source": FieldSpec(sql="t.data_source"),
    "service_tier": FieldSpec(sql="t.service_tier"),
    "pricing_table_version": FieldSpec(sql="t.pricing_table_version"),
    "session": FieldSpec(sql="s.session_key"),
    "workspace": FieldSpec(sql=_WORKSPACE_SQL, anonymize_in_output=True),
    "date": FieldSpec(sql="substr(t.event_at, 1, 10)"),
}

_COST_FILTERS: dict[str, FieldSpec] = {
    "model": FieldSpec(sql="t.model"),
    "data_source": FieldSpec(sql="t.data_source"),
    "service_tier": FieldSpec(sql="t.service_tier"),
    "pricing_table_version": FieldSpec(sql="t.pricing_table_version"),
    "session": FieldSpec(sql="s.session_key"),
    "workspace": FieldSpec(sql=_WORKSPACE_SQL),
}

_COST_METRIC_FIELDS: dict[str, FieldSpec] = {
    "input_tokens": FieldSpec(sql="t.input", numeric=True),
    "output_tokens": FieldSpec(sql="t.output", numeric=True),
    "cache_read_tokens": FieldSpec(sql="t.cache_read", numeric=True),
    "cache_creation_tokens": FieldSpec(sql="t.cache_write", numeric=True),
    "thinking_tokens": FieldSpec(sql="t.reasoning", numeric=True),
    "cost_estimate": FieldSpec(sql="t.cost_estimate", numeric=True),
}


REGISTRIES: dict[str, DomainRegistry] = {
    "events": DomainRegistry(
        name="events",
        base_sql=_EVENTS_BASE,
        time_field="e.event_at",
        dimensions=_EVENTS_DIMENSIONS,
        filters=_EVENTS_FILTERS,
        metric_fields={},  # events has no numeric fields exposed today
    ),
    "incidents": DomainRegistry(
        name="incidents",
        base_sql=_INCIDENTS_BASE,
        time_field="i.created_at",
        dimensions=_INCIDENTS_DIMENSIONS,
        filters=_INCIDENTS_FILTERS,
        metric_fields=_INCIDENTS_METRIC_FIELDS,
    ),
    "cost": DomainRegistry(
        name="cost",
        base_sql=_COST_BASE,
        time_field="t.event_at",
        dimensions=_COST_DIMENSIONS,
        filters=_COST_FILTERS,
        metric_fields=_COST_METRIC_FIELDS,
    ),
}


def get_registry(domain: str) -> DomainRegistry:
    if domain not in REGISTRIES:
        raise KeyError(
            f"unknown aggregation domain: {domain!r} "
            f"(known: {sorted(REGISTRIES)})"
        )
    return REGISTRIES[domain]


__all__ = [
    "DomainRegistry",
    "FieldSpec",
    "REGISTRIES",
    "get_registry",
]
