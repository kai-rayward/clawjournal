"""SQL builder + executor for aggregation queries (phase-1 plan 10).

The builder produces a single parameterized SELECT with:

- One projection per ``--by`` dimension (keyed in output by the
  dimension's logical name).
- One projection per metric (count / sum / avg).
- WHERE clause assembled from filters and ``--since``; every
  user value lands as a ``?`` placeholder. No string interpolation.
- GROUP BY over all dimensions.
- ORDER BY primary metric DESC, then dimension keys ASC for
  deterministic ties.
- LIMIT N.

A second tiny query computes ``other_count`` and ``total`` so the
caller can reconstruct percentages.

Cost-domain queries that include neither ``data_source`` in
``--by`` nor a ``data_source`` filter are auto-partitioned by
``data_source`` (per plan §Security #5: prevents mixing API truth
with estimates in cost rollups). The partition is added as the
first dimension in the output so callers see the split at a glance.
"""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from typing import Any

from clawjournal.events.aggregate.registry import DomainRegistry, get_registry
from clawjournal.events.aggregate.spec import (
    AggregationSpec,
    Metric,
    Predicate,
)


@dataclass(frozen=True)
class AggregationResult:
    """Output of one ``query.run`` call.

    Buckets are ordered by primary metric DESC. ``other_count`` is
    the primary metric's value across rows truncated by ``--limit``
    (``total - sum-of-primary-metric-in-returned-buckets``).
    ``total`` is the primary metric's value across all matching
    rows pre-truncation. Both share the metric's units, so callers
    can reconstruct percentages without a second query.
    """

    spec: AggregationSpec
    buckets: list[dict[str, Any]]
    other_count: int | float
    total: int | float
    elapsed_ms: int
    rows_scanned: int
    auto_partitioned: bool = False


def run(
    spec: AggregationSpec,
    conn: sqlite3.Connection,
) -> AggregationResult:
    """Execute the aggregation. Caller is responsible for opening
    ``conn`` and ensuring the relevant schema exists.

    The bucket query and the summary query both run inside one
    explicit read transaction so the report is internally consistent
    against a single snapshot, even when ``clawjournal serve`` is
    concurrently writing. Without the wrap, a write that lands
    between the two queries would tilt ``total`` and ``rows_scanned``
    against the bucket counts, producing a wrong ``other_count``.
    Mirrors the pattern from ``events.doctor.probes.collect``.
    """

    registry = get_registry(spec.domain)
    effective_spec, auto_partitioned = _maybe_auto_partition(spec, registry)

    bucket_sql, params = _build_bucket_sql(effective_spec, registry)
    summary_sql, summary_params = _build_summary_sql(effective_spec, registry)

    in_explicit_tx = bool(conn.in_transaction)
    if not in_explicit_tx:
        conn.execute("BEGIN")
    try:
        started = time.perf_counter()
        cursor = conn.execute(bucket_sql, params)
        bucket_rows = list(cursor.fetchall())
        summary_row = conn.execute(summary_sql, summary_params).fetchone()
        elapsed_ms = int((time.perf_counter() - started) * 1000)
    finally:
        if not in_explicit_tx:
            try:
                conn.execute("COMMIT")
            except sqlite3.OperationalError:
                pass

    primary_key = effective_spec.metrics[0].output_key
    total_raw = summary_row[0] if summary_row else None
    if total_raw is None:
        total: int | float = 0
    elif isinstance(total_raw, float):
        total = total_raw
    else:
        total = int(total_raw)
    rows_scanned = (
        int(summary_row[1]) if summary_row and summary_row[1] is not None else 0
    )

    buckets = _shape_buckets(bucket_rows, effective_spec)
    primary_in_buckets = sum(b.get(primary_key, 0) for b in buckets)
    if isinstance(total, float) or isinstance(primary_in_buckets, float):
        other_count = max(total - primary_in_buckets, 0.0)
    else:
        other_count = max(int(total) - int(primary_in_buckets), 0)

    return AggregationResult(
        spec=effective_spec,
        buckets=buckets,
        other_count=other_count,
        total=total,
        elapsed_ms=elapsed_ms,
        rows_scanned=rows_scanned,
        auto_partitioned=auto_partitioned,
    )


def _maybe_auto_partition(
    spec: AggregationSpec, registry: DomainRegistry
) -> tuple[AggregationSpec, bool]:
    """Cost queries that don't already isolate ``data_source`` get it
    prepended as the first dimension so API truth and estimates never
    silently mix in a sum."""

    if registry.name != "cost":
        return spec, False
    if "data_source" in spec.dimensions:
        return spec, False
    if any(p.field == "data_source" for p in spec.filters):
        return spec, False
    new_dims = ("data_source",) + spec.dimensions
    if len(new_dims) > 3:
        # Caller already maxed out --by; don't silently drop one of
        # their dimensions. Better to surface the cap as a usage error.
        raise ValueError(
            "cost aggregation auto-partitions by data_source; with three "
            "explicit --by dimensions there's no room. Either drop a "
            "dimension or filter --where data_source=api/estimated."
        )
    return (
        AggregationSpec(
            domain=spec.domain,
            dimensions=new_dims,
            metrics=spec.metrics,
            filters=spec.filters,
            since_iso=spec.since_iso,
            limit=spec.limit,
            canonical=spec.canonical,
        ),
        True,
    )


def _build_bucket_sql(
    spec: AggregationSpec, registry: DomainRegistry
) -> tuple[str, list[Any]]:
    dim_projections = _dim_projections(spec, registry)
    metric_projections = _metric_projections(spec, registry)
    where_clause, where_params = _where_clause(spec, registry)
    primary_metric = spec.metrics[0]

    select_clause = ", ".join(dim_projections + metric_projections)
    group_by = ", ".join(
        f"{i + 1}" for i in range(len(spec.dimensions))
    )
    # Order: primary metric DESC, then dimension keys ASC for stable
    # tie-break. The metric projection alias is its output_key.
    order_keys = [f"{primary_metric.output_key} DESC"]
    order_keys.extend(
        f"{i + 1} ASC NULLS LAST" for i in range(len(spec.dimensions))
    )
    order_by = ", ".join(order_keys)

    sql = (
        f"SELECT {select_clause} "
        f"{registry.base_sql} "
        f"{where_clause}"
        f"GROUP BY {group_by} "
        f"ORDER BY {order_by} "
        f"LIMIT ?"
    )
    return sql, where_params + [spec.limit]


def _build_summary_sql(
    spec: AggregationSpec, registry: DomainRegistry
) -> tuple[str, list[Any]]:
    """Compute ``total`` (primary metric over all matching rows, no
    GROUP BY) and ``rows_scanned`` (raw row count) in one pass."""

    primary_metric = spec.metrics[0]
    total_expr = _metric_expr(primary_metric, registry)
    where_clause, params = _where_clause(spec, registry)
    sql = (
        f"SELECT {total_expr}, COUNT(*) "
        f"{registry.base_sql} "
        f"{where_clause}"
    )
    return sql, params


def _dim_projections(
    spec: AggregationSpec, registry: DomainRegistry
) -> list[str]:
    out: list[str] = []
    for dim in spec.dimensions:
        if dim not in registry.dimensions:
            raise ValueError(
                f"dimension {dim!r} is not registered for domain "
                f"{registry.name!r}"
            )
        out.append(f'{registry.dimensions[dim].sql} AS "{dim}"')
    return out


def _metric_projections(
    spec: AggregationSpec, registry: DomainRegistry
) -> list[str]:
    return [
        f'{_metric_expr(m, registry)} AS "{m.output_key}"' for m in spec.metrics
    ]


def _metric_expr(metric: Metric, registry: DomainRegistry) -> str:
    if metric.kind == "count":
        return "COUNT(*)"
    if metric.field is None:
        raise ValueError(f"metric {metric.kind} requires a field")
    spec = registry.metric_fields.get(metric.field)
    if spec is None or not spec.numeric:
        raise ValueError(
            f"metric field {metric.field!r} is not numeric for domain "
            f"{registry.name!r} "
            f"(allowed: {sorted(k for k, v in registry.metric_fields.items() if v.numeric)})"
        )
    op = "SUM" if metric.kind == "sum" else "AVG"
    return f"COALESCE({op}({spec.sql}), 0)"


def _where_clause(
    spec: AggregationSpec, registry: DomainRegistry
) -> tuple[str, list[Any]]:
    pieces: list[str] = []
    params: list[Any] = []
    for predicate in spec.filters:
        sql, p = _predicate_sql(predicate, registry)
        pieces.append(sql)
        params.extend(p)
    if spec.since_iso is not None:
        pieces.append(f"{registry.time_field} >= ?")
        params.append(spec.since_iso)
    if not pieces:
        return "", params
    return "WHERE " + " AND ".join(pieces) + " ", params


def _predicate_sql(
    predicate: Predicate, registry: DomainRegistry
) -> tuple[str, list[Any]]:
    spec = registry.filters.get(predicate.field)
    if spec is None:
        raise ValueError(
            f"filter field {predicate.field!r} not registered for domain "
            f"{registry.name!r}"
        )
    if predicate.op == "in":
        values = predicate.value
        if not isinstance(values, tuple) or not values:
            raise ValueError(
                f"`in` predicate for {predicate.field!r} requires a "
                f"non-empty tuple"
            )
        placeholders = ",".join("?" for _ in values)
        return f"{spec.sql} IN ({placeholders})", list(values)
    return f"{spec.sql} {predicate.op} ?", [predicate.value]


def _shape_buckets(
    rows: list[Any], spec: AggregationSpec
) -> list[dict[str, Any]]:
    """Convert raw cursor rows into the output bucket shape:
    ``{"key": {<dim>: <value>, ...}, "<metric_output_key>": <value>, ...}``."""

    out: list[dict[str, Any]] = []
    n_dims = len(spec.dimensions)
    for row in rows:
        bucket_key = {
            dim: row[idx] for idx, dim in enumerate(spec.dimensions)
        }
        bucket: dict[str, Any] = {"key": bucket_key}
        for i, metric in enumerate(spec.metrics):
            value = row[n_dims + i]
            if metric.kind == "count":
                bucket["count"] = int(value) if value is not None else 0
            elif metric.kind == "sum":
                # Preserve SQLite's natural type — integer columns return
                # int, REAL columns return float. The previous heuristic
                # (coerce whole floats to int) made type unstable across
                # rows: `sum_cost_estimate` would flip from float to int
                # for buckets whose total happened to be a whole number,
                # surprising JSON consumers that expected a stable shape.
                # COALESCE in SQL ensures `value` is never None here, but
                # we keep the guard for defense.
                bucket[metric.output_key] = value if value is not None else 0
            else:  # avg
                bucket[metric.output_key] = (
                    float(value) if value is not None else 0.0
                )
        out.append(bucket)
    return out


__all__ = ["AggregationResult", "run"]
