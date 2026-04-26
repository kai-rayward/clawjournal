"""Renderers for aggregation results (phase-1 plan 10).

JSON output carries a pinned schema version
(``events_aggregate_schema_version: "1.0"``) and ``_meta`` block
with ``elapsed_ms`` / ``rows_scanned`` / ``request_id``.

Bucket keys for the ``workspace`` dimension go through
``Anonymizer().path()`` so absolute paths render as ``~/...``.
Other dimension keys are emitted verbatim — they're internal enum
values (``client``, ``type``, ``confidence``, ``data_source``, ...)
or already-anonymized session identifiers.
"""

from __future__ import annotations

import json
from io import StringIO
from typing import Any, TextIO

from clawjournal.events.aggregate.query import AggregationResult
from clawjournal.events.aggregate.registry import get_registry
from clawjournal.redaction.anonymizer import Anonymizer

EVENTS_AGGREGATE_SCHEMA_VERSION = "1.0"


def render_json(
    result: AggregationResult,
    *,
    request_id: str | None = None,
) -> str:
    """Return the canonical JSON payload as a string."""

    payload = result_to_payload(result, request_id=request_id)
    return json.dumps(payload, indent=2, sort_keys=True)


def result_to_payload(
    result: AggregationResult,
    *,
    request_id: str | None = None,
) -> dict[str, Any]:
    spec = result.spec
    registry = get_registry(spec.domain)
    anonymizer = Anonymizer()

    buckets_out: list[dict[str, Any]] = []
    for bucket in result.buckets:
        new_key: dict[str, Any] = {}
        for dim, value in bucket["key"].items():
            field_spec = registry.dimensions.get(dim)
            if (
                value is not None
                and field_spec is not None
                and field_spec.anonymize_in_output
                and isinstance(value, str)
            ):
                new_key[dim] = anonymizer.path(value)
            else:
                new_key[dim] = value
        new_bucket = {"key": new_key}
        for k, v in bucket.items():
            if k == "key":
                continue
            new_bucket[k] = v
        buckets_out.append(new_bucket)

    aggregation: dict[str, Any] = {
        "by": list(spec.dimensions),
        "metric": [m.output_key for m in spec.metrics],
        "buckets": buckets_out,
        "other_count": result.other_count,
        "total": result.total,
    }
    if result.auto_partitioned:
        aggregation["auto_partitioned_by"] = "data_source"

    payload: dict[str, Any] = {
        "events_aggregate_schema_version": EVENTS_AGGREGATE_SCHEMA_VERSION,
        "domain": spec.domain,
        "aggregation": aggregation,
        "_meta": {
            "elapsed_ms": result.elapsed_ms,
            "rows_scanned": result.rows_scanned,
        },
    }
    if request_id is not None:
        payload["_meta"]["request_id"] = request_id
    return payload


def render_human(
    result: AggregationResult,
    *,
    stream: TextIO | None = None,
) -> str:
    """Render a tabular human-readable view. Returns the text."""

    spec = result.spec
    buf = StringIO()
    if result.auto_partitioned:
        buf.write(
            "(auto-partitioned by data_source — pass "
            "--where data_source=api or --by data_source explicitly to suppress)\n\n"
        )
    headers = list(spec.dimensions) + [m.output_key for m in spec.metrics]
    buf.write(" | ".join(headers) + "\n")
    buf.write(" | ".join("-" * max(len(h), 3) for h in headers) + "\n")

    registry = get_registry(spec.domain)
    anonymizer = Anonymizer()
    for bucket in result.buckets:
        row_cells: list[str] = []
        for dim in spec.dimensions:
            value = bucket["key"].get(dim)
            field_spec = registry.dimensions.get(dim)
            if (
                value is not None
                and field_spec is not None
                and field_spec.anonymize_in_output
                and isinstance(value, str)
            ):
                value = anonymizer.path(value)
            row_cells.append("∅" if value is None else str(value))
        for metric in spec.metrics:
            row_cells.append(str(bucket.get(metric.output_key, "")))
        buf.write(" | ".join(row_cells) + "\n")

    buf.write(
        f"\n... and {result.other_count} more "
        f"(total {result.total}; {result.rows_scanned} rows scanned in "
        f"{result.elapsed_ms} ms)\n"
    )
    text = buf.getvalue()
    if stream is not None:
        stream.write(text)
    return text


__all__ = [
    "EVENTS_AGGREGATE_SCHEMA_VERSION",
    "render_human",
    "render_json",
    "result_to_payload",
]
