"""Cost ledger for the execution recorder (phase-1 plan 04).

Per-event token + cost accounting on top of the raw event stream from
02. The ledger reads `events.raw_json`, extracts vendor token-usage
blocks via per-client extractors, persists a `token_usage` row keyed
by `event_id`, and runs anomaly detectors (cache_read_collapse,
input_spike, model_shift, service_tier_shift) into `cost_anomalies`.

Every `token_usage` row records `data_source` (`api` vs `estimated`)
so anomaly detectors can ignore comparisons that mix vendor-reported
counts with content-length estimates.
"""

from __future__ import annotations

from clawjournal.events.cost.anomalies import (
    ANOMALY_KINDS,
    AnomalyHit,
    detect_session_anomalies,
)
from clawjournal.events.cost.extract import extract_tokens
from clawjournal.events.cost.ingest import (
    COST_CONSUMER_ID,
    CostIngestSummary,
    ingest_cost_pending,
    rebuild_cost_ledger,
    rebuild_cost_ledger_for_sessions,
)
from clawjournal.events.cost.pricing import (
    PRICING_TABLE,
    PRICING_TABLE_VERSION,
    ModelInfo,
    estimate_cost,
    normalize_model,
)
from clawjournal.events.cost.schema import ensure_cost_schema
from clawjournal.events.cost.types import (
    DATA_SOURCE_API,
    DATA_SOURCE_ESTIMATED,
    VALID_DATA_SOURCES,
    CostAnomaly,
    TokenUsage,
)

__all__ = [
    "ANOMALY_KINDS",
    "AnomalyHit",
    "COST_CONSUMER_ID",
    "CostAnomaly",
    "CostIngestSummary",
    "DATA_SOURCE_API",
    "DATA_SOURCE_ESTIMATED",
    "ModelInfo",
    "PRICING_TABLE",
    "PRICING_TABLE_VERSION",
    "TokenUsage",
    "VALID_DATA_SOURCES",
    "detect_session_anomalies",
    "ensure_cost_schema",
    "estimate_cost",
    "extract_tokens",
    "ingest_cost_pending",
    "normalize_model",
    "rebuild_cost_ledger",
    "rebuild_cost_ledger_for_sessions",
]
