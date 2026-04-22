"""Cost-ledger anomaly detectors.

Four kinds:

- `cache_read_collapse` — cache_read tokens dropped >= 50% between
  two adjacent assistant turns whose input_tokens are within 25% of
  each other (so we're not flagging a small turn against a big one).
  Only fires when both adjacent rows are `data_source='api'`.

- `input_spike` — current `input` exceeds 3x the rolling mean of the
  prior `INPUT_SPIKE_BASELINE_WINDOW` rows. `data_source='estimated'`
  rows are skipped entirely (both as the candidate and as part of the
  baseline window).

- `model_shift` — model id changed between adjacent rows.
  `data_source` is not consulted: the model id is metadata, not a
  count, so estimates participate.

- `service_tier_shift` — service_tier changed between adjacent rows
  with the same model. Surfaced as a separate kind from `model_shift`
  so consumers can distinguish "same model, different latency tier"
  (a routing change) from a model swap.

Detection runs against `token_usage` rows for a session, ordered by
event_at NULLS LAST then event_id. Returns a list of `AnomalyHit`
objects without writing to the database — the ingest layer commits
them via `INSERT OR IGNORE` so re-runs are idempotent under the
`UNIQUE(session_id, kind, turn_event_id)` constraint.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from typing import Any

from clawjournal.events.cost.types import DATA_SOURCE_API

ANOMALY_KINDS = (
    "cache_read_collapse",
    "input_spike",
    "model_shift",
    "service_tier_shift",
)

CACHE_READ_COLLAPSE_DROP_RATIO = 0.5     # 50% drop required
CACHE_READ_COLLAPSE_INPUT_TOLERANCE = 0.25  # adjacent inputs within +/-25%
INPUT_SPIKE_BASELINE_WINDOW = 5
INPUT_SPIKE_MULTIPLIER = 3.0


@dataclass(frozen=True)
class AnomalyHit:
    session_id: int
    turn_event_id: int
    kind: str
    confidence: str
    evidence: dict[str, Any] = field(default_factory=dict)


def detect_session_anomalies(
    conn: sqlite3.Connection, session_id: int
) -> list[AnomalyHit]:
    """Run all anomaly detectors against a session's token_usage rows.

    Reads only — never writes. The caller persists the returned hits
    via `INSERT OR IGNORE` into `cost_anomalies`.
    """
    rows = list(
        conn.execute(
            """
            SELECT event_id, model, input, output, cache_read, cache_write,
                   reasoning, service_tier, data_source, event_at
              FROM token_usage
             WHERE session_id = ?
             ORDER BY event_at IS NULL, event_at, event_id
            """,
            (session_id,),
        )
    )
    if not rows:
        return []

    hits: list[AnomalyHit] = []
    hits.extend(_detect_cache_read_collapse(session_id, rows))
    hits.extend(_detect_input_spike(session_id, rows))
    hits.extend(_detect_model_shift(session_id, rows))
    hits.extend(_detect_service_tier_shift(session_id, rows))
    return hits


def _detect_cache_read_collapse(
    session_id: int, rows: list[sqlite3.Row]
) -> list[AnomalyHit]:
    hits: list[AnomalyHit] = []
    prev: sqlite3.Row | None = None
    for row in rows:
        if row["data_source"] != DATA_SOURCE_API:
            # An estimated row breaks the chain — we don't reason
            # about cache state across an estimate.
            prev = None
            continue
        if prev is None or prev["data_source"] != DATA_SOURCE_API:
            prev = row
            continue
        if not _similar_input(prev["input"], row["input"]):
            prev = row
            continue
        cur_cache = row["cache_read"]
        prev_cache = prev["cache_read"]
        if prev_cache is None or cur_cache is None:
            prev = row
            continue
        if prev_cache <= 0:
            prev = row
            continue
        drop = (prev_cache - cur_cache) / prev_cache
        if drop >= CACHE_READ_COLLAPSE_DROP_RATIO:
            hits.append(
                AnomalyHit(
                    session_id=session_id,
                    turn_event_id=int(row["event_id"]),
                    kind="cache_read_collapse",
                    confidence="high",
                    evidence={
                        "previous_cache_read": prev_cache,
                        "current_cache_read": cur_cache,
                        "drop_ratio": round(drop, 4),
                        "previous_input": prev["input"],
                        "current_input": row["input"],
                        "previous_event_id": int(prev["event_id"]),
                        "drop_threshold": CACHE_READ_COLLAPSE_DROP_RATIO,
                    },
                )
            )
        prev = row
    return hits


def _detect_input_spike(
    session_id: int, rows: list[sqlite3.Row]
) -> list[AnomalyHit]:
    hits: list[AnomalyHit] = []
    baseline: list[int] = []
    for row in rows:
        if row["data_source"] != DATA_SOURCE_API:
            # Estimated rows participate in neither baseline nor candidate.
            continue
        cur = row["input"]
        if cur is None:
            continue
        if len(baseline) >= INPUT_SPIKE_BASELINE_WINDOW:
            mean = sum(baseline) / len(baseline)
            if mean > 0 and cur > mean * INPUT_SPIKE_MULTIPLIER:
                hits.append(
                    AnomalyHit(
                        session_id=session_id,
                        turn_event_id=int(row["event_id"]),
                        kind="input_spike",
                        confidence="high",
                        evidence={
                            "current_input": cur,
                            "baseline_mean": round(mean, 2),
                            "baseline_window": list(baseline),
                            "multiplier": INPUT_SPIKE_MULTIPLIER,
                            "ratio": round(cur / mean, 2),
                        },
                    )
                )
        baseline.append(cur)
        if len(baseline) > INPUT_SPIKE_BASELINE_WINDOW:
            baseline.pop(0)
    return hits


def _detect_model_shift(
    session_id: int, rows: list[sqlite3.Row]
) -> list[AnomalyHit]:
    hits: list[AnomalyHit] = []
    prev_model: str | None = None
    prev_event_id: int | None = None
    for row in rows:
        cur_model = row["model"]
        if cur_model is None:
            continue
        if prev_model is not None and cur_model != prev_model:
            hits.append(
                AnomalyHit(
                    session_id=session_id,
                    turn_event_id=int(row["event_id"]),
                    kind="model_shift",
                    confidence="high",
                    evidence={
                        "previous_model": prev_model,
                        "current_model": cur_model,
                        "previous_event_id": prev_event_id,
                    },
                )
            )
        prev_model = cur_model
        prev_event_id = int(row["event_id"])
    return hits


def _detect_service_tier_shift(
    session_id: int, rows: list[sqlite3.Row]
) -> list[AnomalyHit]:
    hits: list[AnomalyHit] = []
    prev_tier: str | None = None
    prev_model: str | None = None
    prev_event_id: int | None = None
    for row in rows:
        cur_tier = row["service_tier"]
        cur_model = row["model"]
        if cur_tier is None:
            continue
        if (
            prev_tier is not None
            and cur_tier != prev_tier
            and prev_model is not None
            and cur_model is not None
            and cur_model == prev_model
        ):
            hits.append(
                AnomalyHit(
                    session_id=session_id,
                    turn_event_id=int(row["event_id"]),
                    kind="service_tier_shift",
                    confidence="high",
                    evidence={
                        "previous_service_tier": prev_tier,
                        "current_service_tier": cur_tier,
                        "model": cur_model,
                        "previous_event_id": prev_event_id,
                    },
                )
            )
        prev_tier = cur_tier
        prev_model = cur_model
        prev_event_id = int(row["event_id"])
    return hits


def _similar_input(prev_input: int | None, cur_input: int | None) -> bool:
    """Cache-read collapse only fires when adjacent inputs are
    comparable in size — otherwise a tiny turn after a huge one
    naturally has fewer cache reads and we'd false-positive."""
    if prev_input is None or cur_input is None:
        return False
    if prev_input <= 0:
        return False
    ratio = cur_input / prev_input
    lo = 1 - CACHE_READ_COLLAPSE_INPUT_TOLERANCE
    hi = 1 + CACHE_READ_COLLAPSE_INPUT_TOLERANCE
    return lo <= ratio <= hi
