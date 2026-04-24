"""Incidents pipeline (phase-1 plan 05) — currently the loop detector lite.

Reads `events.raw_json` and emits `incidents` rows when the same
shell command (3+ in a row) or the same tool call (5+ in a row)
fires consecutively with the same normalized outcome. Later beats
add new `incidents.kind` values (unchanged-diff heuristics, novel-args
ratio, etc.) into the same table.

Public surface:

- `ensure_incidents_schema(conn)` — creates `incidents` +
  `loop_ingest_state` if absent.
- `ingest_loop_incidents(conn, *, rebuild=False)` — incremental
  driver that scans only sessions touched by new events, recomputes
  loops for each touched session, and replaces that session's
  `incidents` rows so growing runs update in place.
- `detect_session_loops(conn, session_id)` — pure read that returns
  the current set of loop hits for a session without writing.
- `normalize_outcome_text(text)` — stderr/output normalizer used by
  the detector (documented ruleset; testable rule-by-rule).
"""

from __future__ import annotations

from clawjournal.events.incidents.ingest import (
    LOOP_CONSUMER_ID,
    LoopIngestSummary,
    ingest_loop_incidents,
    rebuild_loop_incidents,
    rebuild_loop_incidents_for_sessions,
)
from clawjournal.events.incidents.loop_detector import (
    DEFAULT_SHELL_THRESHOLD,
    DEFAULT_TOOL_CALL_THRESHOLD,
    IncidentHit,
    LoopRule,
    detect_session_loops,
)
from clawjournal.events.incidents.normalize import normalize_outcome_text
from clawjournal.events.incidents.schema import ensure_incidents_schema
from clawjournal.events.incidents.types import LOOP_INCIDENT_KIND, ValidIncidentKinds

__all__ = [
    "DEFAULT_SHELL_THRESHOLD",
    "DEFAULT_TOOL_CALL_THRESHOLD",
    "IncidentHit",
    "LOOP_CONSUMER_ID",
    "LOOP_INCIDENT_KIND",
    "LoopIngestSummary",
    "LoopRule",
    "ValidIncidentKinds",
    "detect_session_loops",
    "ensure_incidents_schema",
    "ingest_loop_incidents",
    "normalize_outcome_text",
    "rebuild_loop_incidents",
    "rebuild_loop_incidents_for_sessions",
]
