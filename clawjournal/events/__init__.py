"""Execution recorder + signal/confidence layer for phase-1 plans 02 + 03."""

from clawjournal.events.capabilities import CAPABILITY_MATRIX, capabilities_json
from clawjournal.events.ingest import EVENT_CONSUMER_ID, IngestSummary, ingest_pending
from clawjournal.events.schema import ensure_schema
from clawjournal.events.types import (
    CONFIDENCE_RANK,
    EVENT_TYPES,
    ClassifiedEvent,
    SessionMeta,
)
from clawjournal.events.view import (
    CanonicalEvent,
    CapabilityState,
    canonical_events,
    capability_join,
    ensure_view_schema,
    fetch_vendor_line,
    write_hook_override,
)

__all__ = [
    "CAPABILITY_MATRIX",
    "CONFIDENCE_RANK",
    "CanonicalEvent",
    "CapabilityState",
    "EVENT_CONSUMER_ID",
    "EVENT_TYPES",
    "ClassifiedEvent",
    "IngestSummary",
    "SessionMeta",
    "canonical_events",
    "capabilities_json",
    "capability_join",
    "ensure_schema",
    "ensure_view_schema",
    "fetch_vendor_line",
    "ingest_pending",
    "write_hook_override",
]
