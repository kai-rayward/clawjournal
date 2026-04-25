"""Events doctor + agent-consumable surface (phase-1 plan 08).

Public API for ``events doctor`` / ``events features`` / ``events docs``.
"""

from __future__ import annotations

from clawjournal.events.doctor.envelope import (
    KIND_INDEX_MISSING,
    KIND_TOPIC_UNKNOWN,
    KIND_UNSPECIFIED,
    KIND_USAGE_ERROR,
    KIND_VERSION_INCOMPATIBLE,
    VALID_KINDS,
    attach_request_id,
    emit_error,
)
from clawjournal.events.doctor.overlay import (
    MAX_OVERLAY_ENTRIES,
    OVERLAY_FILENAME,
    OVERLAY_VERSION,
    effective_matrix,
    overlay_path,
    reset_cache,
    write_overlay_entries,
)
from clawjournal.events.doctor.probes import (
    ClientObservation,
    CostHealth,
    DoctorReport,
    INSTALL_DB_CORRUPT,
    INSTALL_DB_MISSING,
    INSTALL_EVENTS_EMPTY,
    INSTALL_FRESH,
    INSTALL_HEALTHY,
    INSTALL_WORKBENCH_ONLY,
    IncidentHealth,
    TruffleHogStatus,
    VERDICT_COMPATIBLE,
    VERDICT_PARTIAL,
    VERDICT_UNKNOWN_SCHEMA,
    collect,
    config_dir,
    exit_code_for,
    index_db_path,
    report_to_dict,
)
from clawjournal.events.doctor.render import (
    render_human,
    render_json,
    sanitize_for_human,
)

__all__ = [
    "ClientObservation",
    "CostHealth",
    "DoctorReport",
    "INSTALL_DB_CORRUPT",
    "INSTALL_DB_MISSING",
    "INSTALL_EVENTS_EMPTY",
    "INSTALL_FRESH",
    "INSTALL_HEALTHY",
    "INSTALL_WORKBENCH_ONLY",
    "IncidentHealth",
    "KIND_INDEX_MISSING",
    "KIND_TOPIC_UNKNOWN",
    "KIND_UNSPECIFIED",
    "KIND_USAGE_ERROR",
    "KIND_VERSION_INCOMPATIBLE",
    "MAX_OVERLAY_ENTRIES",
    "OVERLAY_FILENAME",
    "OVERLAY_VERSION",
    "TruffleHogStatus",
    "VALID_KINDS",
    "VERDICT_COMPATIBLE",
    "VERDICT_PARTIAL",
    "VERDICT_UNKNOWN_SCHEMA",
    "attach_request_id",
    "collect",
    "config_dir",
    "effective_matrix",
    "emit_error",
    "exit_code_for",
    "index_db_path",
    "overlay_path",
    "render_human",
    "render_json",
    "report_to_dict",
    "reset_cache",
    "sanitize_for_human",
    "write_overlay_entries",
]
