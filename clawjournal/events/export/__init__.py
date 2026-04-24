"""Replay-export bundle assembly + import (phase-1 plan 07).

Public API:

- ``export_session_bundle(conn, session_key, ...) -> ExportSummary``
- ``import_session_bundle(conn, bundle_path, ...) -> ImportSummary``
- ``ensure_export_schema(conn)`` — additive migration creating the
  ``event_source_snippets`` table read by 03's ``events inspect`` as a
  fallback when the vendor JSONL isn't on this machine.
- Version constants ``BUNDLE_SCHEMA_VERSION`` / ``RECORDER_SCHEMA_VERSION``
  / ``EXPORT_BUNDLE_FORMAT``.
"""

from __future__ import annotations

from clawjournal.events.export.bundle import (
    BUNDLE_SCHEMA_VERSION,
    EXPORT_BUNDLE_FORMAT,
    RECORDER_SCHEMA_VERSION,
    ExportError,
    ExportGateBlocked,
    ExportSummary,
    export_session_bundle,
)
from clawjournal.events.export.import_ import (
    ImportError_,
    ImportSummary,
    import_session_bundle,
)
from clawjournal.events.export.schema import ensure_export_schema

__all__ = [
    "BUNDLE_SCHEMA_VERSION",
    "EXPORT_BUNDLE_FORMAT",
    "ExportError",
    "ExportGateBlocked",
    "ExportSummary",
    "ImportError_",
    "ImportSummary",
    "RECORDER_SCHEMA_VERSION",
    "ensure_export_schema",
    "export_session_bundle",
    "import_session_bundle",
]
