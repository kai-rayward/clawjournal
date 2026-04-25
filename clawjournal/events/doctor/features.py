"""Static product feature surface (phase-1 plan 08, ``events features``).

Hand-maintained `_features.yaml` is the single source for both the
`features` array and the `events docs commands` topic. PyYAML is
loaded here on demand — same lazy-import rule as the overlay loader.

No user-derived content; safe to dump unconditionally.
"""

from __future__ import annotations

import importlib.metadata
import importlib.resources
from typing import Any

from clawjournal.events.capabilities import effective_matrix
from clawjournal.events.export.bundle import (
    BUNDLE_SCHEMA_VERSION,
    BUNDLE_SOFT_LIMIT_BYTES,
    RECORDER_SCHEMA_VERSION,
)

EVENTS_FEATURES_SCHEMA_VERSION = "1.0"
_FEATURES_RESOURCE = "_features.yaml"


def _read_clawjournal_version() -> str:
    try:
        return importlib.metadata.version("clawjournal")
    except importlib.metadata.PackageNotFoundError:
        return "unknown"


def _load_features_yaml() -> dict[str, Any]:
    import yaml

    pkg = importlib.resources.files("clawjournal.events.docs")
    text = (pkg / _FEATURES_RESOURCE).read_text(encoding="utf-8")
    data = yaml.safe_load(text)
    if not isinstance(data, dict):
        raise ValueError("_features.yaml must be a mapping at top level")
    return data


def feature_records() -> list[dict[str, str]]:
    """Return the feature records, validated. Used by features + docs."""

    data = _load_features_yaml()
    raw = data.get("features", [])
    if not isinstance(raw, list):
        raise ValueError("_features.yaml `features` must be a list")
    records: list[dict[str, str]] = []
    for entry in raw:
        if not isinstance(entry, dict):
            continue
        records.append(
            {
                "id": str(entry.get("id", "")),
                "command": str(entry.get("command", "")),
                "summary": str(entry.get("summary", "")),
            }
        )
    return records


def _connectors() -> list[str]:
    matrix = effective_matrix()
    seen: set[str] = set()
    for (client, _et), (supported, _r) in matrix.items():
        if supported:
            seen.add(client)
    return sorted(seen)


def features_payload(*, request_id: str | None = None) -> dict[str, Any]:
    """Build the static feature-surface payload."""

    records = feature_records()
    payload: dict[str, Any] = {
        "events_features_schema_version": EVENTS_FEATURES_SCHEMA_VERSION,
        "version": _read_clawjournal_version(),
        "bundle_schema_version": BUNDLE_SCHEMA_VERSION,
        "recorder_schema_version": RECORDER_SCHEMA_VERSION,
        "features": [r["id"] for r in records],
        "connectors": _connectors(),
        "limits": {
            "bundle_soft_limit_bytes": BUNDLE_SOFT_LIMIT_BYTES,
        },
    }
    if request_id is not None:
        payload["_meta"] = {"request_id": request_id}
    return payload


__all__ = [
    "EVENTS_FEATURES_SCHEMA_VERSION",
    "feature_records",
    "features_payload",
]
