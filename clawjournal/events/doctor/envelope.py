"""Structured error envelope shared by ``events doctor`` / ``events
features`` / ``events docs`` (phase-1 plan 08).

Closed ``kind`` enum so AI-agent consumers can switch on it
deterministically. ``message`` and ``hint`` go through
``Anonymizer().text()`` to scrub home-dir paths before emission
(``Anonymizer()`` auto-detects via ``_detect_home_dir()``; in
HOME-not-set environments it becomes a no-op). Success-mode output
does not run through the anonymizer.

``--request-id <id>`` is echoed into ``_meta.request_id`` on both
success and error responses when ``--json`` is set.
"""

from __future__ import annotations

import json
import sys
from typing import TextIO

from clawjournal.redaction.anonymizer import Anonymizer

# Closed kind enum (plan 08 §Closed `kind` enum)
KIND_INDEX_MISSING = "index_missing"
KIND_VERSION_INCOMPATIBLE = "version_incompatible"
KIND_USAGE_ERROR = "usage_error"
KIND_TOPIC_UNKNOWN = "topic_unknown"
KIND_UNSPECIFIED = "unspecified"

VALID_KINDS = frozenset(
    {
        KIND_INDEX_MISSING,
        KIND_VERSION_INCOMPATIBLE,
        KIND_USAGE_ERROR,
        KIND_TOPIC_UNKNOWN,
        KIND_UNSPECIFIED,
    }
)


def _anonymize(text: str) -> str:
    if not text:
        return text
    return Anonymizer().text(text)


def emit_error(
    *,
    code: int,
    kind: str,
    message: str,
    hint: str = "",
    retryable: bool = False,
    request_id: str | None = None,
    json_mode: bool = False,
    stream: TextIO | None = None,
) -> int:
    """Print an error to stderr; return ``code`` so callers can sys.exit it."""

    if kind not in VALID_KINDS:
        # Defensive: every error path should map to a member of the
        # closed enum. Tests assert this, but in production never raise
        # over a misclassification — degrade to ``unspecified``.
        kind = KIND_UNSPECIFIED

    target = stream if stream is not None else sys.stderr
    safe_message = _anonymize(message)
    safe_hint = _anonymize(hint)

    if json_mode:
        payload: dict = {
            "error": {
                "code": code,
                "kind": kind,
                "message": safe_message,
                "hint": safe_hint,
                "retryable": retryable,
            }
        }
        # `_meta` lives at the top level on both success and error
        # responses so agents only have to check one place. Plan §
        # `--request-id` echo: "echoed into _meta.request_id in both
        # success and error responses."
        if request_id is not None:
            payload["_meta"] = {"request_id": request_id}
        target.write(json.dumps(payload, indent=2, sort_keys=True))
        target.write("\n")
    else:
        target.write(safe_message)
        target.write("\n")
        if safe_hint:
            target.write(f"  hint: {safe_hint}\n")
    return code


def attach_request_id(payload: dict, request_id: str | None) -> dict:
    """Attach ``_meta.request_id`` to a success payload when set."""

    if request_id is None:
        return payload
    payload = dict(payload)
    payload["_meta"] = {"request_id": request_id}
    return payload


__all__ = [
    "KIND_INDEX_MISSING",
    "KIND_TOPIC_UNKNOWN",
    "KIND_UNSPECIFIED",
    "KIND_USAGE_ERROR",
    "KIND_VERSION_INCOMPATIBLE",
    "VALID_KINDS",
    "attach_request_id",
    "emit_error",
]
