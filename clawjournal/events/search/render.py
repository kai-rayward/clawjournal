"""Renderers for ``events search`` results (phase-1 plan 11).

Two flows, both privacy-safe:

1. **Snippet redaction first, highlight second.** ``redact_text`` from
   ``clawjournal.redaction.secrets`` is run on the FTS5-emitted snippet
   before any ``<mark>`` tags would land in the output. Plan 11
   §Security #4 — order matters because ``<mark>`` tags would confuse
   the secrets regex. Today the FTS5 snippet uses empty start/end
   markers (see ``query._build_hits_sql``) so the snippet returned
   from SQLite is plain text; the render layer is responsible for
   re-injecting highlights only after redaction. v0.1 emits the
   redacted snippet without highlights — adding back per-token
   ``<mark>`` is a follow-up that needs a tokenizer-aware diff
   between raw and redacted snippet to know where to mark.
2. **Anonymized paths.** ``raw_ref.source_path`` and any path-shaped
   value in ``session_key`` go through ``Anonymizer().text()`` so
   ``/Users/<u>/...`` appears as ``[REDACTED_PATH]``. Plan 11
   §Security #3 — same anonymizer plan 10 uses.

JSON envelope mirrors plan 10's: ``events_search_schema_version``
(pinned), ``hits``, ``rows_matched``, ``rows_returned`` (= len(hits)),
``_meta`` with ``elapsed_ms`` / ``request_id``.
"""

from __future__ import annotations

import json
from io import StringIO
from typing import Any, TextIO

from clawjournal.events.search.query import SearchHit, SearchResult
from clawjournal.redaction.anonymizer import Anonymizer
from clawjournal.redaction.secrets import redact_text

EVENTS_SEARCH_SCHEMA_VERSION = "1.0"


def render_json(
    result: SearchResult,
    *,
    request_id: str | None = None,
) -> str:
    """Return the canonical JSON payload as a string."""

    payload = result_to_payload(result, request_id=request_id)
    return json.dumps(payload, indent=2, sort_keys=True)


def result_to_payload(
    result: SearchResult,
    *,
    request_id: str | None = None,
) -> dict[str, Any]:
    anonymizer = Anonymizer()
    hits_out = [_hit_to_dict(hit, anonymizer=anonymizer) for hit in result.hits]
    payload: dict[str, Any] = {
        "events_search_schema_version": EVENTS_SEARCH_SCHEMA_VERSION,
        "query": result.spec.query,
        "rewritten_match": result.rewritten_match,
        "hits": hits_out,
        "_meta": {
            "elapsed_ms": result.elapsed_ms,
            "rows_matched": result.rows_matched,
            "rows_returned": len(hits_out),
            "include_held": result.spec.include_held,
        },
    }
    if request_id is not None:
        payload["_meta"]["request_id"] = request_id
    return payload


def _hit_to_dict(hit: SearchHit, *, anonymizer: Anonymizer) -> dict[str, Any]:
    safe_session_key = anonymizer.text(hit.session_key)
    safe_source_path = anonymizer.path(hit.source_path) if hit.source_path else ""
    redacted_snippet, _, _ = redact_text(hit.snippet) if hit.snippet else ("", 0, [])
    timeline_url = (
        f"clawjournal://session/{safe_session_key}#event-{hit.event_id}"
    )
    return {
        "event_id": hit.event_id,
        "session_key": safe_session_key,
        "event_at": hit.event_at,
        "client": hit.client,
        "type": hit.type,
        "confidence": hit.confidence,
        "source": hit.source,
        "raw_ref": {
            "source_path": safe_source_path,
            "source_offset": hit.source_offset,
            "seq": hit.seq,
        },
        "snippet": redacted_snippet,
        "bm25": hit.bm25,
        "timeline_url": timeline_url,
    }


def render_human(
    result: SearchResult,
    *,
    stream: TextIO | None = None,
) -> str:
    """Plain-text fallback. One line per hit:
    ``<bm25>  <session_key>  <type>  <event_at>  <snippet>``.
    Snippets go through the same redaction the JSON path uses.
    """

    anonymizer = Anonymizer()
    buf = StringIO()
    if not result.hits:
        buf.write(
            f"no matches for {result.spec.query!r} "
            f"(searched in {result.elapsed_ms} ms)\n"
        )
    else:
        for hit in result.hits:
            safe_session = anonymizer.text(hit.session_key)
            redacted_snippet, _, _ = (
                redact_text(hit.snippet) if hit.snippet else ("", 0, [])
            )
            buf.write(
                f"{hit.bm25:>8.3f}  {safe_session}  {hit.type}  "
                f"{hit.event_at or '∅'}  {redacted_snippet}\n"
            )
        buf.write(
            f"\n{len(result.hits)} of {result.rows_matched} matches "
            f"({result.elapsed_ms} ms)"
        )
        if not result.spec.include_held:
            buf.write(" — held sessions excluded; pass --include-held to include")
        buf.write("\n")
    text = buf.getvalue()
    if stream is not None:
        stream.write(text)
    return text


__all__ = [
    "EVENTS_SEARCH_SCHEMA_VERSION",
    "render_human",
    "render_json",
    "result_to_payload",
]
