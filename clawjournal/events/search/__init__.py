"""Cross-session full-text search over `events.raw_json` (phase-1 plan 11).

Public API:

- ``SearchSpec`` / ``SearchHit`` — parsed, validated request/response shapes.
- ``parse_search_spec`` — build a ``SearchSpec`` from CLI flags + filters.
- ``run`` — execute the search, return ``SearchResult``.
- ``ensure_search_schema`` / ``rebuild_search_index`` — schema bootstrap +
  one-shot rebuild after a ``DELETE FROM events_fts`` or other surgery.
- ``render_json`` / ``render_human`` — output renderers; both anonymize
  paths and run snippets through ``redaction/secrets.py`` before emit.

Module placement mirrors the doctor (plan 08) and aggregate (plan 10)
packages: spec → schema → query → render, with a thin public ``__init__``.
"""

from __future__ import annotations

from clawjournal.events.search.query import (
    SearchHit,
    SearchResult,
    parse_search_spec,
    run,
)
from clawjournal.events.search.render import (
    EVENTS_SEARCH_SCHEMA_VERSION,
    render_human,
    render_json,
    result_to_payload,
)
from clawjournal.events.search.schema import (
    ensure_search_schema,
    rebuild_search_index,
)
from clawjournal.events.search.spec import (
    DEFAULT_LIMIT,
    DEFAULT_SNIPPET_TOKENS,
    HARD_LIMIT_CEILING,
    MAX_QUERY_BYTES,
    MAX_SNIPPET_TOKENS,
    MIN_SNIPPET_TOKENS,
    SearchSpec,
)

__all__ = [
    "DEFAULT_LIMIT",
    "DEFAULT_SNIPPET_TOKENS",
    "EVENTS_SEARCH_SCHEMA_VERSION",
    "HARD_LIMIT_CEILING",
    "MAX_QUERY_BYTES",
    "MAX_SNIPPET_TOKENS",
    "MIN_SNIPPET_TOKENS",
    "SearchHit",
    "SearchResult",
    "SearchSpec",
    "ensure_search_schema",
    "parse_search_spec",
    "rebuild_search_index",
    "render_human",
    "render_json",
    "result_to_payload",
    "run",
]
