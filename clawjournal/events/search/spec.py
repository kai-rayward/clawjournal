"""Search request shape (phase-1 plan 11).

``SearchSpec`` is the parsed, validated form of one
``clawjournal events search <query>`` invocation. CLI handlers build
it; the query executor consumes it. Mirrors plan 10's spec layer:
construction is the validation step, so any spec returned by a CLI
handler is safe to feed straight into ``query.run``.
"""

from __future__ import annotations

from dataclasses import dataclass

from clawjournal.events.aggregate.spec import Predicate

DEFAULT_LIMIT = 50
HARD_LIMIT_CEILING = 1000
# FTS5's ``snippet(t, col, start, end, ellipsis, N)`` takes ``N`` as a
# count of tokens (not characters), with a documented hard ceiling of
# 64. Values above the ceiling are silently clamped by SQLite, so we
# enforce the cap here to avoid advertising a range we can't honor.
# v0.1's original ``--snippet-length`` flag claimed "characters" with
# a 1024 ceiling — round 1 fixes the unit mismatch by renaming the
# constant and the CLI flag.
DEFAULT_SNIPPET_TOKENS = 16
MIN_SNIPPET_TOKENS = 1
MAX_SNIPPET_TOKENS = 64
MAX_QUERY_BYTES = 4096

# Logical names allowed in ``--type=...`` / ``--client=...`` /
# ``--confidence=...`` filters and the underlying SQL projection.
# Same allowlist shape plan 10 uses, so the parsers from
# ``aggregate.filters`` can be reused on ``--where`` style filters in
# a future patch. For v0.1 the CLI exposes only the named flags.
SEARCH_FILTER_FIELDS: dict[str, str] = {
    "client": "e.client",
    "type": "e.type",
    "confidence": "e.confidence",
    "session": "s.session_key",
    "source": "e.source",
}


@dataclass(frozen=True)
class SearchSpec:
    """Validated search request.

    Construction enforces every invariant that the query layer would
    otherwise have to recheck — including the hold-state default,
    the result-set ceiling, and the FTS5 query-size cap.
    """

    query: str
    filters: tuple[Predicate, ...] = ()
    since_iso: str | None = None
    limit: int = DEFAULT_LIMIT
    snippet_tokens: int = DEFAULT_SNIPPET_TOKENS
    include_held: bool = False

    def __post_init__(self) -> None:
        if not self.query or not self.query.strip():
            raise ValueError("search query must be non-empty")
        if len(self.query.encode("utf-8")) > MAX_QUERY_BYTES:
            raise ValueError(
                f"search query exceeds {MAX_QUERY_BYTES}-byte cap "
                f"(got {len(self.query.encode('utf-8'))} bytes)"
            )
        if self.limit <= 0:
            raise ValueError("--limit must be positive")
        if self.limit > HARD_LIMIT_CEILING:
            raise ValueError(
                f"--limit ceiling is {HARD_LIMIT_CEILING} (got {self.limit})"
            )
        if not (MIN_SNIPPET_TOKENS <= self.snippet_tokens <= MAX_SNIPPET_TOKENS):
            raise ValueError(
                f"--snippet-tokens must be between {MIN_SNIPPET_TOKENS} and "
                f"{MAX_SNIPPET_TOKENS} (got {self.snippet_tokens})"
            )
        for predicate in self.filters:
            if predicate.field not in SEARCH_FILTER_FIELDS:
                raise ValueError(
                    f"filter field {predicate.field!r} not allowed for search "
                    f"(allowed: {sorted(SEARCH_FILTER_FIELDS)})"
                )


__all__ = [
    "DEFAULT_LIMIT",
    "DEFAULT_SNIPPET_TOKENS",
    "HARD_LIMIT_CEILING",
    "MAX_QUERY_BYTES",
    "MAX_SNIPPET_TOKENS",
    "MIN_SNIPPET_TOKENS",
    "SEARCH_FILTER_FIELDS",
    "SearchSpec",
]
