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
DEFAULT_SNIPPET_LENGTH = 120
MIN_SNIPPET_LENGTH = 16
MAX_SNIPPET_LENGTH = 1024
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
    snippet_length: int = DEFAULT_SNIPPET_LENGTH
    include_held: bool = False
    canonical: bool = False

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
        if not (MIN_SNIPPET_LENGTH <= self.snippet_length <= MAX_SNIPPET_LENGTH):
            raise ValueError(
                f"--snippet-length must be between {MIN_SNIPPET_LENGTH} and "
                f"{MAX_SNIPPET_LENGTH} (got {self.snippet_length})"
            )
        for predicate in self.filters:
            if predicate.field not in SEARCH_FILTER_FIELDS:
                raise ValueError(
                    f"filter field {predicate.field!r} not allowed for search "
                    f"(allowed: {sorted(SEARCH_FILTER_FIELDS)})"
                )


__all__ = [
    "DEFAULT_LIMIT",
    "DEFAULT_SNIPPET_LENGTH",
    "HARD_LIMIT_CEILING",
    "MAX_QUERY_BYTES",
    "MAX_SNIPPET_LENGTH",
    "MIN_SNIPPET_LENGTH",
    "SEARCH_FILTER_FIELDS",
    "SearchSpec",
]
