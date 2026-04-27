"""SQL builder + executor for ``events search`` (phase-1 plan 11).

Build a single parameterized SELECT that joins ``events_fts`` (FTS5
virtual table, plan 11 §Index shape) to ``events`` and ``event_sessions``,
applies allowlisted filters, optionally excludes held sessions via a
LEFT JOIN to the workbench ``sessions`` table, ranks by BM25 ASC
(FTS5's BM25 returns smaller-is-better — relevance), and limits.

The user's query is bound as a parameter to the FTS5 ``MATCH``
predicate. Field names go through the per-search allowlist
(``spec.SEARCH_FILTER_FIELDS``); operators map to a fixed enum;
values become ``?`` placeholders. No string interpolation.

Hold-state filter: defaults to excluding sessions in
``pending_review`` or ``embargoed`` (plan 11 §Security #6). Workbench
``sessions`` may not have a row for every event_session — the LEFT
JOIN treats ``hold_state IS NULL`` as a non-held state so events from
sessions that haven't been touched by the workbench still surface.
"""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from typing import Any

from clawjournal.events.aggregate.spec import Predicate
from clawjournal.events.search.spec import SEARCH_FILTER_FIELDS, SearchSpec


_HELD_STATES: tuple[str, ...] = ("pending_review", "embargoed")


@dataclass(frozen=True)
class SearchHit:
    """One row of search output before render-layer anonymization /
    snippet redaction. The render layer is where the user-visible
    JSON shape is built; this struct just bundles the SQL row in a
    typed form so the renderer doesn't have to remember tuple offsets."""

    event_id: int
    session_key: str
    event_at: str | None
    client: str
    type: str
    confidence: str
    source: str
    source_path: str
    source_offset: int
    seq: int
    snippet: str
    bm25: float


@dataclass(frozen=True)
class SearchResult:
    """Output of one ``query.run`` call.

    ``hits`` are already ordered by BM25 ascending (FTS5's relevance
    ranking — smaller is closer). ``rows_matched`` is the COUNT(*) of
    matches before ``--limit`` truncation; clients can detect a
    truncated result set with ``len(hits) < rows_matched``.
    """

    spec: SearchSpec
    hits: list[SearchHit]
    rewritten_match: str
    rows_matched: int
    elapsed_ms: int


def parse_search_spec(
    *,
    query: str,
    client: tuple[str, ...] = (),
    type_: tuple[str, ...] = (),
    confidence: tuple[str, ...] = (),
    session: str | None = None,
    source: str | None = None,
    since_iso: str | None = None,
    limit: int,
    snippet_tokens: int,
    include_held: bool,
) -> SearchSpec:
    """Build a validated ``SearchSpec`` from already-parsed CLI args.

    The CLI handler does the per-flag value parsing (CSV split, type
    coercion, etc.) and hands the cleaned values here so this
    function is purely about predicate construction + validation.
    """

    filters: list[Predicate] = []
    if client:
        filters.append(_predicate_for("client", client))
    if type_:
        filters.append(_predicate_for("type", type_))
    if confidence:
        filters.append(_predicate_for("confidence", confidence))
    if session:
        filters.append(Predicate(field="session", op="=", value=session))
    if source:
        filters.append(Predicate(field="source", op="=", value=source))
    return SearchSpec(
        query=query,
        filters=tuple(filters),
        since_iso=since_iso,
        limit=limit,
        snippet_tokens=snippet_tokens,
        include_held=include_held,
    )


def _predicate_for(field: str, values: tuple[str, ...]) -> Predicate:
    """One-or-many CLI value tuples: a single value uses ``=``,
    multiple use ``in``. Both render to a parameterized SQL clause
    in ``_predicate_sql`` below."""

    if len(values) == 1:
        return Predicate(field=field, op="=", value=values[0])
    return Predicate(field=field, op="in", value=tuple(values))


def run(spec: SearchSpec, conn: sqlite3.Connection) -> SearchResult:
    """Execute the search.

    Both queries (hits + rows_matched) run inside a single explicit
    read transaction so the report is internally consistent against
    a single snapshot — same pattern plan 10's aggregator uses for
    the same reason (bucket query + summary query against concurrent
    writes from ``clawjournal serve``).
    """

    hits_sql, hits_params = _build_hits_sql(spec)
    count_sql, count_params = _build_count_sql(spec)

    in_explicit_tx = bool(conn.in_transaction)
    if not in_explicit_tx:
        conn.execute("BEGIN")
    try:
        started = time.perf_counter()
        cursor = conn.execute(hits_sql, hits_params)
        rows = list(cursor.fetchall())
        count_row = conn.execute(count_sql, count_params).fetchone()
        elapsed_ms = int((time.perf_counter() - started) * 1000)
    finally:
        if not in_explicit_tx:
            try:
                conn.execute("COMMIT")
            except sqlite3.OperationalError:
                pass

    rows_matched = int(count_row[0]) if count_row and count_row[0] is not None else 0
    hits = [
        SearchHit(
            event_id=int(row[0]),
            session_key=row[1],
            event_at=row[2],
            client=row[3],
            type=row[4],
            confidence=row[5],
            source=row[6],
            source_path=row[7],
            source_offset=int(row[8]),
            seq=int(row[9]),
            snippet=row[10] or "",
            bm25=float(row[11]) if row[11] is not None else 0.0,
        )
        for row in rows
    ]
    return SearchResult(
        spec=spec,
        hits=hits,
        rewritten_match=spec.query.strip(),
        rows_matched=rows_matched,
        elapsed_ms=elapsed_ms,
    )


def _build_hits_sql(spec: SearchSpec) -> tuple[str, list[Any]]:
    where_clause, params = _build_where(spec)
    # The MATCH parameter MUST be bound (not interpolated). FTS5's
    # snippet() helper gets the highlight markers; the render layer
    # turns them into <mark>...</mark> after running snippet through
    # the secrets redactor (plan 11 §Security #4).
    #
    # FTS5 ``snippet()`` takes max-tokens (not chars), with a hard
    # ceiling of 64. SearchSpec already enforces 1 <= snippet_tokens
    # <= 64, so it is safe to interpolate as a fixed integer literal.
    # Round-1 fix: v0.1 originally exposed ``--snippet-length`` as
    # "characters" with a 1024 ceiling, but FTS5 silently clamped
    # everything >=64 to 64. The flag, spec field, and constants are
    # now ``--snippet-tokens`` to match the FTS5 contract.
    snippet_tokens = spec.snippet_tokens
    sql = (
        "SELECT e.id, s.session_key, e.event_at, e.client, e.type, "
        "       e.confidence, e.source, e.source_path, e.source_offset, "
        "       e.seq, "
        "       snippet(events_fts, 0, '', '', '...', " + str(snippet_tokens) + "), "
        "       bm25(events_fts) "
        "FROM events_fts "
        "JOIN events AS e ON e.id = events_fts.rowid "
        "JOIN event_sessions AS s ON s.id = e.session_id "
        "LEFT JOIN sessions AS ws ON ws.session_key = s.session_key "
        "WHERE events_fts MATCH ? " + where_clause +
        "ORDER BY bm25(events_fts) ASC, e.id ASC "
        "LIMIT ?"
    )
    bound = [spec.query] + params + [spec.limit]
    return sql, bound


def _build_count_sql(spec: SearchSpec) -> tuple[str, list[Any]]:
    where_clause, params = _build_where(spec)
    sql = (
        "SELECT COUNT(*) "
        "FROM events_fts "
        "JOIN events AS e ON e.id = events_fts.rowid "
        "JOIN event_sessions AS s ON s.id = e.session_id "
        "LEFT JOIN sessions AS ws ON ws.session_key = s.session_key "
        "WHERE events_fts MATCH ? " + where_clause
    )
    bound = [spec.query] + params
    return sql, bound


def _build_where(spec: SearchSpec) -> tuple[str, list[Any]]:
    pieces: list[str] = []
    params: list[Any] = []
    for predicate in spec.filters:
        sql, p = _predicate_sql(predicate)
        pieces.append(sql)
        params.extend(p)
    if spec.since_iso is not None:
        pieces.append("e.event_at >= ?")
        params.append(spec.since_iso)
    if not spec.include_held:
        # Held sessions are filtered out by hold_state. Workbench may
        # not have a row for every event_session (sessions that have
        # not yet been touched by the workbench), so NULL hold_state
        # also passes — only the explicit non-default states block.
        placeholders = ",".join("?" for _ in _HELD_STATES)
        pieces.append(
            f"(ws.hold_state IS NULL OR ws.hold_state NOT IN ({placeholders}))"
        )
        params.extend(_HELD_STATES)
    if not pieces:
        return "", params
    return "AND " + " AND ".join(pieces) + " ", params


def _predicate_sql(predicate: Predicate) -> tuple[str, list[Any]]:
    sql_field = SEARCH_FILTER_FIELDS.get(predicate.field)
    if sql_field is None:
        raise ValueError(
            f"filter field {predicate.field!r} not allowed for search "
            f"(allowed: {sorted(SEARCH_FILTER_FIELDS)})"
        )
    if predicate.op == "in":
        values = predicate.value
        if not isinstance(values, tuple) or not values:
            raise ValueError(
                f"`in` predicate for {predicate.field!r} requires a "
                f"non-empty tuple"
            )
        placeholders = ",".join("?" for _ in values)
        return f"{sql_field} IN ({placeholders})", list(values)
    return f"{sql_field} {predicate.op} ?", [predicate.value]


__all__ = ["SearchHit", "SearchResult", "parse_search_spec", "run"]
