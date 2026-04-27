"""Parse ``--where field<op>value`` flags into ``Predicate`` records
(phase-1 plan 10).

Field names go through the domain's filter allowlist; unknown fields
are rejected with a structured error before any SQL is built.
Values for the ``in`` operator are split on ``|``.

Operator parsing picks the **leftmost** operator boundary in the
string, with longest-match as tiebreak when two ops start at the
same position (so ``>=`` beats ``>`` at a tie). Picking leftmost is
what makes ``--where session=claude:my>=proj`` parse correctly:
``=`` at position 7 wins over ``>=`` at position 17, and the value
is the whole substring after the first ``=`` — including the inner
``>=`` literal that's part of the value, not a second operator.
"""

from __future__ import annotations

from clawjournal.events.aggregate.registry import DomainRegistry
from clawjournal.events.aggregate.spec import Predicate

# Operator alphabet. Order in this tuple no longer matters for
# correctness — the parser scans for the leftmost match across all
# of them — but listing the multi-char ops first keeps the source
# readable.
_OPERATORS: tuple[str, ...] = (
    "in:",
    ">=",
    "<=",
    "!=",
    "=",
    ">",
    "<",
)


def parse_where_clauses(
    raw_clauses: list[str], registry: DomainRegistry
) -> tuple[Predicate, ...]:
    """Validate and parse a list of ``field<op>value`` strings.

    Raises ``ValueError`` with an actionable message for any unknown
    field or unparseable clause; the CLI handler maps that to a
    structured ``usage_error`` envelope.
    """

    parsed: list[Predicate] = []
    for raw in raw_clauses:
        parsed.append(_parse_one(raw, registry))
    return tuple(parsed)


def _parse_one(raw: str, registry: DomainRegistry) -> Predicate:
    # Leftmost-match-wins, with longest as tiebreak. Iterating all
    # operators and tracking the smallest index (preferring longer
    # tokens at ties) is the natural way to express it.
    best: tuple[int, str] | None = None
    for op in _OPERATORS:
        idx = raw.find(op)
        if idx <= 0:
            # idx == 0 would mean an empty field name; idx == -1 means
            # not found. Either way, skip.
            continue
        if best is None or idx < best[0] or (idx == best[0] and len(op) > len(best[1])):
            best = (idx, op)

    if best is None:
        raise ValueError(
            f"could not parse --where clause {raw!r}: expected "
            f"field<op>value with op in {{=, !=, >, >=, <, <=, in:}}"
        )

    idx, op_token = best
    field = raw[:idx].strip()
    value_str = raw[idx + len(op_token):].strip()

    if not field:
        raise ValueError(f"--where clause {raw!r} has empty field name")
    if field not in registry.filters:
        raise ValueError(
            f"--where field {field!r} not allowed for domain "
            f"{registry.name!r} (allowed: {sorted(registry.filters)})"
        )

    op = "in" if op_token == "in:" else op_token
    if op == "in":
        if not value_str:
            raise ValueError(
                f"--where {field}in: requires at least one value "
                f"(separator: '|')"
            )
        value: object = tuple(part for part in value_str.split("|") if part)
        if not value:
            raise ValueError(
                f"--where {field}in: requires at least one non-empty value"
            )
    else:
        value = value_str

    return Predicate(field=field, op=op, value=value)


__all__ = ["parse_where_clauses"]
