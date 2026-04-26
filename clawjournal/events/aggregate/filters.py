"""Parse ``--where field<op>value`` flags into ``Predicate`` records
(phase-1 plan 10).

Field names go through the domain's filter allowlist; unknown fields
are rejected with a structured error before any SQL is built.
Values for the ``in`` operator are split on ``|``. Operator parsing
prefers the longest match so ``>=`` doesn't get parsed as ``>`` plus
``=value``.
"""

from __future__ import annotations

from clawjournal.events.aggregate.registry import DomainRegistry
from clawjournal.events.aggregate.spec import Predicate

# Order matters: longest-match first so ``>=`` is found before ``>``.
_OPERATORS_LONGEST_FIRST: tuple[str, ...] = (
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
    op_match: tuple[str, int] | None = None
    for op in _OPERATORS_LONGEST_FIRST:
        idx = raw.find(op)
        if idx > 0:  # field name must be non-empty before the op
            op_match = (op, idx)
            break

    if op_match is None:
        raise ValueError(
            f"could not parse --where clause {raw!r}: expected "
            f"field<op>value with op in {{=, !=, >, >=, <, <=, in:}}"
        )

    op_token, idx = op_match
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
