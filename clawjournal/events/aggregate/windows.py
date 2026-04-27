"""Parse ``--since <duration>`` into an ISO-8601 lower-bound timestamp
(phase-1 plan 10).

Accepted forms:
- ``Nd`` / ``Nh`` / ``Nm`` — N days / hours / minutes ago
- ``today`` — start of today (UTC)
- ``thisweek`` — start of the ISO week (Monday) (UTC)

Returned timestamps are in the same shape ``events.event_at`` /
``incidents.created_at`` / ``token_usage.event_at`` are stored:
``YYYY-MM-DDTHH:MM:SSZ``.
"""

from __future__ import annotations

import re
from datetime import datetime, time, timedelta, timezone

_DURATION_RE = re.compile(r"^(\d+)([dhm])$")


def parse_since(
    raw: str | None, *, now: datetime | None = None
) -> str | None:
    """Return an ISO timestamp lower bound, or ``None`` if ``raw`` is empty."""

    if not raw:
        return None
    if now is None:
        now = datetime.now(tz=timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)

    if raw == "today":
        start = datetime.combine(now.date(), time(0, 0, 0), tzinfo=timezone.utc)
        return _to_iso(start)
    if raw == "thisweek":
        # ISO weeks start Monday. Python's `weekday()` returns 0=Mon.
        start_date = now.date() - timedelta(days=now.weekday())
        start = datetime.combine(start_date, time(0, 0, 0), tzinfo=timezone.utc)
        return _to_iso(start)

    match = _DURATION_RE.match(raw)
    if not match:
        raise ValueError(
            f"--since: unrecognized form {raw!r} "
            f"(expected Nd / Nh / Nm / today / thisweek)"
        )
    n = int(match.group(1))
    unit = match.group(2)
    if n <= 0:
        raise ValueError(f"--since: duration must be positive (got {raw!r})")
    if unit == "d":
        delta = timedelta(days=n)
    elif unit == "h":
        delta = timedelta(hours=n)
    else:
        delta = timedelta(minutes=n)
    return _to_iso(now - delta)


def _to_iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


__all__ = ["parse_since"]
