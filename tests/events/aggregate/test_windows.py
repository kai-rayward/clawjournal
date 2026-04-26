"""--since duration parser tests (plan 10)."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from clawjournal.events.aggregate import parse_since


_NOW = datetime(2026, 4, 22, 14, 30, 0, tzinfo=timezone.utc)


def test_none_returns_none():
    assert parse_since(None) is None
    assert parse_since("") is None


def test_days():
    assert parse_since("7d", now=_NOW) == "2026-04-15T14:30:00Z"


def test_hours():
    assert parse_since("3h", now=_NOW) == "2026-04-22T11:30:00Z"


def test_minutes():
    assert parse_since("90m", now=_NOW) == "2026-04-22T13:00:00Z"


def test_today_is_midnight_utc():
    assert parse_since("today", now=_NOW) == "2026-04-22T00:00:00Z"


def test_thisweek_is_monday_midnight():
    # 2026-04-22 is a Wednesday; monday is 2026-04-20.
    assert parse_since("thisweek", now=_NOW) == "2026-04-20T00:00:00Z"


def test_unrecognized_form():
    with pytest.raises(ValueError) as exc:
        parse_since("yesterday", now=_NOW)
    assert "unrecognized" in str(exc.value).lower()


def test_zero_or_negative_rejected():
    with pytest.raises(ValueError):
        parse_since("0d", now=_NOW)


def test_naive_now_treated_as_utc():
    naive = datetime(2026, 4, 22, 14, 30, 0)
    assert parse_since("1h", now=naive) == "2026-04-22T13:30:00Z"
