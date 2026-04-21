"""Exact-match classifier tests for the claude event pipeline.

Every branch in clawjournal/events/classify/claude.py has at least one
fixture in CLAUDE_CORPUS. Each fixture pins the full event sequence
the classifier emits for that vendor line.
"""

from __future__ import annotations

import pytest

from clawjournal.events.classify import classify_line

from ._fixtures import CLAUDE_CORPUS, ExpectedEvent, Fixture


@pytest.mark.parametrize(
    "fixture", CLAUDE_CORPUS, ids=[f.name for f in CLAUDE_CORPUS]
)
def test_claude_classifier_emits_expected_event_sequence(fixture: Fixture):
    events = classify_line("claude", fixture.line)
    actual = [
        ExpectedEvent(
            type=event.type,
            event_key=event.event_key,
            event_at=event.event_at,
            confidence=event.confidence,
            lossiness=event.lossiness,
        )
        for event in events
    ]
    assert actual == fixture.expected
