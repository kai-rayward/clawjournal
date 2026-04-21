"""Exact-match classifier tests for the openclaw event pipeline."""

from __future__ import annotations

import pytest

from clawjournal.events.classify import classify_line

from ._fixtures import OPENCLAW_CORPUS, ExpectedEvent, Fixture


@pytest.mark.parametrize(
    "fixture", OPENCLAW_CORPUS, ids=[f.name for f in OPENCLAW_CORPUS]
)
def test_openclaw_classifier_emits_expected_event_sequence(fixture: Fixture):
    events = classify_line("openclaw", fixture.line)
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
