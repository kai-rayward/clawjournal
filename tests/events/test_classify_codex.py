"""Exact-match classifier tests for the codex event pipeline."""

from __future__ import annotations

import pytest

from clawjournal.events.classify import classify_line

from ._fixtures import CODEX_CORPUS, ExpectedEvent, Fixture


@pytest.mark.parametrize(
    "fixture", CODEX_CORPUS, ids=[f.name for f in CODEX_CORPUS]
)
def test_codex_classifier_emits_expected_event_sequence(fixture: Fixture):
    events = classify_line("codex", fixture.line)
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
