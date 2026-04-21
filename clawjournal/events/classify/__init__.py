"""Per-client line classifiers for the execution recorder."""

from __future__ import annotations

from typing import Callable

from clawjournal.events.classify import claude, codex, openclaw
from clawjournal.events.types import ClassifiedEvent, SessionMeta

Classifier = Callable[[dict], list[ClassifiedEvent]]
SessionMetaFn = Callable[[dict], SessionMeta]

_CLASSIFIERS: dict[str, tuple[Classifier, SessionMetaFn]] = {
    "claude": (claude.classify, claude.session_meta),
    "codex": (codex.classify, codex.session_meta),
    "openclaw": (openclaw.classify, openclaw.session_meta),
}


def classify_line(client: str, line: dict) -> list[ClassifiedEvent]:
    try:
        classifier = _CLASSIFIERS[client][0]
    except KeyError as exc:
        raise ValueError(f"Unsupported events client: {client}") from exc
    return classifier(line)


def session_meta_for_line(client: str, line: dict) -> SessionMeta:
    try:
        meta_fn = _CLASSIFIERS[client][1]
    except KeyError as exc:
        raise ValueError(f"Unsupported events client: {client}") from exc
    return meta_fn(line)
