"""Topic-based docs corpus loader (phase-1 plan 08, ``events docs``).

Static markdown topics shipped under ``clawjournal/events/docs/``.
Default mode is markdown (printed verbatim); ``--json`` mode parses
the topic into a structured ``{topic, sections, examples, schemas}``
shape with ``events_docs_topic_schema_version: "1.0"``.

The content is **not** run through the anonymizer — author-written
example paths like ``~/.clawjournal/index.db`` are intentional and
survive unchanged. Topics that need to refer to user-specific paths
in instruction prose (e.g. examples.md) write the placeholder tokens
literally (``{HOME}``, ``{INDEX_DB}``); the renderer does not
substitute them.
"""

from __future__ import annotations

import importlib.resources
import json
import re
from typing import Any

EVENTS_DOCS_TOPIC_SCHEMA_VERSION = "1.0"

TOPIC_NAMES: tuple[str, ...] = (
    "guide",
    "commands",
    "schemas",
    "examples",
    "exit-codes",
    "errors",
)
TOPIC_NAME_SET = frozenset(TOPIC_NAMES)


class DocsTopicError(ValueError):
    """Raised for an unknown or malformed topic."""


def _read_topic_markdown(topic: str) -> str:
    if topic not in TOPIC_NAME_SET:
        raise DocsTopicError(f"unknown topic: {topic!r}")
    pkg = importlib.resources.files("clawjournal.events.docs")
    return (pkg / f"{topic}.md").read_text(encoding="utf-8")


_HEADING_RE = re.compile(r"^(#{1,6})\s+(.+?)\s*$")
_FENCE_RE = re.compile(r"^```(\w*)\s*$")


def _split_sections(markdown: str) -> list[dict[str, str]]:
    """Split a markdown doc into ``{heading, body}`` records.

    The first H1 is treated as the topic title and dropped. Subsequent
    H2 headings start new sections. Anything before the first H2 is
    treated as the topic preamble under heading "" (empty string).
    """

    lines = markdown.splitlines()
    sections: list[dict[str, str]] = []
    current_heading = ""
    current_body: list[str] = []
    seen_h1 = False

    for line in lines:
        match = _HEADING_RE.match(line)
        if match:
            level = len(match.group(1))
            text = match.group(2).strip()
            if level == 1 and not seen_h1:
                seen_h1 = True
                continue
            if level == 2:
                if current_body or current_heading:
                    sections.append(
                        {
                            "heading": current_heading,
                            "body": "\n".join(current_body).strip(),
                        }
                    )
                current_heading = text
                current_body = []
                continue
        current_body.append(line)

    if current_body or current_heading:
        sections.append(
            {
                "heading": current_heading,
                "body": "\n".join(current_body).strip(),
            }
        )
    return sections


def _extract_examples(markdown: str) -> list[dict[str, str]]:
    """Pull fenced ``` blocks out of the markdown as ``{title, code}``.

    The "title" is the most recent H2/H3 above the fence (best-effort).
    Used only by ``--json`` mode; the markdown rendering keeps the
    fences inline.
    """

    examples: list[dict[str, str]] = []
    last_heading = ""
    in_fence = False
    fence_lang = ""
    fence_buf: list[str] = []
    for line in markdown.splitlines():
        if in_fence:
            if line.startswith("```"):
                examples.append(
                    {
                        "title": last_heading,
                        "code": "\n".join(fence_buf),
                        "lang": fence_lang,
                    }
                )
                in_fence = False
                fence_buf = []
                fence_lang = ""
            else:
                fence_buf.append(line)
            continue
        match = _FENCE_RE.match(line)
        if match:
            in_fence = True
            fence_lang = match.group(1) or ""
            continue
        h = _HEADING_RE.match(line)
        if h and len(h.group(1)) >= 2:
            last_heading = h.group(2).strip()
    return examples


def topic_payload(topic: str) -> dict[str, Any]:
    """Return the structured ``{topic, sections, examples, schemas}`` shape."""

    markdown = _read_topic_markdown(topic)
    sections = _split_sections(markdown)
    examples = _extract_examples(markdown)
    schemas: list[dict[str, str]] = []
    if topic == "schemas":
        for section in sections:
            if not section["heading"]:
                continue
            schemas.append(
                {
                    "name": section["heading"],
                    "version": EVENTS_DOCS_TOPIC_SCHEMA_VERSION,
                    "shape": section["body"],
                }
            )
    return {
        "events_docs_topic_schema_version": EVENTS_DOCS_TOPIC_SCHEMA_VERSION,
        "topic": topic,
        "sections": sections,
        "examples": examples,
        "schemas": schemas,
    }


def render_topic(
    topic: str,
    *,
    json_mode: bool = False,
    request_id: str | None = None,
) -> str:
    """Return the topic content (markdown by default, JSON if requested)."""

    if json_mode:
        payload = topic_payload(topic)
        if request_id is not None:
            payload["_meta"] = {"request_id": request_id}
        return json.dumps(payload, indent=2, sort_keys=True)
    # Markdown mode: read raw and return verbatim.
    return _read_topic_markdown(topic)


__all__ = [
    "DocsTopicError",
    "EVENTS_DOCS_TOPIC_SCHEMA_VERSION",
    "TOPIC_NAMES",
    "TOPIC_NAME_SET",
    "render_topic",
    "topic_payload",
]
