"""``events docs`` tests — topic enum, JSON shape, drift between surfaces."""

from __future__ import annotations

import json

import pytest

from clawjournal.events.doctor import docs as docs_mod
from clawjournal.events.doctor.docs import (
    DocsTopicError,
    EVENTS_DOCS_TOPIC_SCHEMA_VERSION,
    TOPIC_NAMES,
    render_topic,
    topic_payload,
)
from clawjournal.events.doctor.features import feature_records


def test_topic_names_closed_enum():
    assert set(TOPIC_NAMES) == {
        "guide",
        "commands",
        "schemas",
        "examples",
        "exit-codes",
        "errors",
    }


@pytest.mark.parametrize("topic", TOPIC_NAMES)
def test_each_topic_loads(topic):
    payload = topic_payload(topic)
    assert payload["topic"] == topic
    assert payload["events_docs_topic_schema_version"] == EVENTS_DOCS_TOPIC_SCHEMA_VERSION
    assert isinstance(payload["sections"], list)


def test_unknown_topic_raises():
    with pytest.raises(DocsTopicError):
        topic_payload("nonexistent")


def test_render_markdown_default():
    rendered = render_topic("guide")
    assert rendered.startswith("# Quickstart")  # H1


def test_render_json_carries_request_id():
    rendered = render_topic("guide", json_mode=True, request_id="rq-12")
    payload = json.loads(rendered)
    assert payload["_meta"]["request_id"] == "rq-12"


def test_render_json_omits_meta_without_request_id():
    rendered = render_topic("guide", json_mode=True)
    payload = json.loads(rendered)
    assert "_meta" not in payload


def test_schemas_topic_has_named_schema_records():
    payload = topic_payload("schemas")
    names = {s["name"] for s in payload["schemas"]}
    assert "events doctor --json" in names
    assert "events features --json" in names
    assert "structured error envelope" in names


def test_commands_topic_covers_every_feature_record():
    """Every command in ``_features.yaml`` should have an H2 in
    ``commands.md``. This pins the maintenance contract from plan 08
    §Maintenance contract for new ``events`` subcommands."""

    payload = topic_payload("commands")
    section_headings = {section["heading"] for section in payload["sections"]}
    for record in feature_records():
        # Heading is e.g. "events ingest"; record.command is
        # "clawjournal events ingest".
        expected = record["command"].removeprefix("clawjournal ").strip()
        assert expected in section_headings, (
            f"feature {record['id']!r} ({record['command']!r}) has no "
            f"matching section in commands.md (have: {sorted(section_headings)})"
        )


def test_static_docs_not_anonymized():
    """Plan 08: literal ``~/.clawjournal/index.db`` examples in topics
    must survive unchanged — the anonymizer is not applied to static
    doc content."""

    rendered = render_topic("examples")
    assert "~/.clawjournal/" in rendered or "{HOME}/.clawjournal/" in rendered
    assert "[REDACTED_PATH]" not in rendered


def test_commands_topic_does_not_claim_nonexistent_flags():
    """Round 7: commands.md previously listed ``--yes`` for
    ``events export`` (never wired) and ``--json`` for
    ``events features`` (dropped in round 4 — features always emits
    JSON). Pin the cleaned-up text so the false claims don't drift back.
    """

    rendered = render_topic("commands")

    # events features section's `Flags:` line must not list --json.
    features_idx = rendered.index("## events features")
    next_section = rendered.index("## ", features_idx + 1)
    features_section = rendered[features_idx:next_section]
    flags_line = next(
        (ln for ln in features_section.splitlines() if ln.startswith("Flags:")),
        "",
    )
    assert "--json" not in flags_line, (
        f"events features has no --json flag; commands.md Flags: line "
        f"must not list one (got: {flags_line!r})"
    )
    assert "always emits json" in features_section.lower()

    # events export section's `Flags:` line must not list --yes.
    export_idx = rendered.index("## events export")
    next_section = rendered.index("## ", export_idx + 1)
    export_section = rendered[export_idx:next_section]
    assert "--yes" not in export_section, (
        "events export has no --yes flag; commands.md must not list one"
    )
