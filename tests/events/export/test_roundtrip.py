"""End-to-end round-trip: export a session, re-import into a fresh DB.

The plan's headline acceptance: "events / overrides / incidents /
token_usage matching modulo IDs" plus idempotent re-import.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from clawjournal.events.export import (
    BUNDLE_SCHEMA_VERSION,
    export_session_bundle,
    import_session_bundle,
)
from clawjournal.events.view import write_hook_override

from ._helpers import (
    PERMISSIVE_CONFIG,
    insert_cost_anomaly,
    insert_event,
    insert_event_session,
    insert_incident,
    insert_token_usage,
    make_conn,
)


@pytest.fixture
def populated_conn():
    conn = make_conn()
    sid = insert_event_session(conn, session_key="claude:demo:abc-123")
    e1 = insert_event(
        conn,
        session_id=sid,
        event_type="tool_call",
        event_key="tool_call:tu-1",
        source_offset=0,
        seq=0,
        raw_json={"type": "assistant", "tool": "Read"},
    )
    e2 = insert_event(
        conn,
        session_id=sid,
        event_type="tool_result",
        event_key="tool_result:tu-1",
        source_offset=120,
        seq=0,
        event_at="2026-04-22T09:01:01Z",
        raw_json={"type": "user", "tool_use_id": "tu-1", "content": "ok"},
    )
    e3 = insert_event(
        conn,
        session_id=sid,
        event_type="command_start",
        event_key="command_start:c-1",
        source_offset=240,
        seq=0,
        event_at="2026-04-22T09:01:02Z",
        raw_json={"type": "command", "cmd": "npm test"},
    )
    insert_token_usage(conn, event_id=e2, session_id=sid)
    insert_cost_anomaly(conn, session_id=sid, turn_event_id=e2)
    insert_incident(conn, session_id=sid, first_event_id=e1, last_event_id=e3)

    write_hook_override(
        conn,
        session_key="claude:demo:abc-123",
        event_key="tool_call:tu-1",
        event_type="tool_call",
        source="hook",
        confidence="high",
        lossiness="none",
        event_at="2026-04-22T09:01:00Z",
        payload_json=json.dumps({"corrected": True}),
        origin="hook:pre-tool-use:v1",
    )

    return conn, sid


def _summarize_db(conn) -> dict:
    """Capture the DB state in a form that's ID-agnostic and comparable."""
    events = list(
        conn.execute(
            "SELECT type, event_key, event_at, source, source_path, "
            "source_offset, seq, client, confidence, lossiness, raw_json "
            "FROM events ORDER BY source_path, source_offset, seq"
        )
    )
    overrides = list(
        conn.execute(
            "SELECT event_key, type, source, confidence, lossiness, "
            "event_at, payload_json, origin "
            "FROM event_overrides ORDER BY event_key"
        )
    )
    token_usage = list(
        conn.execute(
            "SELECT model, input, output, data_source, pricing_table_version "
            "FROM token_usage ORDER BY event_id"
        )
    )
    cost_anomalies = list(
        conn.execute(
            "SELECT kind, confidence, evidence_json FROM cost_anomalies ORDER BY id"
        )
    )
    incidents = list(
        conn.execute(
            "SELECT kind, count, confidence, evidence_json FROM incidents ORDER BY id"
        )
    )
    return {
        "events": [dict(r) for r in events],
        "overrides": [dict(r) for r in overrides],
        "token_usage": [dict(r) for r in token_usage],
        "cost_anomalies": [dict(r) for r in cost_anomalies],
        "incidents": [dict(r) for r in incidents],
    }


def test_round_trip_preserves_data(tmp_path, populated_conn, monkeypatch):
    src_conn, _ = populated_conn
    monkeypatch.setattr("clawjournal.config.CONFIG_DIR", tmp_path / ".clawjournal")

    summary = export_session_bundle(
        src_conn,
        "claude:demo:abc-123",
        config=PERMISSIVE_CONFIG,
        allow_no_workbench_row=True,
        skip_global_gates=True,
    )

    out_path = summary.bundle_path
    assert out_path is not None
    assert summary.blocked is False
    assert summary.event_count == 3
    assert summary.override_count == 1
    assert summary.token_usage_count == 1
    assert summary.cost_anomaly_count == 1
    assert summary.incident_count == 1
    assert out_path.exists()

    bundle = json.loads(out_path.read_text(encoding="utf-8"))
    assert bundle["bundle_schema_version"] == BUNDLE_SCHEMA_VERSION
    assert bundle["session"]["session_key"] == "claude:demo:abc-123"
    assert "manifest" in bundle and "sha256" in bundle["manifest"]

    src_summary = _summarize_db(src_conn)

    dst_conn = make_conn()
    import_summary = import_session_bundle(dst_conn, out_path)
    assert import_summary.events_inserted == 3
    assert import_summary.overrides_inserted == 1
    assert import_summary.token_usage_inserted == 1
    assert import_summary.cost_anomalies_inserted == 1
    assert import_summary.incidents_inserted == 1

    dst_summary = _summarize_db(dst_conn)

    assert len(src_summary["events"]) == len(dst_summary["events"])
    src_events_by_key = {
        (e["source_path"], e["source_offset"], e["seq"]): e
        for e in src_summary["events"]
    }

    for dst_ev in dst_summary["events"]:
        # Match by raw_ref but the source_path may differ post-anonymization;
        # match by (offset, seq) since the test uses /tmp paths that don't
        # get anonymized.
        key = (dst_ev["source_path"], dst_ev["source_offset"], dst_ev["seq"])
        assert key in src_events_by_key, (
            f"missing in src: {key!r} (dst paths: "
            f"{[(e['source_path'], e['source_offset'], e['seq']) for e in src_summary['events']]})"
        )
        src_ev = src_events_by_key[key]
        for field in (
            "type", "event_key", "event_at", "source", "client",
            "confidence", "lossiness", "raw_json",
        ):
            assert dst_ev[field] == src_ev[field], f"field {field!r} mismatch"

    assert src_summary["overrides"] == dst_summary["overrides"]
    assert src_summary["token_usage"] == dst_summary["token_usage"]
    assert len(src_summary["cost_anomalies"]) == len(dst_summary["cost_anomalies"])
    assert len(src_summary["incidents"]) == len(dst_summary["incidents"])
    for s, d in zip(src_summary["cost_anomalies"], dst_summary["cost_anomalies"]):
        assert s["kind"] == d["kind"]
        assert s["confidence"] == d["confidence"]
    for s, d in zip(src_summary["incidents"], dst_summary["incidents"]):
        assert s["kind"] == d["kind"]
        assert s["count"] == d["count"]
        assert s["confidence"] == d["confidence"]


def test_idempotent_reimport(tmp_path, populated_conn, monkeypatch):
    src_conn, _ = populated_conn
    monkeypatch.setattr("clawjournal.config.CONFIG_DIR", tmp_path / ".clawjournal")

    summary = export_session_bundle(
        src_conn,
        "claude:demo:abc-123",
        config=PERMISSIVE_CONFIG,
        allow_no_workbench_row=True,
        skip_global_gates=True,
    )
    out_path = summary.bundle_path
    assert out_path is not None

    dst_conn = make_conn()
    first = import_session_bundle(dst_conn, out_path)
    assert first.events_inserted == 3

    counts_before = {
        t: dst_conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        for t in (
            "events",
            "event_overrides",
            "token_usage",
            "cost_anomalies",
            "incidents",
            "event_source_snippets",
        )
    }

    second = import_session_bundle(dst_conn, out_path)
    assert second.events_inserted == 0
    assert second.events_skipped_existing == 3
    assert second.cost_anomalies_inserted == 0
    assert second.incidents_inserted == 0

    counts_after = {
        t: dst_conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        for t in counts_before
    }
    assert counts_before == counts_after


def test_no_reclassification_on_import(tmp_path, populated_conn, monkeypatch):
    """Importer trusts the bundle's `type` even when it's a value the local
    classifier wouldn't produce — the bundle insulates against drift."""
    import hashlib

    src_conn, _ = populated_conn
    monkeypatch.setattr("clawjournal.config.CONFIG_DIR", tmp_path / ".clawjournal")

    summary = export_session_bundle(
        src_conn,
        "claude:demo:abc-123",
        config=PERMISSIVE_CONFIG,
        allow_no_workbench_row=True,
        skip_global_gates=True,
    )
    out_path = summary.bundle_path
    assert out_path is not None

    bundle = json.loads(out_path.read_text(encoding="utf-8"))
    bundle["events"][0]["type"] = "assistant_message"  # different from raw_json's "tool_call"
    # Recompute the manifest sha256 after the modification so the import
    # passes the tamper-detection step. We're simulating an alternative
    # exporter (or a future schema) that might emit a different `type`,
    # not bundle tampering.
    digest_input = {k: v for k, v in bundle.items() if k != "manifest"}
    canonical = json.dumps(
        digest_input, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    bundle["manifest"]["sha256"] = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    out_path.write_text(json.dumps(bundle, indent=2), encoding="utf-8")

    dst_conn = make_conn()
    import_session_bundle(dst_conn, out_path)

    types = [
        r["type"]
        for r in dst_conn.execute(
            "SELECT type FROM events ORDER BY source_offset, seq"
        )
    ]
    assert "assistant_message" in types, (
        f"expected bundle's type to be preserved verbatim; got {types!r}"
    )
