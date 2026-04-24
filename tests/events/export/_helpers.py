"""Shared fixture helpers for replay-export tests."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone

from clawjournal.events.cost.schema import ensure_cost_schema
from clawjournal.events.export.schema import ensure_export_schema
from clawjournal.events.incidents.schema import ensure_incidents_schema
from clawjournal.events.schema import ensure_schema as ensure_events_schema
from clawjournal.events.view import ensure_view_schema


def make_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    ensure_events_schema(conn)
    ensure_view_schema(conn)
    ensure_cost_schema(conn)
    ensure_incidents_schema(conn)
    ensure_export_schema(conn)
    return conn


def insert_event_session(
    conn: sqlite3.Connection,
    *,
    session_key: str,
    client: str = "claude",
    parent_session_key: str | None = None,
    started_at: str | None = "2026-04-22T09:00:00Z",
    ended_at: str | None = "2026-04-22T11:30:00Z",
    status: str = "ended",
    client_version: str | None = "1.42.0",
) -> int:
    cur = conn.execute(
        "INSERT INTO event_sessions (session_key, parent_session_key, client, "
        "client_version, started_at, ended_at, status) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (session_key, parent_session_key, client, client_version, started_at, ended_at, status),
    )
    conn.commit()
    return int(cur.lastrowid)


def insert_event(
    conn: sqlite3.Connection,
    *,
    session_id: int,
    event_type: str,
    event_key: str | None = None,
    event_at: str | None = "2026-04-22T09:01:00Z",
    source: str = "claude-jsonl",
    source_path: str = "/tmp/sample.jsonl",
    source_offset: int = 0,
    seq: int = 0,
    client: str = "claude",
    confidence: str = "high",
    lossiness: str = "none",
    raw_json: dict | str | None = None,
) -> int:
    if raw_json is None:
        raw_json = {"type": event_type, "ts": event_at}
    if not isinstance(raw_json, str):
        raw_json = json.dumps(raw_json, sort_keys=True)
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    cur = conn.execute(
        "INSERT INTO events (session_id, type, event_key, event_at, ingested_at, "
        "source, source_path, source_offset, seq, client, confidence, lossiness, raw_json) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            session_id,
            event_type,
            event_key,
            event_at,
            now,
            source,
            source_path,
            source_offset,
            seq,
            client,
            confidence,
            lossiness,
            raw_json,
        ),
    )
    conn.commit()
    return int(cur.lastrowid)


def insert_token_usage(
    conn: sqlite3.Connection,
    *,
    event_id: int,
    session_id: int,
    model: str = "claude-sonnet-4",
    input: int = 1000,
    output: int = 500,
    data_source: str = "api",
    pricing_table_version: str = "1.0",
    event_at: str = "2026-04-22T09:02:00Z",
) -> None:
    conn.execute(
        "INSERT INTO token_usage (event_id, session_id, model, input, output, "
        "data_source, pricing_table_version, event_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (event_id, session_id, model, input, output, data_source, pricing_table_version, event_at),
    )
    conn.commit()


def insert_cost_anomaly(
    conn: sqlite3.Connection,
    *,
    session_id: int,
    turn_event_id: int | None,
    kind: str = "cache_read_collapse",
    confidence: str = "high",
    evidence: dict | None = None,
) -> None:
    if evidence is None:
        evidence = {"prev_cache_read": 1000, "curr_cache_read": 0}
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    conn.execute(
        "INSERT INTO cost_anomalies (session_id, turn_event_id, kind, "
        "confidence, evidence_json, created_at) VALUES (?, ?, ?, ?, ?, ?)",
        (session_id, turn_event_id, kind, confidence, json.dumps(evidence, sort_keys=True), now),
    )
    conn.commit()


def insert_incident(
    conn: sqlite3.Connection,
    *,
    session_id: int,
    first_event_id: int,
    last_event_id: int,
    kind: str = "loop_exact_repeat",
    count: int = 3,
    confidence: str = "high",
    evidence: dict | None = None,
) -> None:
    if evidence is None:
        evidence = {"command": "npm test", "fingerprint": "abc"}
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    conn.execute(
        "INSERT INTO incidents (session_id, kind, first_event_id, last_event_id, "
        "evidence_json, count, confidence, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (session_id, kind, first_event_id, last_event_id,
         json.dumps(evidence, sort_keys=True), count, confidence, now),
    )
    conn.commit()


PERMISSIVE_CONFIG = {
    "source": "claude",
    "projects_confirmed": True,
    "redact_strings": [],
    "redact_usernames": [],
    "allowlist_entries": [],
    "excluded_projects": [],
}
