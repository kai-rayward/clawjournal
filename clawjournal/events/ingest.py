"""Ingest capture line batches into the execution recorder tables."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from clawjournal.capture import (
    cursor_after,
    get_cursor,
    iter_new_lines,
    iter_source_files,
    set_cursor,
)
from clawjournal.capture.cursors import ensure_schema as ensure_capture_schema
from clawjournal.capture.discovery import SourceFile
from clawjournal.events.classify import classify_line, session_meta_for_line
from clawjournal.events.schema import ensure_schema as ensure_event_schema
from clawjournal.events.types import ClassifiedEvent, validate_classified_event

EVENT_CONSUMER_ID = "events"
IDLE_END_AFTER_SECONDS = 3600

_SOURCE_BY_CLIENT = {
    "claude": "claude-jsonl",
    "codex": "codex-rollout",
    "openclaw": "openclaw-jsonl",
}

_UPSERT_EVENT_SESSION_SQL = """
INSERT INTO event_sessions (
    session_key,
    parent_session_key,
    parent_session_id,
    client,
    client_version,
    started_at,
    ended_at,
    status
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(session_key) DO UPDATE SET
    parent_session_key = COALESCE(
        event_sessions.parent_session_key,
        excluded.parent_session_key
    ),
    parent_session_id = COALESCE(
        event_sessions.parent_session_id,
        excluded.parent_session_id
    ),
    client_version = COALESCE(
        event_sessions.client_version,
        excluded.client_version
    ),
    started_at = CASE
        WHEN event_sessions.started_at IS NULL THEN excluded.started_at
        WHEN excluded.started_at IS NULL THEN event_sessions.started_at
        WHEN excluded.started_at < event_sessions.started_at THEN excluded.started_at
        ELSE event_sessions.started_at
    END,
    ended_at = CASE
        WHEN event_sessions.ended_at IS NULL THEN excluded.ended_at
        WHEN excluded.ended_at IS NULL THEN event_sessions.ended_at
        WHEN excluded.ended_at > event_sessions.ended_at THEN excluded.ended_at
        ELSE event_sessions.ended_at
    END,
    status = CASE
        WHEN event_sessions.status = 'ended' THEN 'ended'
        ELSE excluded.status
    END
"""

_INSERT_EVENT_SQL = """
INSERT OR IGNORE INTO events (
    session_id,
    type,
    event_key,
    event_at,
    ingested_at,
    source,
    source_path,
    source_offset,
    seq,
    client,
    confidence,
    lossiness,
    raw_json
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


@dataclass
class IngestSummary:
    files_scanned: int = 0
    files_with_changes: int = 0
    batches: int = 0
    lines_read: int = 0
    event_rows: int = 0
    _session_keys: set[str] = field(default_factory=set, repr=False)

    def to_dict(self) -> dict[str, int]:
        return {
            "files_scanned": self.files_scanned,
            "files_with_changes": self.files_with_changes,
            "batches": self.batches,
            "lines_read": self.lines_read,
            "event_rows": self.event_rows,
            "sessions_touched": len(self._session_keys),
        }


def ingest_pending(
    conn: sqlite3.Connection,
    *,
    source_filter: str | None = None,
    now: datetime | None = None,
) -> IngestSummary:
    ensure_capture_schema(conn)
    ensure_event_schema(conn)

    summary = IngestSummary()
    latest_mtime_by_session_key: dict[str, float] = {}
    for source in iter_source_files(source_filter=source_filter):
        summary.files_scanned += 1
        latest_mtime_by_session_key[source.session_key] = max(
            latest_mtime_by_session_key.get(source.session_key, float("-inf")),
            _source_last_modified(source),
        )
        cursor = get_cursor(conn, EVENT_CONSUMER_ID, source.path)
        batch = iter_new_lines(source.path, cursor, client=source.client)
        if batch is None:
            continue
        summary.files_with_changes += 1
        summary.batches += 1
        summary.lines_read += len(batch.lines)
        summary._session_keys.add(source.session_key)
        summary.event_rows += _ingest_batch(conn, source, batch, now=now)
    _sweep_idle_sessions(conn, latest_mtime_by_session_key, now=now)
    return summary


def _ingest_batch(
    conn: sqlite3.Connection,
    source: SourceFile,
    batch,
    *,
    now: datetime | None,
) -> int:
    ingested_at = _utc_now_iso(now)
    source_name = _SOURCE_BY_CLIENT[source.client]

    event_rows: list[tuple[Any, ...]] = []
    client_version: str | None = None
    parent_session_id_raw: str | None = None
    closure_seen = False
    started_at: str | None = None
    ended_at: str | None = None

    for line_offset, line_text in zip(batch.line_offsets, batch.lines):
        try:
            parsed = json.loads(line_text)
        except json.JSONDecodeError:
            raw_json = json.dumps(
                {"_unparseable": True, "raw_text": line_text},
                sort_keys=True,
            )
            event_rows.append(
                (
                    "schema_unknown",
                    None,
                    None,
                    ingested_at,
                    source_name,
                    str(source.path),
                    line_offset,
                    0,
                    source.client,
                    "low",
                    "unknown",
                    raw_json,
                )
            )
            continue

        raw_json = json.dumps(parsed, sort_keys=True)
        classified = classify_line(source.client, parsed) or []
        if not classified:
            classified = [
                ClassifiedEvent(
                    type="schema_unknown",
                    event_at=None,
                    event_key=None,
                    confidence="low",
                    lossiness="none",
                )
            ]
        meta = session_meta_for_line(source.client, parsed)
        if client_version is None and meta.client_version:
            client_version = meta.client_version
        if parent_session_id_raw is None and meta.parent_session_id:
            parent_session_id_raw = meta.parent_session_id
        closure_seen = closure_seen or meta.closure_seen

        for seq, classified_event in enumerate(classified):
            validate_classified_event(classified_event)
            if classified_event.event_at is not None:
                started_at = _min_iso(started_at, classified_event.event_at)
                ended_at = _max_iso(ended_at, classified_event.event_at)
            event_rows.append(
                (
                    classified_event.type,
                    classified_event.event_key,
                    classified_event.event_at,
                    ingested_at,
                    source_name,
                    str(source.path),
                    line_offset,
                    seq,
                    source.client,
                    classified_event.confidence,
                    classified_event.lossiness,
                    raw_json,
                )
            )

    parent_session_key = _parent_session_key(source, parent_session_id_raw)
    status = "ended" if closure_seen or _is_idle_stable(batch, now=now) else "active"

    with conn:
        parent_session_id = _lookup_session_id(conn, parent_session_key)
        conn.execute(
            _UPSERT_EVENT_SESSION_SQL,
            (
                source.session_key,
                parent_session_key,
                parent_session_id,
                source.client,
                client_version,
                started_at,
                ended_at,
                status,
            ),
        )
        session_id = _lookup_session_id(conn, source.session_key)
        assert session_id is not None
        conn.executemany(
            _INSERT_EVENT_SQL,
            [
                (
                    session_id,
                    event_type,
                    event_key,
                    event_at,
                    ingested_at_value,
                    source_name_value,
                    source_path,
                    source_offset,
                    seq,
                    client,
                    confidence,
                    lossiness,
                    raw_json,
                )
                for (
                    event_type,
                    event_key,
                    event_at,
                    ingested_at_value,
                    source_name_value,
                    source_path,
                    source_offset,
                    seq,
                    client,
                    confidence,
                    lossiness,
                    raw_json,
                ) in event_rows
            ],
        )
        _backfill_parent_links(conn)

    with conn:
        set_cursor(conn, cursor_after(batch, consumer_id=EVENT_CONSUMER_ID))

    return len(event_rows)


def _lookup_session_id(
    conn: sqlite3.Connection, session_key: str | None
) -> int | None:
    if session_key is None:
        return None
    row = conn.execute(
        "SELECT id FROM event_sessions WHERE session_key = ?",
        (session_key,),
    ).fetchone()
    if row is None:
        return None
    return int(row[0])


def _backfill_parent_links(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        UPDATE event_sessions
           SET parent_session_id = (
               SELECT parent.id
                 FROM event_sessions AS parent
                WHERE parent.session_key = event_sessions.parent_session_key
           )
         WHERE parent_session_id IS NULL
           AND parent_session_key IS NOT NULL
           AND EXISTS (
               SELECT 1
                 FROM event_sessions AS parent
                WHERE parent.session_key = event_sessions.parent_session_key
           )
        """
    )


def _sweep_idle_sessions(
    conn: sqlite3.Connection,
    latest_mtime_by_session_key: dict[str, float],
    *,
    now: datetime | None,
) -> None:
    effective_now = now or datetime.now(timezone.utc)
    idle_keys = [
        session_key
        for session_key, last_modified in latest_mtime_by_session_key.items()
        if (effective_now.timestamp() - last_modified) >= IDLE_END_AFTER_SECONDS
    ]
    if not idle_keys:
        return

    with conn:
        conn.executemany(
            "UPDATE event_sessions SET status = 'ended' WHERE session_key = ? AND status != 'ended'",
            [(session_key,) for session_key in idle_keys],
        )


def _parent_session_key(
    source: SourceFile, parent_session_id_raw: str | None
) -> str | None:
    if parent_session_id_raw is None:
        return None
    if parent_session_id_raw.startswith("claude:"):
        return parent_session_id_raw
    if source.client != "claude":
        return None
    return f"claude:{source.project_dir_name}:{parent_session_id_raw}"


def _is_idle_stable(batch, *, now: datetime | None) -> bool:
    effective_now = now or datetime.now(timezone.utc)
    return (effective_now.timestamp() - batch.last_modified) >= IDLE_END_AFTER_SECONDS


def _source_last_modified(source: SourceFile) -> float:
    try:
        return source.path.stat().st_mtime
    except FileNotFoundError:
        return float("-inf")


def _utc_now_iso(now: datetime | None) -> str:
    effective_now = now or datetime.now(timezone.utc)
    return effective_now.astimezone(timezone.utc).isoformat().replace(
        "+00:00", "Z"
    )


def _min_iso(current: str | None, incoming: str) -> str:
    if current is None:
        return incoming
    return incoming if incoming < current else current


def _max_iso(current: str | None, incoming: str) -> str:
    if current is None:
        return incoming
    return incoming if incoming > current else current
