"""Read-time signal/confidence layer for the execution recorder.

Exposes:

- `ensure_view_schema(conn)` — additive migration creating `event_overrides`
  (named to avoid collision with `events.schema.ensure_schema`).
- `canonical_events(conn, session_id, *, prefer_source=None)` — deduped,
  override-promoted event stream. Readers use this rather than reading
  `events` directly.
- `capability_join(conn, session_id)` — three-state capability report per
  event type (present / missing / supported_but_absent), with counts split
  between base (`events`) and override (`event_overrides`) producers.
- `fetch_vendor_line(source_path, source_offset)` — best-effort retrieval
  of the vendor JSONL line an event references. Returns None when the
  file is gone, the offset is past EOF, or the line is partial. Does NOT
  detect rotation/replacement (01/02 don't persist inode).
- `write_hook_override(conn, *, session_key, event_key, event_type, source,
  confidence, lossiness, event_at, payload_json, origin)` — the one
  supported path for Beat 3+ hooks to land higher-confidence data.

`events` stays append-only; all overwrite state lives in `event_overrides`.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, NamedTuple

from clawjournal.events.capabilities import CAPABILITY_MATRIX
from clawjournal.events.types import (
    CONFIDENCE_RANK,
    EVENT_TYPES,
    VALID_LOSSINESS,
    VALID_SOURCES,
)


# --- schema --------------------------------------------------------------- #

_WRITABLE_CONFIDENCE = {"high", "medium", "low"}  # "missing" is read-time only


def _sql_in_list(values) -> str:
    return "(" + ", ".join(f"'{v}'" for v in sorted(values)) + ")"


_SCHEMA_SQL = f"""
CREATE TABLE IF NOT EXISTS event_overrides (
    session_id    INTEGER NOT NULL REFERENCES event_sessions(id) ON DELETE CASCADE,
    event_key     TEXT    NOT NULL,
    type          TEXT    NOT NULL CHECK (type IN {_sql_in_list(EVENT_TYPES)}),
    source        TEXT    NOT NULL CHECK (source IN {_sql_in_list(VALID_SOURCES)}),
    confidence    TEXT    NOT NULL CHECK (confidence IN {_sql_in_list(_WRITABLE_CONFIDENCE)}),
    lossiness     TEXT    NOT NULL CHECK (lossiness IN {_sql_in_list(VALID_LOSSINESS)}),
    event_at      TEXT,
    payload_json  TEXT    NOT NULL,
    origin        TEXT,
    created_at    TEXT    NOT NULL,
    PRIMARY KEY (session_id, event_key)
);
CREATE INDEX IF NOT EXISTS idx_event_overrides_session ON event_overrides(session_id);
"""


def ensure_view_schema(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(_SCHEMA_SQL)


# --- types ---------------------------------------------------------------- #


@dataclass(frozen=True)
class CanonicalEvent:
    type: str
    event_key: str | None
    event_at: str | None
    source: str
    confidence: str
    lossiness: str
    raw_json: str | None
    payload_json: str | None
    raw_ref: tuple[str, int, int] | None
    origin: str | None


class CapabilityState(NamedTuple):
    event_type: str
    state: str                     # "present" | "missing" | "supported_but_absent"
    reason: str
    observed_base_count: int
    observed_override_count: int


# --- hook override writer -------------------------------------------------- #


_UPSERT_SQL = """
INSERT INTO event_overrides (
    session_id, event_key, type, source, confidence, lossiness,
    event_at, payload_json, origin, created_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(session_id, event_key) DO UPDATE SET
    type         = excluded.type,
    source       = excluded.source,
    confidence   = excluded.confidence,
    lossiness    = excluded.lossiness,
    event_at     = excluded.event_at,
    payload_json = excluded.payload_json,
    origin       = excluded.origin,
    created_at   = excluded.created_at
  WHERE
    CASE excluded.confidence WHEN 'high' THEN 3 WHEN 'medium' THEN 2 WHEN 'low' THEN 1 ELSE 0 END
    >
    CASE event_overrides.confidence WHEN 'high' THEN 3 WHEN 'medium' THEN 2 WHEN 'low' THEN 1 ELSE 0 END
"""


def write_hook_override(
    conn: sqlite3.Connection,
    *,
    session_key: str,
    event_key: str,
    event_type: str,
    source: str,
    confidence: str,
    lossiness: str,
    event_at: str | None,
    payload_json: str,
    origin: str | None,
) -> bool:
    """Write a hook-originated override. Returns True if the row landed
    (fresh insert or a strict-greater override replacement), False if the
    rank guard rejected the write.

    Raises ValueError on invalid enum inputs; KeyError if the session_key
    doesn't exist in `event_sessions` yet (hooks fire against live
    sessions that 02's ingest has already created).
    """
    if event_type not in EVENT_TYPES:
        raise ValueError(f"Unsupported event type: {event_type}")
    if source not in VALID_SOURCES:
        raise ValueError(f"Unsupported source: {source}")
    if confidence not in _WRITABLE_CONFIDENCE:
        raise ValueError(
            f"Unsupported confidence for write: {confidence} "
            "(valid: high / medium / low; 'missing' is read-time only)"
        )
    if lossiness not in VALID_LOSSINESS:
        raise ValueError(f"Unsupported lossiness: {lossiness}")
    if not event_key:
        raise ValueError("event_key is required for overrides")
    if not isinstance(payload_json, str):
        raise ValueError("payload_json must be a JSON string")
    try:
        json.loads(payload_json)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ValueError("payload_json must be valid JSON") from exc

    session_id = _resolve_session_id(conn, session_key)
    if session_id is None:
        raise KeyError(f"session_key not found in event_sessions: {session_key}")

    created_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    with conn:
        cursor = conn.execute(
            _UPSERT_SQL,
            (
                session_id,
                event_key,
                event_type,
                source,
                confidence,
                lossiness,
                event_at,
                payload_json,
                origin,
                created_at,
            ),
        )
        return cursor.rowcount > 0


def _resolve_session_id(conn: sqlite3.Connection, session_key: str) -> int | None:
    row = conn.execute(
        "SELECT id FROM event_sessions WHERE session_key = ?",
        (session_key,),
    ).fetchone()
    return None if row is None else int(row[0])


# --- canonical reader ----------------------------------------------------- #


_BASE_SELECT = """
SELECT type, event_key, event_at, source, confidence, lossiness,
       raw_json, source_path, source_offset, seq
  FROM events
 WHERE session_id = ?
"""

_BASE_ORDER = " ORDER BY event_at IS NULL, event_at, source_path, source_offset, seq"


_OVERRIDE_SELECT = """
SELECT event_key, type, source, confidence, lossiness,
       event_at, payload_json, origin
  FROM event_overrides
 WHERE session_id = ?
 ORDER BY event_at IS NULL, event_at, event_key
"""


def canonical_events(
    conn: sqlite3.Connection,
    session_id: int,
    *,
    prefer_source: str | None = None,
) -> Iterator[CanonicalEvent]:
    """Yield the deduped, override-promoted event stream for a session.

    Rules (summary; full spec in docs/plans/phase-1/03-*):
    - Base rows with NULL event_key pass through unchanged.
    - For each event_key, keep the first base row seen in canonical
      order; skip later duplicates. If an override exists and
      CONFIDENCE_RANK[override] >= CONFIDENCE_RANK[base], the override
      wins (takes type/source/confidence/lossiness/event_at/origin/
      payload_json; base contributes raw_json/raw_ref).
    - Overrides whose event_key never appears on a base row emit as
      hook-only CanonicalEvents (raw_json=None, raw_ref=None).
    - `prefer_source` filters base rows before dedup (e.g.
      "claude-jsonl" drops LA duplicates). Note: if `prefer_source`
      filters out a base row that had a matching override, the override
      will emit as hook-only (no base in scope to merge raw_json against).
      Callers wanting override-promoted rows across all sources should
      either leave `prefer_source` unset or accept this asymmetry.
    """
    overrides = {row["event_key"]: dict(row) for row in conn.execute(_OVERRIDE_SELECT, (session_id,))}

    sql = _BASE_SELECT
    params: list = [session_id]
    if prefer_source is not None:
        sql += " AND source = ?"
        params.append(prefer_source)
    sql += _BASE_ORDER

    emitted_keys: set[str] = set()
    for row in conn.execute(sql, params):
        event_key = row["event_key"]
        base = dict(row)

        if event_key is None:
            yield _canonical_from_base(base)
            continue

        if event_key in emitted_keys:
            continue  # cross-source duplicate

        emitted_keys.add(event_key)
        override = overrides.get(event_key)
        if override is not None and _override_beats_base(override, base):
            yield _canonical_override_wins(override, base)
        else:
            yield _canonical_from_base(base)

    for event_key, override in overrides.items():
        if event_key in emitted_keys:
            continue
        yield _canonical_hook_only(override)


def _override_beats_base(override: dict, base: dict) -> bool:
    return (
        CONFIDENCE_RANK.get(override["confidence"], 0)
        >= CONFIDENCE_RANK.get(base["confidence"], 0)
    )


def _canonical_from_base(base: dict) -> CanonicalEvent:
    return CanonicalEvent(
        type=base["type"],
        event_key=base["event_key"],
        event_at=base["event_at"],
        source=base["source"],
        confidence=base["confidence"],
        lossiness=base["lossiness"],
        raw_json=base["raw_json"],
        payload_json=None,
        raw_ref=(base["source_path"], base["source_offset"], base["seq"]),
        origin=None,
    )


def _canonical_override_wins(override: dict, base: dict) -> CanonicalEvent:
    return CanonicalEvent(
        type=override["type"],
        event_key=override["event_key"],
        event_at=override["event_at"],
        source=override["source"],
        confidence=override["confidence"],
        lossiness=override["lossiness"],
        raw_json=base["raw_json"],
        payload_json=override["payload_json"],
        raw_ref=(base["source_path"], base["source_offset"], base["seq"]),
        origin=override["origin"],
    )


def _canonical_hook_only(override: dict) -> CanonicalEvent:
    return CanonicalEvent(
        type=override["type"],
        event_key=override["event_key"],
        event_at=override["event_at"],
        source=override["source"],
        confidence=override["confidence"],
        lossiness=override["lossiness"],
        raw_json=None,
        payload_json=override["payload_json"],
        raw_ref=None,
        origin=override["origin"],
    )


# --- capability join ------------------------------------------------------ #


def capability_join(conn: sqlite3.Connection, session_id: int) -> list[CapabilityState]:
    """Report the per-event-type capability state for a session.

    Uses the session's client (from event_sessions.client) + the static
    CAPABILITY_MATRIX to classify each of EVENT_TYPES as present,
    missing, or supported_but_absent — with counts split between base
    and override producers so consumers can distinguish vendor-observed
    from hook-synthesized.
    """
    row = conn.execute(
        "SELECT client FROM event_sessions WHERE id = ?", (session_id,)
    ).fetchone()
    if row is None:
        raise KeyError(f"session_id not found: {session_id}")
    client = row["client"]

    base_counts = {
        r["type"]: r["n"]
        for r in conn.execute(
            "SELECT type, COUNT(*) AS n FROM events WHERE session_id = ? GROUP BY type",
            (session_id,),
        )
    }
    override_counts = {
        r["type"]: r["n"]
        for r in conn.execute(
            "SELECT type, COUNT(*) AS n FROM event_overrides WHERE session_id = ? GROUP BY type",
            (session_id,),
        )
    }

    states: list[CapabilityState] = []
    for event_type in EVENT_TYPES:
        base_n = base_counts.get(event_type, 0)
        override_n = override_counts.get(event_type, 0)
        supported, matrix_reason = CAPABILITY_MATRIX.get(
            (client, event_type), (False, "not emitted by this client")
        )

        if base_n + override_n > 0:
            state = "present"
            reason = "observed in this session"
        elif supported:
            state = "supported_but_absent"
            reason = matrix_reason
        else:
            state = "missing"
            reason = matrix_reason

        states.append(
            CapabilityState(
                event_type=event_type,
                state=state,
                reason=reason,
                observed_base_count=base_n,
                observed_override_count=override_n,
            )
        )
    return states


# --- vendor-line fetch ---------------------------------------------------- #


_FETCH_CHUNK = 65_536
_MAX_LINE_BYTES = 16 * 1024 * 1024  # safety cap — real vendor lines are KB-sized


def fetch_vendor_line(source_path: str | Path, source_offset: int) -> str | None:
    """Best-effort retrieval of the JSONL line at (source_path, source_offset).

    Returns None if:
    - the file is missing;
    - the offset is past EOF;
    - no terminating newline exists between source_offset and EOF
      (partial trailing line — matches 01's "skip incomplete lines"
      semantics);
    - the line would exceed `_MAX_LINE_BYTES`, whether because no
      newline appears within the cap or because the terminating
      newline sits past it (defends against a very long or adversarial
      line).

    Does NOT detect rotation / replacement — the file at source_path may
    have been swapped since the event was ingested. Phase 1 accepts this
    limit; see docs/plans/phase-1/03-*.
    """
    try:
        with open(source_path, "rb") as f:
            f.seek(source_offset)
            chunks: list[bytes] = []
            total = 0
            while True:
                block = f.read(_FETCH_CHUNK)
                if not block:
                    return None  # partial line — EOF with no newline
                newline_pos = block.find(b"\n")
                if newline_pos >= 0:
                    absolute_newline = total + newline_pos
                    if absolute_newline > _MAX_LINE_BYTES:
                        return None  # newline exists, but only after the safety cap
                    chunks.append(block)
                    complete = b"".join(chunks)[:absolute_newline]
                    if complete.endswith(b"\r"):
                        complete = complete[:-1]
                    return complete.decode("utf-8", errors="replace")
                next_total = total + len(block)
                if next_total > _MAX_LINE_BYTES:
                    return None  # pathological: no newline within safety cap
                total = next_total
                chunks.append(block)
    except (FileNotFoundError, IsADirectoryError):
        return None
    except OSError:
        return None
