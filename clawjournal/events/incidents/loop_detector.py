"""Loop detector (lite) — flag exact-repeat command and tool-call runs.

The detector walks a session's events in canonical order and computes
a per-event "fingerprint" from the canonical payload (override
`payload_json` when present, else base `raw_json`) plus the matching
`tool_result` row. Consecutive events sharing a fingerprint are
grouped into a run; runs meeting the per-rule threshold become
`incidents` rows of kind `loop_exact_repeat`.

Per-rule thresholds (spec):
- shell command runs (`command_start` events) need **3** repeats.
- generic tool-call runs (`tool_call` events that are NOT shell)
  need **5** repeats.

The "outcome" portion of the fingerprint is the normalized result
text from the paired `tool_result` row (see `normalize.py`), plus an
availability bit so a missing result doesn't collapse with a real
empty-string result. If a tool_call has no paired result (e.g.
mid-execution), the detector treats the run as `confidence='medium'`
since we can't see whether the result diverged.

Cross-session matching is not done. Cross-event-key matching is
strict: a `tool_call` and a `tool_result` are paired only by the
suffix of their `event_key` (`tool_call:<id>` ↔ `tool_result:<id>`),
which 02 already populates.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass, field
from typing import Any

from clawjournal.events.classify.common import is_shell_tool
from clawjournal.events.incidents.normalize import normalize_outcome_text
from clawjournal.events.incidents.types import LOOP_INCIDENT_KIND
from clawjournal.events.view import canonical_events

DEFAULT_SHELL_THRESHOLD = 3
DEFAULT_TOOL_CALL_THRESHOLD = 5


@dataclass(frozen=True)
class LoopRule:
    """A per-event-type loop rule. `kind` is the resulting incident
    kind (always `loop_exact_repeat` today; the field exists so
    future rules can subclass within the same table)."""

    event_type: str
    threshold: int
    kind: str = LOOP_INCIDENT_KIND


DEFAULT_RULES: tuple[LoopRule, ...] = (
    LoopRule(event_type="command_start", threshold=DEFAULT_SHELL_THRESHOLD),
    LoopRule(event_type="tool_call", threshold=DEFAULT_TOOL_CALL_THRESHOLD),
)


@dataclass(frozen=True)
class IncidentHit:
    session_id: int
    kind: str
    first_event_id: int
    last_event_id: int
    count: int
    confidence: str
    evidence: dict[str, Any] = field(default_factory=dict)


# Run breakers: events that signal genuine new external input or
# context resets between two same-typed actions. `user_message` is the
# only one today — the assistant's own reasoning + tool-result
# bookkeeping are NOT breakers, since otherwise back-to-back
# auto-retries wouldn't register as a loop.
_RUN_BREAKERS = frozenset({"user_message", "compaction"})

# Shell-command fingerprint fields. Only these keys from a Bash/shell
# tool's args contribute to the run-identity key — other fields (e.g.
# Claude Bash's `description`, `timeout`, `run_in_background`) are
# model-narrated or ergonomic and would otherwise hide loops whenever
# those fields drift between identical retries.
_SHELL_FINGERPRINT_FIELDS = frozenset({"command", "cmd", "workdir", "cwd"})
_OUTCOME_PRESENT = "present"
_OUTCOME_MISSING = "missing"


@dataclass
class _CandidateRow:
    event_id: int
    event_type: str
    event_key: str | None
    fingerprint: tuple | None  # None = unparseable eligible row


@dataclass(frozen=True)
class _SessionEvent:
    event_id: int | None
    event_type: str
    event_key: str | None
    event_at: str | None
    raw_json: str | None
    payload_json: str | None


def detect_session_loops(
    conn: sqlite3.Connection,
    session_id: int,
    *,
    rules: tuple[LoopRule, ...] = DEFAULT_RULES,
) -> list[IncidentHit]:
    """Pure read — return the current loop hits for a session.

    Each rule is evaluated independently against the events of its
    own eligible type. Adjacency is measured in the canonical event
    stream: events of other types are transparent (a `tool_result`
    between two `command_start`s does NOT break the run, since it's
    the bookkeeping side of the first command), but a `user_message`
    or `compaction` event resets adjacency — the next attempt is then
    a response to new context, not a blind retry.
    """
    row = conn.execute(
        "SELECT client FROM event_sessions WHERE id = ?",
        (session_id,),
    ).fetchone()
    if row is None:
        return []
    client = row["client"]

    rows = _load_canonical_session_events(conn, session_id)
    if not rows:
        return []

    result_text_by_tool_id = _collect_result_texts(rows, client=client)

    hits: list[IncidentHit] = []
    for rule in rules:
        candidates = _build_rule_candidates(
            rows,
            rule,
            result_text_by_tool_id,
            client=client,
        )
        hits.extend(_emit_runs_for_rule(session_id, candidates, rule))
    return hits


# --- candidate construction ----------------------------------------------- #


def _load_canonical_session_events(
    conn: sqlite3.Connection,
    session_id: int,
) -> list[_SessionEvent]:
    if not _has_event_overrides_table(conn):
        return _load_base_session_events(conn, session_id)

    event_id_by_raw_ref = {
        (row["source_path"], int(row["source_offset"]), int(row["seq"])): int(row["id"])
        for row in conn.execute(
            """
            SELECT id, source_path, source_offset, seq
              FROM events
             WHERE session_id = ?
            """,
            (session_id,),
        )
    }

    stream: list[_SessionEvent] = []
    for event in canonical_events(conn, session_id):
        event_id = None
        if event.raw_ref is not None:
            event_id = event_id_by_raw_ref.get(event.raw_ref)
        stream.append(
            _SessionEvent(
                event_id=event_id,
                event_type=event.type,
                event_key=event.event_key,
                event_at=event.event_at,
                raw_json=event.raw_json,
                payload_json=event.payload_json,
            )
        )
    return _restore_hook_only_breaker_order(stream)


def _has_event_overrides_table(conn: sqlite3.Connection) -> bool:
    row = conn.execute(
        """
        SELECT 1
          FROM sqlite_master
         WHERE type = 'table' AND name = 'event_overrides'
        """
    ).fetchone()
    return row is not None


def _load_base_session_events(
    conn: sqlite3.Connection,
    session_id: int,
) -> list[_SessionEvent]:
    rows = conn.execute(
        """
        SELECT id, type, event_key, event_at, raw_json
          FROM events
         WHERE session_id = ?
         ORDER BY event_at IS NULL, event_at, source_path, source_offset, seq
        """,
        (session_id,),
    ).fetchall()
    emitted_keys: set[str] = set()
    stream: list[_SessionEvent] = []
    for row in rows:
        event_key = row["event_key"]
        if event_key is not None:
            if event_key in emitted_keys:
                continue
            emitted_keys.add(event_key)
        stream.append(
            _SessionEvent(
                event_id=int(row["id"]),
                event_type=row["type"],
                event_key=event_key,
                event_at=row["event_at"],
                raw_json=row["raw_json"],
                payload_json=None,
            )
        )
    return stream


def _restore_hook_only_breaker_order(
    stream: list[_SessionEvent],
) -> list[_SessionEvent]:
    ordered: list[_SessionEvent] = []
    for event in stream:
        if not _is_hook_only_breaker(event):
            ordered.append(event)
            continue
        insert_at = len(ordered)
        event_key = _session_event_sort_key(event)
        for idx, existing in enumerate(ordered):
            if _session_event_sort_key(existing) > event_key:
                insert_at = idx
                break
        ordered.insert(insert_at, event)
    return ordered


def _is_hook_only_breaker(event: _SessionEvent) -> bool:
    return (
        event.event_id is None
        and event.event_type in _RUN_BREAKERS
        and event.event_at is not None
    )


def _session_event_sort_key(event: _SessionEvent) -> tuple[bool, str | None]:
    return (event.event_at is None, event.event_at)


def _collect_result_texts(rows, *, client: str) -> dict[str, str]:
    """Map `tool_id -> normalized result text` for paired lookup.

    Rows already come from `canonical_events`, so cross-source
    duplicates have been removed before pairing."""
    out: dict[str, str] = {}
    for row in rows:
        payload_json = _effective_payload_json(row)
        if row.event_type != "tool_result" or payload_json is None:
            continue
        tool_id = _tool_result_id_from_event_key(row.event_key)
        if tool_id is None:
            continue
        if tool_id in out:
            continue  # first one wins
        try:
            parsed = json.loads(payload_json)
        except (TypeError, json.JSONDecodeError):
            continue
        if not isinstance(parsed, dict):
            continue
        text = _result_text_for_client(client, parsed, tool_id=tool_id)
        if text is None:
            continue
        out[tool_id] = normalize_outcome_text(text)
    return out


def _build_rule_candidates(
    rows,
    rule: LoopRule,
    results: dict[str, str],
    *,
    client: str,
) -> list[_CandidateRow]:
    """For a single rule, project the canonical stream down to (a)
    eligible-type rows and (b) breaker rows that reset adjacency.

    Other event types are transparent — they don't appear in the
    candidate list, so two `command_start`s with a `tool_result`
    between them stay adjacent for the run-grouping pass.
    """
    candidates: list[_CandidateRow] = []
    for row in rows:
        event_type = row.event_type
        if event_type == rule.event_type:
            # Hook-only synthetic events have no `events.id` to cite
            # in an incident row. Treat them as transparent — the
            # surrounding real events remain adjacent.
            if row.event_id is None:
                continue
            payload_json = _effective_payload_json(row)
            if payload_json is None:
                candidates.append(
                    _CandidateRow(
                        event_id=int(row.event_id),
                        event_type=event_type,
                        event_key=row.event_key,
                        fingerprint=None,
                    )
                )
                continue
            try:
                parsed = json.loads(payload_json)
            except (TypeError, json.JSONDecodeError):
                # Eligible row whose payload won't parse: include it
                # with fingerprint=None so it breaks the run rather
                # than silently extending it.
                candidates.append(
                    _CandidateRow(
                        event_id=int(row.event_id),
                        event_type=event_type,
                        event_key=row.event_key,
                        fingerprint=None,
                    )
                )
                continue
            if not isinstance(parsed, dict):
                candidates.append(
                    _CandidateRow(
                        event_id=int(row.event_id),
                        event_type=event_type,
                        event_key=row.event_key,
                        fingerprint=None,
                    )
                )
                continue
            fingerprint = _fingerprint_for(
                client=client,
                event_type=event_type,
                event_key=row.event_key,
                parsed=parsed,
                results=results,
            )
            candidates.append(
                _CandidateRow(
                    event_id=int(row.event_id),
                    event_type=event_type,
                    event_key=row.event_key,
                    fingerprint=fingerprint,
                )
            )
        elif event_type in _RUN_BREAKERS:
            # Breakers never cite an event_id in the incident row, but
            # hook-only overrides may have raw_ref=None → no id. Use 0
            # as a harmless sentinel; it's never read back.
            candidates.append(
                _CandidateRow(
                    event_id=int(row.event_id) if row.event_id is not None else 0,
                    event_type=event_type,
                    event_key=row.event_key,
                    fingerprint=None,
                )
            )
        # Other types are transparent — drop them.
    return candidates


def _emit_runs_for_rule(
    session_id: int,
    candidates: list[_CandidateRow],
    rule: LoopRule,
):
    run: list[_CandidateRow] = []
    cur_fingerprint: tuple | None = None

    def maybe_emit():
        nonlocal run, cur_fingerprint
        if not run or cur_fingerprint is None or len(run) < rule.threshold:
            run = []
            return None
        confidence = "high" if _fingerprint_has_observed_outcome(cur_fingerprint) else "medium"
        hit = IncidentHit(
            session_id=session_id,
            kind=rule.kind,
            first_event_id=run[0].event_id,
            last_event_id=run[-1].event_id,
            count=len(run),
            confidence=confidence,
            evidence={
                "event_type": rule.event_type,
                "fingerprint": _serialize_fingerprint(cur_fingerprint),
                "threshold": rule.threshold,
                "first_event_id": run[0].event_id,
                "last_event_id": run[-1].event_id,
                "event_ids": [c.event_id for c in run],
            },
        )
        run = []
        return hit

    for cand in candidates:
        if cand.fingerprint is None:
            hit = maybe_emit()
            if hit is not None:
                yield hit
            cur_fingerprint = None
            continue
        if cand.fingerprint != cur_fingerprint:
            hit = maybe_emit()
            if hit is not None:
                yield hit
            cur_fingerprint = cand.fingerprint
            run = [cand]
        else:
            run.append(cand)

    hit = maybe_emit()
    if hit is not None:
        yield hit


# --- per-client fingerprint extraction ------------------------------------ #


def _fingerprint_for(
    *,
    client: str,
    event_type: str,
    event_key: str | None,
    parsed: dict,
    results: dict[str, str],
) -> tuple | None:
    """Compute the comparison key for an eligible event.

    Returns `None` when the event is eligible by type but lacks the
    fields needed to compare (e.g. an unparseable payload). A None
    fingerprint breaks adjacent runs the same way a non-eligible
    event does.
    """
    tool_id = _tool_id_from_event_key(event_key)
    outcome_state = _OUTCOME_MISSING
    outcome = ""
    inline_outcome = _inline_outcome_for_client(client, event_type, parsed)
    if inline_outcome is not None:
        outcome_state = _OUTCOME_PRESENT
        outcome = normalize_outcome_text(inline_outcome)
    elif tool_id is not None and tool_id in results:
        outcome_state = _OUTCOME_PRESENT
        outcome = results[tool_id]

    if event_type == "command_start":
        command, args_key = _command_signature_for_client(
            client,
            parsed,
            tool_id=tool_id,
        )
        if not command:
            return None
        return ("command_start", command, args_key, outcome_state, outcome)
    if event_type == "tool_call":
        name, args_key = _tool_call_signature_for_client(
            client,
            parsed,
            tool_id=tool_id,
        )
        if not name:
            return None
        # Shell tool calls already get a `command_start` companion
        # event with the more meaningful (command, outcome) signature
        # — skip them here to avoid double-counting against a lower
        # threshold.
        if _is_shell_tool_name(name):
            return None
        return ("tool_call", name, args_key, outcome_state, outcome)
    return None


def _serialize_fingerprint(fingerprint: tuple) -> list:
    """Project an in-memory fingerprint tuple into the form stored in
    `incidents.evidence_json`.

    The live fingerprint carries raw command / tool args plus the
    normalized paired-`tool_result` text. None of those slots have been
    through the workbench redaction pipeline, so persisting them
    verbatim in `evidence_json` would leak sensitive input or output to
    any consumer that reads incidents directly. We keep only the
    structural markers (`event_type`, outcome availability) in cleartext
    and replace every comparison slot with a truncated sha256 so audit +
    grouping remain possible without storing raw payloads; the original
    text stays reachable via `first_event_id` / `last_event_id` through
    the normal redaction paths.
    """
    if not fingerprint:
        return []
    out: list[Any] = [fingerprint[0]]
    availability = _fingerprint_outcome_state(fingerprint)
    state_index = len(fingerprint) - 2
    outcome_index = len(fingerprint) - 1
    for index, value in enumerate(fingerprint[1:], start=1):
        if index == state_index:
            out.append(value)
            continue
        if index == outcome_index and availability == _OUTCOME_MISSING:
            out.append("")  # preserve the "no outcome available" signal
            continue
        out.append(_hashed_fingerprint_slot(value))
    return out


def _hashed_fingerprint_slot(value: object) -> str:
    if value == "":
        return ""
    if isinstance(value, str):
        text = value
    else:
        text = _canonical_args(value)
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]
    return f"sha256:{digest}"


def _fingerprint_has_observed_outcome(fingerprint: tuple) -> bool:
    return _fingerprint_outcome_state(fingerprint) == _OUTCOME_PRESENT


def _fingerprint_outcome_state(fingerprint: tuple) -> str:
    if len(fingerprint) >= 2 and fingerprint[-2] in {_OUTCOME_PRESENT, _OUTCOME_MISSING}:
        return fingerprint[-2]
    return _OUTCOME_MISSING


def _tool_id_from_event_key(event_key: str | None) -> str | None:
    return _event_key_suffix(event_key, "command_start:", "tool_call:")


def _tool_result_id_from_event_key(event_key: str | None) -> str | None:
    return _event_key_suffix(event_key, "tool_result:")


def _event_key_suffix(event_key: str | None, *prefixes: str) -> str | None:
    if not event_key:
        return None
    for prefix in prefixes:
        if event_key.startswith(prefix):
            return event_key[len(prefix) :]
    return None


def _command_signature_for_client(
    client: str,
    parsed: dict,
    *,
    tool_id: str | None,
) -> tuple[str | None, str]:
    if client in ("claude", "openclaw"):
        # Either a Bash tool_use carrying input.command, or a
        # bashExecution role carrying a top-level command string.
        message = parsed.get("message")
        if isinstance(message, dict):
            if message.get("role") == "bashExecution":
                command = message.get("command")
                if isinstance(command, str) and command.strip():
                    return command, _canonical_args({"command": command})
            block = _matching_tool_call_block(message, expected_tool_id=tool_id)
            if block is None:
                return None, ""
            name = block.get("name")
            if not _is_shell_tool_name(name):
                return None, ""
            args = block.get("input") if "input" in block else block.get("arguments")
            return _command_signature_from_args(args)
        return None, ""
    if client == "codex":
        payload = parsed.get("payload")
        if not isinstance(payload, dict):
            return None, ""
        if payload.get("type") not in ("function_call", "custom_tool_call"):
            return None, ""
        if not _is_shell_tool_name(payload.get("name")):
            return None, ""
        args = payload.get("arguments")
        if isinstance(args, str):
            try:
                args_obj = json.loads(args)
            except json.JSONDecodeError:
                stripped = args.strip()
                return (stripped or None), stripped
        elif isinstance(args, dict):
            args_obj = args
        else:
            return None, ""
        return _command_signature_from_args(args_obj)
    return None, ""


def _command_signature_from_args(args: object) -> tuple[str | None, str]:
    args_key = _shell_fingerprint_key(args)
    return _shell_command_from_args(args), args_key


def _shell_fingerprint_key(args: object) -> str:
    """Canonicalize *only* the shell-identity fields of `args`.

    Prevents Claude Bash's `description`/`timeout`/`run_in_background`
    drift from hiding otherwise-identical retries.
    """
    if not isinstance(args, dict):
        return _canonical_args(args)
    filtered = {
        k: args[k] for k in _SHELL_FINGERPRINT_FIELDS if k in args
    }
    return _canonical_args(filtered)


def _shell_command_from_args(args: object) -> str | None:
    if not isinstance(args, dict):
        return None
    for key in ("command", "cmd"):
        command = _command_value_to_string(args.get(key))
        if command is not None:
            return command
    return None


def _command_value_to_string(value: object) -> str | None:
    if isinstance(value, list):
        joined = " ".join(str(part) for part in value).strip()
        return joined or None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    return None


def _tool_call_signature_for_client(
    client: str,
    parsed: dict,
    *,
    tool_id: str | None,
) -> tuple[str | None, str]:
    if client in ("claude", "openclaw"):
        message = parsed.get("message")
        if not isinstance(message, dict):
            return None, ""
        block = _matching_tool_call_block(message, expected_tool_id=tool_id)
        if block is None:
            return None, ""
        name = block.get("name")
        if not isinstance(name, str):
            return None, ""
        args = block.get("input") if "input" in block else block.get("arguments")
        return name, _canonical_args(args)
    if client == "codex":
        payload = parsed.get("payload")
        if not isinstance(payload, dict):
            return None, ""
        if payload.get("type") not in ("function_call", "custom_tool_call"):
            return None, ""
        name = payload.get("name") if isinstance(payload.get("name"), str) else None
        args = payload.get("arguments")
        if isinstance(args, str):
            try:
                args_obj = json.loads(args)
            except json.JSONDecodeError:
                return name, args.strip()
            return name, _canonical_args(args_obj)
        return name, _canonical_args(args)
    return None, ""


def _canonical_args(args: object) -> str:
    """Return a stable string representation of a tool's args."""
    if args is None:
        return ""
    try:
        return json.dumps(args, sort_keys=True)
    except (TypeError, ValueError):
        return str(args)


def _is_shell_tool_name(name: object) -> bool:
    return is_shell_tool(name) or (
        isinstance(name, str) and name.strip().lower() in {"sh", "zsh"}
    )


def _result_text_for_client(
    client: str,
    parsed: dict,
    *,
    tool_id: str | None,
) -> str | None:
    """Pull the human-readable result text from a tool_result row.

    Falls back to a stable JSON dump if the wire format puts the
    payload somewhere unexpected — better to compare structured
    fallback than to lose the comparison entirely."""
    if client in ("claude", "openclaw"):
        message = parsed.get("message")
        if isinstance(message, dict):
            if client == "openclaw" and message.get("role") == "toolResult":
                message_tool_id = _tool_result_message_id(message)
                if (
                    tool_id is not None
                    and message_tool_id is not None
                    and message_tool_id != tool_id
                ):
                    return None
                text = _flatten_text(message.get("content"))
                if text is not None:
                    return text
            block = _matching_tool_result_block(
                message,
                expected_tool_id=tool_id,
            )
            if block is not None:
                text = _flatten_text(block.get("content"))
                if text is not None:
                    return text
        return None
    if client == "codex":
        payload = parsed.get("payload")
        if not isinstance(payload, dict):
            return None
        if payload.get("type") not in (
            "function_call_output",
            "custom_tool_call_output",
        ):
            return None
        out = payload.get("output")
        if isinstance(out, str):
            try:
                wrapped = json.loads(out)
            except json.JSONDecodeError:
                # If the raw text carries a codex `Exit code:` / `Wall
                # time:` / `Output:` preamble, let the parser own the
                # answer even when the `Output:` marker is missing —
                # that case returns None, which propagates out as
                # "missing outcome" so the run scores confidence=medium
                # rather than collapsing into a spurious empty match.
                if _is_codex_metadata_format(out):
                    return _parse_codex_plain_text_output(out)
                return out
            if isinstance(wrapped, dict):
                inner = wrapped.get("output")
                if isinstance(inner, str):
                    return inner
                return json.dumps(wrapped, sort_keys=True)
            return out
        if isinstance(out, list):
            text = _flatten_text(out)
            if text is None:
                return None
            if _is_codex_metadata_format(text):
                return _parse_codex_plain_text_output(text)
            return text
        return None
    return None


def _effective_payload_json(row: _SessionEvent) -> str | None:
    return row.payload_json if row.payload_json is not None else row.raw_json


def _inline_outcome_for_client(
    client: str,
    event_type: str,
    parsed: dict,
) -> str | None:
    if event_type != "command_start":
        return None
    if client not in ("claude", "openclaw"):
        return None
    message = parsed.get("message")
    if not isinstance(message, dict):
        return None
    if message.get("role") != "bashExecution":
        return None
    return _flatten_text(message.get("output"))


def _flatten_text(value: object) -> str | None:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        chunks: list[str] = []
        for block in value:
            if isinstance(block, str):
                chunks.append(block)
            elif isinstance(block, dict):
                text = block.get("text")
                if isinstance(text, str):
                    chunks.append(text)
        if chunks:
            return "\n".join(chunks)
    return None


def _is_codex_metadata_format(text: str) -> bool:
    """True when `text` carries a codex `Exit code:` / `Wall time:` /
    `Output:` preamble. Scan only the first few lines so unrelated
    output that happens to embed one of those phrases later doesn't
    misclassify."""
    for line in text.splitlines()[:5]:
        if (
            line.startswith("Exit code: ")
            or line.startswith("Wall time: ")
            or line == "Output:"
        ):
            return True
    return False


def _parse_codex_plain_text_output(text: str) -> str | None:
    """Extract the post-`Output:` payload from a codex plain-text result.

    Returns:
    - The stripped output text when a well-formed `Exit code` /
      `Wall time` / `Output:` preamble is present.
    - `None` when no codex metadata prefix is recognized (fall back to
      the raw text) OR when metadata is present but the `Output:`
      marker never arrives (truncated rollout — indistinguishable from
      "no observable outcome"; the caller treats `None` as missing so
      the run is scored `confidence="medium"` instead of collapsing
      into a spurious empty-outcome match).

    Prefix detection is gated on `saw_output_marker` so that `Exit
    code:` / `Wall time:` lines embedded inside the real output (e.g.
    a shell log echoing its child's exit status) are preserved
    verbatim instead of being silently dropped.
    """
    saw_metadata = False
    saw_output_marker = False
    output_lines: list[str] = []
    for line in text.splitlines():
        if not saw_output_marker:
            if line.startswith("Exit code: "):
                saw_metadata = True
                continue
            if line.startswith("Wall time: "):
                saw_metadata = True
                continue
            if line == "Output:":
                saw_metadata = True
                saw_output_marker = True
                continue
        else:
            output_lines.append(line)
    if not saw_metadata or not saw_output_marker:
        return None
    return "\n".join(output_lines).strip()


def _matching_tool_call_block(
    message: dict,
    *,
    expected_tool_id: str | None,
) -> dict | None:
    content = message.get("content")
    if not isinstance(content, list):
        return None
    for block in content:
        if not isinstance(block, dict):
            continue
        if block.get("type") not in ("tool_use", "toolCall"):
            continue
        if expected_tool_id is None:
            return block
        if _tool_call_block_id(block) == expected_tool_id:
            return block
    return None


def _tool_call_block_id(block: dict) -> str | None:
    for key in ("id", "toolUseId", "toolCallId"):
        value = block.get(key)
        if isinstance(value, str) and value.strip():
            return value
    return None


def _matching_tool_result_block(
    message: dict,
    *,
    expected_tool_id: str | None,
) -> dict | None:
    content = message.get("content")
    if not isinstance(content, list):
        return None
    for block in content:
        if not isinstance(block, dict):
            continue
        if block.get("type") != "tool_result":
            continue
        if expected_tool_id is None:
            return block
        if _tool_result_block_id(block) == expected_tool_id:
            return block
    return None


def _tool_result_block_id(block: dict) -> str | None:
    for key in ("tool_use_id", "toolUseId", "tool_call_id", "toolCallId", "id"):
        value = block.get(key)
        if isinstance(value, str) and value.strip():
            return value
    return None


def _tool_result_message_id(message: dict) -> str | None:
    for key in ("toolCallId", "toolUseId", "tool_call_id", "tool_use_id"):
        value = message.get(key)
        if isinstance(value, str) and value.strip():
            return value
    return None
