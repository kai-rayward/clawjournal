"""Loop detector (lite) — flag exact-repeat command and tool-call runs.

The detector walks a session's events in canonical order and computes
a per-event "fingerprint" from `events.raw_json` plus the matching
`tool_result` row. Consecutive events sharing a fingerprint are
grouped into a run; runs meeting the per-rule threshold become
`incidents` rows of kind `loop_exact_repeat`.

Per-rule thresholds (spec):
- shell command runs (`command_start` events) need **3** repeats.
- generic tool-call runs (`tool_call` events that are NOT shell)
  need **5** repeats.

The "outcome" portion of the fingerprint is the normalized result
text from the paired `tool_result` row (see `normalize.py`). If a
tool_call has no paired result (e.g. mid-execution), the outcome is
the empty string — two such events still match each other but the
detector treats the run as `confidence='medium'` since we can't see
whether the result diverged.

Cross-session matching is not done. Cross-event-key matching is
strict: a `tool_call` and a `tool_result` are paired only by the
suffix of their `event_key` (`tool_call:<id>` ↔ `tool_result:<id>`),
which 02 already populates.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from typing import Any

from clawjournal.events.incidents.normalize import normalize_outcome_text
from clawjournal.events.incidents.types import LOOP_INCIDENT_KIND

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


@dataclass
class _CandidateRow:
    event_id: int
    event_type: str
    event_key: str | None
    raw: dict
    fingerprint: tuple | None  # None = unparseable eligible row


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
    rows = conn.execute(
        """
        SELECT id, type, event_key, client, raw_json, source_path, source_offset, seq
          FROM events
         WHERE session_id = ?
         ORDER BY event_at IS NULL, event_at, source_path, source_offset, seq
        """,
        (session_id,),
    ).fetchall()
    if not rows:
        return []

    result_text_by_tool_id = _collect_result_texts(rows)

    hits: list[IncidentHit] = []
    for rule in rules:
        candidates = _build_rule_candidates(rows, rule, result_text_by_tool_id)
        hits.extend(_emit_runs_for_rule(session_id, candidates, rule))
    return hits


# --- candidate construction ----------------------------------------------- #


def _collect_result_texts(rows) -> dict[str, str]:
    """Map `tool_id -> normalized result text` for paired lookup.

    Only the *latest* tool_result wins if there are duplicates across
    sources (native + LA emit the same logical event); cross-source
    dedup happens at the read layer via 03's canonical_events for
    consumers that need it, but for fingerprint comparison we just
    pick whichever lands first — they should produce identical
    normalized text by construction."""
    out: dict[str, str] = {}
    for row in rows:
        if row["type"] != "tool_result":
            continue
        event_key = row["event_key"]
        if not event_key or not event_key.startswith("tool_result:"):
            continue
        tool_id = event_key[len("tool_result:") :]
        if tool_id in out:
            continue  # first one wins
        try:
            parsed = json.loads(row["raw_json"])
        except (TypeError, json.JSONDecodeError):
            continue
        text = _result_text_for_client(row["client"], parsed)
        if text is None:
            continue
        out[tool_id] = normalize_outcome_text(text)
    return out


def _build_rule_candidates(
    rows, rule: LoopRule, results: dict[str, str]
) -> list[_CandidateRow]:
    """For a single rule, project the canonical stream down to (a)
    eligible-type rows and (b) breaker rows that reset adjacency.

    Other event types are transparent — they don't appear in the
    candidate list, so two `command_start`s with a `tool_result`
    between them stay adjacent for the run-grouping pass.
    """
    candidates: list[_CandidateRow] = []
    for row in rows:
        event_type = row["type"]
        if event_type == rule.event_type:
            try:
                parsed = json.loads(row["raw_json"])
            except (TypeError, json.JSONDecodeError):
                # Eligible row whose payload won't parse: include it
                # with fingerprint=None so it breaks the run rather
                # than silently extending it.
                candidates.append(
                    _CandidateRow(
                        event_id=int(row["id"]),
                        event_type=event_type,
                        event_key=row["event_key"],
                        raw={},
                        fingerprint=None,
                    )
                )
                continue
            fingerprint = _fingerprint_for(
                client=row["client"],
                event_type=event_type,
                event_key=row["event_key"],
                parsed=parsed,
                results=results,
            )
            candidates.append(
                _CandidateRow(
                    event_id=int(row["id"]),
                    event_type=event_type,
                    event_key=row["event_key"],
                    raw=parsed,
                    fingerprint=fingerprint,
                )
            )
        elif event_type in _RUN_BREAKERS:
            candidates.append(
                _CandidateRow(
                    event_id=int(row["id"]),
                    event_type=event_type,
                    event_key=row["event_key"],
                    raw={},
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
        # The fingerprint's last element is the (normalized) outcome
        # text; an empty string means we couldn't read it, so the
        # run's confidence drops.
        confidence = "high" if cur_fingerprint[-1] != "" else "medium"
        hit = IncidentHit(
            session_id=session_id,
            kind=rule.kind,
            first_event_id=run[0].event_id,
            last_event_id=run[-1].event_id,
            count=len(run),
            confidence=confidence,
            evidence={
                "event_type": rule.event_type,
                "fingerprint": list(cur_fingerprint),
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
    outcome = results.get(tool_id, "") if tool_id is not None else ""

    if event_type == "command_start":
        command = _command_string_for_client(client, parsed)
        if not command:
            return None
        return ("command_start", command, outcome)
    if event_type == "tool_call":
        name, args_key = _tool_call_signature_for_client(client, parsed)
        if not name:
            return None
        # Shell tool calls already get a `command_start` companion
        # event with the more meaningful (command, outcome) signature
        # — skip them here to avoid double-counting against a lower
        # threshold.
        if _is_shell_tool_name(name):
            return None
        return ("tool_call", name, args_key, outcome)
    return None


def _tool_id_from_event_key(event_key: str | None) -> str | None:
    if not event_key:
        return None
    for prefix in ("command_start:", "tool_call:"):
        if event_key.startswith(prefix):
            return event_key[len(prefix) :]
    return None


def _command_string_for_client(client: str, parsed: dict) -> str | None:
    if client in ("claude", "openclaw"):
        # Either a Bash tool_use carrying input.command, or a
        # bashExecution role carrying a top-level command string.
        message = parsed.get("message")
        if isinstance(message, dict):
            if message.get("role") == "bashExecution":
                command = message.get("command")
                if isinstance(command, str) and command.strip():
                    return command
            content = message.get("content")
            if isinstance(content, list):
                for block in content:
                    if not isinstance(block, dict):
                        continue
                    if block.get("type") not in ("tool_use", "toolCall"):
                        continue
                    name = block.get("name")
                    if not _is_shell_tool_name(name):
                        continue
                    inp = block.get("input")
                    if isinstance(inp, dict):
                        cmd = inp.get("command")
                        if isinstance(cmd, str) and cmd.strip():
                            return cmd
                    return None
        return None
    if client == "codex":
        payload = parsed.get("payload")
        if not isinstance(payload, dict):
            return None
        if payload.get("type") not in ("function_call", "custom_tool_call"):
            return None
        if not _is_shell_tool_name(payload.get("name")):
            return None
        args = payload.get("arguments")
        if isinstance(args, str):
            try:
                args_obj = json.loads(args)
            except json.JSONDecodeError:
                return args.strip() or None
        elif isinstance(args, dict):
            args_obj = args
        else:
            return None
        cmd = args_obj.get("command") if isinstance(args_obj, dict) else None
        if isinstance(cmd, list):
            return " ".join(str(p) for p in cmd)
        if isinstance(cmd, str) and cmd.strip():
            return cmd
        return None
    return None


def _tool_call_signature_for_client(
    client: str, parsed: dict
) -> tuple[str | None, str]:
    if client in ("claude", "openclaw"):
        message = parsed.get("message")
        if not isinstance(message, dict):
            return None, ""
        content = message.get("content")
        if not isinstance(content, list):
            return None, ""
        for block in content:
            if not isinstance(block, dict):
                continue
            if block.get("type") not in ("tool_use", "toolCall"):
                continue
            name = block.get("name")
            if not isinstance(name, str):
                continue
            args = block.get("input") if "input" in block else block.get("arguments")
            return name, _canonical_args(args)
        return None, ""
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
    if not isinstance(name, str):
        return False
    lowered = name.lower()
    return lowered in {"bash", "shell", "sh", "zsh"}


def _result_text_for_client(client: str, parsed: dict) -> str | None:
    """Pull the human-readable result text from a tool_result row.

    Falls back to a stable JSON dump if the wire format puts the
    payload somewhere unexpected — better to compare structured
    fallback than to lose the comparison entirely."""
    if client in ("claude", "openclaw"):
        message = parsed.get("message")
        if isinstance(message, dict):
            content = message.get("content")
            if isinstance(content, list):
                for block in content:
                    if not isinstance(block, dict):
                        continue
                    if block.get("type") != "tool_result":
                        continue
                    inner = block.get("content")
                    text = _flatten_text(inner)
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
                return out
            if isinstance(wrapped, dict):
                inner = wrapped.get("output")
                if isinstance(inner, str):
                    return inner
                return json.dumps(wrapped, sort_keys=True)
            return out
        if isinstance(out, list):
            return _flatten_text(out)
        return None
    return None


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
