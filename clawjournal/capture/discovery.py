"""Source file discovery for the capture adapter.

Phase 1 steps 1 and 1b cover:
- Claude Code native projects (`~/.claude/projects/**`), including subagent
  streams under nested session dirs.
- Claude Desktop local-agent-mode sessions (`LOCAL_AGENT_DIR`), mirroring
  `parser._scan_local_agent_sessions()`:
    - Wrappers with malformed JSON, non-dict payloads, or no `cliSessionId`
      are skipped entirely. The parser drops those wrappers at parser.py
      lines ~220 / ~227; the capture adapter must drop the same ones so a
      step-2 Scanner rewire does not regress behavior.
    - Inside a wrapper's session dir, the nested
      `.claude/projects/-sessions-<processName>` directory is preferred,
      with a first-subdirectory fallback when no match exists. Only that
      single nested project dir is tailed, matching the parser's
      behavior (which never walks multiple nested project dirs).
    - `workspace_key` comes from `userSelectedFolders[0]` with a
      `_cowork_<session_id>` fallback.
- Codex (`~/.codex/sessions/**`, `~/.codex/archived_sessions/*.jsonl`),
  grouped by the cwd extracted from each session's metadata.

Other clients (Gemini, OpenCode, OpenClaw, Kimi, Cursor, Copilot, Aider,
Custom) stay on the legacy direct-scan path until migration step 5.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

from clawjournal.parsing import parser

_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class SourceFile:
    path: Path
    client: str
    project_dir_name: str
    size_bytes: int


def iter_source_files(
    *, source_filter: str | None = None
) -> Iterator[SourceFile]:
    normalized = (source_filter or "").strip().lower()
    want_claude = normalized in ("", "auto", "all", "both", parser.CLAUDE_SOURCE)
    want_codex = normalized in ("", "auto", "all", "both", parser.CODEX_SOURCE)

    if want_claude:
        yield from _iter_claude_native_files()
        yield from _iter_local_agent_files()
    if want_codex:
        yield from _iter_codex_files()


# ---------- Claude native ----------


def _iter_claude_native_files() -> Iterator[SourceFile]:
    projects_dir = parser.PROJECTS_DIR
    if not projects_dir.exists():
        return
    for project_dir in sorted(projects_dir.iterdir()):
        if not project_dir.is_dir():
            continue
        for jsonl in sorted(project_dir.glob("*.jsonl")):
            yield _make_source_file(jsonl, parser.CLAUDE_SOURCE, project_dir.name)
        for child in sorted(project_dir.iterdir()):
            if not child.is_dir():
                continue
            subagents = child / "subagents"
            if subagents.is_dir():
                for jsonl in sorted(subagents.glob("agent-*.jsonl")):
                    yield _make_source_file(
                        jsonl, parser.CLAUDE_SOURCE, project_dir.name
                    )


# ---------- Claude Desktop local-agent-mode ----------


def _iter_local_agent_files() -> Iterator[SourceFile]:
    root = parser.LOCAL_AGENT_DIR
    if not root.exists():
        return
    try:
        roots = sorted(root.iterdir())
    except OSError:
        return
    for root_entry in roots:
        if not (root_entry.is_dir() and _UUID_RE.match(root_entry.name)):
            continue
        try:
            workspaces = sorted(root_entry.iterdir())
        except OSError:
            continue
        for workspace_entry in workspaces:
            if not (workspace_entry.is_dir() and _UUID_RE.match(workspace_entry.name)):
                continue
            yield from _iter_workspace_files(workspace_entry)


def _iter_workspace_files(workspace_dir: Path) -> Iterator[SourceFile]:
    try:
        entries = sorted(workspace_dir.iterdir())
    except OSError:
        return
    for wrapper_path in entries:
        if not (
            wrapper_path.is_file()
            and wrapper_path.name.startswith("local_")
            and wrapper_path.name.endswith(".json")
        ):
            continue
        session_dir = wrapper_path.with_suffix("")
        if not session_dir.is_dir():
            continue
        wrapper = _load_local_agent_wrapper(wrapper_path)
        if wrapper is None:
            # Wrapper failed a validity gate (malformed JSON, non-dict,
            # or missing cliSessionId). Parser drops these too — skip
            # the whole workspace session rather than tailing files
            # that will never be indexed downstream.
            continue
        workspace_key = _workspace_key_from_wrapper(wrapper, session_dir)
        yield from _iter_local_agent_session_files(
            session_dir, wrapper, workspace_key
        )


def _load_local_agent_wrapper(wrapper_path: Path) -> dict | None:
    """Mirror the parser's wrapper validity gates (parser.py:220-227).

    Returns None for malformed JSON, non-dict payloads, or missing
    `cliSessionId`. The capture adapter and the parser must skip the
    same wrappers, or step-2 Scanner parity regresses.
    """
    try:
        payload = json.loads(wrapper_path.read_text())
    except (OSError, ValueError):
        return None
    if not isinstance(payload, dict):
        return None
    if not payload.get("cliSessionId"):
        return None
    return payload


def _workspace_key_from_wrapper(wrapper: dict, session_dir: Path) -> str:
    user_folders = wrapper.get("userSelectedFolders") or []
    if (
        isinstance(user_folders, list)
        and user_folders
        and isinstance(user_folders[0], str)
        and user_folders[0]
        and user_folders[0] != "/"
    ):
        return user_folders[0].rstrip("/").replace("/", "-")
    session_id = (
        wrapper.get("sessionId") or wrapper.get("cliSessionId") or session_dir.name
    )
    return f"_cowork_{session_id}"


def _iter_local_agent_session_files(
    session_dir: Path, wrapper: dict, workspace_key: str
) -> Iterator[SourceFile]:
    nested_projects_root = session_dir / ".claude" / "projects"
    nested_project_dir = _pick_nested_project_dir(nested_projects_root, wrapper)
    if nested_project_dir is not None:
        for jsonl in sorted(nested_project_dir.glob("*.jsonl")):
            yield _make_source_file(jsonl, parser.CLAUDE_SOURCE, workspace_key)
        for child in sorted(nested_project_dir.iterdir()):
            if not child.is_dir():
                continue
            subagents = child / "subagents"
            if subagents.is_dir():
                for jsonl in sorted(subagents.glob("agent-*.jsonl")):
                    yield _make_source_file(
                        jsonl, parser.CLAUDE_SOURCE, workspace_key
                    )
    audit = session_dir / "audit.jsonl"
    if audit.is_file():
        yield _make_source_file(audit, parser.CLAUDE_SOURCE, workspace_key)


def _pick_nested_project_dir(
    nested_projects_root: Path, wrapper: dict
) -> Path | None:
    """Mirror parser.py:247-253 — prefer `-sessions-<processName>`, else
    fall back to a single subdirectory. The parser uses unsorted iterdir
    for the fallback and picks the first match, which is OS-dependent; we
    sort so capture is reproducible. The only observable difference is
    in workspaces with multiple nested project dirs AND no match for the
    wrapper's processName — already an irregular state.
    """
    if not nested_projects_root.is_dir():
        return None
    process_name = wrapper.get("processName", "") or ""
    safe_process_name = process_name.replace("/", "-")
    candidate = nested_projects_root / f"-sessions-{safe_process_name}"
    if candidate.is_dir():
        return candidate
    for d in sorted(nested_projects_root.iterdir()):
        if d.is_dir():
            return d
    return None


# ---------- Codex ----------


def _iter_codex_files() -> Iterator[SourceFile]:
    seen: set[Path] = set()
    if parser.CODEX_SESSIONS_DIR.exists():
        for path in sorted(parser.CODEX_SESSIONS_DIR.rglob("*.jsonl")):
            if path in seen:
                continue
            seen.add(path)
            yield _make_source_file(path, parser.CODEX_SOURCE, _codex_cwd(path))
    if parser.CODEX_ARCHIVED_DIR.exists():
        for path in sorted(parser.CODEX_ARCHIVED_DIR.glob("*.jsonl")):
            if path in seen:
                continue
            seen.add(path)
            yield _make_source_file(path, parser.CODEX_SOURCE, _codex_cwd(path))


def _codex_cwd(session_file: Path) -> str:
    cwd = parser._extract_codex_cwd(session_file)
    return cwd or parser.UNKNOWN_CODEX_CWD


# ---------- shared ----------


def _make_source_file(path: Path, client: str, project_dir_name: str) -> SourceFile:
    try:
        size = path.stat().st_size
    except FileNotFoundError:
        size = 0
    return SourceFile(
        path=path,
        client=client,
        project_dir_name=project_dir_name,
        size_bytes=size,
    )
