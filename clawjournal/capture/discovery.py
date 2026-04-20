"""Source file discovery for the capture adapter.

Phase 1 steps 1 and 1b cover:

- Claude Code native projects (`~/.claude/projects/**`). Root `<uuid>.jsonl`
  files are always yielded. Subagent streams under `<uuid>/subagents/` are
  yielded ONLY when there is no sibling `<uuid>.jsonl` (mirrors
  `parser._find_subagent_only_sessions` + the root_stems check at
  parser.py:1088). For rooted sessions the parser consumes the subagents
  as part of the root transcript, not separately, so the adapter must
  skip them to avoid step-2 double-ingestion.

- Claude Desktop local-agent-mode sessions (`LOCAL_AGENT_DIR`), mirroring
  `parser._scan_local_agent_sessions()` plus the transcript selection at
  parser.py:862:
    - Wrappers with malformed JSON, non-dict payloads, or no
      `cliSessionId` are skipped entirely (parity with parser.py:220-227).
    - The nested `.claude/projects/-sessions-<processName>` directory is
      preferred, with a first-subdirectory fallback when no match exists
      (sorted for reproducibility; the parser's unsorted iterdir is
      OS-dependent). Only one nested project dir is tailed per wrapper.
    - Only `{chosen_nested_project_dir}/{cliSessionId}.jsonl` is yielded
      per wrapper — the single transcript file the current parser reads.
    - Wrappers whose `cliSessionId` matches a native session UUID in the
      SAME workspace_key are dropped, mirroring the dedupe at
      parser.py:399-400. Prevents step-2 Scanner from double-ingesting a
      session that exists in both native and local-agent layouts.
    - Workspaces without a nested project dir surface nothing
      (parser.py:415 filters those out of discovery).
    - `workspace_key` comes from `userSelectedFolders[0]` with a
      `_cowork_<session_id>` fallback.

- Codex (`~/.codex/sessions/**`, `~/.codex/archived_sessions/*.jsonl`),
  grouped by the cwd extracted from each session's metadata.

Broader coverage (audit files, local-agent subagents, other clients) is
deferred to migration step 5, when the legacy parser discovery is folded
into the capture adapter.
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
        native_ids_by_project = _compute_native_session_ids()
        yield from _iter_claude_native_files()
        yield from _iter_local_agent_files(native_ids_by_project)
    if want_codex:
        yield from _iter_codex_files()


# ---------- Claude native ----------


def _compute_native_session_ids() -> dict[str, set[str]]:
    """Return {project_dir_name: {session UUIDs with a native transcript}}.

    Mirrors `parser._get_native_session_ids`: a UUID counts as native if it
    appears as a root `<uuid>.jsonl` file OR as a subagent-only session
    directory (a UUID-named dir with subagents but no root jsonl). Used to
    dedupe local-agent wrappers whose `cliSessionId` matches a native
    session in the same workspace (parser.py:399-400).
    """
    projects_dir = parser.PROJECTS_DIR
    result: dict[str, set[str]] = {}
    if not projects_dir.exists():
        return result
    for project_dir in projects_dir.iterdir():
        if not project_dir.is_dir():
            continue
        ids: set[str] = {f.stem for f in project_dir.glob("*.jsonl")}
        for entry in project_dir.iterdir():
            if entry.is_dir() and entry.name not in ids:
                subagents = entry / "subagents"
                if subagents.is_dir() and any(subagents.glob("agent-*.jsonl")):
                    ids.add(entry.name)
        result[project_dir.name] = ids
    return result


def _iter_claude_native_files() -> Iterator[SourceFile]:
    projects_dir = parser.PROJECTS_DIR
    if not projects_dir.exists():
        return
    for project_dir in sorted(projects_dir.iterdir()):
        if not project_dir.is_dir():
            continue
        root_stems: set[str] = set()
        for jsonl in sorted(project_dir.glob("*.jsonl")):
            root_stems.add(jsonl.stem)
            yield _make_source_file(jsonl, parser.CLAUDE_SOURCE, project_dir.name)
        # Subagent-only sessions: only yield for UUID-named dirs with no
        # sibling <uuid>.jsonl. Mirrors parser._find_subagent_only_sessions.
        # For rooted sessions, subagents are consumed as part of the root
        # transcript, so surfacing them here would double-ingest under
        # step-2 parity.
        for child in sorted(project_dir.iterdir()):
            if not child.is_dir() or child.name in root_stems:
                continue
            subagents = child / "subagents"
            if subagents.is_dir():
                for jsonl in sorted(subagents.glob("agent-*.jsonl")):
                    yield _make_source_file(
                        jsonl, parser.CLAUDE_SOURCE, project_dir.name
                    )


# ---------- Claude Desktop local-agent-mode ----------


def _iter_local_agent_files(
    native_ids_by_project: dict[str, set[str]],
) -> Iterator[SourceFile]:
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
            yield from _iter_workspace_files(workspace_entry, native_ids_by_project)


def _iter_workspace_files(
    workspace_dir: Path,
    native_ids_by_project: dict[str, set[str]],
) -> Iterator[SourceFile]:
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
            continue
        workspace_key = _workspace_key_from_wrapper(wrapper, session_dir)
        cli_session_id = wrapper["cliSessionId"]
        # Dedupe: if a native project at this workspace_key already has a
        # session with the same UUID, skip the local-agent transcript to
        # avoid double-ingest. Mirrors parser.py:399-400.
        if cli_session_id in native_ids_by_project.get(workspace_key, ()):
            continue
        yield from _iter_local_agent_transcript(
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


def _iter_local_agent_transcript(
    session_dir: Path, wrapper: dict, workspace_key: str
) -> Iterator[SourceFile]:
    """Yield the single transcript file the parser actually reads for this
    wrapper: `{chosen_nested_project_dir}/{cliSessionId}.jsonl`.

    Deliberately does NOT yield:
    - other `.jsonl` files in the chosen nested project dir (parser
      consumes only the cliSessionId-named one per wrapper at
      parser.py:862),
    - subagent streams under the chosen nested project dir (parser does
      not recurse into them for local-agent mode),
    - per-session `audit.jsonl` at the session-dir root (parser records
      its path for metadata/size accounting only at parser.py:264).

    Wrappers without a nested project dir yield nothing, mirroring
    parser.py:415's `parseable` filter.
    """
    nested_projects_root = session_dir / ".claude" / "projects"
    nested_project_dir = _pick_nested_project_dir(nested_projects_root, wrapper)
    if nested_project_dir is None:
        return
    cli_session_id = wrapper["cliSessionId"]
    transcript = nested_project_dir / f"{cli_session_id}.jsonl"
    if transcript.is_file():
        yield _make_source_file(transcript, parser.CLAUDE_SOURCE, workspace_key)


def _pick_nested_project_dir(
    nested_projects_root: Path, wrapper: dict
) -> Path | None:
    """Mirror parser.py:247-253 — prefer `-sessions-<processName>`, else
    fall back to a single subdirectory. Parser uses unsorted iterdir,
    which is OS-dependent; we sort for reproducibility. Any observable
    difference is limited to workspaces with multiple nested project
    dirs AND no match for the wrapper's processName — already an
    irregular state.
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
