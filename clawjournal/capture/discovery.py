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
      preferred, with a first-subdirectory fallback when no match exists.
      The fallback mirrors the parser's raw `iterdir()` order exactly,
      even though that order is OS/filesystem dependent. Only one nested
      project dir is tailed per wrapper.
    - Only `{chosen_nested_project_dir}/{cliSessionId}.jsonl` is yielded
      per wrapper — the single transcript file the current parser reads.
    - Semantic dedupe against native Claude sessions is intentionally
      deferred to downstream consumers. The current parser suppresses a
      local-agent duplicate only after a native transcript actually
      parses successfully; discovery cannot know that from filenames
      alone without risking fallback loss.
    - Duplicate local-agent wrappers with the same `cliSessionId` are
      likewise surfaced individually. The parser handles those with
      session-level dedupe and fallback; capture stays at the physical
      source-file layer.
    - Workspaces without a nested project dir surface nothing
      (parser.py:415 filters those out of discovery).
    - `workspace_key` comes from `userSelectedFolders[0]` with a
      `_cowork_<session_id>` fallback.

- Codex (`~/.codex/sessions/**`, `~/.codex/archived_sessions/*.jsonl`),
  grouped by the cwd extracted from each session's metadata.

- OpenClaw (`~/.openclaw/agents/*/sessions/*.jsonl`), grouped by the cwd
  read from each file's first-line `type: "session"` header (mirrors
  `parser._build_openclaw_project_index` / `_extract_openclaw_cwd`).
  Unparseable or header-less files fall back to `UNKNOWN_OPENCLAW_CWD`
  so step-2 Scanner preserves the `openclaw:<cwd-name>` project group.

Broader coverage (audit files, local-agent subagents, other clients) is
deferred to migration step 5, when the legacy parser discovery is folded
into the capture adapter.

`iter_source_files()` stays at the physical-file layer because cursoring
and line-level streaming are file-based. Parser-facing consumers should
use `iter_parse_inputs()` to recover the current parser's logical units:
native subagent-only sessions collapse to their enclosing session dir,
and native/local-agent duplicates share a `session_key` with explicit
priority ordering instead of forcing each consumer to rediscover Claude
precedence rules. When callers pass only a changed-file subset,
`iter_parse_inputs()` rehydrates the full discovered Claude session
family for those `session_key`s before grouping so native-first fallback
and subagent-session merging still match the current parser.
"""

from __future__ import annotations

import json
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator

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
    session_key: str
    parse_path: Path
    parse_kind: str
    parse_priority: int


@dataclass(frozen=True)
class ParseInput:
    session_key: str
    client: str
    project_dir_name: str
    parse_path: Path
    parse_kind: str
    priority: int
    source_paths: tuple[Path, ...]
    size_bytes: int


def iter_source_files(
    *, source_filter: str | None = None
) -> Iterator[SourceFile]:
    normalized = (source_filter or "").strip().lower()
    want_claude = normalized in ("", "auto", "all", "both", parser.CLAUDE_SOURCE)
    want_codex = normalized in ("", "auto", "all", "both", parser.CODEX_SOURCE)
    want_openclaw = normalized in ("", "auto", "all", "both", parser.OPENCLAW_SOURCE)

    if want_claude:
        yield from _iter_claude_native_files()
        yield from _iter_local_agent_files()
    if want_codex:
        yield from _iter_codex_files()
    if want_openclaw:
        yield from _iter_openclaw_files()


def iter_parse_inputs(
    *,
    source_filter: str | None = None,
    source_files: Iterable[SourceFile] | None = None,
) -> Iterator[ParseInput]:
    """Coalesce physical source files into parser-facing logical inputs.

    The current parser consumes some Claude sources at a coarser grain than
    individual files:
    - native subagent-only sessions parse as one session-dir merge,
    - native/local-agent duplicates are a single logical session with
      precedence (native first, then local-agent fallback),
    - duplicate local-agent wrappers are multiple fallback candidates for
      the same logical session.

    `iter_parse_inputs()` preserves the physical file layer for cursoring,
    but emits parser-ready candidates ordered by session-family discovery
    order, with candidates for the same session kept consecutive and
    ordered by priority / first-seen position. If callers provide only a
    changed-file subset, Claude `session_key`s are first expanded back
    to the full discovered family so native/local fallback precedence
    and subagent-session merging are still preserved. Downstream
    consumers can group consecutive entries by `session_key` and try
    candidates in order until one parses.
    """

    files = list(source_files) if source_files is not None else list(
        iter_source_files(source_filter=source_filter)
    )
    files = _expand_claude_parse_families(files)
    groups: dict[tuple[str, str, str, Path, str, int], list[SourceFile]] = defaultdict(list)
    for source in files:
        groups[
            (
                source.session_key,
                source.client,
                source.project_dir_name,
                source.parse_path,
                source.parse_kind,
                source.parse_priority,
            )
        ].append(source)

    session_first_seen: dict[str, int] = {}
    first_seen: dict[tuple[str, str, str, Path, str, int], int] = {}
    for index, source in enumerate(files):
        session_first_seen.setdefault(source.session_key, index)
        key = (
            source.session_key,
            source.client,
            source.project_dir_name,
            source.parse_path,
            source.parse_kind,
            source.parse_priority,
        )
        first_seen.setdefault(key, index)

    for key in sorted(
        groups,
        key=lambda item: (session_first_seen[item[0]], item[5], first_seen[item]),
    ):
        members = groups[key]
        yield ParseInput(
            session_key=key[0],
            client=key[1],
            project_dir_name=key[2],
            parse_path=key[3],
            parse_kind=key[4],
            priority=key[5],
            source_paths=tuple(member.path for member in members),
            size_bytes=sum(member.size_bytes for member in members),
        )


# ---------- Claude native ----------


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
            yield _make_source_file(
                jsonl,
                parser.CLAUDE_SOURCE,
                project_dir.name,
                session_key=_claude_session_key(project_dir.name, jsonl.stem),
                parse_path=jsonl,
                parse_kind="file",
                parse_priority=0,
            )
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
                        jsonl,
                        parser.CLAUDE_SOURCE,
                        project_dir.name,
                        session_key=_claude_session_key(project_dir.name, child.name),
                        parse_path=child,
                        parse_kind="claude_subagent_session",
                        parse_priority=0,
                    )


# ---------- Claude Desktop local-agent-mode ----------


def _iter_local_agent_files() -> Iterator[SourceFile]:
    root = parser.LOCAL_AGENT_DIR
    if not root.exists():
        return
    try:
        roots = list(root.iterdir())
    except OSError:
        return
    for root_entry in roots:
        if not (root_entry.is_dir() and _UUID_RE.match(root_entry.name)):
            continue
        try:
            workspaces = list(root_entry.iterdir())
        except OSError:
            continue
        for workspace_entry in workspaces:
            if not (workspace_entry.is_dir() and _UUID_RE.match(workspace_entry.name)):
                continue
            yield from _iter_workspace_files(workspace_entry)


def _iter_workspace_files(
    workspace_dir: Path,
) -> Iterator[SourceFile]:
    try:
        entries = list(workspace_dir.iterdir())
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
        yield _make_source_file(
            transcript,
            parser.CLAUDE_SOURCE,
            workspace_key,
            session_key=_claude_session_key(workspace_key, cli_session_id),
            parse_path=transcript,
            parse_kind="file",
            parse_priority=1,
        )


def _pick_nested_project_dir(
    nested_projects_root: Path, wrapper: dict
) -> Path | None:
    """Mirror parser.py:247-253 exactly — prefer `-sessions-<processName>`,
    else fall back to the first subdirectory yielded by raw `iterdir()`.
    """
    if not nested_projects_root.is_dir():
        return None
    process_name = wrapper.get("processName", "") or ""
    safe_process_name = process_name.replace("/", "-")
    candidate = nested_projects_root / f"-sessions-{safe_process_name}"
    if candidate.is_dir():
        return candidate
    for d in nested_projects_root.iterdir():
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
            yield _make_source_file(
                path,
                parser.CODEX_SOURCE,
                _codex_cwd(path),
                session_key=f"codex:{path}",
                parse_path=path,
                parse_kind="file",
                parse_priority=0,
            )
    if parser.CODEX_ARCHIVED_DIR.exists():
        for path in sorted(parser.CODEX_ARCHIVED_DIR.glob("*.jsonl")):
            if path in seen:
                continue
            seen.add(path)
            yield _make_source_file(
                path,
                parser.CODEX_SOURCE,
                _codex_cwd(path),
                session_key=f"codex:{path}",
                parse_path=path,
                parse_kind="file",
                parse_priority=0,
            )


def _codex_cwd(session_file: Path) -> str:
    cwd = parser._extract_codex_cwd(session_file)
    return cwd or parser.UNKNOWN_CODEX_CWD


# ---------- OpenClaw ----------


def _iter_openclaw_files() -> Iterator[SourceFile]:
    agents_dir = parser.OPENCLAW_AGENTS_DIR
    if not agents_dir.exists():
        return
    try:
        agent_dirs = sorted(agents_dir.iterdir())
    except OSError:
        return
    for agent_dir in agent_dirs:
        sessions_dir = agent_dir / "sessions"
        if not sessions_dir.is_dir():
            continue
        for session_file in sorted(sessions_dir.glob("*.jsonl")):
            cwd = parser._extract_openclaw_cwd(session_file) or parser.UNKNOWN_OPENCLAW_CWD
            yield _make_source_file(
                session_file,
                parser.OPENCLAW_SOURCE,
                cwd,
                session_key=f"openclaw:{session_file}",
                parse_path=session_file,
                parse_kind="file",
                parse_priority=0,
            )


# ---------- shared ----------


def _claude_session_key(project_dir_name: str, session_id: str) -> str:
    return f"claude:{project_dir_name}:{session_id}"


def _expand_claude_parse_families(files: list[SourceFile]) -> list[SourceFile]:
    """Expand a changed-file subset back to full Claude session families.

    The current parser resolves Claude precedence at the session level:
    - native root/local-agent duplicates compete within one session key,
    - duplicate local-agent wrappers are alternate candidates,
    - subagent-only sessions merge every `agent-*.jsonl` file in the dir.

    If a caller hands us only changed `SourceFile`s, grouping those files
    directly would lose that context. Re-scan Claude discovery and pull in
    every physical source whose `session_key` matches a changed Claude
    source before we coalesce into parser-facing `ParseInput`s.
    """
    claude_keys = {
        source.session_key
        for source in files
        if source.client == parser.CLAUDE_SOURCE
    }
    if not claude_keys:
        return files

    discovered_by_session: dict[str, list[SourceFile]] = defaultdict(list)
    for source in iter_source_files(source_filter=parser.CLAUDE_SOURCE):
        if source.session_key in claude_keys:
            discovered_by_session[source.session_key].append(source)

    expanded: list[SourceFile] = []
    seen_paths: set[Path] = set()
    emitted_sessions: set[str] = set()

    for source in files:
        if source.client != parser.CLAUDE_SOURCE:
            if source.path not in seen_paths:
                expanded.append(source)
                seen_paths.add(source.path)
            continue

        if source.session_key in emitted_sessions:
            continue
        emitted_sessions.add(source.session_key)

        family = discovered_by_session.get(source.session_key)
        if family:
            for member in family:
                if member.path not in seen_paths:
                    expanded.append(member)
                    seen_paths.add(member.path)
            continue

        if source.path not in seen_paths:
            expanded.append(source)
            seen_paths.add(source.path)
    return expanded


def _make_source_file(
    path: Path,
    client: str,
    project_dir_name: str,
    *,
    session_key: str,
    parse_path: Path,
    parse_kind: str,
    parse_priority: int,
) -> SourceFile:
    try:
        size = path.stat().st_size
    except FileNotFoundError:
        size = 0
    return SourceFile(
        path=path,
        client=client,
        project_dir_name=project_dir_name,
        size_bytes=size,
        session_key=session_key,
        parse_path=parse_path,
        parse_kind=parse_kind,
        parse_priority=parse_priority,
    )
