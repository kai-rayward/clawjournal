import json

import pytest

from clawjournal.capture import discovery
from clawjournal.parsing import parser


@pytest.fixture
def isolated_homedir(tmp_path, monkeypatch):
    """Monkeypatch every parser path the capture adapter looks at so a test
    exercising Claude doesn't pick up the developer's real Codex history.
    Tests populate whichever subdirectory they care about."""
    monkeypatch.setattr(parser, "PROJECTS_DIR", tmp_path / "claude" / "projects")
    monkeypatch.setattr(parser, "CODEX_SESSIONS_DIR", tmp_path / "codex" / "sessions")
    monkeypatch.setattr(
        parser, "CODEX_ARCHIVED_DIR", tmp_path / "codex" / "archived_sessions"
    )
    monkeypatch.setattr(parser, "LOCAL_AGENT_DIR", tmp_path / "local_agent")
    return tmp_path


def _write_codex_session(path, cwd):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"type": "session_meta", "payload": {"cwd": cwd}}) + "\n"
    )


def _write_local_agent_wrapper(
    workspace_dir,
    name,
    *,
    cli_session_id,
    session_id,
    process_name,
    user_folder=None,
):
    wrapper = workspace_dir / f"local_{name}.json"
    payload = {
        "cliSessionId": cli_session_id,
        "sessionId": session_id,
        "processName": process_name,
    }
    if user_folder is not None:
        payload["userSelectedFolders"] = [user_folder]
    wrapper.write_text(json.dumps(payload))
    session_dir = wrapper.with_suffix("")
    session_dir.mkdir()
    return wrapper, session_dir


# ---------- Claude native ----------


def test_claude_native_discovery_yields_sessions_and_subagents(isolated_homedir):
    proj = isolated_homedir / "claude" / "projects" / "myproject"
    proj.mkdir(parents=True)
    (proj / "session-a.jsonl").write_text("{}\n")
    (proj / "session-b.jsonl").write_text("{}\n")
    subagents = proj / "session-a" / "subagents"
    subagents.mkdir(parents=True)
    (subagents / "agent-1.jsonl").write_text("{}\n")

    files = list(discovery.iter_source_files(source_filter="claude"))
    rel = sorted(str(f.path.relative_to(isolated_homedir)) for f in files)
    assert rel == [
        "claude/projects/myproject/session-a.jsonl",
        "claude/projects/myproject/session-a/subagents/agent-1.jsonl",
        "claude/projects/myproject/session-b.jsonl",
    ]
    assert all(f.client == "claude" for f in files)
    assert all(f.project_dir_name == "myproject" for f in files)


# ---------- Claude Desktop local-agent (step 1b) ----------


def test_local_agent_discovery_uses_user_folder_workspace_key(isolated_homedir):
    root_uuid = isolated_homedir / "local_agent" / "11111111-2222-3333-4444-555555555555"
    workspace_uuid = root_uuid / "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    workspace_uuid.mkdir(parents=True)

    _, session_dir = _write_local_agent_wrapper(
        workspace_uuid,
        "abc",
        cli_session_id="cli-1",
        session_id="sess-1",
        process_name="myproc",
        user_folder="/Users/me/ws-one",
    )
    projects_dir = session_dir / ".claude" / "projects" / "-sessions-myproc"
    projects_dir.mkdir(parents=True)
    (projects_dir / "inside.jsonl").write_text("{}\n")
    subagents = projects_dir / "inside" / "subagents"
    subagents.mkdir(parents=True)
    (subagents / "agent-9.jsonl").write_text("{}\n")
    (session_dir / "audit.jsonl").write_text("{}\n")

    files = list(discovery.iter_source_files(source_filter="claude"))
    names = sorted(f.path.name for f in files)
    assert names == ["agent-9.jsonl", "audit.jsonl", "inside.jsonl"]
    assert {f.project_dir_name for f in files} == {"-Users-me-ws-one"}
    assert all(f.client == "claude" for f in files)


def test_local_agent_discovery_falls_back_to_cowork_key_without_user_folder(
    isolated_homedir,
):
    root_uuid = isolated_homedir / "local_agent" / "11111111-2222-3333-4444-555555555555"
    workspace_uuid = root_uuid / "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    workspace_uuid.mkdir(parents=True)

    _, session_dir = _write_local_agent_wrapper(
        workspace_uuid,
        "def",
        cli_session_id="cli-2",
        session_id="sess-2",
        process_name="otherproc",
    )
    projects_dir = session_dir / ".claude" / "projects" / "-sessions-otherproc"
    projects_dir.mkdir(parents=True)
    (projects_dir / "inside2.jsonl").write_text("{}\n")

    files = list(discovery.iter_source_files(source_filter="claude"))
    assert len(files) == 1
    assert files[0].project_dir_name == "_cowork_sess-2"


def test_local_agent_skips_non_uuid_directories(isolated_homedir):
    root = isolated_homedir / "local_agent"
    root.mkdir()
    (root / "not-a-uuid").mkdir()
    (root / "not-a-uuid" / "stray.json").write_text("{}")

    files = list(discovery.iter_source_files(source_filter="claude"))
    assert files == []


def test_local_agent_missing_directory_is_a_no_op(isolated_homedir):
    # local_agent dir never created
    files = list(discovery.iter_source_files(source_filter="claude"))
    assert files == []


# ---------- wrapper validity gates (mirror parser.py:220-227) ----------


def _make_workspace_dirs(isolated_homedir):
    root_uuid = isolated_homedir / "local_agent" / "11111111-2222-3333-4444-555555555555"
    workspace_uuid = root_uuid / "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    workspace_uuid.mkdir(parents=True)
    return workspace_uuid


def test_local_agent_skips_wrapper_with_malformed_json(isolated_homedir):
    workspace_uuid = _make_workspace_dirs(isolated_homedir)
    wrapper = workspace_uuid / "local_bad.json"
    wrapper.write_text("{not json")
    session_dir = wrapper.with_suffix("")
    session_dir.mkdir()
    proj_dir = session_dir / ".claude" / "projects" / "-sessions-x"
    proj_dir.mkdir(parents=True)
    (proj_dir / "should-not-show.jsonl").write_text("{}\n")
    (session_dir / "audit.jsonl").write_text("{}\n")

    files = list(discovery.iter_source_files(source_filter="claude"))
    assert files == []


def test_local_agent_skips_wrapper_that_is_not_a_dict(isolated_homedir):
    workspace_uuid = _make_workspace_dirs(isolated_homedir)
    wrapper = workspace_uuid / "local_list.json"
    wrapper.write_text(json.dumps([1, 2, 3]))
    session_dir = wrapper.with_suffix("")
    session_dir.mkdir()
    proj_dir = session_dir / ".claude" / "projects" / "-sessions-x"
    proj_dir.mkdir(parents=True)
    (proj_dir / "should-not-show.jsonl").write_text("{}\n")

    files = list(discovery.iter_source_files(source_filter="claude"))
    assert files == []


def test_local_agent_skips_wrapper_without_cli_session_id(isolated_homedir):
    workspace_uuid = _make_workspace_dirs(isolated_homedir)
    wrapper = workspace_uuid / "local_missing.json"
    wrapper.write_text(json.dumps({"sessionId": "sess-3", "processName": "x"}))
    session_dir = wrapper.with_suffix("")
    session_dir.mkdir()
    proj_dir = session_dir / ".claude" / "projects" / "-sessions-x"
    proj_dir.mkdir(parents=True)
    (proj_dir / "should-not-show.jsonl").write_text("{}\n")

    files = list(discovery.iter_source_files(source_filter="claude"))
    assert files == []


# ---------- nested project dir selection (mirror parser.py:247-253) ----------


def test_local_agent_prefers_sessions_processname_dir_over_others(isolated_homedir):
    workspace_uuid = _make_workspace_dirs(isolated_homedir)
    _, session_dir = _write_local_agent_wrapper(
        workspace_uuid,
        "pref",
        cli_session_id="cli-5",
        session_id="sess-5",
        process_name="myproc",
        user_folder="/Users/me/ws",
    )
    expected = session_dir / ".claude" / "projects" / "-sessions-myproc"
    expected.mkdir(parents=True)
    (expected / "match.jsonl").write_text("{}\n")
    # Extra unrelated dir — must not be tailed when the processName dir exists
    stray = session_dir / ".claude" / "projects" / "stray"
    stray.mkdir(parents=True)
    (stray / "stray.jsonl").write_text("{}\n")

    files = list(discovery.iter_source_files(source_filter="claude"))
    names = sorted(f.path.name for f in files)
    assert names == ["match.jsonl"]


def test_local_agent_falls_back_to_a_single_nested_dir_when_processname_missing(
    isolated_homedir,
):
    workspace_uuid = _make_workspace_dirs(isolated_homedir)
    _, session_dir = _write_local_agent_wrapper(
        workspace_uuid,
        "fb",
        cli_session_id="cli-6",
        session_id="sess-6",
        process_name="unexpected",  # no -sessions-unexpected dir below
        user_folder="/Users/me/ws",
    )
    alt = session_dir / ".claude" / "projects" / "-sessions-fallback"
    alt.mkdir(parents=True)
    (alt / "picked.jsonl").write_text("{}\n")
    extra = session_dir / ".claude" / "projects" / "-sessions-other"
    extra.mkdir(parents=True)
    (extra / "skipped.jsonl").write_text("{}\n")

    files = list(discovery.iter_source_files(source_filter="claude"))
    names = sorted(f.path.name for f in files)
    # Only the alphabetically-first fallback dir is picked (sorted iterdir).
    # Parser's unsorted fallback is OS-dependent; the adapter is deterministic.
    assert names == ["picked.jsonl"]


# ---------- Codex ----------


def test_codex_discovery_uses_extracted_cwd(isolated_homedir):
    sessions = isolated_homedir / "codex" / "sessions" / "2026" / "04" / "19"
    _write_codex_session(sessions / "rollout-a.jsonl", "/Users/me/proj-active")
    archived = isolated_homedir / "codex" / "archived_sessions"
    _write_codex_session(archived / "rollout-old.jsonl", "/Users/me/proj-old")

    files = list(discovery.iter_source_files(source_filter="codex"))
    by_name = {f.path.name: f for f in files}
    assert set(by_name) == {"rollout-a.jsonl", "rollout-old.jsonl"}
    assert by_name["rollout-a.jsonl"].project_dir_name == "/Users/me/proj-active"
    assert by_name["rollout-old.jsonl"].project_dir_name == "/Users/me/proj-old"
    assert all(f.client == "codex" for f in files)


def test_codex_discovery_falls_back_to_unknown_cwd_when_missing_metadata(
    isolated_homedir,
):
    archived = isolated_homedir / "codex" / "archived_sessions"
    archived.mkdir(parents=True)
    (archived / "rollout-nometa.jsonl").write_text(
        json.dumps({"type": "turn_start"}) + "\n"
    )

    files = list(discovery.iter_source_files(source_filter="codex"))
    assert len(files) == 1
    assert files[0].project_dir_name == parser.UNKNOWN_CODEX_CWD


# ---------- auto and unknown filters ----------


def test_auto_filter_yields_all_supported_clients(isolated_homedir):
    native_proj = isolated_homedir / "claude" / "projects" / "p1"
    native_proj.mkdir(parents=True)
    (native_proj / "s.jsonl").write_text("{}\n")
    codex = isolated_homedir / "codex" / "sessions"
    _write_codex_session(codex / "r.jsonl", "/cwd")

    files = list(discovery.iter_source_files())
    clients = {f.client for f in files}
    assert clients == {"claude", "codex"}


def test_unknown_source_returns_nothing(isolated_homedir):
    files = list(discovery.iter_source_files(source_filter="gemini"))
    assert files == []


def test_size_bytes_matches_file_size(isolated_homedir):
    proj = isolated_homedir / "claude" / "projects" / "p"
    proj.mkdir(parents=True)
    (proj / "s.jsonl").write_text("{}\n")
    files = list(discovery.iter_source_files(source_filter="claude"))
    assert all(f.size_bytes == 3 for f in files)
