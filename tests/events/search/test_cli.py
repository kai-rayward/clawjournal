"""CLI-surface tests for `events search` (plan 11).

Each test invokes the CLI as a subprocess against a fixture HOME so
the developer's real ``~/.clawjournal`` is untouched. The seed step
is itself a subprocess that reconfigures ``CONFIG_DIR`` / ``INDEX_DB``
to point at the fixture HOME, mirroring the plan 10 ``test_cli.py``
pattern so the workbench's own schema setup runs (the workbench
``sessions`` table has columns the search hold-state filter joins
against).
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest


@pytest.fixture
def isolated_home(tmp_path):
    return {
        "HOME": str(tmp_path),
        "PATH": "/usr/bin:/bin",
        "PYTHONPATH": str(Path(__file__).parent.parent.parent.parent),
        "CLAWJOURNAL_SKIP_TRUFFLEHOG": "1",
    }


def _run(args, env):
    return subprocess.run(
        [sys.executable, "-m", "clawjournal.cli", *args],
        capture_output=True,
        text=True,
        env=env,
        timeout=30,
    )


def _seed_db(home: Path, *, with_event: bool = True) -> None:
    """Initialize a fixture index.db with one session and one matching
    event. Runs in a subprocess so module-level config rewrites
    (``CONFIG_DIR`` / ``INDEX_DB``) affect ``open_index`` cleanly."""

    cfg = Path(home) / ".clawjournal"
    cfg.mkdir(parents=True, exist_ok=True)
    insert_event = (
        "conn.execute('INSERT INTO events (session_id, type, event_at, "
        "ingested_at, source, source_path, source_offset, seq, client, "
        "confidence, lossiness, raw_json) VALUES "
        "(1, \\'tool_result\\', \\'2026-04-21T10:00:00Z\\', "
        "\\'2026-04-21T10:00:00Z\\', \\'claude-jsonl\\', \\'/x\\', 0, 0, "
        "\\'claude\\', \\'high\\', \\'none\\', "
        "\\'{\\\"text\\\": \\\"401 Unauthorized: authentication failed\\\"}\\')')\n"
        if with_event else "pass\n"
    )
    seed = Path(home) / "_seed.py"
    seed.write_text(
        "from pathlib import Path\n"
        "import os\n"
        f"home = Path({str(home)!r})\n"
        "os.environ['HOME'] = str(home)\n"
        "import clawjournal.config as cfg\n"
        "cfg.CONFIG_DIR = home / '.clawjournal'\n"
        "cfg.CONFIG_FILE = cfg.CONFIG_DIR / 'config.json'\n"
        "import clawjournal.workbench.index as wb\n"
        "wb.CONFIG_DIR = home / '.clawjournal'\n"
        "wb.INDEX_DB = home / '.clawjournal' / 'index.db'\n"
        "from clawjournal.events.schema import ensure_schema\n"
        "from clawjournal.events.search import ensure_search_schema\n"
        "from clawjournal.workbench.index import open_index\n"
        "conn = open_index()\n"
        "ensure_schema(conn)\n"
        "ensure_search_schema(conn)\n"
        "conn.execute(\"INSERT INTO event_sessions (session_key, client, "
        "started_at, status) VALUES ('claude:proj:s1', 'claude', "
        "'2026-04-21T10:00:00Z', 'ended')\")\n"
        + insert_event +
        "conn.commit()\n"
        "conn.close()\n",
        encoding="utf-8",
    )
    subprocess.run([sys.executable, str(seed)], check=True, timeout=30)


def test_search_finds_seeded_event(isolated_home, tmp_path):
    _seed_db(tmp_path)
    result = _run(["events", "search", "authentication", "--json"], isolated_home)
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["events_search_schema_version"] == "1.0"
    assert len(payload["hits"]) == 1
    assert payload["hits"][0]["client"] == "claude"


def test_missing_query_returns_usage_error(isolated_home, tmp_path):
    _seed_db(tmp_path)
    result = _run(["events", "search", "--json"], isolated_home)
    assert result.returncode == 2, result.stderr
    # Error envelopes go to stderr; successful payloads to stdout.
    payload = json.loads(result.stderr)
    assert payload["error"]["kind"] == "usage_error"


def test_invalid_fts_query_returns_usage_error(isolated_home, tmp_path):
    _seed_db(tmp_path)
    # Unbalanced quote — FTS5 raises a syntax error → usage_error.
    result = _run(["events", "search", '"unbalanced', "--json"], isolated_home)
    assert result.returncode == 2, result.stderr
    payload = json.loads(result.stderr)
    assert payload["error"]["kind"] == "usage_error"


def test_hyphen_bareword_query_maps_to_usage_error(isolated_home, tmp_path):
    """FTS5 parses ``rate-limit`` (unquoted) as a column filter on
    column ``rate`` and raises ``no such column``. Users who typed a
    hyphen-bareword actually want the phrase query ``"rate-limit"``;
    the CLI maps the SQLite error to usage_error code 2 so agents
    see the right exit code rather than the catch-all 9. Documented
    in commands.md."""

    _seed_db(tmp_path)
    result = _run(
        ["events", "search", "rate-limit", "--json"], isolated_home,
    )
    assert result.returncode == 2, result.stderr
    payload = json.loads(result.stderr)
    assert payload["error"]["kind"] == "usage_error"
    assert "no such column" in payload["error"]["message"].lower()


def test_phrase_query_for_hyphen_bareword_works(isolated_home, tmp_path):
    """Counterpart to the bareword test: wrapping the hyphen-token in
    phrase quotes is the documented workaround and must succeed."""

    _seed_db(tmp_path)
    # Update the seeded event so it actually contains rate-limit.
    seed = tmp_path / "_update.py"
    seed.write_text(
        "from pathlib import Path\n"
        "import os\n"
        f"home = Path({str(tmp_path)!r})\n"
        "os.environ['HOME'] = str(home)\n"
        "import clawjournal.config as cfg\n"
        "cfg.CONFIG_DIR = home / '.clawjournal'\n"
        "import clawjournal.workbench.index as wb\n"
        "wb.CONFIG_DIR = home / '.clawjournal'\n"
        "wb.INDEX_DB = home / '.clawjournal' / 'index.db'\n"
        "from clawjournal.workbench.index import open_index\n"
        "conn = open_index()\n"
        "conn.execute(\"UPDATE events SET raw_json = "
        "'{\\\"text\\\": \\\"rate-limit exceeded\\\"}'\")\n"
        "conn.commit()\n"
        "conn.close()\n",
        encoding="utf-8",
    )
    subprocess.run([sys.executable, str(seed)], check=True, timeout=30)

    result = _run(
        ["events", "search", '"rate-limit"', "--json"], isolated_home,
    )
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert len(payload["hits"]) == 1


def test_missing_events_table_maps_to_index_missing(isolated_home, tmp_path):
    """Workbench DB exists (open_index ran) but `events` does not.
    Agents should see exit code 3 with a hint pointing at
    `events ingest`."""

    cfg = tmp_path / ".clawjournal"
    cfg.mkdir(parents=True, exist_ok=True)
    # Bring up the workbench DB without running ensure_schema /
    # ensure_search_schema so neither `events` nor `events_fts` exist.
    seed = tmp_path / "_seed.py"
    seed.write_text(
        "from pathlib import Path\n"
        "import os\n"
        f"home = Path({str(tmp_path)!r})\n"
        "os.environ['HOME'] = str(home)\n"
        "import clawjournal.config as cfg\n"
        "cfg.CONFIG_DIR = home / '.clawjournal'\n"
        "cfg.CONFIG_FILE = cfg.CONFIG_DIR / 'config.json'\n"
        "import clawjournal.workbench.index as wb\n"
        "wb.CONFIG_DIR = home / '.clawjournal'\n"
        "wb.INDEX_DB = home / '.clawjournal' / 'index.db'\n"
        "from clawjournal.workbench.index import open_index\n"
        "conn = open_index()\n"
        "conn.close()\n",
        encoding="utf-8",
    )
    subprocess.run([sys.executable, str(seed)], check=True, timeout=30)

    result = _run(["events", "search", "anything", "--json"], isolated_home)
    assert result.returncode == 3, result.stderr
    payload = json.loads(result.stderr)
    assert payload["error"]["kind"] == "index_missing"


def test_request_id_echoed_in_meta(isolated_home, tmp_path):
    _seed_db(tmp_path)
    result = _run(
        ["events", "search", "authentication", "--json", "--request-id", "req-77"],
        isolated_home,
    )
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["_meta"]["request_id"] == "req-77"


def test_rebuild_index_succeeds(isolated_home, tmp_path):
    _seed_db(tmp_path)
    result = _run(
        ["events", "search", "--rebuild-index", "--json"], isolated_home,
    )
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["events_search_rebuild"] == "ok"


def test_oversize_query_rejected(isolated_home, tmp_path):
    _seed_db(tmp_path)
    big = "a" * 5000
    result = _run(["events", "search", big, "--json"], isolated_home)
    assert result.returncode == 2, result.stderr
    payload = json.loads(result.stderr)
    assert payload["error"]["kind"] == "usage_error"


def test_snippet_tokens_above_64_rejected_at_cli(isolated_home, tmp_path):
    """Round 1: ``--snippet-tokens`` is enforced at the spec layer to
    fail loudly above FTS5's 64-token internal cap, instead of letting
    SQLite silently clamp. The CLI surface must surface the spec
    rejection as ``usage_error`` (exit 2)."""

    _seed_db(tmp_path)
    result = _run(
        ["events", "search", "authentication", "--snippet-tokens", "100", "--json"],
        isolated_home,
    )
    assert result.returncode == 2, result.stderr
    payload = json.loads(result.stderr)
    assert payload["error"]["kind"] == "usage_error"
    assert "snippet-tokens" in payload["error"]["message"]


def test_features_topic_advertises_search(isolated_home):
    """Drift gate: `events features --json` must list events.search
    once 11 ships. The ``features`` field is the list of ids."""

    result = _run(["events", "features"], isolated_home)
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert "events.search" in payload["features"]
