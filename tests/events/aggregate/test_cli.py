"""End-to-end CLI tests for the three aggregation subcommands (plan 10).

Subprocess invocations against a fixture HOME so the tests don't
collide with the developer's real ``~/.clawjournal`` index.
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


def _seed_db(home: Path) -> None:
    """Create a minimal index.db with two sessions and four events."""

    cfg = Path(home) / ".clawjournal"
    cfg.mkdir(parents=True, exist_ok=True)
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
        "from clawjournal.workbench.index import open_index\n"
        "conn = open_index()\n"
        "ensure_schema(conn)\n"
        "conn.execute(\"INSERT INTO event_sessions (session_key, client, started_at, status) VALUES ('claude:proj:s1', 'claude', '2026-04-21T10:00:00Z', 'ended')\")\n"
        "conn.execute(\"INSERT INTO event_sessions (session_key, client, started_at, status) VALUES ('codex:/tmp/proj_b', 'codex', '2026-04-21T10:00:00Z', 'ended')\")\n"
        "for i, (sid, t, c, src) in enumerate([\n"
        "    (1, 'user_message', 'claude', 'claude-jsonl'),\n"
        "    (1, 'tool_call', 'claude', 'claude-jsonl'),\n"
        "    (1, 'tool_call', 'claude', 'claude-jsonl'),\n"
        "    (2, 'user_message', 'codex', 'codex-rollout'),\n"
        "]):\n"
        "    conn.execute('INSERT INTO events (session_id, type, event_at, ingested_at, source, source_path, source_offset, seq, client, confidence, lossiness, raw_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)', (sid, t, '2026-04-21T10:00:00Z', '2026-04-21T10:00:00Z', src, '/x', 0, i, c, 'high', 'none', '{}'))\n"
        "conn.commit()\n"
        "conn.close()\n",
        encoding="utf-8",
    )
    subprocess.run([sys.executable, str(seed)], check=True, timeout=30)


def test_events_aggregate_basic(isolated_home, tmp_path):
    _seed_db(tmp_path)
    result = _run(
        ["events", "aggregate", "--by", "client,type", "--json"], isolated_home
    )
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["events_aggregate_schema_version"] == "1.0"
    assert payload["aggregation"]["total"] == 4
    assert {b["key"]["client"] for b in payload["aggregation"]["buckets"]} == {
        "claude",
        "codex",
    }


def test_events_aggregate_request_id_echoed(isolated_home, tmp_path):
    _seed_db(tmp_path)
    result = _run(
        [
            "events",
            "aggregate",
            "--by",
            "client",
            "--json",
            "--request-id",
            "rq-cli",
        ],
        isolated_home,
    )
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["_meta"]["request_id"] == "rq-cli"


def test_events_aggregate_missing_by_returns_usage_error(isolated_home, tmp_path):
    _seed_db(tmp_path)
    result = _run(
        ["events", "aggregate", "--json"], isolated_home
    )
    assert result.returncode == 2
    payload = json.loads(result.stderr)
    assert payload["error"]["kind"] == "usage_error"


def test_events_aggregate_unknown_dimension(isolated_home, tmp_path):
    _seed_db(tmp_path)
    result = _run(
        ["events", "aggregate", "--by", "raw_json", "--json"],
        isolated_home,
    )
    assert result.returncode == 2
    payload = json.loads(result.stderr)
    assert payload["error"]["kind"] == "usage_error"
    assert "raw_json" in payload["error"]["message"]


def test_events_aggregate_unknown_filter_field(isolated_home, tmp_path):
    _seed_db(tmp_path)
    result = _run(
        [
            "events",
            "aggregate",
            "--by",
            "client",
            "--where",
            "raw_json=secret",
            "--json",
        ],
        isolated_home,
    )
    assert result.returncode == 2
    payload = json.loads(result.stderr)
    assert payload["error"]["kind"] == "usage_error"


def test_incidents_aggregate_works_when_table_exists(isolated_home, tmp_path):
    _seed_db(tmp_path)
    # Add an incidents row.
    seed = Path(tmp_path) / "_incidents_seed.py"
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
        "from clawjournal.events.incidents.schema import ensure_incidents_schema\n"
        "from clawjournal.workbench.index import open_index\n"
        "conn = open_index()\n"
        "ensure_incidents_schema(conn)\n"
        "conn.execute(\"INSERT INTO incidents (session_id, kind, first_event_id, last_event_id, evidence_json, count, confidence, created_at) VALUES (1, 'loop_exact_repeat', 1, 2, '{}', 3, 'high', '2026-04-21T10:00:00Z')\")\n"
        "conn.commit()\n"
        "conn.close()\n",
        encoding="utf-8",
    )
    subprocess.run([sys.executable, str(seed)], check=True, timeout=30)

    result = _run(
        [
            "events",
            "incidents",
            "aggregate",
            "--by",
            "kind",
            "--json",
        ],
        isolated_home,
    )
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["domain"] == "incidents"
    assert payload["aggregation"]["buckets"][0]["key"]["kind"] == "loop_exact_repeat"


def test_canonical_flag_rejected_with_usage_error(isolated_home, tmp_path):
    """Round 1: --canonical is in the parser but the wire-up to
    canonical_events() is deferred to a follow-up. Refuse loudly
    rather than silently no-op (which would give raw-events results
    when the user asked for deduped)."""

    _seed_db(tmp_path)
    result = _run(
        [
            "events",
            "aggregate",
            "--by",
            "client",
            "--canonical",
            "--json",
        ],
        isolated_home,
    )
    assert result.returncode == 2
    payload = json.loads(result.stderr)
    assert payload["error"]["kind"] == "usage_error"
    assert "canonical" in payload["error"]["message"].lower()


def test_cost_three_explicit_dims_with_auto_partition_returns_usage_error(
    isolated_home, tmp_path
):
    """Round 1: when the user maxes the 3-dim cap on a cost query and
    auto-partition would have to drop one, refuse with a usage error
    (code 2) — not the catch-all (code 9). Auto-partition raises
    ValueError from inside ``run``; the CLI must classify that as a
    usage error."""

    _seed_db(tmp_path)
    seed = Path(tmp_path) / "_cost_3dim_seed.py"
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
        "from clawjournal.events.cost.schema import ensure_cost_schema\n"
        "from clawjournal.workbench.index import open_index\n"
        "conn = open_index()\n"
        "ensure_cost_schema(conn)\n"
        "conn.execute(\"INSERT INTO token_usage (event_id, session_id, model, input, data_source, event_at) VALUES (1, 1, 'claude-3-5-sonnet', 100, 'api', '2026-04-21T10:00:00Z')\")\n"
        "conn.commit()\n"
        "conn.close()\n",
        encoding="utf-8",
    )
    subprocess.run([sys.executable, str(seed)], check=True, timeout=30)

    result = _run(
        [
            "events",
            "cost",
            "aggregate",
            "--by",
            "model,session,date",
            "--metric",
            "sum:input_tokens",
            "--json",
        ],
        isolated_home,
    )
    assert result.returncode == 2, result.stderr
    payload = json.loads(result.stderr)
    assert payload["error"]["kind"] == "usage_error"
    assert "data_source" in payload["error"]["message"]


def test_cost_aggregate_auto_partitions(isolated_home, tmp_path):
    _seed_db(tmp_path)
    seed = Path(tmp_path) / "_cost_seed.py"
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
        "from clawjournal.events.cost.schema import ensure_cost_schema\n"
        "from clawjournal.workbench.index import open_index\n"
        "conn = open_index()\n"
        "ensure_cost_schema(conn)\n"
        "conn.execute(\"INSERT INTO token_usage (event_id, session_id, model, input, data_source, event_at) VALUES (1, 1, 'claude-3-5-sonnet', 100, 'api', '2026-04-21T10:00:00Z')\")\n"
        "conn.execute(\"INSERT INTO token_usage (event_id, session_id, model, input, data_source, event_at) VALUES (2, 1, 'claude-3-5-sonnet', 200, 'estimated', '2026-04-21T10:00:00Z')\")\n"
        "conn.commit()\n"
        "conn.close()\n",
        encoding="utf-8",
    )
    subprocess.run([sys.executable, str(seed)], check=True, timeout=30)

    result = _run(
        [
            "events",
            "cost",
            "aggregate",
            "--by",
            "model",
            "--metric",
            "sum:input_tokens",
            "--json",
        ],
        isolated_home,
    )
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    agg = payload["aggregation"]
    assert agg.get("auto_partitioned_by") == "data_source"
    assert "data_source" in agg["by"]
    sums = {
        b["key"]["data_source"]: b["sum_input_tokens"]
        for b in agg["buckets"]
    }
    assert sums == {"api": 100, "estimated": 200}
