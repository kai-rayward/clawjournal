"""Tests for the security-refactor daemon API surface.

Covers bearer-token auth, findings endpoints (GET grouped + ungrouped,
PATCH bulk + global), findings allowlist CRUD, hold-history endpoint,
update_session hold_state extension, and force-rescan endpoints.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from http.client import HTTPConnection
from pathlib import Path
from threading import Thread

import pytest

from clawjournal.findings import (
    hash_entity,
    reset_salt_cache,
    write_findings_to_db,
)
from clawjournal.paths import API_TOKEN_FILENAME
from clawjournal.redaction.secrets import scan_session_for_findings
from clawjournal.workbench.daemon import WorkbenchHandler
from clawjournal.workbench.index import open_index, upsert_sessions


_FAKE_JWT = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"
    ".eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkphbmUifQ"
    ".abcdefghijABCDEFGH0123456789"
)


@pytest.fixture
def index_setup(tmp_path, monkeypatch):
    monkeypatch.setattr("clawjournal.workbench.index.INDEX_DB", tmp_path / "index.db")
    monkeypatch.setattr("clawjournal.workbench.index.BLOBS_DIR", tmp_path / "blobs")
    monkeypatch.setattr("clawjournal.workbench.index.CONFIG_DIR", tmp_path)
    reset_salt_cache()

    # Seed a settled session with a detectable secret so findings rows exist.
    old = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    blob = {
        "session_id": "sess-1",
        "project": "demo",
        "source": "claude",
        "model": "claude-sonnet-4",
        "start_time": old,
        "end_time": old,
        "git_branch": "main",
        "display_title": "t",
        "messages": [
            {"role": "user", "content": f"JWT: {_FAKE_JWT}", "thinking": "",
             "tool_uses": []},
        ],
        "stats": {"user_messages": 1, "assistant_messages": 0, "tool_uses": 0,
                  "input_tokens": 1, "output_tokens": 0},
    }

    conn = open_index()
    try:
        upsert_sessions(conn, [blob])
        raw = scan_session_for_findings(blob)
        write_findings_to_db(conn, "sess-1", raw, revision="v1:seed")
        conn.execute("UPDATE sessions SET findings_revision='v1:seed' WHERE session_id='sess-1'")
        conn.commit()
    finally:
        conn.close()
    yield tmp_path
    reset_salt_cache()


@pytest.fixture
def server(index_setup):
    from http.server import ThreadingHTTPServer
    srv = ThreadingHTTPServer(("127.0.0.1", 0), WorkbenchHandler)
    port = srv.server_address[1]
    thread = Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    yield port
    srv.shutdown()


def _token() -> str:
    # Resolve at call time — the fixture monkeypatches `INDEX_DB` after
    # this module imports, so capturing it at import time gives the
    # user's real `~/.clawjournal` path.
    from clawjournal.workbench.index import INDEX_DB
    return (Path(str(INDEX_DB)).parent / API_TOKEN_FILENAME).read_text().strip()


def _headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {_token()}"}


def _request(port, method, path, *, body=None, headers=None):
    conn = HTTPConnection("127.0.0.1", port, timeout=5)
    hdrs = dict(_headers())
    if headers is not None:
        hdrs.update(headers)
    payload = None
    if body is not None:
        payload = json.dumps(body).encode()
        hdrs["Content-Type"] = "application/json"
    conn.request(method, path, body=payload, headers=hdrs)
    resp = conn.getresponse()
    raw = resp.read().decode()
    ct = resp.getheader("Content-Type", "")
    data = json.loads(raw) if raw and ct.startswith("application/json") else raw
    return resp.status, data


class TestAuth:
    def test_missing_header_returns_401_empty_body(self, server):
        conn = HTTPConnection("127.0.0.1", server, timeout=5)
        conn.request("GET", "/api/sessions/sess-1/findings")
        resp = conn.getresponse()
        assert resp.status == 401
        assert resp.read() == b""

    def test_wrong_token_returns_401(self, server):
        conn = HTTPConnection("127.0.0.1", server, timeout=5)
        conn.request("GET", "/api/sessions/sess-1/findings",
                     headers={"Authorization": "Bearer not-the-token"})
        resp = conn.getresponse()
        assert resp.status == 401

    def test_static_path_bypasses_auth(self, server):
        conn = HTTPConnection("127.0.0.1", server, timeout=5)
        conn.request("GET", "/")
        resp = conn.getresponse()
        # Not 401 — either 200 (placeholder) or another success-ish status.
        assert resp.status != 401

    def test_options_preflight_does_not_require_auth(self, server):
        conn = HTTPConnection("127.0.0.1", server, timeout=5)
        conn.request("OPTIONS", "/api/sessions/sess-1/findings")
        resp = conn.getresponse()
        assert resp.status == 200


class TestFindingsEndpoints:
    def test_list_grouped(self, server):
        status, data = _request(server, "GET", "/api/sessions/sess-1/findings?group_by=entity")
        assert status == 200
        assert "entities" in data
        assert data["total"] >= 1
        entity = data["entities"][0]
        assert "entity_hash" in entity
        assert "sample_preview" in entity
        # Masked preview; the raw match never appears.
        dumped = json.dumps(data)
        assert _FAKE_JWT not in dumped

    def test_list_ungrouped(self, server):
        status, data = _request(server, "GET", "/api/sessions/sess-1/findings")
        assert status == 200
        assert "findings" in data
        assert data["total"] >= 1
        finding = data["findings"][0]
        assert finding["engine"] == "regex_secrets"
        assert "preview" in finding
        assert _FAKE_JWT not in json.dumps(data)

    def test_status_filter(self, server):
        # No findings are ignored yet; filtering should still work syntactically.
        status, data = _request(server, "GET", "/api/sessions/sess-1/findings?status=ignored")
        assert status == 200
        assert data["total"] == 0

    def test_patch_accept(self, server):
        status, listing = _request(server, "GET", "/api/sessions/sess-1/findings")
        fid = listing["findings"][0]["finding_id"]
        status, data = _request(server, "PATCH", "/api/findings",
                                body={"finding_ids": [fid], "status": "accepted"})
        assert status == 200
        assert data["updated"] >= 1
        assert data["allowlisted"] is False

    def test_patch_ignore_with_global_adds_allowlist_row(self, server):
        status, listing = _request(server, "GET", "/api/sessions/sess-1/findings")
        fid = listing["findings"][0]["finding_id"]
        status, data = _request(server, "PATCH", "/api/findings",
                                body={"finding_ids": [fid], "status": "ignored",
                                      "global": True, "reason": "team-wide"})
        assert status == 200
        assert data["allowlisted"] is True
        # Allowlist grew.
        status, entries = _request(server, "GET", "/api/findings/allowlist")
        assert status == 200
        assert entries["total"] >= 1

    def test_patch_rejects_bad_status(self, server):
        status, data = _request(server, "PATCH", "/api/findings",
                                body={"finding_ids": ["x"], "status": "bogus"})
        assert status == 400

    def test_patch_rejects_empty_finding_ids(self, server):
        status, _ = _request(server, "PATCH", "/api/findings",
                             body={"finding_ids": [], "status": "accepted"})
        assert status == 400


class TestFindingsAllowlistEndpoints:
    def test_add_list_remove_roundtrip(self, server):
        status, add = _request(server, "POST", "/api/findings/allowlist",
                               body={"entity_text": "noreply@example.com",
                                     "entity_type": "email",
                                     "entity_label": "bot",
                                     "reason": "policy"})
        assert status == 200
        allowlist_id = add["entry"]["allowlist_id"]
        assert "entity_hash" not in add["entry"]
        # List.
        status, listed = _request(server, "GET", "/api/findings/allowlist")
        assert status == 200
        assert allowlist_id in {e["allowlist_id"] for e in listed["entries"]}
        # Remove.
        status, removed = _request(server, "DELETE",
                                   f"/api/findings/allowlist/{allowlist_id}")
        assert status == 200
        assert removed["removed"] is True

    def test_add_requires_entity_text(self, server):
        status, _ = _request(server, "POST", "/api/findings/allowlist", body={})
        assert status == 400

    def test_remove_unknown_returns_404(self, server):
        status, _ = _request(server, "DELETE", "/api/findings/allowlist/does-not-exist")
        assert status == 404


class TestHoldHistoryEndpoint:
    def test_returns_origin_row(self, server):
        status, data = _request(server, "GET", "/api/sessions/sess-1/hold-history")
        assert status == 200
        # Upserted sessions get an auto-origin history row.
        assert data["total"] >= 1
        assert data["history"][0]["to_state"] == "auto_redacted"
        assert data["history"][0]["changed_by"] == "auto"


class TestUpdateSessionHoldState:
    def test_patch_sets_hold_state_and_writes_history(self, server):
        status, _ = _request(server, "POST", "/api/sessions/sess-1",
                             body={"hold_state": "pending_review", "reason": "r"})
        assert status == 200
        status, data = _request(server, "GET", "/api/sessions/sess-1/hold-history")
        assert status == 200
        assert [h["to_state"] for h in data["history"]] == ["auto_redacted", "pending_review"]
        assert data["history"][-1]["reason"] == "r"
        assert data["history"][-1]["changed_by"] == "user"

    def test_embargo_past_returns_400(self, server):
        status, data = _request(server, "POST", "/api/sessions/sess-1",
                                body={"hold_state": "embargoed",
                                      "embargo_until": "2020-01-01T00:00:00+00:00"})
        assert status == 400
        assert "future" in data["error"]

    def test_invalid_hold_state_returns_400(self, server):
        status, _ = _request(server, "POST", "/api/sessions/sess-1",
                             body={"hold_state": "frozen"})
        assert status == 400


class TestTokenInjection:
    def test_index_html_carries_bearer_token(self, server):
        """The daemon injects `window.__CLAWJOURNAL_API_TOKEN__` into index.html
        so the same-origin frontend bundle can authenticate automatically."""
        conn = HTTPConnection("127.0.0.1", server, timeout=5)
        conn.request("GET", "/")  # SPA root — static file, no auth needed
        resp = conn.getresponse()
        body = resp.read().decode()
        # Placeholder served when frontend isn't built is also HTML, so injection
        # must run against whatever HTML we return.
        if "<html" in body.lower():
            assert "__CLAWJOURNAL_API_TOKEN__" in body
            assert _token() in body


class TestForceRescanEndpoints:
    def test_post_session_scan_rebuilds(self, server):
        # Wipe findings rows; the endpoint should rebuild them.
        from clawjournal.workbench.index import INDEX_DB
        db = sqlite3.connect(str(INDEX_DB))
        db.execute("DELETE FROM findings WHERE session_id='sess-1'")
        db.execute("UPDATE sessions SET findings_revision = NULL WHERE session_id='sess-1'")
        db.commit()
        db.close()

        status, data = _request(server, "POST", "/api/sessions/sess-1/scan")
        assert status == 200
        assert data["status"] == "rebuilt"
        assert data["count"] >= 1

    def test_post_session_scan_missing_blob_404(self, server):
        status, _ = _request(server, "POST", "/api/sessions/does-not-exist/scan")
        assert status == 404
