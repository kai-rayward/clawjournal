"""Render-layer tests: anonymization, snippet redaction, JSON shape."""

from __future__ import annotations

import json

from clawjournal.events.search import SearchSpec, render_human, render_json
from clawjournal.events.search.query import SearchHit, SearchResult


def _result(*, snippet="ok", session_key="claude:proj:s1", source_path="/x"):
    hit = SearchHit(
        event_id=1,
        session_key=session_key,
        event_at="2026-04-21T10:00:00Z",
        client="claude",
        type="tool_result",
        confidence="high",
        source="claude-jsonl",
        source_path=source_path,
        source_offset=0,
        seq=0,
        snippet=snippet,
        bm25=1.5,
    )
    return SearchResult(
        spec=SearchSpec(query="ok"),
        hits=[hit],
        rewritten_match="ok",
        rows_matched=1,
        elapsed_ms=3,
    )


def test_json_shape_pins_schema_version_and_meta():
    payload = json.loads(render_json(_result(), request_id="req-1"))
    assert payload["events_search_schema_version"] == "1.0"
    assert payload["query"] == "ok"
    assert payload["_meta"]["rows_matched"] == 1
    assert payload["_meta"]["rows_returned"] == 1
    assert payload["_meta"]["request_id"] == "req-1"
    assert payload["_meta"]["include_held"] is False


def test_path_in_source_path_is_anonymized(monkeypatch):
    """Anonymizer's path matching is rooted at the current user's
    ``$HOME`` (detected via ``os.path.expanduser('~')``), so the test
    sets HOME to match the literal path used in the fixture — same
    pattern plan 10's render tests use."""

    monkeypatch.setenv("HOME", "/Users/synthetic-user")
    payload = json.loads(
        render_json(_result(source_path="/Users/synthetic-user/proj/file.jsonl"))
    )
    src = payload["hits"][0]["raw_ref"]["source_path"]
    assert "synthetic-user" not in src
    assert src == "[REDACTED_PATH]"


def test_session_key_with_embedded_path_is_anonymized(monkeypatch):
    monkeypatch.setenv("HOME", "/Users/synthetic-user")
    payload = json.loads(
        render_json(_result(session_key="codex:/Users/synthetic-user/workspace"))
    )
    sk = payload["hits"][0]["session_key"]
    assert "synthetic-user" not in sk
    assert sk.startswith("codex:")


def test_snippet_redacts_secrets():
    """Plan 11 §Security #4: a snippet containing a secret-shaped
    token must be redacted before emission. We use an OpenAI-shaped
    key (sk-...) since the regex catches that with high confidence."""

    secret_like = "sk-" + "A" * 48
    payload = json.loads(
        render_json(_result(snippet=f"prefix {secret_like} suffix"))
    )
    snippet = payload["hits"][0]["snippet"]
    assert secret_like not in snippet
    assert "[" in snippet  # placeholder marker landed


def test_human_render_includes_redacted_snippet():
    secret_like = "sk-" + "B" * 48
    text = render_human(_result(snippet=f"prefix {secret_like} suffix"))
    assert secret_like not in text


def test_human_render_no_hits_message():
    result = SearchResult(
        spec=SearchSpec(query="missing"),
        hits=[],
        rewritten_match="missing",
        rows_matched=0,
        elapsed_ms=2,
    )
    text = render_human(result)
    assert "no matches" in text


def test_timeline_url_includes_anonymized_session_key(monkeypatch):
    monkeypatch.setenv("HOME", "/Users/synthetic-user")
    payload = json.loads(
        render_json(_result(session_key="codex:/Users/synthetic-user/proj"))
    )
    url = payload["hits"][0]["timeline_url"]
    assert "synthetic-user" not in url
    assert "#event-1" in url
