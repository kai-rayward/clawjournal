"""Regression tests for the multi-pass review fixes.

These pin the behavior change for each bug found by the security +
correctness reviewers (and my own pass) so future refactors can't
silently regress.

Tests covered:
- Atomicity: a mid-import failure rolls everything back, including
  overrides written before the failure.
- Tamper detection: bundle whose content was modified after export
  fails the manifest sha256 verify on import.
- token_usage idempotency: re-importing an older bundle does NOT
  overwrite locally-recosted values.
- Snippet key includes source: two paths that anonymize to the same
  redacted form don't overwrite each other.
- Override created_at preserved across re-imports.
- $TMPDIR is accepted as an --out destination on macOS.
- recorder_schema_version major mismatch rejected.
- Snippet key parse rejects malformed shapes.
- Empty session round-trips cleanly.
- Child whose parent isn't in bundle nor local DB lands with
  parent_session_id NULL.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from pathlib import Path

import pytest

from clawjournal.events.export import (
    ExportError,
    ImportError_,
    export_session_bundle,
    import_session_bundle,
)

from ._helpers import (
    PERMISSIVE_CONFIG,
    insert_cost_anomaly,
    insert_event,
    insert_event_session,
    insert_incident,
    insert_workbench_session,
    insert_token_usage,
    make_conn,
)


# --------------------------------------------------------------------------- #
# atomicity
# --------------------------------------------------------------------------- #


def test_import_atomicity_rolls_back_overrides_on_later_failure(
    tmp_path, monkeypatch
):
    """If the import fails AFTER overrides land, those overrides must
    roll back. Pre-fix bug: nested `with conn:` in write_hook_override
    committed mid-import."""
    src = make_conn()
    sid = insert_event_session(src, session_key="claude:atomic:s")
    insert_event(
        src,
        session_id=sid,
        event_type="user_message",
        event_key="msg:1",
        source_path="/tmp/atomic.jsonl",
        source_offset=0,
        seq=0,
        raw_json={"text": "hi"},
    )
    from clawjournal.events.view import write_hook_override

    write_hook_override(
        src,
        session_key="claude:atomic:s",
        event_key="msg:1",
        event_type="user_message",
        source="hook",
        confidence="high",
        lossiness="none",
        event_at=None,
        payload_json=json.dumps({"corrected": True}),
        origin="test",
    )

    monkeypatch.setattr("clawjournal.config.CONFIG_DIR", tmp_path / ".clawjournal")
    summary = export_session_bundle(
        src,
        "claude:atomic:s",
        config=PERMISSIVE_CONFIG,
        allow_no_workbench_row=True,
        skip_global_gates=True,
    )

    dst = make_conn()

    # Inject a failure into the snippet insert step (after overrides land
    # but before commit). The whole import must roll back.
    import clawjournal.events.export.import_ as imp_mod

    real_insert_snippets = imp_mod._insert_snippets

    def _failing_snippets(conn, snippets):
        real_insert_snippets(conn, {})  # do nothing
        raise RuntimeError("simulated failure mid-import")

    monkeypatch.setattr(imp_mod, "_insert_snippets", _failing_snippets)

    with pytest.raises(RuntimeError, match="simulated failure"):
        import_session_bundle(dst, summary.bundle_path)

    overrides_after = dst.execute(
        "SELECT COUNT(*) FROM event_overrides"
    ).fetchone()[0]
    events_after = dst.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    assert overrides_after == 0, (
        f"overrides leaked across rolled-back import: {overrides_after}"
    )
    assert events_after == 0, (
        f"events leaked across rolled-back import: {events_after}"
    )


# --------------------------------------------------------------------------- #
# tamper detection
# --------------------------------------------------------------------------- #


def test_tampered_bundle_fails_sha256_check(tmp_path, monkeypatch):
    src = make_conn()
    sid = insert_event_session(src, session_key="claude:tamper:s")
    insert_event(
        src,
        session_id=sid,
        event_type="user_message",
        source_path="/tmp/x.jsonl",
        raw_json={"text": "original"},
    )

    monkeypatch.setattr("clawjournal.config.CONFIG_DIR", tmp_path / ".clawjournal")
    summary = export_session_bundle(
        src,
        "claude:tamper:s",
        config=PERMISSIVE_CONFIG,
        allow_no_workbench_row=True,
        skip_global_gates=True,
    )

    bundle = json.loads(summary.bundle_path.read_text(encoding="utf-8"))
    bundle["events"][0]["raw_json"] = json.dumps({"text": "TAMPERED"})
    # Do NOT recompute the sha256 — we want the verifier to catch the change.
    summary.bundle_path.write_text(json.dumps(bundle, indent=2), encoding="utf-8")

    dst = make_conn()
    with pytest.raises(ImportError_, match="sha256 mismatch"):
        import_session_bundle(dst, summary.bundle_path)


# --------------------------------------------------------------------------- #
# token_usage idempotency (no OR REPLACE clobber)
# --------------------------------------------------------------------------- #


def test_token_usage_reimport_does_not_clobber_local_recosting(tmp_path, monkeypatch):
    """Re-import of a bundle whose token_usage rows have OLD values
    must not overwrite values the local DB has already updated
    (e.g. a re-cost against a newer pricing table)."""
    src = make_conn()
    sid = insert_event_session(src, session_key="claude:cost:s")
    e1 = insert_event(
        src,
        session_id=sid,
        event_type="user_message",
        source_path="/tmp/cost.jsonl",
        raw_json={"x": 1},
    )
    insert_token_usage(
        src,
        event_id=e1,
        session_id=sid,
        model="old-model",
    )

    monkeypatch.setattr("clawjournal.config.CONFIG_DIR", tmp_path / ".clawjournal")
    summary = export_session_bundle(
        src,
        "claude:cost:s",
        config=PERMISSIVE_CONFIG,
        allow_no_workbench_row=True,
        skip_global_gates=True,
    )

    dst = make_conn()
    import_session_bundle(dst, summary.bundle_path)

    # Locally re-cost: simulate a different pricing run that updates the
    # cost_estimate / pricing_table_version in place.
    dst.execute(
        "UPDATE token_usage SET model = 'locally-recosted', cost_estimate = 9.99, "
        "pricing_table_version = 'local-v2'"
    )
    dst.commit()

    # Re-import. Pre-fix: INSERT OR REPLACE clobbered the local values.
    # Post-fix: INSERT OR IGNORE preserves them.
    import_session_bundle(dst, summary.bundle_path)

    row = dst.execute(
        "SELECT model, cost_estimate, pricing_table_version FROM token_usage"
    ).fetchone()
    assert row["model"] == "locally-recosted", (
        f"local re-cost was clobbered: model={row['model']!r}"
    )
    assert row["cost_estimate"] == 9.99
    assert row["pricing_table_version"] == "local-v2"


# --------------------------------------------------------------------------- #
# snippet key includes source (collision fix)
# --------------------------------------------------------------------------- #


def test_snippet_key_distinguishes_sources(tmp_path, monkeypatch, mock_anonymizer):
    """Two events with different sources but same (path, offset, seq) —
    pre-fix: snippet from one would silently overwrite the other.
    Post-fix: snippet keys include `source` so both survive."""
    src = make_conn()
    sid = insert_event_session(src, session_key="claude:snip:s")

    real1 = tmp_path / "a.jsonl"
    real1.write_text('{"v": "claude"}\n', encoding="utf-8")
    real2 = tmp_path / "b.jsonl"
    real2.write_text('{"v": "codex"}\n', encoding="utf-8")

    insert_event(
        src,
        session_id=sid,
        event_type="user_message",
        source="claude-jsonl",
        source_path=str(real1),
        source_offset=0,
        seq=0,
        raw_json={"x": 1},
    )
    insert_event(
        src,
        session_id=sid,
        event_type="user_message",
        source="codex-rollout",
        source_path=str(real2),
        source_offset=0,
        seq=0,
        raw_json={"x": 2},
    )

    monkeypatch.setattr("clawjournal.config.CONFIG_DIR", tmp_path / ".clawjournal")
    summary = export_session_bundle(
        src,
        "claude:snip:s",
        config=PERMISSIVE_CONFIG,
        allow_no_workbench_row=True,
        skip_global_gates=True,
    )

    bundle = json.loads(summary.bundle_path.read_text(encoding="utf-8"))
    snippets = bundle["source_snippets"]
    # Both snippets should be present, distinguished by source prefix.
    keys = list(snippets.keys())
    assert len(keys) == 2, f"expected 2 distinct snippet keys, got {keys!r}"
    assert any(k.startswith("claude-jsonl:") for k in keys)
    assert any(k.startswith("codex-rollout:") for k in keys)


# --------------------------------------------------------------------------- #
# override created_at preserved
# --------------------------------------------------------------------------- #


def test_override_created_at_preserved_on_import(tmp_path, monkeypatch):
    """Re-importing the same bundle should not mutate
    `event_overrides.created_at` — it should match the bundle's
    recorded value exactly."""
    src = make_conn()
    sid = insert_event_session(src, session_key="claude:ts:s")
    insert_event(
        src,
        session_id=sid,
        event_type="user_message",
        event_key="msg:1",
        source_path="/tmp/ts.jsonl",
        raw_json={"x": 1},
    )
    from clawjournal.events.view import write_hook_override

    write_hook_override(
        src,
        session_key="claude:ts:s",
        event_key="msg:1",
        event_type="user_message",
        source="hook",
        confidence="high",
        lossiness="none",
        event_at=None,
        payload_json=json.dumps({"v": 1}),
        origin="test",
        created_at="2026-01-01T00:00:00Z",
    )

    monkeypatch.setattr("clawjournal.config.CONFIG_DIR", tmp_path / ".clawjournal")
    summary = export_session_bundle(
        src,
        "claude:ts:s",
        config=PERMISSIVE_CONFIG,
        allow_no_workbench_row=True,
        skip_global_gates=True,
    )

    dst = make_conn()
    import_session_bundle(dst, summary.bundle_path)
    first = dst.execute("SELECT created_at FROM event_overrides").fetchone()[0]
    assert first == "2026-01-01T00:00:00Z"

    # Re-import. created_at must NOT advance to the importer's wall-clock.
    import_session_bundle(dst, summary.bundle_path)
    second = dst.execute("SELECT created_at FROM event_overrides").fetchone()[0]
    assert second == first


# --------------------------------------------------------------------------- #
# $TMPDIR accepted as --out
# --------------------------------------------------------------------------- #


def test_explicit_tmpdir_out_path_accepted(monkeypatch):
    """The output-path validator must accept paths under the platform
    tempdir (e.g. /var/folders/.../T/... on macOS), not just /tmp."""
    src = make_conn()
    sid = insert_event_session(src, session_key="claude:tmp:s")
    insert_event(
        src,
        session_id=sid,
        event_type="user_message",
        source_path="/tmp/x.jsonl",
        raw_json={"x": 1},
    )

    sys_tmp = Path(tempfile.gettempdir()).resolve()
    out_path = sys_tmp / "test_explicit_tmpdir_bundle.json"
    try:
        summary = export_session_bundle(
            src,
            "claude:tmp:s",
            output_path=out_path,
            config=PERMISSIVE_CONFIG,
            allow_no_workbench_row=True,
            skip_global_gates=True,
        )
        assert summary.bundle_path == out_path
        assert out_path.exists()
    finally:
        out_path.unlink(missing_ok=True)


# --------------------------------------------------------------------------- #
# recorder_schema_version validation
# --------------------------------------------------------------------------- #


def test_recorder_schema_major_mismatch_rejected(tmp_path, monkeypatch):
    src = make_conn()
    sid = insert_event_session(src, session_key="claude:rsv:s")
    insert_event(
        src,
        session_id=sid,
        event_type="user_message",
        source_path="/tmp/x.jsonl",
        raw_json={"x": 1},
    )

    monkeypatch.setattr("clawjournal.config.CONFIG_DIR", tmp_path / ".clawjournal")
    summary = export_session_bundle(
        src,
        "claude:rsv:s",
        config=PERMISSIVE_CONFIG,
        allow_no_workbench_row=True,
        skip_global_gates=True,
    )

    bundle = json.loads(summary.bundle_path.read_text(encoding="utf-8"))
    bundle["recorder_schema_version"] = "99.0"
    # Recompute sha256 so the tamper check doesn't fire first
    digest_input = {k: v for k, v in bundle.items() if k != "manifest"}
    canonical = json.dumps(
        digest_input, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    bundle["manifest"]["sha256"] = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    summary.bundle_path.write_text(json.dumps(bundle, indent=2), encoding="utf-8")

    dst = make_conn()
    with pytest.raises(ImportError_, match="recorder_schema_version major"):
        import_session_bundle(dst, summary.bundle_path)


def test_malformed_bundle_minor_version_rejected(tmp_path, monkeypatch):
    """A bundle whose minor version doesn't parse as an integer must be
    rejected outright — pre-fix this silently coerced to -1 and accepted
    the bundle."""
    src = make_conn()
    sid = insert_event_session(src, session_key="claude:minor:s")
    insert_event(
        src,
        session_id=sid,
        event_type="user_message",
        source_path="/tmp/x.jsonl",
        raw_json={"x": 1},
    )

    monkeypatch.setattr("clawjournal.config.CONFIG_DIR", tmp_path / ".clawjournal")
    summary = export_session_bundle(
        src,
        "claude:minor:s",
        config=PERMISSIVE_CONFIG,
        allow_no_workbench_row=True,
        skip_global_gates=True,
    )

    bundle = json.loads(summary.bundle_path.read_text(encoding="utf-8"))
    bundle["bundle_schema_version"] = "1.0abc"
    digest_input = {k: v for k, v in bundle.items() if k != "manifest"}
    canonical = json.dumps(
        digest_input, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    bundle["manifest"]["sha256"] = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    summary.bundle_path.write_text(json.dumps(bundle, indent=2), encoding="utf-8")

    dst = make_conn()
    with pytest.raises(ImportError_, match="malformed bundle_schema_version minor"):
        import_session_bundle(dst, summary.bundle_path)


# --------------------------------------------------------------------------- #
# edge cases
# --------------------------------------------------------------------------- #


def test_empty_session_round_trips_cleanly(tmp_path, monkeypatch):
    """A session with zero events should export + import without error."""
    src = make_conn()
    insert_event_session(src, session_key="claude:empty:s")

    monkeypatch.setattr("clawjournal.config.CONFIG_DIR", tmp_path / ".clawjournal")
    summary = export_session_bundle(
        src,
        "claude:empty:s",
        config=PERMISSIVE_CONFIG,
        allow_no_workbench_row=True,
        skip_global_gates=True,
    )
    assert summary.event_count == 0
    assert summary.snippet_count == 0

    dst = make_conn()
    import_summary = import_session_bundle(dst, summary.bundle_path)
    assert import_summary.events_inserted == 0
    assert "claude:empty:s" in import_summary.session_keys


def test_orphan_child_lands_with_null_parent_session_id(tmp_path, monkeypatch):
    """A child whose parent_session_key references a session NOT in
    the bundle and NOT in the local DB inserts with parent_session_id
    NULL (per 02 NULL-and-backfill semantics)."""
    src = make_conn()
    # Insert an "orphaned child" — claims a parent that doesn't exist.
    cur = src.execute(
        "INSERT INTO event_sessions (session_key, parent_session_key, client, status) "
        "VALUES (?, ?, ?, ?)",
        ("claude:orphan:child", "claude:gone:parent", "claude", "active"),
    )
    src.commit()
    cid = int(cur.lastrowid)
    insert_event(
        src,
        session_id=cid,
        event_type="user_message",
        source_path="/tmp/orphan.jsonl",
        raw_json={"x": 1},
    )

    monkeypatch.setattr("clawjournal.config.CONFIG_DIR", tmp_path / ".clawjournal")
    summary = export_session_bundle(
        src,
        "claude:orphan:child",
        config=PERMISSIVE_CONFIG,
        allow_no_workbench_row=True,
        skip_global_gates=True,
    )

    dst = make_conn()
    import_session_bundle(dst, summary.bundle_path)

    row = dst.execute(
        "SELECT parent_session_key, parent_session_id FROM event_sessions "
        "WHERE session_key = 'claude:orphan:child'"
    ).fetchone()
    assert row["parent_session_key"] == "claude:gone:parent"
    assert row["parent_session_id"] is None


def test_malformed_raw_ref_three_tuple_rejected(tmp_path, monkeypatch):
    """Bundles with a 3-element raw_ref (the abandoned legacy shape)
    must be rejected — the importer can't reliably bind cross-references."""
    src = make_conn()
    sid = insert_event_session(src, session_key="claude:bad:s")
    insert_event(
        src,
        session_id=sid,
        event_type="user_message",
        source_path="/tmp/x.jsonl",
        raw_json={"x": 1},
    )
    monkeypatch.setattr("clawjournal.config.CONFIG_DIR", tmp_path / ".clawjournal")
    summary = export_session_bundle(
        src,
        "claude:bad:s",
        config=PERMISSIVE_CONFIG,
        allow_no_workbench_row=True,
        skip_global_gates=True,
    )

    bundle = json.loads(summary.bundle_path.read_text(encoding="utf-8"))
    # Truncate raw_ref to legacy 3-tuple shape
    bundle["events"][0]["raw_ref"] = bundle["events"][0]["raw_ref"][1:]
    digest_input = {k: v for k, v in bundle.items() if k != "manifest"}
    canonical = json.dumps(
        digest_input, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    bundle["manifest"]["sha256"] = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    summary.bundle_path.write_text(json.dumps(bundle, indent=2), encoding="utf-8")

    dst = make_conn()
    with pytest.raises(ImportError_, match="malformed raw_ref"):
        import_session_bundle(dst, summary.bundle_path)


def test_no_snippets_import_restores_local_source_path_for_inspect(
    tmp_path, monkeypatch
):
    """When a no-snippets bundle is imported on the original machine, use
    the local workbench raw_source_path so events inspect can still read
    the vendor JSONL line. The bundle itself continues to carry the
    anonymized raw_ref path."""
    home = tmp_path / "home"
    source_file = home / ".claude" / "projects" / "-repo" / "sess.jsonl"
    source_file.parent.mkdir(parents=True)
    source_file.write_text('{"text": "inspect me"}\n', encoding="utf-8")
    monkeypatch.setattr(
        "clawjournal.redaction.anonymizer._detect_home_dir",
        lambda: (str(home), home.name),
    )

    src = make_conn()
    sid = insert_event_session(src, session_key="claude:-repo:sess")
    insert_workbench_session(
        src,
        session_id="wb-inspect",
        session_key="claude:-repo:sess",
        raw_source_path=str(source_file),
    )
    insert_event(
        src,
        session_id=sid,
        event_type="user_message",
        event_key="msg:1",
        source_path=str(source_file),
        source_offset=0,
        seq=0,
        raw_json={"text": "inspect me"},
    )

    monkeypatch.setattr("clawjournal.config.CONFIG_DIR", tmp_path / ".clawjournal")
    summary = export_session_bundle(
        src,
        "claude:-repo:sess",
        config=PERMISSIVE_CONFIG,
        include_snippets=False,
        skip_global_gates=True,
    )
    bundle = json.loads(summary.bundle_path.read_text(encoding="utf-8"))
    assert "source_snippets" not in bundle
    assert bundle["events"][0]["raw_ref"][1].startswith("[REDACTED_PATH_")

    dst = make_conn()
    insert_workbench_session(
        dst,
        session_id="wb-inspect",
        session_key="claude:-repo:sess",
        raw_source_path=str(source_file),
    )
    import_session_bundle(dst, summary.bundle_path)

    row = dst.execute(
        "SELECT source_path, source_offset FROM events WHERE event_key = 'msg:1'"
    ).fetchone()
    assert row["source_path"] == str(source_file)

    from clawjournal.events.view import fetch_vendor_line

    assert fetch_vendor_line(row["source_path"], row["source_offset"]) == (
        '{"text": "inspect me"}'
    )


def test_rebuild_derived_is_scoped_to_imported_sessions(tmp_path, monkeypatch):
    """--rebuild-derived must refresh only bundle sessions and leave local
    sessions' cost/incident rows alone."""
    src = make_conn()
    imported_sid = insert_event_session(src, session_key="claude:rebuild:imported")
    imported_event = insert_event(
        src,
        session_id=imported_sid,
        event_type="assistant_message",
        event_key="assistant:usage",
        source_path="/tmp/imported.jsonl",
        source_offset=0,
        seq=0,
        raw_json={
            "type": "assistant",
            "message": {
                "model": "claude-sonnet-4",
                "usage": {"input_tokens": 12, "output_tokens": 5},
            },
        },
    )
    insert_token_usage(
        src,
        event_id=imported_event,
        session_id=imported_sid,
        model="bundle-stale-model",
        input=1,
        output=1,
    )
    insert_cost_anomaly(src, session_id=imported_sid, turn_event_id=imported_event)
    insert_incident(
        src,
        session_id=imported_sid,
        first_event_id=imported_event,
        last_event_id=imported_event,
    )

    monkeypatch.setattr("clawjournal.config.CONFIG_DIR", tmp_path / ".clawjournal")
    summary = export_session_bundle(
        src,
        "claude:rebuild:imported",
        config=PERMISSIVE_CONFIG,
        allow_no_workbench_row=True,
        skip_global_gates=True,
    )

    dst = make_conn()
    local_sid = insert_event_session(dst, session_key="claude:local:keep")
    local_event = insert_event(
        dst,
        session_id=local_sid,
        event_type="assistant_message",
        event_key="assistant:local",
        source_path="/tmp/local.jsonl",
        source_offset=0,
        seq=0,
        raw_json={"type": "assistant", "message": {"text": "already processed"}},
    )
    insert_token_usage(
        dst,
        event_id=local_event,
        session_id=local_sid,
        model="locally-recosted",
        input=999,
        output=111,
    )
    insert_cost_anomaly(dst, session_id=local_sid, turn_event_id=local_event)
    insert_incident(
        dst,
        session_id=local_sid,
        first_event_id=local_event,
        last_event_id=local_event,
    )

    import_summary = import_session_bundle(
        dst,
        summary.bundle_path,
        rebuild_derived=True,
    )

    assert import_summary.token_usage_inserted == 1
    assert import_summary.cost_anomalies_inserted == 0
    assert import_summary.incidents_inserted == 0

    local_usage = dst.execute(
        "SELECT model, input, output FROM token_usage WHERE session_id = ?",
        (local_sid,),
    ).fetchone()
    assert dict(local_usage) == {
        "model": "locally-recosted",
        "input": 999,
        "output": 111,
    }
    assert (
        dst.execute(
            "SELECT COUNT(*) FROM cost_anomalies WHERE session_id = ?",
            (local_sid,),
        ).fetchone()[0]
        == 1
    )
    assert (
        dst.execute(
            "SELECT COUNT(*) FROM incidents WHERE session_id = ?",
            (local_sid,),
        ).fetchone()[0]
        == 1
    )

    imported_usage = dst.execute(
        """
        SELECT tu.model, tu.input, tu.output
          FROM token_usage tu
          JOIN event_sessions es ON es.id = tu.session_id
         WHERE es.session_key = 'claude:rebuild:imported'
        """
    ).fetchone()
    assert dict(imported_usage) == {
        "model": "claude-sonnet-4",
        "input": 12,
        "output": 5,
    }
    assert (
        dst.execute(
            """
            SELECT COUNT(*)
              FROM cost_anomalies ca
              JOIN event_sessions es ON es.id = ca.session_id
             WHERE es.session_key = 'claude:rebuild:imported'
            """
        ).fetchone()[0]
        == 0
    )
    assert (
        dst.execute(
            """
            SELECT COUNT(*)
              FROM incidents i
              JOIN event_sessions es ON es.id = i.session_id
             WHERE es.session_key = 'claude:rebuild:imported'
            """
        ).fetchone()[0]
        == 0
    )


@pytest.fixture
def mock_anonymizer(monkeypatch):
    monkeypatch.setattr(
        "clawjournal.redaction.anonymizer._detect_home_dir",
        lambda: ("/Users/testuser", "testuser"),
    )
