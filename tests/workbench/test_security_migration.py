"""Tests for the security-refactor schema migration and install-file bootstrap."""

import os
import sqlite3
import threading
from pathlib import Path

import pytest

from clawjournal.paths import (
    API_TOKEN_FILENAME,
    HASH_SALT_BYTES,
    HASH_SALT_FILENAME,
    ensure_hash_salt,
    ensure_install_files,
)
from clawjournal.workbench.index import (
    BACKFILL_WINDOW,
    SECURITY_SCHEMA_VERSION,
    open_index,
    upsert_sessions,
)


@pytest.fixture
def fresh_db(tmp_path, monkeypatch):
    db_path = tmp_path / "index.db"
    blobs_path = tmp_path / "blobs"
    monkeypatch.setattr("clawjournal.workbench.index.INDEX_DB", db_path)
    monkeypatch.setattr("clawjournal.workbench.index.BLOBS_DIR", blobs_path)
    return db_path


def _columns(conn, table):
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _indexes(conn, table):
    return {row[1] for row in conn.execute(f"PRAGMA index_list({table})").fetchall()}


def _make_session(session_id, end_time):
    return {
        "session_id": session_id,
        "project": "p",
        "source": "claude",
        "model": "claude-sonnet-4",
        "start_time": "2025-01-01T00:00:00+00:00",
        "end_time": end_time,
        "git_branch": "main",
        "messages": [{"role": "user", "content": "hi", "tool_uses": []}],
        "stats": {"user_messages": 1, "assistant_messages": 0, "tool_uses": 0,
                  "input_tokens": 1, "output_tokens": 0},
    }


class TestFreshInstall:
    def test_creates_findings_tables(self, fresh_db):
        conn = open_index()
        try:
            names = {row[0] for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()}
            assert {"findings", "findings_allowlist", "session_hold_history"} <= names
        finally:
            conn.close()

    def test_adds_sessions_security_columns(self, fresh_db):
        conn = open_index()
        try:
            cols = _columns(conn, "sessions")
            assert {
                "hold_state", "embargo_until",
                "findings_revision", "findings_backfill_needed",
            } <= cols
        finally:
            conn.close()

    def test_advances_schema_version(self, fresh_db):
        conn = open_index()
        try:
            version = conn.execute("PRAGMA user_version").fetchone()[0]
            assert version == SECURITY_SCHEMA_VERSION
        finally:
            conn.close()

    def test_fresh_install_does_not_flag_backfill(self, fresh_db):
        conn = open_index()
        try:
            count = conn.execute(
                "SELECT COUNT(*) FROM sessions WHERE findings_backfill_needed = 1"
            ).fetchone()[0]
            assert count == 0
        finally:
            conn.close()


class TestUpsertHoldState:
    def test_new_session_default_hold_state(self, fresh_db):
        conn = open_index()
        try:
            upsert_sessions(conn, [_make_session("s1", "2025-01-01T00:10:00+00:00")])
            row = conn.execute(
                "SELECT hold_state FROM sessions WHERE session_id='s1'"
            ).fetchone()
            assert row["hold_state"] == "auto_redacted"
        finally:
            conn.close()

    def test_new_session_writes_origin_history(self, fresh_db):
        conn = open_index()
        try:
            upsert_sessions(conn, [_make_session("s1", "2025-01-01T00:10:00+00:00")])
            rows = conn.execute(
                "SELECT from_state, to_state, changed_by, reason "
                "FROM session_hold_history WHERE session_id='s1'"
            ).fetchall()
            assert len(rows) == 1
            assert rows[0]["from_state"] is None
            assert rows[0]["to_state"] == "auto_redacted"
            assert rows[0]["changed_by"] == "auto"
        finally:
            conn.close()

    def test_reindex_preserves_hold_state_and_history(self, fresh_db):
        conn = open_index()
        try:
            sess = _make_session("s1", "2025-01-01T00:10:00+00:00")
            upsert_sessions(conn, [sess])
            # Simulate a user release + a stored findings revision.
            conn.execute(
                "UPDATE sessions SET hold_state='released', findings_revision='v1:abc' "
                "WHERE session_id='s1'"
            )
            conn.execute(
                "INSERT INTO session_hold_history "
                "(history_id, session_id, from_state, to_state, embargo_until, "
                " changed_by, changed_at, reason) "
                "VALUES ('h-user', 's1', 'auto_redacted', 'released', NULL, "
                " 'user', '2099-01-01T01:00:00+00:00', 'ready')"
            )
            conn.commit()

            # Re-index the same session.
            upsert_sessions(conn, [sess])

            row = conn.execute(
                "SELECT hold_state, findings_revision FROM sessions WHERE session_id='s1'"
            ).fetchone()
            assert row["hold_state"] == "released"
            assert row["findings_revision"] == "v1:abc"

            history = conn.execute(
                "SELECT history_id, changed_by FROM session_hold_history "
                "WHERE session_id='s1' ORDER BY changed_at"
            ).fetchall()
            # Origin 'auto' row + the 'user' transition we inserted. No duplicates.
            assert [h["changed_by"] for h in history] == ["auto", "user"]
        finally:
            conn.close()

    def test_reindex_preserves_findings_rows(self, fresh_db):
        """Findings attached to a session survive a re-index (no CASCADE fires)."""
        conn = open_index()
        try:
            upsert_sessions(conn, [_make_session("s1", "2025-01-01T00:10:00+00:00")])
            conn.execute(
                "INSERT INTO findings "
                "(finding_id, session_id, engine, rule, entity_type, "
                " entity_hash, entity_length, field, offset, length, "
                " revision, created_at) "
                "VALUES ('f1', 's1', 'regex_secrets', 'jwt', 'jwt', "
                " 'deadbeef', 10, 'content', 0, 10, 'v1:abc', '2025-01-01T00:00:00+00:00')"
            )
            conn.commit()
            upsert_sessions(conn, [_make_session("s1", "2025-01-01T00:10:00+00:00")])
            count = conn.execute(
                "SELECT COUNT(*) FROM findings WHERE session_id='s1'"
            ).fetchone()[0]
            assert count == 1
        finally:
            conn.close()


class TestLegacyMigration:
    """Exercise the v1 → v2 migration path on a pre-security-refactor DB."""

    def _build_v1_db(self, db_path, session_rows):
        """Create a minimal pre-security-refactor DB at PRAGMA user_version=1.

        Only the columns SCHEMA_SQL's session-level indexes touch
        (project, source, start_time) need to exist — the subsequent
        ALTER-column loop in open_index() adds everything else the
        code paths under test care about.
        """
        conn = sqlite3.connect(str(db_path))
        conn.execute(
            """CREATE TABLE sessions (
                session_id         TEXT PRIMARY KEY,
                project            TEXT NOT NULL,
                source             TEXT NOT NULL,
                start_time         TEXT,
                end_time           TEXT,
                review_status      TEXT DEFAULT 'new',
                indexed_at         TEXT NOT NULL
            )"""
        )
        for row in session_rows:
            conn.execute(
                "INSERT INTO sessions (session_id, project, source, start_time, "
                " end_time, review_status, indexed_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (row[0], row[1], row[2], row[3], row[3], row[4], row[5]),
            )
        conn.execute("PRAGMA user_version = 1")
        conn.commit()
        conn.close()

    def test_backfill_hold_state_and_history(self, tmp_path, monkeypatch):
        db_path = tmp_path / "index.db"
        self._build_v1_db(db_path, [
            ("approved-sess", "p", "claude", "2025-03-01", "approved", "2025-01-01"),
            ("new-sess",      "p", "claude", "2025-02-01", "new",      "2025-01-01"),
        ])
        monkeypatch.setattr("clawjournal.workbench.index.INDEX_DB", db_path)
        monkeypatch.setattr("clawjournal.workbench.index.BLOBS_DIR", tmp_path / "blobs")

        conn = open_index()
        try:
            rows = {r["session_id"]: r["hold_state"] for r in conn.execute(
                "SELECT session_id, hold_state FROM sessions"
            )}
            assert rows["approved-sess"] == "released"
            assert rows["new-sess"] == "auto_redacted"

            history = conn.execute(
                "SELECT session_id, from_state, to_state, changed_by, reason "
                "FROM session_hold_history ORDER BY session_id"
            ).fetchall()
            assert len(history) == 2
            for row in history:
                assert row["from_state"] is None
                assert row["changed_by"] == "migration"
                assert row["reason"] == "schema migration backfill"
            # to_state matches the backfilled hold_state.
            assert {r["session_id"]: r["to_state"] for r in history} == rows
        finally:
            conn.close()

    def test_backfill_flags_recent_window(self, tmp_path, monkeypatch):
        db_path = tmp_path / "index.db"
        rows = [
            (f"s{i:04d}", "p", "claude", f"2025-{(i % 12) + 1:02d}-01", "new", "2025-01-01")
            for i in range(BACKFILL_WINDOW + 25)
        ]
        self._build_v1_db(db_path, rows)
        monkeypatch.setattr("clawjournal.workbench.index.INDEX_DB", db_path)
        monkeypatch.setattr("clawjournal.workbench.index.BLOBS_DIR", tmp_path / "blobs")

        conn = open_index()
        try:
            flagged = conn.execute(
                "SELECT COUNT(*) FROM sessions WHERE findings_backfill_needed = 1"
            ).fetchone()[0]
            assert flagged == BACKFILL_WINDOW
        finally:
            conn.close()

    def test_migration_is_idempotent(self, tmp_path, monkeypatch):
        db_path = tmp_path / "index.db"
        self._build_v1_db(db_path, [
            ("s1", "p", "claude", "2025-03-01", "new", "2025-01-01"),
        ])
        monkeypatch.setattr("clawjournal.workbench.index.INDEX_DB", db_path)
        monkeypatch.setattr("clawjournal.workbench.index.BLOBS_DIR", tmp_path / "blobs")

        conn1 = open_index()
        conn1.close()
        conn2 = open_index()
        try:
            history_count = conn2.execute(
                "SELECT COUNT(*) FROM session_hold_history WHERE session_id='s1'"
            ).fetchone()[0]
            assert history_count == 1
            version = conn2.execute("PRAGMA user_version").fetchone()[0]
            assert version == SECURITY_SCHEMA_VERSION
        finally:
            conn2.close()


class TestInstallFiles:
    def test_creates_salt_with_0600_mode(self, tmp_path):
        ensure_install_files(tmp_path)
        salt_path = tmp_path / HASH_SALT_FILENAME
        token_path = tmp_path / API_TOKEN_FILENAME
        assert salt_path.exists()
        assert token_path.exists()
        # Owner-read-write only.
        assert (salt_path.stat().st_mode & 0o777) == 0o600
        assert (token_path.stat().st_mode & 0o777) == 0o600
        assert len(salt_path.read_bytes()) == HASH_SALT_BYTES

    def test_repeated_calls_return_same_bytes(self, tmp_path):
        first_salt, first_token = ensure_install_files(tmp_path)
        second_salt, second_token = ensure_install_files(tmp_path)
        assert first_salt == second_salt
        assert first_token == second_token

    def test_concurrent_creation_is_atomic(self, tmp_path):
        """Two threads racing to create the salt see identical bytes."""
        results: list[bytes] = []
        errors: list[BaseException] = []
        barrier = threading.Barrier(8)

        def worker():
            try:
                barrier.wait()
                results.append(ensure_hash_salt(tmp_path))
            except BaseException as exc:
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []
        assert len(set(results)) == 1
        assert len(results[0]) == HASH_SALT_BYTES
        # Exactly one file exists on disk.
        assert (tmp_path / HASH_SALT_FILENAME).read_bytes() == results[0]

    def test_open_index_bootstraps_files_next_to_db(self, tmp_path, monkeypatch):
        monkeypatch.setattr("clawjournal.workbench.index.INDEX_DB", tmp_path / "index.db")
        monkeypatch.setattr("clawjournal.workbench.index.BLOBS_DIR", tmp_path / "blobs")
        conn = open_index()
        conn.close()
        assert (tmp_path / HASH_SALT_FILENAME).exists()
        assert (tmp_path / API_TOKEN_FILENAME).exists()
