"""Tests for the TruffleHog share-time gate."""

import json
import subprocess
from pathlib import Path

import pytest

from clawjournal.redaction import trufflehog


class TestMaskSecret:
    def test_short_value_fully_hidden(self):
        assert trufflehog.mask_secret("abc") == "***"
        assert trufflehog.mask_secret("12345678") == "***"

    def test_generic_keeps_4_prefix_4_suffix(self):
        raw = "sk-abcdef1234567890"
        masked = trufflehog.mask_secret(raw)
        assert masked.startswith("sk-a")
        assert masked.endswith("7890")
        assert "***" in masked
        assert raw not in masked

    def test_npm_tokens_keep_longer_prefix(self):
        raw = "npm_1234567890abcdefGHIJ"
        masked = trufflehog.mask_secret(raw)
        # npm_ prefix keeps 8 leading chars so reviewers recognize the type.
        assert masked.startswith("npm_1234")
        assert masked.endswith("GHIJ")


class TestParseFinding:
    def test_verified_wins_over_error(self):
        record = {
            "DetectorName": "GitHub",
            "Verified": True,
            "Raw": "ghp_abc1234567890defghijklmnop",
            "SourceMetadata": {"Data": {"Filesystem": {"line": 42}}},
        }
        finding = trufflehog._parse_finding(record)
        assert finding is not None
        assert finding.status == "verified"
        assert finding.line == 42
        assert finding.raw_sha256 is not None
        assert finding.raw_sha256.startswith("sha256:")
        assert "ghp_" in finding.masked  # prefix preserved
        assert record["Raw"] not in finding.masked  # raw never leaks

    def test_verification_error_classified_as_unknown(self):
        record = {
            "DetectorName": "AWS",
            "Verified": False,
            "ExtraData": {"verification_error": "connection refused"},
            "Raw": "AKIAIOSFODNN7EXAMPLE",
        }
        finding = trufflehog._parse_finding(record)
        assert finding is not None
        assert finding.status == "unknown"

    def test_no_verification_error_is_unverified(self):
        record = {
            "DetectorName": "Stripe",
            "Verified": False,
            "Raw": "sk_live_abcdefghijklmnopqrstuv",
        }
        finding = trufflehog._parse_finding(record)
        assert finding is not None
        assert finding.status == "unverified"

    def test_missing_detector_returns_none(self):
        assert trufflehog._parse_finding({"Raw": "x"}) is None


class TestScanFile:
    """scan_file exercises the subprocess contract — we mock subprocess.run
    to fake TruffleHog output while asserting the CLI flags we pass."""

    def _enable_real_scan(self, monkeypatch):
        monkeypatch.delenv(trufflehog.SKIP_ENV_VAR, raising=False)
        monkeypatch.setattr(trufflehog, "is_available", lambda: True)

    def test_bypass_env_var_short_circuits(self, tmp_path, monkeypatch):
        target = tmp_path / "sessions.jsonl"
        target.write_text("{}\n")
        # Autouse fixture already sets SKIP_ENV_VAR=1; verify bypass path.
        called = {"n": 0}

        def fake_run(*args, **kwargs):
            called["n"] += 1
            raise AssertionError("subprocess should not run under bypass")

        monkeypatch.setattr(subprocess, "run", fake_run)
        report = trufflehog.scan_file(target)
        assert report.bypassed is True
        assert report.blocking is False
        assert report.block_reason is None
        assert called["n"] == 0

    def test_missing_binary_reports_blocking(self, tmp_path, monkeypatch):
        monkeypatch.delenv(trufflehog.SKIP_ENV_VAR, raising=False)
        monkeypatch.setattr(trufflehog, "is_available", lambda: False)
        target = tmp_path / "sessions.jsonl"
        target.write_text("{}\n")

        report = trufflehog.scan_file(target)
        assert report.binary_missing is True
        assert report.blocking is True
        assert report.block_reason == "trufflehog-not-installed"

    def test_clean_pass_produces_non_blocking_report(self, tmp_path, monkeypatch):
        self._enable_real_scan(monkeypatch)
        target = tmp_path / "sessions.jsonl"
        target.write_text('{"hello":"world"}\n')

        def fake_run(cmd, **kwargs):
            assert cmd[0] == "trufflehog"
            assert "filesystem" in cmd
            assert "--no-update" in cmd
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

        monkeypatch.setattr(subprocess, "run", fake_run)
        report = trufflehog.scan_file(target)
        assert report.findings == []
        assert report.blocking is False
        assert report.binary_missing is False

    def test_findings_are_parsed_deduped_and_block(self, tmp_path, monkeypatch):
        self._enable_real_scan(monkeypatch)
        target = tmp_path / "sessions.jsonl"
        target.write_text("x\n")

        finding = {
            "DetectorName": "GitHub",
            "Verified": True,
            "Raw": "ghp_abc1234567890defghijklmnop",
            "SourceMetadata": {"Data": {"Filesystem": {"line": 7}}},
        }
        duplicate = dict(finding)
        other = {
            "DetectorName": "Slack",
            "Verified": False,
            "Raw": "xoxb-abcdefghij-klmnopqrst-uvwxyz1234567",
            "SourceMetadata": {"Data": {"Filesystem": {"line": 19}}},
        }
        stdout = "\n".join(json.dumps(x) for x in (finding, duplicate, other)) + "\n"

        monkeypatch.setattr(
            subprocess,
            "run",
            lambda cmd, **kwargs: subprocess.CompletedProcess(cmd, 183, stdout=stdout, stderr=""),
        )
        report = trufflehog.scan_file(target)
        assert len(report.findings) == 2  # duplicate collapsed
        assert report.verified == 1
        assert report.unverified == 1
        assert report.blocking is True
        assert report.block_reason == "trufflehog-findings"
        # Ordered by line.
        assert report.findings[0].line == 7
        assert report.findings[1].line == 19
        # Raw values never appear in the public summary.
        summary = report.summary()
        payload = json.dumps(summary)
        assert finding["Raw"] not in payload
        assert other["Raw"] not in payload
        assert "GitHub" in summary["top_detectors"]

    def test_unexpected_exit_code_raises(self, tmp_path, monkeypatch):
        self._enable_real_scan(monkeypatch)
        target = tmp_path / "sessions.jsonl"
        target.write_text("x\n")
        monkeypatch.setattr(
            subprocess,
            "run",
            lambda cmd, **kwargs: subprocess.CompletedProcess(
                cmd, 2, stdout="", stderr="boom",
            ),
        )
        with pytest.raises(RuntimeError, match="trufflehog exited with 2"):
            trufflehog.scan_file(target)


class TestWriteReport:
    def test_report_round_trips_without_raw_values(self, tmp_path):
        report = trufflehog.TruffleHogReport(
            scanned_path="/x",
            scanned_sha256="sha256:abcd",
            findings=[
                trufflehog.TruffleHogFinding(
                    detector="GitHub",
                    status="verified",
                    line=1,
                    masked="ghp_a***4567",
                    raw_sha256="sha256:deadbeef",
                )
            ],
            verified=1,
            top_detectors=["GitHub"],
        )
        out = tmp_path / "report.json"
        trufflehog.write_report(out, report)
        payload = json.loads(out.read_text())
        assert payload["summary"]["findings"] == 1
        assert payload["findings"][0]["masked"] == "ghp_a***4567"
        assert "raw" not in json.dumps(payload).lower() or "raw_sha256" in json.dumps(payload)


class TestFormatBlockMessage:
    def test_missing_binary_includes_install_hint(self):
        report = trufflehog.TruffleHogReport(
            scanned_path="/x", scanned_sha256="sha256:0", binary_missing=True,
        )
        msg = trufflehog.format_block_message(report)
        assert "brew install trufflehog" in msg
        assert "CLAWJOURNAL_SKIP_TRUFFLEHOG" in msg

    def test_findings_include_masked_examples(self):
        report = trufflehog.TruffleHogReport(
            scanned_path="/x",
            scanned_sha256="sha256:0",
            findings=[
                trufflehog.TruffleHogFinding(
                    detector="GitHub", status="verified", line=5,
                    masked="ghp_a***4567", raw_sha256="sha256:x",
                )
            ],
            verified=1,
        )
        msg = trufflehog.format_block_message(report)
        assert "verified=1" in msg
        assert "ghp_a***4567" in msg
