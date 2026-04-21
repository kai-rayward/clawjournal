"""TruffleHog post-redaction scanner.

Runs as a mandatory gate on every share export: after our layered
redaction produces the final ``sessions.jsonl``, TruffleHog scans
that output as an independent oracle. Any finding (verified,
unverified, or unknown) blocks the export and leaves the directory
intact for debugging.

TruffleHog is invoked as a subprocess — it is AGPL-3.0 and must not
be linked in-process. Install via ``brew install trufflehog`` or the
upstream Go binary. The escape hatch ``CLAWJOURNAL_SKIP_TRUFFLEHOG=1``
exists for CI / development only and is recorded in the share
manifest so downstream reviewers can tell a scanned share from a
bypassed one.
"""

import hashlib
import json
import os
import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

SKIP_ENV_VAR = "CLAWJOURNAL_SKIP_TRUFFLEHOG"

# Detectors we ask TruffleHog to skip. The gate's "any finding blocks"
# contract is deliberately strict, so this list exists only for detectors
# that collide with structural content in agent session files.
#
# - ``refiner`` (refiner.io user-feedback platform): its pattern is
#   "the word 'refiner' followed by a UUID", which trips on any project
#   name containing that substring (e.g. ``tracerefinery``) since
#   Claude/Codex session files are full of tool-use / session UUIDs
#   stored nearby. Verification against api.refiner.io correctly
#   returns ``unverified`` for those, so they are never real leaks.
EXCLUDED_DETECTORS: tuple[str, ...] = ("refiner",)

INSTALL_HINT = (
    "TruffleHog is required to export shares but was not found on PATH.\n"
    "Install it with:\n"
    "  macOS:  brew install trufflehog\n"
    "  Linux:  https://github.com/trufflesecurity/trufflehog#floppy_disk-installation\n"
    "Or set CLAWJOURNAL_SKIP_TRUFFLEHOG=1 to bypass (unsafe — the share "
    "may leak secrets that survived our redaction layers)."
)

FindingStatus = Literal["verified", "unverified", "unknown"]


@dataclass
class TruffleHogFinding:
    detector: str
    status: FindingStatus
    line: int | None
    masked: str
    raw_sha256: str | None


@dataclass
class TruffleHogReport:
    scanned_path: str
    scanned_sha256: str
    findings: list[TruffleHogFinding] = field(default_factory=list)
    verified: int = 0
    unverified: int = 0
    unknown: int = 0
    top_detectors: list[str] = field(default_factory=list)
    bypassed: bool = False
    binary_missing: bool = False

    @property
    def blocking(self) -> bool:
        if self.bypassed:
            return False
        if self.binary_missing:
            return True
        return len(self.findings) > 0

    @property
    def block_reason(self) -> str | None:
        if self.bypassed:
            return None
        if self.binary_missing:
            return "trufflehog-not-installed"
        if self.findings:
            return "trufflehog-findings"
        return None

    def summary(self) -> dict:
        """Public summary safe for the share manifest — no raw values."""
        return {
            "findings": len(self.findings),
            "verified": self.verified,
            "unverified": self.unverified,
            "unknown": self.unknown,
            "top_detectors": list(self.top_detectors),
            "bypassed": self.bypassed,
            "binary_missing": self.binary_missing,
            "examples": [
                {
                    "detector": f.detector,
                    "status": f.status,
                    "line": f.line,
                    "masked": f.masked,
                }
                for f in self.findings[:5]
            ],
        }


def is_available() -> bool:
    return shutil.which("trufflehog") is not None


def is_bypassed() -> bool:
    return os.environ.get(SKIP_ENV_VAR) == "1"


def mask_secret(raw: str) -> str:
    """Partial-mask a raw secret for human triage.

    Preserves the first 4 chars and last 4 chars so reviewers can
    recognize the credential type without seeing the full value.
    ``npm_``-prefixed tokens keep 8 leading chars since the prefix
    itself is the type marker.
    """
    if len(raw) <= 8:
        return "***"
    prefix_len = min(8, len(raw) - 4) if raw.startswith("npm_") else min(4, len(raw) - 4)
    suffix_len = min(4, len(raw) - prefix_len)
    return f"{raw[:prefix_len]}***{raw[-suffix_len:]}"


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return f"sha256:{h.hexdigest()}"


def _parse_finding(parsed: dict) -> TruffleHogFinding | None:
    detector = parsed.get("DetectorName")
    if not isinstance(detector, str):
        return None

    if parsed.get("Verified") is True:
        status: FindingStatus = "verified"
    else:
        extra = parsed.get("ExtraData") if isinstance(parsed.get("ExtraData"), dict) else None
        has_verification_error = False
        if extra:
            for key in ("verification_error", "verificationError", "error"):
                value = extra.get(key)
                if isinstance(value, str) and value.strip():
                    has_verification_error = True
                    break
        status = "unknown" if has_verification_error else "unverified"

    line_no: int | None = None
    source_metadata = parsed.get("SourceMetadata")
    if isinstance(source_metadata, dict):
        data = source_metadata.get("Data")
        if isinstance(data, dict):
            filesystem = data.get("Filesystem")
            if isinstance(filesystem, dict) and isinstance(filesystem.get("line"), int):
                line_no = filesystem["line"]

    raw = parsed.get("Raw")
    raw_str = raw if isinstance(raw, str) and raw else None
    raw_sha = (
        f"sha256:{hashlib.sha256(raw_str.encode()).hexdigest()}" if raw_str else None
    )
    masked = mask_secret(raw_str) if raw_str else "[REDACTED]"

    return TruffleHogFinding(
        detector=detector,
        status=status,
        line=line_no,
        masked=masked,
        raw_sha256=raw_sha,
    )


def scan_file(path: Path) -> TruffleHogReport:
    """Scan ``path`` with TruffleHog. Returns a report.

    Never raises on missing-binary or on findings (both are encoded in
    the returned report). Only raises when the subprocess itself fails
    in an unexpected way (non-recognized exit code, inability to spawn).
    """
    scanned_sha256 = _sha256_file(path)

    if is_bypassed():
        return TruffleHogReport(
            scanned_path=str(path),
            scanned_sha256=scanned_sha256,
            bypassed=True,
        )

    if not is_available():
        return TruffleHogReport(
            scanned_path=str(path),
            scanned_sha256=scanned_sha256,
            binary_missing=True,
        )

    args = [
        "trufflehog",
        "filesystem",
        str(path),
        "-j",
        "--results=verified,unknown,unverified",
        "--no-color",
        "--no-update",
    ]
    if EXCLUDED_DETECTORS:
        args.append(f"--exclude-detectors={','.join(EXCLUDED_DETECTORS)}")

    result = subprocess.run(
        args,
        capture_output=True,
        text=True,
        check=False,
    )
    # TruffleHog exits 0 on clean and 183 on findings in some versions;
    # accept either as a successful scan.
    if result.returncode not in (0, 183):
        raise RuntimeError(
            "trufflehog exited with "
            f"{result.returncode}: "
            f"{result.stderr.strip() or result.stdout.strip()}"
        )

    findings: list[TruffleHogFinding] = []
    detector_counts: dict[str, int] = {}
    verified = unverified = unknown = 0
    seen_keys: set[tuple] = set()

    for raw_line in result.stdout.splitlines():
        raw_line = raw_line.strip()
        if not raw_line:
            continue
        try:
            parsed = json.loads(raw_line)
        except json.JSONDecodeError:
            continue
        if not isinstance(parsed, dict):
            continue
        finding = _parse_finding(parsed)
        if finding is None:
            continue
        key = (finding.detector, finding.status, finding.line, finding.raw_sha256)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        findings.append(finding)
        detector_counts[finding.detector] = detector_counts.get(finding.detector, 0) + 1
        if finding.status == "verified":
            verified += 1
        elif finding.status == "unverified":
            unverified += 1
        else:
            unknown += 1

    findings.sort(key=lambda f: (f.line if f.line is not None else 10**9, f.detector))
    top = sorted(detector_counts.items(), key=lambda item: (-item[1], item[0]))[:8]

    return TruffleHogReport(
        scanned_path=str(path),
        scanned_sha256=scanned_sha256,
        findings=findings,
        verified=verified,
        unverified=unverified,
        unknown=unknown,
        top_detectors=[detector for detector, _ in top],
    )


def scan_text(text: str) -> TruffleHogReport:
    """Scan an in-memory string by dropping it to a temp file.

    Used by the Redact step, which wants a per-session preview scan
    without managing its own temp dir. The final authoritative gate
    still runs on the merged sessions.jsonl at Package time.
    """
    import tempfile

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".jsonl", delete=False, encoding="utf-8"
    ) as tf:
        tf.write(text)
        tmp_path = Path(tf.name)
    try:
        return scan_file(tmp_path)
    finally:
        tmp_path.unlink(missing_ok=True)


def write_report(path: Path, report: TruffleHogReport) -> None:
    payload = {
        "scanned_path": report.scanned_path,
        "scanned_sha256": report.scanned_sha256,
        "bypassed": report.bypassed,
        "binary_missing": report.binary_missing,
        "findings": [
            {
                "detector": f.detector,
                "status": f.status,
                "line": f.line,
                "masked": f.masked,
                "raw_sha256": f.raw_sha256,
            }
            for f in report.findings
        ],
        "summary": report.summary(),
    }
    path.write_text(json.dumps(payload, indent=2) + "\n")


def format_block_message(report: TruffleHogReport) -> str:
    if report.bypassed:
        return "TruffleHog was bypassed via CLAWJOURNAL_SKIP_TRUFFLEHOG."
    if report.binary_missing:
        return INSTALL_HINT
    examples = ", ".join(
        f"L{f.line if f.line is not None else '?'} {f.status} {f.detector} {f.masked}"
        for f in report.findings[:5]
    )
    suffix = "" if len(report.findings) <= 5 else f" (+{len(report.findings) - 5} more)"
    return (
        f"TruffleHog blocked the share: {len(report.findings)} finding(s) "
        f"(verified={report.verified}, unverified={report.unverified}, "
        f"unknown={report.unknown}). Examples: {examples}{suffix}"
    )
