"""Outcome-text normalization for the loop detector.

Two failures of the same command rarely produce byte-identical
output: timestamps differ, PIDs rotate, absolute paths embed the
user's home, whitespace gets reflowed by the terminal. The detector
compares *normalized* text instead, with a small explicit ruleset:

1. **Strip ISO-8601 timestamps.** Matches `2026-04-21T10:00:00`,
   optionally followed by `.<frac>` and `Z` / `+00:00` / `-08:00`.
2. **Strip absolute paths.** macOS (`/Users/...`, `/var/...`,
   `/private/...`, `/tmp/...`) and Linux (`/home/...`) home-rooted
   paths get replaced with `<PATH>`. The replacement preserves the
   leading slash so a literal `/usr/bin/foo` style binary path
   doesn't collapse with a user-rooted path.
3. **Strip PIDs.** `pid 12345`, `[12345]`, and `process 12345`
   patterns become `pid <PID>` / `[<PID>]` / `process <PID>`.
4. **Collapse whitespace runs** (including the run lengths produced
   by stripping bigger tokens out) to a single space.
5. **Strip leading/trailing whitespace.**

Each rule is a module-level regex so tests can pin them
individually. The order matters: timestamps and PIDs must be
stripped before whitespace collapse, otherwise the surrounding
spaces tilt the comparison.
"""

from __future__ import annotations

import re

# 1. ISO-8601 timestamps: 2026-04-21T10:00:00(.fff)?(Z|+00:00|-08:00)?
_ISO_TIMESTAMP = re.compile(
    r"\b\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}"
    r"(?:\.\d+)?"
    r"(?:Z|[+-]\d{2}:?\d{2})?\b"
)

# 2. Absolute paths under user-rooted prefixes. Handles the common
# unquoted case plus quoted paths whose segments contain spaces.
_PATH_PLAIN_SEGMENT = r"[^/\s\"'<>(){}\[\],;]+"
_PATH_SPACED_SEGMENT = rf"{_PATH_PLAIN_SEGMENT}(?: {_PATH_PLAIN_SEGMENT})*"
_HOME_ROOTED_PATH = re.compile(
    rf"(?:"
    rf"(?<=[\"'`])/(?:Users|home|var|private|tmp)/[^\"'`]+(?=[\"'`])"
    rf"|"
    rf"/(?:Users|home|var|private|tmp)/(?:{_PATH_SPACED_SEGMENT}/)*{_PATH_PLAIN_SEGMENT}"
    rf")"
)

# 3a. `pid 12345`, `PID: 12345`, `process 12345`
_PID_INLINE = re.compile(r"\b(pid|process)\s*[:=]?\s*\d+\b", re.IGNORECASE)
# 3b. Bare `[12345]` style. Limited digit count to avoid eating GUIDs.
_PID_BRACKETED = re.compile(r"\[(\d{2,7})\]")

# 4. Runs of whitespace, including newline-only terminal reflow.
_WHITESPACE_RUN = re.compile(r"\s+")

PATH_PLACEHOLDER = "<PATH>"
PID_PLACEHOLDER = "<PID>"
TIMESTAMP_PLACEHOLDER = "<TS>"


def normalize_outcome_text(text: str | None) -> str:
    """Apply the documented ruleset and return the normalized form.

    Returns the empty string for `None` / non-string input so callers
    can compare safely.
    """
    if not isinstance(text, str):
        return ""
    out = _ISO_TIMESTAMP.sub(TIMESTAMP_PLACEHOLDER, text)
    out = _HOME_ROOTED_PATH.sub(PATH_PLACEHOLDER, out)
    out = _PID_INLINE.sub(lambda m: f"{m.group(1)} {PID_PLACEHOLDER}", out)
    out = _PID_BRACKETED.sub(f"[{PID_PLACEHOLDER}]", out)
    out = _WHITESPACE_RUN.sub(" ", out)
    return out.strip()
