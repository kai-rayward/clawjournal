"""PII review/apply helpers for exported ClawJournal JSONL files."""

from __future__ import annotations

import json
import re
import tempfile
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, TypedDict

from ..scoring.backends import (
    BACKEND_CHOICES,
    PROMPTS_DIR,
    resolve_backend,
    run_default_agent_task,
)


MAX_LLM_TEXT_CHARS = 12000


class PIIFinding(TypedDict, total=False):
    session_id: str
    message_index: int
    field: str
    entity_text: str
    entity_type: str
    confidence: float
    reason: str
    replacement: str
    source: str


PLACEHOLDER_BY_TYPE: dict[str, str] = {
    "person_name": "[REDACTED_NAME]",
    "email": "[REDACTED_EMAIL]",
    "phone": "[REDACTED_PHONE]",
    "username": "[REDACTED_USERNAME]",
    "user_id": "[REDACTED_USER_ID]",
    "org_name": "[REDACTED_ORG]",
    "project_name": "[REDACTED_PROJECT]",
    "private_url": "[REDACTED_URL]",
    "domain": "[REDACTED_DOMAIN]",
    "address": "[REDACTED_ADDRESS]",
    "location": "[REDACTED_LOCATION]",
    "bot_name": "[REDACTED_BOT]",
    "device_id": "[REDACTED_DEVICE_ID]",
    "path": "[REDACTED_PATH]",
    "custom_sensitive": "[REDACTED]",
}

ALLOWED_ENTITY_TYPES = set(PLACEHOLDER_BY_TYPE) | {"custom_sensitive"}


def replacement_for_type(entity_type: str) -> str:
    return PLACEHOLDER_BY_TYPE.get(entity_type, "[REDACTED]")


def load_jsonl_sessions(path: Path) -> list[dict[str, Any]]:
    sessions: list[dict[str, Any]] = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            sessions.append(json.loads(line))
    return sessions


def write_jsonl_sessions(path: Path, sessions: Iterable[dict[str, Any]]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for session in sessions:
            f.write(json.dumps(session, ensure_ascii=False) + "\n")


def load_findings(path: Path) -> list[PIIFinding]:
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, dict) and "findings" in data:
        raw = data["findings"]
    else:
        raw = data
    if not isinstance(raw, list):
        raise ValueError("Findings file must contain a list or an object with a 'findings' list.")
    return [normalize_finding(item) for item in raw]


def write_findings(path: Path, findings: list[PIIFinding], meta: dict[str, Any] | None = None) -> None:
    payload: dict[str, Any] = {"findings": findings}
    if meta:
        payload.update(meta)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def normalize_finding(finding: dict[str, Any]) -> PIIFinding:
    entity_type = str(finding.get("entity_type") or "custom_sensitive")
    entity_text = str(finding.get("entity_text") or "")
    replacement = str(finding.get("replacement") or replacement_for_type(entity_type))
    confidence = finding.get("confidence", 1.0)
    try:
        confidence = float(confidence)
    except (TypeError, ValueError):
        confidence = 1.0
    return PIIFinding(
        session_id=str(finding.get("session_id") or ""),
        message_index=int(finding.get("message_index") or 0),
        field=str(finding.get("field") or "content"),
        entity_text=entity_text,
        entity_type=entity_type,
        confidence=max(0.0, min(1.0, confidence)),
        reason=str(finding.get("reason") or ""),
        replacement=replacement,
        source=str(finding.get("source") or "rule"),
    )


def merge_findings(findings: list[PIIFinding], min_confidence: float = 0.0) -> list[PIIFinding]:
    filtered = [f for f in findings if f.get("entity_text") and float(f.get("confidence", 0.0)) >= min_confidence]
    grouped: dict[tuple[str, int, str], list[PIIFinding]] = {}
    for finding in filtered:
        key = (finding.get("session_id", ""), int(finding.get("message_index", 0)), finding.get("field", "content"))
        grouped.setdefault(key, []).append(finding)

    merged: list[PIIFinding] = []
    for items in grouped.values():
        items.sort(key=lambda f: (-len(f.get("entity_text", "")), -float(f.get("confidence", 0.0)), f.get("entity_type", "")))
        chosen: list[PIIFinding] = []
        for item in items:
            text = item.get("entity_text", "")
            text_lower = text.lower()
            if any(text_lower == existing.get("entity_text", "").lower() for existing in chosen):
                continue
            if any(text_lower in existing.get("entity_text", "").lower() for existing in chosen):
                continue
            chosen.append(item)
        merged.extend(chosen)
    return sorted(merged, key=lambda f: (f.get("session_id", ""), int(f.get("message_index", 0)), f.get("field", "content"), -len(f.get("entity_text", ""))))


def apply_findings_to_text(text: str, findings: list[PIIFinding]) -> tuple[str, int]:
    if not text or not findings:
        return text, 0
    ordered = sorted(
        [f for f in findings if f.get("entity_text")],
        key=lambda f: (-len(f.get("entity_text", "")), -float(f.get("confidence", 0.0))),
    )
    count = 0
    result = text
    for finding in ordered:
        target = finding.get("entity_text", "")
        replacement = finding.get("replacement") or replacement_for_type(str(finding.get("entity_type") or "custom_sensitive"))
        if len(target) < 3:
            continue
        escaped = re.escape(target)
        pattern = re.compile(rf"(?<!\w){escaped}(?!\w)", re.IGNORECASE)
        result, n = pattern.subn(replacement, result)
        count += n
    return result, count


def apply_findings_to_session(session: dict[str, Any], findings: list[PIIFinding], min_confidence: float = 0.0) -> tuple[dict[str, Any], int]:
    total = 0
    session_id = str(session.get("session_id") or "")

    # Collect all unique entity findings for this session (across all fields)
    session_findings = [
        f for f in merge_findings(findings, min_confidence=min_confidence)
        if f.get("session_id") == session_id
    ]
    if not session_findings:
        return session, 0

    # Apply every finding to every text field in the session, not just the
    # specific field where it was detected.  PII entities (usernames, names,
    # tokens) typically appear across multiple fields.

    # Apply to top-level metadata fields
    for meta_field in ("project", "git_branch", "display_title"):
        value = session.get(meta_field)
        if isinstance(value, str):
            new_value, n = apply_findings_to_text(value, session_findings)
            session[meta_field] = new_value
            total += n

    messages = session.get("messages", [])
    if not isinstance(messages, list):
        return session, 0

    for msg in messages:
        if not isinstance(msg, dict):
            continue
        for field in ("content", "thinking"):
            value = msg.get(field)
            if isinstance(value, str):
                new_value, n = apply_findings_to_text(value, session_findings)
                msg[field] = new_value
                total += n
        for tool_use in msg.get("tool_uses", []):
            if not isinstance(tool_use, dict):
                continue
            for branch in ("input", "output"):
                value = tool_use.get(branch)
                if isinstance(value, dict):
                    for key in list(value.keys()):
                        if isinstance(value[key], str):
                            new_value, n = apply_findings_to_text(value[key], session_findings)
                            value[key] = new_value
                            total += n
                elif isinstance(value, str):
                    new_value, n = apply_findings_to_text(value, session_findings)
                    tool_use[branch] = new_value
                    total += n
    return session, total


def _truncate_for_llm(text: str, max_chars: int = MAX_LLM_TEXT_CHARS) -> str:
    if len(text) <= max_chars:
        return text
    head = text[: max_chars // 2]
    tail = text[-(max_chars // 2):]
    return head + "\n\n[...TRUNCATED FOR PII REVIEW...]\n\n" + tail


_PII_RUBRIC_FILE = PROMPTS_DIR / "pii_review" / "rubric.md"

# Inline fallback — used only if the rubric file is missing.
_FALLBACK_PII_RUBRIC = """\
You are a PII reviewer for coding-agent conversation traces that will be published as open datasets.
Your job: find text that could identify a real person, organization, or private system.

Return ONLY valid JSON: an array of finding objects. No prose, no markdown fences.

## What to flag (MUST flag if present)

### High confidence (0.85–1.0)
- **person_name**: Real human names. First+last, or distinctive first names in context (e.g., "Kai said", "from Alice"). Not generic words that happen to be names.
- **email**: Full email addresses (user@domain.tld). Not noreply@ or generic service addresses.
- **phone**: Phone numbers in any format (+1-555-123-4567, (555) 123 4567, etc.).
- **username**: GitHub handles, Telegram usernames, SSH user names, bot names — anywhere a handle identifies a person. Includes handles in URLs (github.com/handle), CLI commands (gh repo view handle/repo), git configs, commit metadata.
- **user_id**: Numeric user/chat/account IDs. Telegram chat IDs, Slack user IDs, etc. Not UUIDs, session IDs, or commit SHAs.
- **custom_sensitive**: API tokens, bot tokens (especially Telegram format: digits:alphanumeric), service credentials that survived earlier redaction.

### Medium confidence (0.60–0.84)
- **org_name**: Company, client, or internal organization names when they appear identifying. "Acme Corp", "Initech", client project codenames. Not public products (GitHub, OpenAI, AWS).
- **project_name**: Internal/private project codenames, private repo names, internal tool names. Not public open-source projects.
- **private_url**: URLs pointing to internal systems, private repos, intranet sites, or containing usernames/org names. Not public docs, npm, PyPI, Stack Overflow.
- **domain**: Private or corporate domains (acme-internal.com, dev.mycompany.io). Not public domains (github.com, google.com).
- **device_id**: Device names (kais-macbook-pro, my-workstation-01), hostnames with personal identifiers, hardware serial numbers.

### Lower confidence (0.40–0.59)
- **address**: Physical addresses, office locations ("123 Main St", "Building 4, Floor 2").
- **location**: City + context that narrows to a person ("our SF office", "the Tokyo team"). Not just generic city mentions.
- **bot_name**: Bot/service account names that could trace back to a person or team.

## What NOT to flag (skip these)
- Already-redacted placeholders: [REDACTED_*], [REDACTED], ***
- Public product/service names: GitHub, OpenAI, Anthropic, Telegram, Docker, AWS, GCP, Hugging Face, npm, PyPI
- Localhost, 127.0.0.1, 0.0.0.0, example.com, test.com
- Generic technical terms, function/class/variable names
- Open-source project names (tensorflow, pytorch, react, clawjournal)
- Public documentation URLs
- Version numbers, build IDs, commit SHAs, UUIDs
- Standard paths (/usr/bin, /tmp, /etc)

## Confidence calibration
- 0.95+: Unambiguous PII (full name + context, email, phone, explicit username)
- 0.85–0.94: Very likely PII (handle in URL, numeric user ID in metadata)
- 0.70–0.84: Likely PII but could be a project/product name
- 0.50–0.69: Possible PII, needs human review
- Below 0.50: Don't flag — too speculative

## Output schema
Each finding must be:
{
  "entity_text": "exact text to redact",
  "entity_type": "person_name"|"email"|"phone"|"username"|"user_id"|"org_name"|"project_name"|"private_url"|"domain"|"address"|"location"|"bot_name"|"device_id"|"custom_sensitive",
  "confidence": <number 0.0–1.0>,
  "reason": "brief explanation"
}
"""


def _load_pii_rubric() -> str:
    """Load the PII review rubric from file, with inline fallback."""
    if _PII_RUBRIC_FILE.exists():
        return _PII_RUBRIC_FILE.read_text(encoding="utf-8")
    return _FALLBACK_PII_RUBRIC


# Keep module-level constant for backward compat (tests, cli imports).
PII_REVIEW_RUBRIC = _load_pii_rubric()


def _build_pii_review_prompt(payload: dict[str, Any], rubric: str | None = None) -> str:
    return (
        (rubric or PII_REVIEW_RUBRIC)
        + "\n## Text to review\n"
        + json.dumps(payload, ensure_ascii=False)
        + "\n"
    )


def _extract_json_array(text: str) -> list[dict[str, Any]]:
    text = text.strip()
    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, list) else []
    except json.JSONDecodeError:
        pass
    match = re.search(r"\[.*\]", text, re.DOTALL)
    if not match:
        return []
    try:
        parsed = json.loads(match.group(0))
        return parsed if isinstance(parsed, list) else []
    except json.JSONDecodeError:
        return []


def _normalize_llm_findings(session_id: str, message_index: int, field: str, findings: list[dict[str, Any]], source: str) -> list[PIIFinding]:
    out: list[PIIFinding] = []
    for finding in findings:
        entity_type = str(finding.get("entity_type") or "custom_sensitive")
        if entity_type not in ALLOWED_ENTITY_TYPES:
            entity_type = "custom_sensitive"
        normalized = normalize_finding({
            "session_id": session_id,
            "message_index": message_index,
            "field": field,
            "entity_text": finding.get("entity_text") or "",
            "entity_type": entity_type,
            "confidence": finding.get("confidence", 0.0),
            "reason": finding.get("reason") or "",
            "replacement": replacement_for_type(entity_type),
            "source": source,
        })
        if normalized.get("entity_text"):
            out.append(normalized)
    return out


_PII_PROMPT_FILE = PROMPTS_DIR / "pii_review" / "system.md"

# Safety valve for session-level batching.  Modern agent CLIs have large
# context windows (Claude Opus: 1M tokens, Codex/OpenClaw: 200K+).  At ~4
# chars/token, 2M chars ≈ 500K tokens — half the largest context window,
# leaving ample room for rubric + reasoning.  In practice this never splits;
# the largest sessions we've seen are ~120K chars (~30K tokens).
_BATCH_CHAR_LIMIT = 2_000_000


def _write_batch_inputs(tmp_path: Path, session_id: str, work_items: list[tuple[str, int, str, str]], rubric: str | None) -> None:
    """Write batched PII review inputs: one JSONL file with all text chunks."""
    lines: list[str] = []
    for _, message_index, field, text in work_items:
        lines.append(json.dumps({
            "message_index": message_index,
            "field": field,
            "text": _truncate_for_llm(text),
        }, ensure_ascii=False))
    (tmp_path / "texts_to_review.jsonl").write_text("\n".join(lines), encoding="utf-8")
    (tmp_path / "context.json").write_text(json.dumps({"session_id": session_id}), encoding="utf-8")
    (tmp_path / "PII_RUBRIC.md").write_text(rubric or PII_REVIEW_RUBRIC, encoding="utf-8")


def _read_batch_findings(tmp_path: Path, session_id: str, source: str, stdout: str = "") -> list[PIIFinding]:
    """Read findings.json containing batched findings with message_index and field per entry."""
    raw: list[dict] = []
    findings_path = tmp_path / "findings.json"
    if findings_path.exists():
        try:
            parsed = json.loads(findings_path.read_text(encoding="utf-8"))
            if isinstance(parsed, list):
                raw = parsed
        except json.JSONDecodeError:
            pass
    if not raw:
        raw = _extract_json_array(stdout)

    out: list[PIIFinding] = []
    for finding in raw:
        if not isinstance(finding, dict):
            continue
        msg_idx = finding.get("message_index", 0)
        field = finding.get("field", "content")
        entity_type = finding.get("entity_type") or "custom_sensitive"
        if entity_type not in ALLOWED_ENTITY_TYPES:
            entity_type = "custom_sensitive"
        normalized = normalize_finding({
            "session_id": session_id,
            "message_index": msg_idx,
            "field": field,
            "entity_text": finding.get("entity_text") or "",
            "entity_type": entity_type,
            "confidence": finding.get("confidence", 0.0),
            "reason": finding.get("reason") or "",
            "replacement": replacement_for_type(entity_type),
            "source": source,
        })
        if normalized.get("entity_text"):
            out.append(normalized)
    return out


_BATCH_TASK_PROMPT = (
    "Review texts_to_review.jsonl for PII. Each line is a JSON object with "
    "message_index, field, and text. Read PII_RUBRIC.md and context.json. "
    "Write findings.json with a JSON array. Each finding must include: "
    "message_index, field, entity_text, entity_type, confidence, reason. "
    "Write [] if no PII found."
)


def _split_into_batches(work_items: list[tuple[str, int, str, str]], char_limit: int = _BATCH_CHAR_LIMIT) -> list[list[tuple[str, int, str, str]]]:
    """Split work items into batches that fit within char_limit."""
    batches: list[list[tuple[str, int, str, str]]] = []
    current: list[tuple[str, int, str, str]] = []
    current_chars = 0
    for item in work_items:
        item_chars = min(len(item[3]), MAX_LLM_TEXT_CHARS)
        if current and current_chars + item_chars > char_limit:
            batches.append(current)
            current = []
            current_chars = 0
        current.append(item)
        current_chars += item_chars
    if current:
        batches.append(current)
    return batches


def _review_batch(session_id: str, work_items: list[tuple[str, int, str, str]], *, rubric: str | None, backend: str = "auto", timeout_seconds: int = 180) -> list[PIIFinding]:
    """Review a batch of text chunks via the shared agent runner."""
    resolved = resolve_backend(backend)
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        _write_batch_inputs(tmp_path, session_id, work_items, rubric)

        # Build OpenClaw-specific message with absolute paths
        openclaw_msg = None
        if resolved == "openclaw":
            openclaw_msg = (
                "Review texts_to_review.jsonl for PII. Each line has message_index, field, text.\n"
                f"Read: {tmp_path / 'PII_RUBRIC.md'} and {tmp_path / 'context.json'}.\n"
                "Return a JSON array of findings, each with: message_index, field, entity_text, "
                "entity_type, confidence, reason. Return [] if no PII. No markdown fences."
            )

        try:
            result = run_default_agent_task(
                backend=resolved,
                cwd=tmp_path,
                system_prompt_file=_PII_PROMPT_FILE,
                task_prompt=_BATCH_TASK_PROMPT,
                timeout_seconds=timeout_seconds,
                codex_sandbox="read-only",
                codex_output_file="findings.json",
                openclaw_message=openclaw_msg,
            )
        except RuntimeError as exc:
            raise RuntimeError(f"PII review failed for session {session_id}: {exc}") from exc

        return _read_batch_findings(tmp_path, session_id, resolved, result.stdout)


def _review_text_with_agent(session_id: str, message_index: int, field: str, text: str, *, rubric: str | None = None, backend: str = "auto") -> list[PIIFinding]:
    """Review a single text chunk — used by backward-compat wrappers."""
    if not text.strip():
        return []
    items = [(session_id, message_index, field, text)]
    return _review_batch(session_id, items, rubric=rubric, backend=backend)


def _collect_text_work_items(session: dict[str, Any]) -> list[tuple[str, int, str, str]]:
    """Extract all (session_id, message_index, field, text) tuples from a session."""
    session_id = str(session.get("session_id") or "")
    messages = session.get("messages", [])
    if not isinstance(messages, list):
        return []
    work_items: list[tuple[str, int, str, str]] = []
    for i, msg in enumerate(messages):
        if not isinstance(msg, dict):
            continue
        for field in ("content", "thinking"):
            value = msg.get(field)
            if isinstance(value, str) and value.strip():
                work_items.append((session_id, i, field, value))
        for tool_index, tool_use in enumerate(msg.get("tool_uses", [])):
            if not isinstance(tool_use, dict):
                continue
            for branch in ("input", "output"):
                value = tool_use.get(branch)
                if isinstance(value, dict):
                    for key, nested in value.items():
                        if isinstance(nested, str) and nested.strip():
                            work_items.append((session_id, i, f"tool_uses[{tool_index}].{branch}.{key}", nested))
                elif isinstance(value, str) and value.strip():
                    work_items.append((session_id, i, f"tool_uses[{tool_index}].{branch}", value))
    return work_items


def review_session_pii_with_agent(session: dict[str, Any], *, backend: str = "auto", ignore_errors: bool = False, rubric: str | None = None, max_workers: int = 4) -> list[PIIFinding]:
    """Review a session for PII using session-level batching (one agent call per batch)."""
    work_items = _collect_text_work_items(session)
    if not work_items:
        return []

    session_id = str(session.get("session_id") or "")
    batches = _split_into_batches(work_items)

    findings: list[PIIFinding] = []
    errors: list[RuntimeError] = []

    if len(batches) == 1:
        # Single batch — no parallelism needed
        try:
            findings.extend(_review_batch(session_id, batches[0], rubric=rubric, backend=backend))
        except RuntimeError as exc:
            if not ignore_errors:
                raise
    else:
        # Multiple batches — run in parallel
        with ThreadPoolExecutor(max_workers=min(max_workers, len(batches))) as pool:
            futures = {
                pool.submit(_review_batch, session_id, batch, rubric=rubric, backend=backend): batch
                for batch in batches
            }
            for future in as_completed(futures):
                try:
                    findings.extend(future.result())
                except RuntimeError as exc:
                    if not ignore_errors:
                        errors.append(exc)
        if errors:
            raise errors[0]

    return merge_findings(findings)


def review_session_pii_hybrid(
    session: dict[str, Any],
    *,
    ignore_llm_errors: bool = True,
    rubric: str | None = None,
    backend: str = "auto",
    return_coverage: bool = False,
) -> list[PIIFinding] | tuple[list[PIIFinding], str]:
    """Run hybrid PII detection (rule-based + AI agent).

    When *return_coverage* is True, returns a tuple of (findings, coverage)
    where coverage is ``"full"`` if AI detection succeeded or ``"rules_only"``
    if the AI backend was unavailable or errored.
    """
    rule_findings = review_session_pii(session)
    coverage = "full"
    try:
        agent_findings = review_session_pii_with_agent(
            session, backend=backend, ignore_errors=False, rubric=rubric,
        )
    except Exception:
        if not ignore_llm_errors:
            raise
        agent_findings = []
        coverage = "rules_only"
    merged = merge_findings(rule_findings + agent_findings)
    if return_coverage:
        return merged, coverage
    return merged


_GITHUB_URL_PUBLIC_ORGS = frozenset({
    "anthropics", "anthropic", "openai", "google", "microsoft", "meta",
    "facebook", "aws", "hashicorp", "vercel", "supabase", "huggingface",
    "pytorch", "tensorflow", "golang", "rust-lang", "python", "nodejs",
    "actions", "github", "cli", "docker", "kubernetes", "helm", "npm",
    "homebrew", "apache", "mozilla", "jetbrains", "gradle", "maven",
})


def _content_findings_for_text(session_id: str, message_index: int, field: str, text: str) -> list[PIIFinding]:
    """Scan free-form text for PII patterns beyond JSON metadata."""
    findings: list[PIIFinding] = []
    patterns: list[tuple[str, str, str, float, int]] = [
        # GitHub user/org in URLs — group 1 is the username/org
        (r"github\.com/([A-Za-z0-9_.-]{2,})", "username", "GitHub username/org in URL", 0.85, 1),
        (r"raw\.githubusercontent\.com/([A-Za-z0-9_.-]{2,})", "username", "GitHub username/org in raw URL", 0.85, 1),
        # Email-like identifiers (require user@domain.tld format)
        (r"([A-Za-z0-9_.+-]{3,}@[A-Za-z0-9.-]+\.[A-Za-z]{2,})", "email", "Email address", 0.90, 1),
        # Partial email / identifier with @ (e.g., "jane.doe@" in tabular output)
        (r"([A-Za-z0-9_.+-]{3,})@(?=\s|$)", "email", "Email-like identifier (truncated)", 0.75, 1),
        # Telegram bot tokens: numeric_id:alphanumeric_token
        (r"(\d{8,}:[A-Za-z0-9_-]{30,})", "custom_sensitive", "Likely Telegram bot token", 0.95, 1),
        # Hostnames with personal identifiers (e.g., kais-macbook-pro, alice-desktop)
        (r"\b([a-z][a-z0-9]*s?-(?:macbook|imac|laptop|desktop|pc|workstation|server)-?[a-z0-9]*)\b", "device_id", "Likely personal hostname", 0.80, 1),
        # Absolute home-directory paths (leaks username and directory structure)
        (r"(/(?:Users|home)/[A-Za-z0-9._-]{2,}/[^\s\"'`,;)}\]]{3,})", "path", "Home-directory file path", 0.85, 1),
        # Private/internal IP addresses (not localhost)
        (r"\b(10\.\d{1,3}\.\d{1,3}\.\d{1,3})\b", "custom_sensitive", "Private IP address (10.x)", 0.70, 1),
        (r"\b(172\.(?:1[6-9]|2\d|3[01])\.\d{1,3}\.\d{1,3})\b", "custom_sensitive", "Private IP address (172.16-31.x)", 0.70, 1),
        (r"\b(192\.168\.\d{1,3}\.\d{1,3})\b", "custom_sensitive", "Private IP address (192.168.x)", 0.70, 1),
    ]
    for pattern, entity_type, reason, confidence, group in patterns:
        for match in re.finditer(pattern, text):
            entity_text = match.group(group).strip()
            if not entity_text or len(entity_text) < 3:
                continue
            # skip already-redacted placeholders
            if entity_text.startswith("[") and entity_text.endswith("]"):
                continue
            # skip well-known public GitHub orgs
            if "GitHub" in reason and entity_text.lower() in _GITHUB_URL_PUBLIC_ORGS:
                continue
            # skip noreply / no-reply email addresses
            if entity_type == "email" and entity_text.lower().startswith(("noreply@", "no-reply@")):
                continue
            findings.append(normalize_finding({
                "session_id": session_id,
                "message_index": message_index,
                "field": field,
                "entity_text": entity_text,
                "entity_type": entity_type,
                "confidence": confidence,
                "reason": reason,
                "replacement": replacement_for_type(entity_type),
                "source": "rule",
            }))
    return findings


def _metadata_findings_for_text(session_id: str, message_index: int, field: str, text: str) -> list[PIIFinding]:
    findings: list[PIIFinding] = []
    _Q = r'\\?"'  # match both `"` and `\"`
    patterns: list[tuple[str, str, str, str, float]] = [
        (rf'{_Q}username{_Q}\s*:\s*{_Q}([^"\\]{{3,}}){_Q}', "username", "Likely username in metadata block", "rule", 0.98),
        (rf'{_Q}sender_id{_Q}\s*:\s*{_Q}([^"\\]{{3,}}){_Q}', "user_id", "Likely sender/user ID in metadata block", "rule", 0.98),
        (rf'{_Q}(?:user_id|chat_id|account_id|sender_id|from_id){_Q}\s*:\s*{_Q}([^"\\]{{3,}}){_Q}', "user_id", "Likely user/chat/account ID in metadata block", "rule", 0.95),
        (rf'{_Q}id{_Q}\s*:\s*{_Q}(\d{{5,}}){_Q}', "user_id", "Likely numeric user ID in metadata block", "rule", 0.75),
        (rf'{_Q}name{_Q}\s*:\s*{_Q}([^"\\]{{3,}}){_Q}', "person_name", "Likely person name in metadata block", "rule", 0.82),
        (rf'{_Q}sender{_Q}\s*:\s*{_Q}([^"\\]{{3,}}){_Q}', "person_name", "Likely sender name in metadata block", "rule", 0.82),
        (rf'{_Q}label{_Q}\s*:\s*{_Q}([^"\\]{{3,}}){_Q}', "person_name", "Likely identifying label in metadata block", "rule", 0.75),
    ]
    for pattern, entity_type, reason, source, confidence in patterns:
        for match in re.finditer(pattern, text):
            entity_text = match.group(1).strip()
            if entity_text.startswith("[") and entity_text.endswith("]"):
                continue
            findings.append(normalize_finding({
                "session_id": session_id,
                "message_index": message_index,
                "field": field,
                "entity_text": entity_text,
                "entity_type": entity_type,
                "confidence": confidence,
                "reason": reason,
                "replacement": replacement_for_type(entity_type),
                "source": source,
            }))
    return findings


def _scan_text_for_pii(session_id: str, message_index: int, field: str, text: str) -> list[PIIFinding]:
    """Run both metadata and content PII scans on a text value."""
    findings = _metadata_findings_for_text(session_id, message_index, field, text)
    findings.extend(_content_findings_for_text(session_id, message_index, field, text))
    return findings


def review_session_pii(session: dict[str, Any]) -> list[PIIFinding]:
    findings: list[PIIFinding] = []
    session_id = str(session.get("session_id") or "")

    # Scan top-level metadata fields for PII
    for meta_field in ("project", "git_branch", "display_title"):
        value = session.get(meta_field)
        if isinstance(value, str) and value.strip():
            findings.extend(_content_findings_for_text(session_id, -1, meta_field, value))

    messages = session.get("messages", [])
    if not isinstance(messages, list):
        return findings
    for i, msg in enumerate(messages):
        if not isinstance(msg, dict):
            continue
        for field in ("content", "thinking"):
            value = msg.get(field)
            if isinstance(value, str):
                findings.extend(_scan_text_for_pii(session_id, i, field, value))
        for tool_index, tool_use in enumerate(msg.get("tool_uses", [])):
            if not isinstance(tool_use, dict):
                continue
            for branch in ("input", "output"):
                value = tool_use.get(branch)
                if isinstance(value, dict):
                    for key, nested in value.items():
                        if isinstance(nested, str):
                            field = f"tool_uses[{tool_index}].{branch}.{key}"
                            findings.extend(_scan_text_for_pii(session_id, i, field, nested))
                elif isinstance(value, str):
                    field = f"tool_uses[{tool_index}].{branch}"
                    findings.extend(_scan_text_for_pii(session_id, i, field, value))
    return merge_findings(findings)
