"""Depth-level formatting for session data.

Formats session content at three depth tiers:
- workflow: pure structure, zero content (tool types, timing, stats)
- summary: type-level descriptions, no implementation details
- full: everything with standard redaction applied
"""

import re
from typing import Any

# Extension → human-readable file type
_EXT_TO_TYPE: dict[str, str] = {
    ".py": "python file",
    ".js": "javascript file",
    ".ts": "typescript file",
    ".tsx": "typescript file",
    ".jsx": "javascript file",
    ".rs": "rust file",
    ".go": "go file",
    ".java": "java file",
    ".rb": "ruby file",
    ".c": "c file",
    ".cpp": "c++ file",
    ".h": "header file",
    ".cs": "c# file",
    ".swift": "swift file",
    ".kt": "kotlin file",
    ".sh": "shell script",
    ".bash": "shell script",
    ".zsh": "shell script",
    ".html": "html file",
    ".css": "css file",
    ".scss": "css file",
    ".json": "config file",
    ".yaml": "config file",
    ".yml": "config file",
    ".toml": "config file",
    ".ini": "config file",
    ".cfg": "config file",
    ".env": "config file",
    ".xml": "config file",
    ".md": "docs file",
    ".rst": "docs file",
    ".txt": "text file",
    ".sql": "sql file",
    ".proto": "protobuf file",
    ".graphql": "graphql file",
    ".dockerfile": "dockerfile",
    ".lock": "lockfile",
}

# Command first-token → category
_CMD_CATEGORIES: dict[str, str] = {
    "pytest": "test", "jest": "test", "vitest": "test", "mocha": "test",
    "cargo": "build",  # cargo test, cargo build, etc. — use "build" as default
    "go": "build",
    "make": "build", "cmake": "build",
    "npm": "package", "yarn": "package", "pnpm": "package", "pip": "package",
    "brew": "package", "apt": "package", "apt-get": "package",
    "git": "vcs", "gh": "vcs", "svn": "vcs",
    "python": "run", "python3": "run", "node": "run", "ruby": "run",
    "docker": "container", "podman": "container", "docker-compose": "container",
    "terraform": "infra", "kubectl": "infra", "helm": "infra",
    "aws": "infra", "gcloud": "infra", "az": "infra",
    "curl": "network", "wget": "network", "httpie": "network",
    "mkdir": "filesystem", "rm": "filesystem", "mv": "filesystem",
    "cp": "filesystem", "chmod": "filesystem", "ln": "filesystem", "touch": "filesystem",
    "tsc": "build", "webpack": "build", "vite": "build", "esbuild": "build",
    "gradle": "build", "mvn": "build",
    "cat": "read", "head": "read", "tail": "read", "less": "read",
    "grep": "search", "rg": "search", "find": "search", "fd": "search",
    "sed": "edit", "awk": "edit",
    "ls": "filesystem", "pwd": "filesystem", "cd": "filesystem",
}


def _file_type_from_path(path: str) -> str:
    """Map a file path to a human-readable type label."""
    lower = path.lower()
    # Check for test files
    basename = lower.rsplit("/", 1)[-1] if "/" in lower else lower
    if basename.startswith("test_") or basename.endswith("_test.py") or "_test." in basename:
        return "test file"
    if "/test/" in lower or "/tests/" in lower or "/__tests__/" in lower:
        return "test file"
    # Check extension
    for ext, ftype in _EXT_TO_TYPE.items():
        if lower.endswith(ext):
            return ftype
    # Special names
    if basename in ("dockerfile", "makefile", "cmakelists.txt"):
        return "dockerfile" if "docker" in basename else "build file"
    return "file"


def _command_category(command: str) -> str:
    """Map a shell command string to a category."""
    if not command:
        return "other"
    # Handle common patterns: "cd foo && pytest" → take the last meaningful command
    parts = re.split(r"\s*&&\s*|\s*;\s*|\s*\|\|\s*", command)
    cmd = parts[-1].strip() if parts else command
    first_token = cmd.split()[0] if cmd.split() else ""
    # Strip path prefix
    first_token = first_token.rsplit("/", 1)[-1] if "/" in first_token else first_token
    return _CMD_CATEGORIES.get(first_token, "other")


def _detect_test_command(command: str) -> bool:
    """Detect if a command is a test invocation."""
    test_keywords = ("pytest", "jest", "vitest", "mocha", "cargo test", "go test",
                     "npm test", "yarn test", "make test", "python -m pytest",
                     "python3 -m pytest", "python -m unittest", "python3 -m unittest")
    lower = command.lower()
    return any(kw in lower for kw in test_keywords)


def _count_lines(text: str | None) -> int:
    """Count lines in a text block."""
    if not text:
        return 0
    return text.count("\n") + (1 if text and not text.endswith("\n") else 0)


def _truncate_first_sentence(text: str, max_words: int = 15) -> str:
    """Extract first sentence, truncate to max_words, replace code/paths/URLs."""
    if not text:
        return ""
    # Take first line or first sentence
    first_line = text.split("\n")[0].strip()
    # Strip XML-like tags first (so they don't consume word slots)
    first_line = re.sub(r"<[^>]+>", "", first_line).strip()
    # Truncate at sentence boundary
    for sep in (". ", "! ", "? "):
        if sep in first_line:
            first_line = first_line[:first_line.index(sep) + 1]
            break
    # Truncate to max words
    words = first_line.split()
    if len(words) > max_words:
        first_line = " ".join(words[:max_words]) + "..."
    # Replace code blocks
    first_line = re.sub(r"`[^`]+`", "[code]", first_line)
    # Replace URLs (before file paths so URLs aren't partially matched)
    first_line = re.sub(r"https?://\S+", "[url]", first_line)
    # Replace file paths (things with / and a dot extension)
    first_line = re.sub(r"[\w./\-]+/[\w./\-]+\.\w+", "[file]", first_line)
    return first_line


def _extract_user_text(message: dict[str, Any]) -> str:
    """Extract plain text from a user message."""
    content = message.get("content")
    if not content:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for block in content:
            if isinstance(block, str):
                parts.append(block)
            elif isinstance(block, dict) and block.get("type") == "text":
                parts.append(block.get("text", ""))
        return " ".join(parts)
    return ""


def extract_workflow_steps(
    messages: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Extract tool-use steps from session messages.

    Returns a list of step dicts with keys:
        tool, detail, lines, outcome, status
    """
    steps: list[dict[str, Any]] = []
    for msg in messages:
        tool_uses = msg.get("tool_uses", [])
        for tu in tool_uses:
            tool_name = tu.get("tool", tu.get("name", "unknown"))
            tool_input = tu.get("input") or {}
            tool_output = tu.get("output") or {}
            status = tu.get("status", "success")
            output_text = ""
            if isinstance(tool_output, dict):
                output_text = tool_output.get("text", "")
            elif isinstance(tool_output, str):
                output_text = tool_output

            step: dict[str, Any] = {
                "tool": _normalize_tool_name(tool_name),
                "detail": "",
                "lines": None,
                "outcome": None,
                "status": status,
            }

            # Extract detail based on tool type
            if tool_name.lower() in ("read", "view"):
                path = tool_input.get("file_path", "")
                step["detail"] = path
                # Count output lines
                if output_text:
                    step["lines"] = _count_lines(output_text)

            elif tool_name.lower() in ("edit", "write", "notebookedit"):
                path = tool_input.get("file_path", "")
                step["detail"] = path
                old = tool_input.get("old_string", "")
                new = tool_input.get("new_string", "")
                content = tool_input.get("content", "")
                if old or new:
                    step["lines"] = max(_count_lines(old), _count_lines(new))
                elif content:
                    step["lines"] = _count_lines(content)

            elif tool_name.lower() in ("bash", "execute", "terminal", "command"):
                command = tool_input.get("command", "")
                step["detail"] = command
                if _detect_test_command(command):
                    step["tool"] = "test"
                    # Try to extract test results from output
                    step["outcome"] = _parse_test_outcome(output_text)
                else:
                    step["tool"] = _command_category(command) or "bash"

            elif tool_name.lower() in ("grep", "search", "glob"):
                pattern = tool_input.get("pattern", "")
                step["detail"] = pattern
                step["tool"] = "search"

            elif tool_name.lower() in ("agent", "task"):
                # Show description if available, not the full prompt
                desc = tool_input.get("description", "")
                step["detail"] = desc[:80] if desc else ""

            else:
                # Truncate unknown tool inputs to avoid dumping huge payloads
                if isinstance(tool_input, dict):
                    # Pick the shortest useful field
                    for key in ("file_path", "path", "command", "query", "name"):
                        if tool_input.get(key):
                            step["detail"] = str(tool_input[key])[:80]
                            break
                    else:
                        step["detail"] = ""
                else:
                    step["detail"] = str(tool_input)[:80] if tool_input else ""

            steps.append(step)
    return steps


def _normalize_tool_name(name: str) -> str:
    """Normalize tool names to a small set."""
    lower = name.lower()
    if lower in ("read", "view", "cat"):
        return "read"
    if lower in ("edit", "write", "notebookedit"):
        return "edit"
    if lower in ("bash", "execute", "terminal", "command"):
        return "bash"
    if lower in ("grep", "search", "glob", "find"):
        return "search"
    if lower in ("agent", "task"):
        return "agent"
    return lower


def _parse_test_outcome(output: str) -> str | None:
    """Try to extract test results from command output."""
    if not output:
        return None
    # pytest: "5 passed, 1 failed"
    m = re.search(r"(\d+)\s+passed(?:.*?(\d+)\s+failed)?", output)
    if m:
        passed = int(m.group(1))
        failed = int(m.group(2)) if m.group(2) else 0
        total = passed + failed
        if failed:
            return f"{passed}/{total} passed"
        return f"{total}/{total} passed"
    # jest/vitest: "Tests: 3 passed, 1 failed, 4 total"
    m = re.search(r"Tests:\s*(\d+)\s+passed(?:,\s*(\d+)\s+failed)?(?:,\s*(\d+)\s+total)?", output)
    if m:
        passed = int(m.group(1))
        failed = int(m.group(2)) if m.group(2) else 0
        total = int(m.group(3)) if m.group(3) else passed + failed
        if failed:
            return f"{passed}/{total} passed"
        return f"{total}/{total} passed"
    # Generic pass/fail detection
    lower = output.lower()
    if "passed" in lower and "failed" not in lower and "error" not in lower:
        return "passed"
    if "failed" in lower or "error" in lower:
        return "failed"
    return None


def format_workflow_step(step: dict[str, Any], depth: str) -> dict[str, Any]:
    """Format a single workflow step at the given depth.

    Returns a dict with: tool, detail, lines, outcome
    """
    result: dict[str, Any] = {"tool": step["tool"]}

    if depth == "workflow":
        # Anonymize everything
        if step["detail"]:
            if step["tool"] in ("read", "edit"):
                result["detail"] = _file_type_from_path(step["detail"])
            elif step["tool"] in ("bash", "test", "vcs", "build", "package",
                                  "run", "container", "infra", "network",
                                  "filesystem", "search", "other"):
                result["detail"] = None  # category is in tool name
            else:
                result["detail"] = None
        if step.get("lines"):
            result["lines"] = step["lines"]
        if step.get("outcome"):
            result["outcome"] = step["outcome"]

    elif depth == "summary":
        if step["tool"] in ("read", "edit"):
            result["detail"] = _file_type_from_path(step["detail"]) if step["detail"] else None
        elif step["tool"] == "test":
            result["detail"] = None  # outcome is enough
        else:
            result["detail"] = None
        if step.get("lines"):
            result["lines"] = step["lines"]
        if step.get("outcome"):
            result["outcome"] = step["outcome"]

    elif depth == "full":
        result["detail"] = step.get("detail")
        if step.get("lines"):
            result["lines"] = step["lines"]
        if step.get("outcome"):
            result["outcome"] = step["outcome"]

    return result


def format_step_text(step: dict[str, Any]) -> str:
    """Render a single formatted step as a text string."""
    parts = [step["tool"].capitalize()]
    detail = step.get("detail")
    if detail:
        parts[0] += f" {detail}"
    lines = step.get("lines")
    if lines:
        parts.append(f"({lines} lines)")
    outcome = step.get("outcome")
    if outcome:
        parts.append(f"({outcome})")
    return " ".join(parts)


def format_workflow_oneliner(steps: list[dict[str, Any]]) -> str:
    """Create an arrow-separated one-liner from formatted steps.

    If >7 steps, shows first 5 and last 2 with '...' in between.
    """
    if not steps:
        return ""

    texts = [format_step_text(s) for s in steps]

    if len(texts) <= 7:
        return " → ".join(texts)

    # Truncate: first 5 + ... + last 2
    truncated = texts[:5] + [f"... {len(texts) - 7} more ..."] + texts[-2:]
    return " → ".join(truncated)


def format_session_at_depth(
    session: dict[str, Any],
    depth: str = "summary",
) -> dict[str, Any]:
    """Format a session's content at the specified depth level.

    Args:
        session: Full session dict with messages, metadata, etc.
        depth: One of 'workflow', 'summary', 'full'

    Returns:
        Dict with: title, summary_line, workflow_steps, workflow_oneliner, stats
    """
    messages = session.get("messages", [])

    # Title
    if depth == "workflow":
        # At workflow depth, no content — use session ID only
        sid = session.get("session_id", "unknown")
        title = f"Session {sid[:8]}"
    else:
        title = session.get("display_title") or ""

    # Summary line (first user message, truncated)
    summary_line = ""
    if depth in ("summary", "full"):
        for msg in messages:
            if msg.get("role") == "user":
                user_text = _extract_user_text(msg)
                if user_text:
                    if depth == "summary":
                        summary_line = _truncate_first_sentence(user_text)
                    else:
                        summary_line = _truncate_first_sentence(user_text, max_words=30)
                    break

    # Extract and format workflow steps
    raw_steps = extract_workflow_steps(messages)
    formatted_steps = [format_workflow_step(s, depth) for s in raw_steps]
    oneliner = format_workflow_oneliner(formatted_steps)

    # Stats
    stats = {
        "user_messages": session.get("user_messages", 0),
        "assistant_messages": session.get("assistant_messages", 0),
        "tool_uses": session.get("tool_uses", 0),
        "total_tokens": (session.get("input_tokens", 0) or 0)
                        + (session.get("output_tokens", 0) or 0),
    }

    return {
        "title": title,
        "summary_line": summary_line,
        "workflow_steps": formatted_steps,
        "workflow_oneliner": oneliner,
        "stats": stats,
    }
