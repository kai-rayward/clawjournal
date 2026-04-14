"""Tests for clawjournal.scoring.depth — depth-level formatting."""

import pytest

from clawjournal.scoring.depth import (
    _command_category,
    _file_type_from_path,
    _parse_test_outcome,
    _truncate_first_sentence,
    extract_workflow_steps,
    format_session_at_depth,
    format_step_text,
    format_workflow_oneliner,
    format_workflow_step,
)


class TestFileTypeFromPath:
    def test_python(self):
        assert _file_type_from_path("src/auth.py") == "python file"

    def test_typescript(self):
        assert _file_type_from_path("app/page.tsx") == "typescript file"

    def test_config_json(self):
        assert _file_type_from_path("config.json") == "config file"

    def test_config_yaml(self):
        assert _file_type_from_path("settings.yaml") == "config file"

    def test_test_file_prefix(self):
        assert _file_type_from_path("test_auth.py") == "test file"

    def test_test_file_suffix(self):
        assert _file_type_from_path("auth_test.py") == "test file"

    def test_test_directory(self):
        assert _file_type_from_path("tests/test_foo.py") == "test file"

    def test_markdown(self):
        assert _file_type_from_path("README.md") == "docs file"

    def test_lockfile(self):
        assert _file_type_from_path("package-lock.json") == "config file"

    def test_unknown(self):
        assert _file_type_from_path("data.xyz") == "file"

    def test_lock_extension(self):
        assert _file_type_from_path("Cargo.lock") == "lockfile"


class TestCommandCategory:
    def test_pytest(self):
        assert _command_category("pytest tests/") == "test"

    def test_git(self):
        assert _command_category("git status") == "vcs"

    def test_npm(self):
        assert _command_category("npm install foo") == "package"

    def test_chained_command(self):
        assert _command_category("cd src && pytest") == "test"

    def test_empty(self):
        assert _command_category("") == "other"

    def test_unknown(self):
        assert _command_category("some-custom-tool") == "other"

    def test_docker(self):
        assert _command_category("docker build .") == "container"

    def test_python_run(self):
        assert _command_category("python main.py") == "run"


class TestParseTestOutcome:
    def test_pytest_passed(self):
        assert _parse_test_outcome("5 passed in 1.2s") == "5/5 passed"

    def test_pytest_mixed(self):
        assert _parse_test_outcome("3 passed, 1 failed") == "3/4 passed"

    def test_no_output(self):
        assert _parse_test_outcome("") is None

    def test_generic_passed(self):
        assert _parse_test_outcome("All tests passed!") == "passed"

    def test_generic_failed(self):
        assert _parse_test_outcome("Error: test failed") == "failed"


class TestTruncateFirstSentence:
    def test_basic(self):
        result = _truncate_first_sentence("Fix the bug in the code.")
        assert result == "Fix the bug in the code."

    def test_long_truncation(self):
        text = " ".join(["word"] * 20)
        result = _truncate_first_sentence(text, max_words=5)
        assert result == "word word word word word..."

    def test_code_replacement(self):
        result = _truncate_first_sentence("Fix the `auth_handler` function")
        assert result == "Fix the [code] function"

    def test_url_replacement(self):
        result = _truncate_first_sentence("Check https://example.com/api for docs")
        assert result == "Check [url] for docs"

    def test_file_path_replacement(self):
        result = _truncate_first_sentence("Edit src/auth/middleware.py")
        assert result == "Edit [file]"

    def test_empty(self):
        assert _truncate_first_sentence("") == ""

    def test_multiline(self):
        result = _truncate_first_sentence("First line\nSecond line")
        assert result == "First line"


class TestExtractWorkflowSteps:
    def test_read_tool(self):
        messages = [
            {
                "role": "assistant",
                "tool_uses": [
                    {
                        "tool": "Read",
                        "input": {"file_path": "/src/auth.py"},
                        "output": {"text": "line1\nline2\nline3"},
                        "status": "success",
                    }
                ],
            }
        ]
        steps = extract_workflow_steps(messages)
        assert len(steps) == 1
        assert steps[0]["tool"] == "read"
        assert steps[0]["detail"] == "/src/auth.py"
        assert steps[0]["lines"] == 3

    def test_edit_tool(self):
        messages = [
            {
                "role": "assistant",
                "tool_uses": [
                    {
                        "tool": "Edit",
                        "input": {
                            "file_path": "/src/auth.py",
                            "old_string": "a\nb",
                            "new_string": "c\nd\ne",
                        },
                        "output": {},
                        "status": "success",
                    }
                ],
            }
        ]
        steps = extract_workflow_steps(messages)
        assert len(steps) == 1
        assert steps[0]["tool"] == "edit"
        assert steps[0]["lines"] == 3  # max of old (2) and new (3)

    def test_bash_test_command(self):
        messages = [
            {
                "role": "assistant",
                "tool_uses": [
                    {
                        "tool": "Bash",
                        "input": {"command": "pytest tests/"},
                        "output": {"text": "5 passed in 1.2s"},
                        "status": "success",
                    }
                ],
            }
        ]
        steps = extract_workflow_steps(messages)
        assert len(steps) == 1
        assert steps[0]["tool"] == "test"
        assert steps[0]["outcome"] == "5/5 passed"

    def test_bash_non_test(self):
        messages = [
            {
                "role": "assistant",
                "tool_uses": [
                    {
                        "tool": "Bash",
                        "input": {"command": "git status"},
                        "output": {"text": "On branch main"},
                        "status": "success",
                    }
                ],
            }
        ]
        steps = extract_workflow_steps(messages)
        assert len(steps) == 1
        assert steps[0]["tool"] == "vcs"

    def test_no_tool_uses(self):
        messages = [{"role": "user", "content": "hello"}]
        steps = extract_workflow_steps(messages)
        assert steps == []

    def test_multiple_steps(self):
        messages = [
            {
                "role": "assistant",
                "tool_uses": [
                    {"tool": "Read", "input": {"file_path": "a.py"}, "output": {"text": "x"}, "status": "success"},
                    {"tool": "Edit", "input": {"file_path": "a.py", "old_string": "x", "new_string": "y"}, "output": {}, "status": "success"},
                ],
            }
        ]
        steps = extract_workflow_steps(messages)
        assert len(steps) == 2


class TestFormatWorkflowStep:
    def test_workflow_depth_read(self):
        step = {"tool": "read", "detail": "/src/auth.py", "lines": 50, "outcome": None, "status": "success"}
        result = format_workflow_step(step, "workflow")
        assert result["detail"] == "python file"
        assert result["lines"] == 50

    def test_workflow_depth_bash(self):
        step = {"tool": "vcs", "detail": "git status", "lines": None, "outcome": None, "status": "success"}
        result = format_workflow_step(step, "workflow")
        assert result["detail"] is None

    def test_summary_depth(self):
        step = {"tool": "read", "detail": "/src/auth.py", "lines": 50, "outcome": None, "status": "success"}
        result = format_workflow_step(step, "summary")
        assert result["detail"] == "python file"

    def test_full_depth(self):
        step = {"tool": "read", "detail": "/src/auth.py", "lines": 50, "outcome": None, "status": "success"}
        result = format_workflow_step(step, "full")
        assert result["detail"] == "/src/auth.py"


class TestFormatStepText:
    def test_read_with_detail(self):
        step = {"tool": "read", "detail": "python file", "lines": 50}
        assert format_step_text(step) == "Read python file (50 lines)"

    def test_test_with_outcome(self):
        step = {"tool": "test", "outcome": "3/4 passed"}
        assert format_step_text(step) == "Test (3/4 passed)"

    def test_tool_only(self):
        step = {"tool": "search"}
        assert format_step_text(step) == "Search"


class TestFormatWorkflowOneliner:
    def test_short(self):
        steps = [{"tool": "read", "detail": "python file"}, {"tool": "edit", "detail": "python file"}]
        result = format_workflow_oneliner(steps)
        assert "→" in result
        assert "Read" in result
        assert "Edit" in result

    def test_truncation(self):
        steps = [{"tool": f"step{i}"} for i in range(10)]
        result = format_workflow_oneliner(steps)
        assert "more" in result

    def test_empty(self):
        assert format_workflow_oneliner([]) == ""


class TestFormatSessionAtDepth:
    @pytest.fixture
    def sample_session(self):
        return {
            "session_id": "test-123",
            "display_title": "Fix auth bug",
            "source": "openclaw",
            "model": "claude-sonnet-4-20250514",
            "duration_seconds": 1380,
            "ai_quality_score": 4,
            "outcome_badge": "tests_passed",
            "user_messages": 8,
            "assistant_messages": 8,
            "tool_uses": 12,
            "input_tokens": 3200,
            "output_tokens": 1000,
            "messages": [
                {"role": "user", "content": "Fix the authentication bug in src/auth.py"},
                {
                    "role": "assistant",
                    "tool_uses": [
                        {"tool": "Read", "input": {"file_path": "src/auth.py"}, "output": {"text": "code\nhere"}, "status": "success"},
                        {"tool": "Edit", "input": {"file_path": "src/auth.py", "old_string": "old", "new_string": "new\nline"}, "output": {}, "status": "success"},
                    ],
                },
                {
                    "role": "assistant",
                    "tool_uses": [
                        {"tool": "Bash", "input": {"command": "pytest tests/"}, "output": {"text": "4 passed"}, "status": "success"},
                    ],
                },
            ],
        }

    def test_workflow_depth(self, sample_session):
        result = format_session_at_depth(sample_session, "workflow")
        assert result["title"] == "Session test-123"  # workflow hides display_title
        assert result["summary_line"] == ""  # no summary at workflow depth
        assert len(result["workflow_steps"]) == 3
        # File paths should be anonymized
        assert result["workflow_steps"][0]["detail"] == "python file"

    def test_summary_depth(self, sample_session):
        result = format_session_at_depth(sample_session, "summary")
        assert result["title"] == "Fix auth bug"
        assert result["summary_line"]  # should have a summary
        assert "authentication" in result["summary_line"].lower()

    def test_full_depth(self, sample_session):
        result = format_session_at_depth(sample_session, "full")
        assert result["title"] == "Fix auth bug"
        assert result["summary_line"]
        # Full depth preserves original paths
        assert result["workflow_steps"][0]["detail"] == "src/auth.py"

    def test_stats(self, sample_session):
        result = format_session_at_depth(sample_session, "summary")
        assert result["stats"]["user_messages"] == 8
        assert result["stats"]["total_tokens"] == 4200
