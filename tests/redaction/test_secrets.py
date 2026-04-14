"""Tests for clawjournal.redaction.secrets — secret detection and redaction."""

import pytest

from clawjournal.redaction.secrets import (
    CONFIDENCE,
    REDACTED,
    SECRET_PLACEHOLDER,
    SECRET_PATTERNS,
    _check_user_allowlist,
    _has_mixed_char_types,
    _shannon_entropy,
    redact_custom_strings,
    redact_session,
    redact_text,
    scan_text,
)


# --- _shannon_entropy ---


class TestShannonEntropy:
    def test_empty_string(self):
        assert _shannon_entropy("") == 0.0

    def test_single_char(self):
        assert _shannon_entropy("a") == 0.0

    def test_repeated_char(self):
        assert _shannon_entropy("aaaa") == 0.0

    def test_two_equal_chars(self):
        # "ab" -> each has prob 0.5 -> entropy = 1.0
        assert _shannon_entropy("ab") == pytest.approx(1.0)

    def test_four_distinct_chars(self):
        # "abcd" -> each prob 0.25 -> entropy = 2.0
        assert _shannon_entropy("abcd") == pytest.approx(2.0)

    def test_high_entropy_random_string(self):
        # A realistic high-entropy string
        s = "aB3xZ9qR2mK7pL4wN8yJ5tF1hG6"
        assert _shannon_entropy(s) > 3.5

    def test_low_entropy_repetitive(self):
        s = "aaabbb"
        assert _shannon_entropy(s) < 1.5


# --- _has_mixed_char_types ---


class TestHasMixedCharTypes:
    def test_upper_only(self):
        assert _has_mixed_char_types("ABCDEF") is False

    def test_lower_only(self):
        assert _has_mixed_char_types("abcdef") is False

    def test_digit_only(self):
        assert _has_mixed_char_types("123456") is False

    def test_upper_lower_no_digit(self):
        assert _has_mixed_char_types("AbCdEf") is False

    def test_upper_digit_no_lower(self):
        assert _has_mixed_char_types("ABC123") is False

    def test_lower_digit_no_upper(self):
        assert _has_mixed_char_types("abc123") is False

    def test_mixed_all_three(self):
        assert _has_mixed_char_types("aB3xZ9") is True

    def test_empty_string(self):
        assert _has_mixed_char_types("") is False


# --- scan_text ---


class TestScanText:
    def test_empty_text(self):
        assert scan_text("") == []

    def test_no_secrets(self):
        assert scan_text("Hello, this is normal text.") == []

    def test_jwt_token(self):
        jwt = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"
        findings = scan_text(jwt)
        assert any(f["type"] == "jwt" for f in findings)

    def test_jwt_partial(self):
        partial = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9eyJzdWI"
        findings = scan_text(partial)
        assert any(f["type"] in ("jwt", "jwt_partial") for f in findings)

    def test_db_url(self):
        url = "postgres://myuser:s3cretP4ss@db.example.com:5432/mydb"
        findings = scan_text(url)
        assert any(f["type"] == "db_url" for f in findings)

    def test_anthropic_key(self):
        key = "sk-ant-api03-abcdefghijklmnopqrstuvwxyz"
        findings = scan_text(key)
        assert any(f["type"] == "anthropic_key" for f in findings)

    def test_openai_key(self):
        key = "sk-" + "a" * 48
        findings = scan_text(key)
        assert any(f["type"] == "openai_key" for f in findings)

    def test_hf_token(self):
        token = "hf_" + "a" * 30
        findings = scan_text(token)
        assert any(f["type"] == "hf_token" for f in findings)

    def test_github_token(self):
        token = "ghp_" + "a" * 36
        findings = scan_text(token)
        assert any(f["type"] == "github_token" for f in findings)

    def test_pypi_token(self):
        token = "pypi-" + "a" * 60
        findings = scan_text(token)
        assert any(f["type"] == "pypi_token" for f in findings)

    def test_npm_token(self):
        token = "npm_" + "a" * 36
        findings = scan_text(token)
        assert any(f["type"] == "npm_token" for f in findings)

    def test_aws_key(self):
        key = "AKIAIOSFODNN7EXAMPLE"
        findings = scan_text(key)
        assert any(f["type"] == "aws_key" for f in findings)

    def test_aws_secret(self):
        text = "aws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        findings = scan_text(text)
        assert any(f["type"] == "aws_secret" for f in findings)

    def test_slack_token(self):
        token = "xoxb-" + "1234567890-" * 3 + "abcdef"
        findings = scan_text(token)
        assert any(f["type"] == "slack_token" for f in findings)

    def test_discord_webhook(self):
        url = "https://discord.com/api/webhooks/1234567890/abcdefghijklmnopqrstuvwxyz1234"
        findings = scan_text(url)
        assert any(f["type"] == "discord_webhook" for f in findings)

    def test_private_key(self):
        key = "-----BEGIN RSA PRIVATE KEY-----\nMIIEpAIB...\n-----END RSA PRIVATE KEY-----"
        findings = scan_text(key)
        assert any(f["type"] == "private_key" for f in findings)

    def test_cli_token_flag(self):
        text = "mycli --token abcdefghijklmnop"
        findings = scan_text(text)
        assert any(f["type"] == "cli_token_flag" for f in findings)

    def test_env_secret(self):
        text = 'SECRET="my_very_secret_value_here"'
        findings = scan_text(text)
        assert any(f["type"] == "env_secret" for f in findings)

    def test_generic_secret(self):
        text = 'api_key = "aB3xZ9qR2mK7pL4wN8yJ5tF"'
        findings = scan_text(text)
        assert any(f["type"] == "generic_secret" for f in findings)

    def test_bearer_token(self):
        jwt = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"
        text = f"Authorization: Bearer {jwt}"
        findings = scan_text(text)
        assert any(f["type"] in ("bearer", "jwt") for f in findings)

    def test_ip_address(self):
        text = "Server at 203.0.113.42 is down"
        findings = scan_text(text)
        assert any(f["type"] == "ip_address" for f in findings)

    def test_url_token(self):
        text = "https://api.example.com?key=aB3xZ9qR2mK7"
        findings = scan_text(text)
        assert any(f["type"] == "url_token" for f in findings)

    def test_email(self):
        text = "Contact support@company.com for help"
        findings = scan_text(text)
        assert any(f["type"] == "email" for f in findings)

    def test_high_entropy_string(self):
        # Quoted string with high entropy, mixed chars, no dots, >= 40 chars
        s = "aB3xZ9qR2mK7pL4wN8yJ5tF1hG6cD0eW2vU8iOkX"
        assert len(s) >= 40
        assert _has_mixed_char_types(s)
        assert _shannon_entropy(s) >= 3.5
        assert s.count(".") <= 2
        text = f'key = "{s}"'
        findings = scan_text(text)
        assert any(f["type"] == "high_entropy" for f in findings)


# --- Allowlist ---


class TestAllowlist:
    def test_noreply_email(self):
        text = "From noreply@example.com"
        findings = scan_text(text)
        # noreply@ should be allowlisted
        assert not any(f["type"] == "email" and "noreply" in f["match"] for f in findings)

    def test_example_com_email(self):
        text = "user@example.com"
        findings = scan_text(text)
        assert not any(f["type"] == "email" and "example.com" in f["match"] for f in findings)

    def test_private_ip_192(self):
        text = "Host is at 192.168.1.100"
        findings = scan_text(text)
        assert not any(f["type"] == "ip_address" for f in findings)

    def test_private_ip_10(self):
        text = "Host is at 10.0.0.1"
        findings = scan_text(text)
        assert not any(f["type"] == "ip_address" for f in findings)

    def test_private_ip_172(self):
        text = "Host is at 172.16.0.1"
        findings = scan_text(text)
        assert not any(f["type"] == "ip_address" for f in findings)

    def test_pytest_decorator(self):
        text = "@pytest.mark.parametrize"
        findings = scan_text(text)
        assert not any(f["type"] == "email" for f in findings)

    def test_example_db_url(self):
        text = "postgres://user:pass@localhost:5432/mydb"
        findings = scan_text(text)
        assert not any(f["type"] == "db_url" for f in findings)

    def test_example_db_url_username_password(self):
        text = "postgres://username:password@localhost:5432/mydb"
        findings = scan_text(text)
        assert not any(f["type"] == "db_url" for f in findings)

    def test_google_dns_allowlisted(self):
        text = "DNS: 8.8.8.8"
        findings = scan_text(text)
        assert not any(f["type"] == "ip_address" for f in findings)

    def test_cloudflare_dns_allowlisted(self):
        text = "DNS: 1.1.1.1"
        findings = scan_text(text)
        assert not any(f["type"] == "ip_address" for f in findings)

    def test_anthropic_email(self):
        text = "noreply@anthropic.com"
        findings = scan_text(text)
        assert not any(f["type"] == "email" and "anthropic.com" in f["match"] for f in findings)

    def test_app_decorator_not_email(self):
        text = "@app.route('/api')"
        findings = scan_text(text)
        assert not any(f["type"] == "email" for f in findings)


# --- redact_text ---


class TestRedactText:
    def test_no_secrets(self):
        text = "Hello world, no secrets here."
        result, count, _log = redact_text(text)
        assert result == text
        assert count == 0

    def test_empty_text(self):
        text, count, _log = redact_text("")
        assert text == ""
        assert count == 0

    def test_single_secret(self):
        key = "sk-ant-api03-abcdefghijklmnopqrstuvwxyz"
        text = f"My key is {key}"
        result, count, _log = redact_text(text)
        assert "[REDACTED_ANTHROPIC_KEY]" in result
        assert key not in result
        assert count == 1

    def test_multiple_secrets(self):
        text = "Key: sk-ant-api03-abcdefghijklmnopqrstuvwxyz and email: user@company.com"
        result, count, _log = redact_text(text)
        assert count >= 2
        assert "sk-ant-" not in result
        assert "user@company.com" not in result

    def test_overlapping_matches(self):
        # JWT contains both jwt and jwt_partial patterns - dedup should handle
        jwt = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"
        result, count, _log = redact_text(jwt)
        assert jwt not in result
        assert count >= 1

    def test_none_text(self):
        result, count, log = redact_text(None)
        assert result is None
        assert count == 0
        assert log == []


# --- redact_custom_strings ---


class TestRedactCustomStrings:
    def test_empty_text(self):
        result, count = redact_custom_strings("", ["secret"])
        assert result == ""
        assert count == 0

    def test_empty_strings_list(self):
        result, count = redact_custom_strings("hello secret", [])
        assert result == "hello secret"
        assert count == 0

    def test_short_string_skipped(self):
        result, count = redact_custom_strings("ab cd", ["ab"])
        assert result == "ab cd"
        assert count == 0

    def test_word_boundary_matching(self):
        result, count = redact_custom_strings("my secret_domain.com is here", ["secret_domain.com"])
        assert "[REDACTED_CUSTOM]" in result
        assert count == 1

    def test_multiple_replacements(self):
        result, count = redact_custom_strings(
            "foo myname bar myname baz", ["myname"]
        )
        assert "myname" not in result
        assert count == 2

    def test_none_text(self):
        result, count = redact_custom_strings(None, ["secret"])
        assert result is None
        assert count == 0

    def test_none_strings(self):
        result, count = redact_custom_strings("hello", None)
        assert result == "hello"
        assert count == 0

    def test_3_char_string_no_word_boundary(self):
        # len(target) == 3, uses escaped (no word boundary)
        result, count = redact_custom_strings("fooabc bar abc", ["abc"])
        # With no word boundary for 3-char, should match in "fooabc" as escaped substring
        assert count >= 1


# --- redact_session ---


class TestRedactSession:
    def test_empty_messages(self):
        session = {"messages": []}
        result, count, _log = redact_session(session)
        assert result["messages"] == []
        assert count == 0

    def test_redacts_content(self):
        session = {
            "messages": [
                {"content": "Key: sk-ant-api03-abcdefghijklmnopqrstuvwxyz"},
            ]
        }
        result, count, _log = redact_session(session)
        assert "[REDACTED_ANTHROPIC_KEY]" in result["messages"][0]["content"]
        assert count >= 1

    def test_redacts_thinking(self):
        session = {
            "messages": [
                {"thinking": "The key is sk-ant-api03-abcdefghijklmnopqrstuvwxyz"},
            ]
        }
        result, count, _log = redact_session(session)
        assert "[REDACTED_ANTHROPIC_KEY]" in result["messages"][0]["thinking"]
        assert count >= 1

    def test_redacts_tool_use_input(self):
        session = {
            "messages": [
                {
                    "tool_uses": [
                        {"input": "sk-ant-api03-abcdefghijklmnopqrstuvwxyz"},
                    ]
                },
            ]
        }
        result, count, _log = redact_session(session)
        assert "[REDACTED_ANTHROPIC_KEY]" in result["messages"][0]["tool_uses"][0]["input"]
        assert count >= 1

    def test_redacts_secrets_in_tool_use_input_dict(self):
        session = {
            "messages": [
                {
                    "tool_uses": [
                        {
                            "input": {
                                "file_path": "test.py",
                                "old_string": "key = 'sk-ant-api03-abcdefghijklmnopqrstuvwxyz'",
                                "new_string": "key = os.environ['API_KEY']",
                            },
                        }
                    ],
                },
            ]
        }
        result, count, _log = redact_session(session)
        inp = result["messages"][0]["tool_uses"][0]["input"]
        assert "sk-ant-" not in inp["old_string"]
        assert "[REDACTED_ANTHROPIC_KEY]" in inp["old_string"]
        assert count >= 1

    def test_custom_strings_redacted(self):
        session = {
            "messages": [
                {"content": "My company is Acme Corp and we use Acme Corp tools"},
            ]
        }
        result, count, _log = redact_session(session, custom_strings=["Acme Corp"])
        assert "Acme Corp" not in result["messages"][0]["content"]
        assert count >= 1

    def test_no_content_fields_skipped(self):
        session = {
            "messages": [
                {"role": "user"},  # no content, thinking, or tool_uses
            ]
        }
        result, count, _log = redact_session(session)
        assert count == 0

    def test_none_content_skipped(self):
        session = {
            "messages": [
                {"content": None, "thinking": None},
            ]
        }
        result, count, _log = redact_session(session)
        assert count == 0


# --- Confidence ---


class TestConfidence:
    def test_scan_text_includes_confidence(self):
        key = "sk-ant-api03-abcdefghijklmnopqrstuvwxyz"
        findings = scan_text(key)
        assert len(findings) >= 1
        assert "confidence" in findings[0]
        assert isinstance(findings[0]["confidence"], float)

    def test_high_confidence_patterns(self):
        """Known prefixed patterns should have confidence >= 0.90."""
        cases = [
            ("sk-ant-api03-abcdefghijklmnopqrstuvwxyz", "anthropic_key"),
            ("ghp_" + "a" * 36, "github_token"),
            ("hf_" + "a" * 30, "hf_token"),
        ]
        for text, expected_type in cases:
            findings = scan_text(text)
            match = next(f for f in findings if f["type"] == expected_type)
            assert match["confidence"] >= 0.90, f"{expected_type} should be high confidence"

    def test_medium_confidence_patterns(self):
        """Structural patterns should have 0.70 <= confidence < 0.90."""
        text = "postgres://myuser:s3cretP4ss@db.example.com:5432/mydb"
        findings = scan_text(text)
        match = next(f for f in findings if f["type"] == "db_url")
        assert 0.70 <= match["confidence"] < 0.90

    def test_low_confidence_patterns(self):
        """Heuristic patterns should have confidence < 0.70."""
        text = "Server at 203.0.113.42 is down"
        findings = scan_text(text)
        match = next(f for f in findings if f["type"] == "ip_address")
        assert match["confidence"] < 0.70

    def test_all_pattern_types_have_confidence(self):
        """Every pattern type in CONFIDENCE map should be a float in [0, 1]."""
        for ptype, conf in CONFIDENCE.items():
            assert 0.0 <= conf <= 1.0, f"{ptype} confidence {conf} out of range"

    def test_every_secret_pattern_has_confidence(self):
        """Every pattern in SECRET_PATTERNS must have an entry in CONFIDENCE."""
        for name, _pattern in SECRET_PATTERNS:
            assert name in CONFIDENCE, (
                f"Pattern '{name}' in SECRET_PATTERNS has no CONFIDENCE entry — "
                f"it would silently fall back to 0.5"
            )

    def test_redact_text_returns_log(self):
        key = "sk-ant-api03-abcdefghijklmnopqrstuvwxyz"
        text = f"My key is {key}"
        _result, count, log = redact_text(text)
        assert count == 1
        assert len(log) == 1
        assert log[0]["type"] == "anthropic_key"
        assert log[0]["confidence"] == 0.98
        assert log[0]["original_length"] == len(key)

    def test_log_does_not_contain_match_text(self):
        """Log entries must not leak the actual secret."""
        key = "sk-ant-api03-abcdefghijklmnopqrstuvwxyz"
        _result, _count, log = redact_text(key)
        for entry in log:
            assert "match" not in entry
            for v in entry.values():
                if isinstance(v, str):
                    assert key not in v

    def test_log_context_for_low_confidence(self):
        """Low-confidence findings should include context_before/context_after."""
        text = "Server at 203.0.113.42 is down"
        _result, _count, log = redact_text(text)
        ip_entry = next(e for e in log if e["type"] == "ip_address")
        assert "context_before" in ip_entry
        assert "context_after" in ip_entry

    def test_no_context_for_high_confidence(self):
        """High-confidence findings should NOT include context."""
        key = "sk-ant-api03-abcdefghijklmnopqrstuvwxyz"
        _result, _count, log = redact_text(key)
        entry = log[0]
        assert "context_before" not in entry
        assert "context_after" not in entry

    def test_redact_session_log_has_field(self):
        session = {
            "messages": [
                {"content": "Key: sk-ant-api03-abcdefghijklmnopqrstuvwxyz"},
            ]
        }
        _result, _count, log = redact_session(session)
        assert len(log) >= 1
        assert log[0]["field"] == "content"
        assert log[0]["message_index"] == 0

    def test_redact_session_log_tool_input(self):
        session = {
            "messages": [
                {
                    "tool_uses": [
                        {"input": "sk-ant-api03-abcdefghijklmnopqrstuvwxyz"},
                    ]
                },
            ]
        }
        _result, _count, log = redact_session(session)
        assert any(e["field"] == "tool_input" for e in log)


# --- User Allowlist ---


class TestUserAllowlist:
    def test_exact_match_skips_finding(self):
        text = "Server at 203.0.113.42 is down"
        allowlist = [{"type": "exact", "text": "203.0.113.42"}]
        findings = scan_text(text, user_allowlist=allowlist)
        assert not any(f["match"] == "203.0.113.42" for f in findings)

    def test_exact_match_no_effect_on_other(self):
        text = "IPs: 203.0.113.42 and 198.51.100.1"
        allowlist = [{"type": "exact", "text": "203.0.113.42"}]
        findings = scan_text(text, user_allowlist=allowlist)
        assert any(f["match"] == "198.51.100.1" for f in findings)

    def test_pattern_match_skips(self):
        text = "Server at 203.0.113.42 is down"
        allowlist = [{"type": "pattern", "regex": r"203\.0\.113\.\d+"}]
        findings = scan_text(text, user_allowlist=allowlist)
        assert not any(f["type"] == "ip_address" for f in findings)

    def test_category_match_skips(self):
        text = "Server at 203.0.113.42 is down"
        allowlist = [{"type": "category", "match_type": "ip_address"}]
        findings = scan_text(text, user_allowlist=allowlist)
        assert not any(f["type"] == "ip_address" for f in findings)

    def test_allowlist_propagated_through_redact_session(self):
        session = {
            "messages": [
                {"content": "Server at 203.0.113.42 is down"},
            ]
        }
        allowlist = [{"type": "exact", "text": "203.0.113.42"}]
        result, count, log = redact_session(session, user_allowlist=allowlist)
        # The IP should NOT be redacted
        assert "203.0.113.42" in result["messages"][0]["content"]

    def test_allowlist_precedence_over_pattern(self):
        """Allowlist should prevent redaction even when pattern matches."""
        email = "important@partner.com"
        text = f"Contact {email} for details"
        allowlist = [{"type": "exact", "text": email}]
        findings = scan_text(text, user_allowlist=allowlist)
        assert not any(f["match"] == email for f in findings)

    def test_check_user_allowlist_empty(self):
        assert _check_user_allowlist("test", "email", None) is False
        assert _check_user_allowlist("test", "email", []) is False

    def test_invalid_regex_does_not_crash(self):
        """A malformed regex in user config should not crash the scan."""
        text = "Server at 203.0.113.42 is down"
        allowlist = [{"type": "pattern", "regex": "[unclosed"}]
        # Should not raise — the invalid entry is silently skipped
        findings = scan_text(text, user_allowlist=allowlist)
        assert any(f["type"] == "ip_address" for f in findings)
