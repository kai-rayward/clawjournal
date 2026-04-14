# Security Policy

ClawJournal processes coding-agent conversation logs that may contain secrets, credentials, proprietary code, and personal information. Treat trace data as sensitive by default.

## Scope

Security issues include:

- secret or credential leakage
- PII redaction failures
- path or username anonymization failures
- unintended network transmission of local trace data
- browser workbench exposures that bypass the intended local-only model

## Reporting

Do not post sensitive exploit details, real secrets, or private trace samples in a public issue.

Preferred path:

1. Use GitHub private vulnerability reporting for this repository if it is enabled.
2. If private reporting is not available, open a minimal public issue requesting a secure contact path without including exploit details or sensitive data.

## Handling Guidance

- Reproduce with synthetic data whenever possible.
- Redact all secrets, tokens, emails, usernames, and local paths before sharing logs.
- If the issue affects exported bundles, describe whether it impacts local export, hosted/self-hosted share flows, or both.

## Non-Goals

- Support questions and feature requests are not security reports.
- Findings that require a deliberately self-hosted ingest deployment should clearly state that assumption.
