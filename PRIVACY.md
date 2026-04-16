# Privacy and sharing

ClawJournal is designed to be usable without uploading anything.

## What stays local

- `clawjournal scan`, `serve`, `inbox`, `search`, `score`, `export`, and `bundle-export` run locally.
- The browser workbench is local. If you install from source, `clawjournal serve` opens your own machine at `localhost:8384`.
- `bundle-export` writes files to disk. It does not contact a server.
- If you never configure `CLAWJOURNAL_INGEST_URL` and never run `bundle-share` or `share`, nothing is uploaded.

## Automatic redaction

Local session views (the workbench UI at `localhost:8384`) show session content as it was recorded, including your own home-directory paths and username. Redaction runs at the points where data leaves your machine or goes into an LLM prompt:

- the Share **Redact** step (step 2) and any bundle/export command
- the AI scoring pipeline, before the judge is called

At those boundaries, ClawJournal redacts several classes of sensitive data:

| Type | Result |
|------|--------|
| Home-directory paths | Replaced with `[REDACTED_PATH]` |
| Usernames | Replaced with `[REDACTED_USERNAME]` |
| Email addresses | Replaced with `[REDACTED_EMAIL]` |
| API keys and tokens | Replaced with typed placeholders such as `[REDACTED_OPENAI_KEY]`, `[REDACTED_GITHUB_TOKEN]`, `[REDACTED_JWT]` |
| Database URLs and password-like assignments | Replaced with typed placeholders |
| Private keys | Replaced with `[REDACTED_PRIVATE_KEY]` |
| Public IP addresses | Replaced with `[REDACTED_IP]` |
| Suspicious high-entropy strings | Replaced with `[REDACTED_SECRET]` |
| Export timestamps | Coarsened to hour-level precision |

You can also add custom strings and extra usernames to redact through `clawjournal config`.

## AI-assisted PII review

Automatic secret redaction is useful, but it is not perfect. For higher confidence, run:

```bash
clawjournal export --pii-review --pii-apply
```

That second layer can catch identifying text such as:

- names
- usernames and user IDs
- org names
- private project names
- private URLs and domains
- phone numbers and addresses
- device names and location-like text

Review is still your responsibility before publishing anything.

## What a local bundle contains

`clawjournal bundle-export <bundle_id>` writes:

- `sessions.jsonl`
- `manifest.json`

Depending on how you export, bundle content can include user messages, assistant messages, tool calls, model metadata, token counts, and timestamps. Extended thinking can be excluded from regular exports with `--no-thinking`.

## Optional upload flow

Uploading is a separate path from local export.

- Uploading is disabled unless `CLAWJOURNAL_INGEST_URL` is configured.
- The ingest URL must use `https://`, except for `localhost` and `127.0.0.1` during local development.
- Upload commands are `clawjournal bundle-share <bundle_id>` or `clawjournal share ...`.
- You can inspect what would be included with `clawjournal share --preview --status approved`.

### Email verification

If you use the upload flow, ClawJournal requires:

```bash
clawjournal verify-email you@university.edu
clawjournal verify-email you@university.edu --code <CODE>
```

The `.edu` email is used for verification and short-lived upload authorization. It is not included in the exported bundle itself.

## Practical guidance

- If you only want local review, stop at `scan`, `serve`, `export`, or `bundle-export`.
- If you want to distribute data yourself, use `bundle-export` and share the files however you choose.
- If you want network upload, configure ingest explicitly and treat that as a separate opt-in step.

For security reporting and threat-model scope, see [SECURITY.md](SECURITY.md).
