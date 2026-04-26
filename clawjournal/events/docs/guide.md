# Quickstart for AI agents

`clawjournal` is a local-first execution recorder for coding-agent
sessions. The `events` subcommands give an AI agent a stable,
versioned interface to a user's session history and the current
install's health.

## Detecting capability before you act

```
clawjournal events features --json
clawjournal events doctor --json
```

`events features` is static (no DB read); use it to learn which
subcommands exist and what schema versions you can rely on. `events
doctor` is dynamic (reads `{INDEX_DB}`); use it to learn the install's
state — fresh, partially compatible, schema-drifted, or healthy.

If `events doctor` returns exit code 3 (`index_missing`), the user
hasn't run `clawjournal scan` yet — surface that as a setup step, not
a failure.

## End-to-end recorder workflow

1. Ingest pending vendor JSONLs into the local index:
   `clawjournal events ingest --json`
2. Inspect specific events when investigating a session:
   `clawjournal events inspect <event-id> --json`
3. Compute cost ledger and detect cost anomalies:
   `clawjournal events cost ingest --json`
4. Detect repeat-loop incidents:
   `clawjournal events incidents detect --json`
5. Package a session for archival or bug reports:
   `clawjournal events export <session-key> --out bundle.json`
6. Re-hydrate a bundle in another install:
   `clawjournal events import bundle.json`

## When in doubt

- Run `clawjournal events doctor` and read the warnings array.
- Read the `commands` topic for one section per subcommand:
  `clawjournal events docs commands`.
- Read the `errors` topic for the closed `kind` enum:
  `clawjournal events docs errors`.

All `events` subcommands accept `--json` for structured output.
