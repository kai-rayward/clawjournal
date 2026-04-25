# Commands

One section per `events` subcommand. The subcommand list is derived
from `_features.yaml`; the prose below is hand-written and may lag the
list when new tickets land.

## events ingest

Drain pending raw execution events into `{INDEX_DB}`. Reads vendor
JSONLs from each connector's directory (e.g. `~/.claude/projects/`,
`~/.codex/sessions/`) and inserts new rows into `events` /
`event_sessions`. Idempotent on cursor state.

Flags: `--source {auto|<connector>}`, `--json`.

## events inspect

Inspect a single event, its vendor line, and any override. Selects by
`event_id` (positional) or by `--session <key> --event-key <key>`.
Useful for "why did this row classify as X" investigations.

Flags: `--json`, `--truncate N`.

## events capabilities

Dump the wheel-shipped per-client event-type capability matrix as
JSON. Reads `CAPABILITY_MATRIX` directly — the user overlay at
`{HOME}/.clawjournal/capability_overlay.yaml` is **not** applied here
(use `events doctor` to see overlay-aware verdicts; the overlay file
itself is the source of truth for what was added).

## events cost ingest

Extract token usage from already-recorded events and detect anomalies
(cache_read_collapse, input_spike, model_shift, service_tier_shift).
Writes to `token_usage` and `cost_anomalies`.

Flags: `--rebuild`, `--json`.

## events incidents detect

Detect exact-repeat command and tool-call loops. Writes incidents to
the `incidents` table keyed by `(session, kind, first_event_id)`.

## events export

Package a session into a self-describing JSON bundle (events,
overrides, token_usage, cost_anomalies, incidents, optional
source_snippets, manifest). Subject to the same share-time gates as
`bundle-export`: hold-state, project confirmation, findings.

Required: `<session-key>`. Flags: `--out`, `--no-snippets`,
`--no-children`, `--allow-no-workbench-row`, `--yes`, `--pretty` /
`--compact`, `--json`.

## events import

Import a bundle JSON file produced by `events export`. Inserts events
via `INSERT OR IGNORE` against the bundle's identity tuple
`(source, source_path, source_offset, seq)`. Round-trip is identity-
modulo-IDs.

Required: `<bundle.json>`. Flags: `--rebuild-derived`, `--json`.

## events doctor

Diagnose this install. Reports clawjournal version, schema versions
(bundle / recorder / workbench-security), TruffleHog binary status,
clients observed in `event_sessions`, cost-ledger health (when 04 has
run), incidents (when 05 has run), and warnings.

Flags: `--json`, `--fix`, `--request-id <id>`.

Exit codes: 0 (compatible / fresh / events-empty), 1
(partially-compatible / workbench-only), 2 (usage error), 3 (index
DB missing), 5 (index DB unreadable), 6 (unknown-schema), 9
(unspecified).

## events features

Static product feature surface for AI agents. Lists shipped
subcommands, connectors, schema versions, and limits. No user-derived
content.

Flags: `--json` (default), `--request-id <id>`.

## events docs

Topic-based docs corpus. Topics: `guide`, `commands`, `schemas`,
`examples`, `exit-codes`, `errors`. Default mode is markdown;
`--json` returns a structured shape.

Required: `<topic>`. Flags: `--json`, `--request-id <id>`.
