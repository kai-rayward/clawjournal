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

## events aggregate

Cross-session aggregation over the `events` table. Emits top-N
buckets with counts and an `other_count` tail. Bucket keys for the
`workspace` dimension are anonymized via
`clawjournal.redaction.anonymizer.Anonymizer().path()` —
home-rooted absolute paths render as the literal `[REDACTED_PATH]`
placeholder (consistent with every other share-time anonymized
field). Non-path workspace segments (e.g. claude project names)
pass through unchanged.

Required: `--by <dim>[,<dim>...]` (up to 3 dimensions). Allowed
dimensions: `client`, `type`, `confidence`, `source`, `lossiness`,
`session`, `workspace`, `date`, `hour`. `date` buckets by
`YYYY-MM-DD` (UTC); `hour` buckets by `YYYY-MM-DDTHH` (UTC).
Allowed `--where` fields: `client`, `type`, `confidence`, `source`,
`session`, `workspace`, `event_at`. Operators: `=`, `!=`, `>`,
`>=`, `<`, `<=`, `in:v1|v2|...`.

Metrics: `count` (default). The events domain does not currently
expose numeric metric fields, so `sum:<field>` / `avg:<field>` are
parsed but always reject — those metrics are useful on the cost
domain (`events cost aggregate`) and the incidents `count` column
(`events incidents aggregate`).

Flags: `--metric` (default `count`), `--where`, `--since
Nd|Nh|Nm|today|thisweek`, `--limit N` (default 10, ceiling 1000),
`--canonical` (reserved; raises a usage error in v0.1 — see plan 10
§Canonical vs raw for the deferred wire-up), `--json`,
`--request-id <id>`.

## events cost ingest

Extract token usage from already-recorded events and detect anomalies
(cache_read_collapse, input_spike, model_shift, service_tier_shift).
Writes to `token_usage` and `cost_anomalies`.

Flags: `--rebuild`, `--json`.

## events cost aggregate

Cross-session aggregation over `token_usage`. **Auto-partitions by
`data_source`** when neither `--by data_source` nor
`--where data_source=...` is set, so API truth and local estimates
never silently mix in a sum.

Required: `--by`. Allowed dimensions: `model`, `provider`,
`data_source`, `service_tier`, `pricing_table_version`, `session`,
`workspace`, `date`. Allowed metrics: `count`, `sum:<field>`,
`avg:<field>` over `input_tokens`, `output_tokens`,
`cache_read_tokens`, `cache_creation_tokens`, `thinking_tokens`,
`cost_estimate`.

Flags: `--metric`, `--where`, `--since`, `--limit`, `--json`,
`--request-id <id>`.

## events incidents detect

Detect exact-repeat command and tool-call loops. Writes incidents to
the `incidents` table keyed by `(session, kind, first_event_id)`.

Flags: `--rebuild`, `--json`.

## events incidents aggregate

Cross-session aggregation over the `incidents` table.

Required: `--by`. Allowed dimensions: `kind`, `confidence`,
`session`, `workspace`, `date`. Metric `count` (default), or
`sum:count` / `avg:count` over the per-incident `count` column
(distinct from the aggregation count).

Flags: `--metric`, `--where`, `--since`, `--limit`, `--json`,
`--request-id <id>`.

## events export

Package a session into a self-describing JSON bundle (events,
overrides, token_usage, cost_anomalies, incidents, optional
source_snippets, manifest). Subject to the same share-time gates as
`bundle-export`: hold-state, project confirmation, findings.

Required: `<session-key>`. Flags: `--out`, `--no-snippets`,
`--no-children`, `--allow-no-workbench-row`, `--pretty` /
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
content. Always emits JSON — there is no `--json` flag.

Flags: `--request-id <id>`.

## events docs

Topic-based docs corpus. Topics: `guide`, `commands`, `schemas`,
`examples`, `exit-codes`, `errors`. Default mode is markdown;
`--json` returns a structured shape.

Required: `<topic>`. Flags: `--json`, `--request-id <id>`.
