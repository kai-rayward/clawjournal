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

Caveat: `--limit N` truncates by primary metric DESC across the
whole result, including the prepended `data_source` partition. If
one partition's totals dominate, all top-N buckets may come from
that single partition and the other partition's rows fall into
the `other_count` tail. Use `--where data_source=api` (or
`estimated`) to scope to one partition explicitly, and check
`total - sum-of-bucket-primary-metric` to detect a truncated
partition.

Required: `--by`. Allowed dimensions: `model`, `provider`,
`data_source`, `service_tier`, `pricing_table_version`, `session`,
`workspace`, `date`. Allowed `--where` fields: `model`,
`data_source`, `service_tier`, `pricing_table_version`, `session`,
`workspace` (note: `provider` is a derivable dimension but not a
filterable field; filter by `model` instead). Allowed metrics:
`count`, `sum:<field>`, `avg:<field>` over `input_tokens`,
`output_tokens`, `cache_read_tokens`, `cache_creation_tokens`,
`thinking_tokens`, `cost_estimate`.

Flags: `--metric`, `--where`, `--since`, `--limit`, `--json`,
`--request-id <id>`.

## events incidents detect

Detect exact-repeat command and tool-call loops. Writes incidents to
the `incidents` table keyed by `(session, kind, first_event_id)`.

Flags: `--rebuild`, `--json`.

## events incidents aggregate

Cross-session aggregation over the `incidents` table.

Required: `--by`. Allowed dimensions: `kind`, `confidence`,
`session`, `workspace`, `date`. Allowed `--where` fields: `kind`,
`confidence`, `session`, `workspace`, `created_at`. Metric `count`
(default), or `sum:count` / `avg:count` over the per-incident
`count` column (distinct from the aggregation count).

Flags: `--metric`, `--where`, `--since`, `--limit`, `--json`,
`--request-id <id>`.

## events search

Cross-session full-text search over `events.raw_json` using SQLite
FTS5. Backed by an external-content virtual table `events_fts` in
`{INDEX_DB}`; insert/delete/update triggers on `events` keep it in
lockstep. The schema is bootstrapped on first use — no separate
migration step.

Required: `<query>` (FTS5 MATCH expression — phrase queries
`"tool error"`, AND/OR/NOT, prefix `auth*`, NEAR/N). Empty terms
without operators are AND-ed by FTS5 implicitly. Query is
parameterized (bound to `MATCH ?`) — never string-interpolated.

Tokenizer: `unicode61 remove_diacritics 2 tokenchars '-_'`. Hyphen
and underscore are part of tokens, so `snake_case` and `kebab-case`
index as single tokens; a search for `rate limit` does NOT match
`rate-limit` (tradeoff documented in plan 11 §Open questions).

To search for a bareword that contains a hyphen (e.g. `rate-limit`,
`my-tool`), wrap it in phrase quotes: `events search '"rate-limit"'`.
FTS5's query parser treats unquoted `foo-bar` as a column filter on
the non-existent column `foo` and raises `no such column` — the CLI
maps that error to a usage_error with this hint.

Allowed filter flags: `--client`, `--type`, `--confidence`,
`--session`, `--source` (each repeat or comma-separate; `--session`
takes a single value), `--since Nd|Nh|Nm|today|thisweek`. All
filters land as parameterized predicates.

Hold-state: events from sessions in `pending_review` or active
`embargoed` are excluded by default. Pass `--include-held` to
surface them. Sessions that have not been touched by the workbench
(no `sessions` row) are NOT held — they pass through unchanged.

Output bucket-key values run through `Anonymizer().path()` for
`raw_ref.source_path` and `Anonymizer().text()` for `session_key`,
matching the `events aggregate` treatment. Snippets run through
`redaction/secrets.py` before emit so a fixture with a known secret
in `raw_json` produces a search hit whose snippet shows
`[SECRET_REDACTED]`, not the plaintext (plan 11 §Security #4).

Result-set caps: default `--limit 50`, ceiling 1000. Query string
cap: 4096 bytes (UTF-8). Snippet window: default 16 tokens, range
1-64 (FTS5's documented hard ceiling on `snippet()` is 64 tokens
— values above are silently clamped, so the cap is enforced
locally to fail loudly instead).

Index storage caveat: FTS5 indexes the literal contents of
`events.raw_json`, so anything in the original vendor JSONL line
ends up in `~/.clawjournal/index.db`'s FTS structures. The index
file inherits the same filesystem permissions as the source
JSONL — no new exfiltration surface — but if you redact secrets
from the underlying `events.raw_json` post-hoc, re-run
`events search --rebuild-index` to scrub the FTS table too. Plan 11
§Security #5.

Special modes:
- `--rebuild-index` reindexes `events_fts` from `events` (FTS5's
  documented `'rebuild'` command). Use after `DELETE FROM events_fts`
  or any surgery on `events` that bypassed the triggers.

Flags: `--client`, `--type`, `--confidence`, `--session`,
`--source`, `--since`, `--limit`, `--snippet-tokens`,
`--include-held`, `--rebuild-index`, `--json`, `--request-id <id>`.

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
