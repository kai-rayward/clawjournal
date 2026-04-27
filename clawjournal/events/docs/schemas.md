# Schemas

JSON-shape contracts for the `events` agent-consumable surfaces.
Each shape is identified by a `*_schema_version` field at its top
level; minor bumps are additive (older consumers ignore unknown
fields), major bumps require consumers to re-fetch and re-validate.

## events doctor --json

```
events_doctor_schema_version: "1.0"
install_state: "fresh"|"workbench-only"|"events-empty"|"db-missing"|"db-corrupt"|"healthy"
install_hint: <human-readable string>
clawjournal_version: <semver>
bundle_schema_version: <semver>
recorder_schema_version: <semver>
security_schema_version: <int|null>
config_dir: <absolute path>
index_db_path: <absolute path>
events_count: <int>
sessions_count: <int>
trufflehog: { state: "present"|"missing"|"unparseable-version", version: <string|null> }
clients: [
  {
    client: <string>,
    client_version: <string>,
    sessions_count: <int>,
    event_types_observed: [<string>, ...],
    unknown_event_types: [<string>, ...],
    schema_unknown_rows: <int>,
    matrix_supported_count: <int>,
    verdict: "compatible"|"partially-compatible"|"unknown-schema"
  },
  ...
]
fs_clients: [<string>, ...]
cost: { token_usage_rows, cost_anomalies_rows, last_event_id, last_event_at } | null
incidents: { counts_by_kind: { <kind>: <int> }, last_event_id } | null
warnings: [ { kind: <string>, message: <string> }, ... ]
_meta: { request_id: <string> }   # only when --request-id is set
```

## events features --json

```
events_features_schema_version: "1.0"
version: <clawjournal semver>
bundle_schema_version: <semver>
recorder_schema_version: <semver>
features: [<feature-id>, ...]       # see _features.yaml
connectors: [<client-name>, ...]     # supported in CAPABILITY_MATRIX
limits: { bundle_soft_limit_bytes: <int> }
_meta: { request_id: <string> }   # only when --request-id is set
```

## events docs <topic> --json

```
events_docs_topic_schema_version: "1.0"
topic: <topic-name>
sections: [ { heading: <string>, body: <markdown> }, ... ]
examples: [ { title: <string>, code: <string> }, ... ]
schemas: [ { name: <string>, version: <semver>, shape: <markdown> }, ... ]
_meta: { request_id: <string> }   # only when --request-id is set
```

## events aggregate --json (also: events incidents aggregate, events cost aggregate)

The same envelope is emitted by all three aggregation subcommands;
``domain`` distinguishes them. ``buckets`` are sorted by primary
metric DESC with deterministic dimension-key ASC tie-break.

```
events_aggregate_schema_version: "1.0"
domain: "events"|"incidents"|"cost"
aggregation: {
  by: [<dim-name>, ...],            # 1-3 dimensions
  metric: [<metric-output-key>, ...],  # e.g. "count", "sum_input_tokens"
  buckets: [
    {
      "key": { <dim-name>: <value-or-null>, ... },
      "count": <int>,                # always present when count is a metric
      "sum_<field>": <int|float>,    # one per sum: metric
      "avg_<field>": <float>         # one per avg: metric
    },
    ...
  ],
  other_count: <int|float>,           # primary-metric value of rows truncated by --limit
  total: <int|float>,                 # primary-metric value over all matching rows
  auto_partitioned_by: "data_source"  # cost domain only, when fired
}
_meta: {
  elapsed_ms: <int>,
  rows_scanned: <int>,                # COUNT(*) of post-WHERE rows
  request_id: <string>                # only when --request-id is set
}
```

`workspace` and `session` bucket-key values that contain
home-rooted absolute paths are anonymized via
``Anonymizer().text()`` before emission (rendered as
``[REDACTED_PATH]`` or ``codex:[REDACTED_PATH]`` for embedded
paths). Plan 10 §Security #2.

## events search --json

```
events_search_schema_version: "1.0"
query: <string>                        # the user's MATCH expression, verbatim
rewritten_match: <string>              # the expression actually bound to MATCH
hits: [
  {
    "event_id": <int>,
    "session_key": <string>,           # anonymized via Anonymizer().text()
    "event_at": <iso-timestamp|null>,
    "client": <string>,
    "type": <string>,
    "confidence": <string>,
    "source": <string>,
    "raw_ref": {
      "source_path": <string>,         # anonymized via Anonymizer().path()
      "source_offset": <int>,
      "seq": <int>
    },
    "snippet": <string>,               # secrets redacted; v0.1 emits no <mark>
    "bm25": <float>,                   # FTS5 relevance, smaller is closer
    "timeline_url": <string>           # clawjournal://session/<key>#event-<id>
  },
  ...
]
_meta: {
  elapsed_ms: <int>,
  rows_matched: <int>,                 # COUNT(*) before --limit truncation
  rows_returned: <int>,                # = len(hits)
  include_held: <bool>,                # echoes the user's --include-held flag
  request_id: <string>                 # only when --request-id is set
}
```

`session_key` and `raw_ref.source_path` are anonymized before
emission. Snippets are run through `clawjournal/redaction/secrets.py`
before emit, so secrets that the regex would catch on export render
as `[SECRET_REDACTED]` here too. Plan 11 §Security #3 + #4.

## structured error envelope

When `--json` is set on the new agent commands and an error occurs:

```
{
  "error": {
    "code": <int>,
    "kind": "index_missing"|"version_incompatible"|"usage_error"|"topic_unknown"|"unspecified",
    "message": <string>,        # anonymized: home-dir paths replaced with [REDACTED_PATH]
    "hint": <string>,           # anonymized
    "retryable": <bool>
  },
  "_meta": { "request_id": <string> }   # only when --request-id is set; top-level
}
```

Note: `_meta` lives at the top level on both success and error
responses so agents only have to check one location.

## bundle (plan 07, summary)

```
bundle_schema_version: "1.0"
recorder_schema_version: "1.0"
session: { session_key, client, started_at, ended_at, status, ... }
events: [ ... ]
event_overrides: [ ... ]
token_usage: [ ... ]
cost_anomalies: [ ... ]
incidents: [ ... ]
capabilities: { ... }
source_snippets: { "<path>:<offset>:<seq>": <text>, ... }
manifest: { sha256, trufflehog: { ... }, redaction_summary: { ... } }
```

See `clawjournal/events/export/bundle.py` for the canonical schema
and `BUNDLE_SOFT_LIMIT_BYTES` for the size warning threshold (50 MB).
