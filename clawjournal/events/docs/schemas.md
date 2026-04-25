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

## structured error envelope

When `--json` is set on the new agent commands and an error occurs:

```
{
  "error": {
    "code": <int>,
    "kind": "index_missing"|"version_incompatible"|"usage_error"|"topic_unknown"|"unspecified",
    "message": <string>,        # anonymized: home-dir paths replaced with [REDACTED_PATH]
    "hint": <string>,           # anonymized
    "retryable": <bool>,
    "_meta": { "request_id": <string> }   # only when --request-id is set
  }
}
```

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
