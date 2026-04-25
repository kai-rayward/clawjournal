# Errors

Structured-error envelope shape (when `--json` is set on the new
agent commands and an error occurs):

```
{
  "error": {
    "code": <int>,
    "kind": <string>,
    "message": <string>,
    "hint": <string>,
    "retryable": <bool>
  },
  "_meta": { "request_id": <string> }
}
```

`_meta` is at the top level (alongside `error`), not nested inside —
matching the success-response shape so agents only check one location.

## Closed `kind` enum

Agents can switch on `kind` deterministically. The current set:

- `index_missing` — `~/.clawjournal/index.db` is absent. Expected exit
  code 3. Resolve by running `clawjournal scan`.
- `version_incompatible` — observed client version is ahead of the
  matrix, or an unknown client appeared. Expected exit code 6.
  Resolve by upgrading clawjournal or running `events doctor --fix`
  (only handles additive drift).
- `usage_error` — bad flag, missing required argument. Expected exit
  code 2.
- `topic_unknown` — `events docs <topic>` invoked with an unknown
  topic. Expected exit code 9. Hint enumerates valid topics.
- `unspecified` — catch-all for paths that haven't been classified
  yet. Expected exit code 9. File a bug if you see this in the wild.

Future tickets that introduce new error sites extend this enum and
bump `events_doctor_schema_version` / equivalent. Old consumers
should treat unknown `kind` values as `unspecified`.

## Anonymization

`message` and `hint` are passed through
`clawjournal.redaction.anonymizer.Anonymizer()` before emission. Home-
directory paths render as `[REDACTED_PATH]` and the basename of `~`
as `[REDACTED_USERNAME]`. In environments where `HOME` is not set,
the anonymizer is a no-op and `events doctor` surfaces a top-level
`warnings: [{kind: "home_not_set", ...}]` entry so the user can see
that error messages will leak local paths.

Success-path output is **not** anonymized — the canonical paths
(`~/.clawjournal/index.db`, etc.) intentionally appear verbatim.

## `--request-id` correlation

When `--request-id <id>` is supplied alongside `--json`, the id is
echoed into `_meta.request_id` on both success and error responses.
The id is opaque (any string the caller chooses) and is logged alone
— never paired with the request body — for correlation with audit
trails or upstream telemetry.
