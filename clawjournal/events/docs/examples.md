# Examples

Runnable command sequences for common workflows. Substitute
`{HOME}` for your home directory and `{INDEX_DB}` for
`{HOME}/.clawjournal/index.db` when copy-pasting.

## Diagnose, then ingest

```
clawjournal events doctor
clawjournal events ingest --json
clawjournal events doctor --json
```

If the first invocation reports `install_state: "fresh"`, run
`clawjournal scan` first to populate the workbench schema.

## Inspect a single event

```
clawjournal events ingest
clawjournal events inspect 1234 --json
```

Or by session + event_key:

```
clawjournal events inspect --session "claude:my-app:abc-123" \
  --event-key "tool_call:tu-1" --json
```

## Compute costs and detect anomalies

```
clawjournal events cost ingest --json
clawjournal events doctor --json | jq '.cost'
```

## Detect repeat-loop incidents

```
clawjournal events incidents detect --json
clawjournal events doctor --json | jq '.incidents.counts_by_kind'
```

## Export a session as a bundle, then import elsewhere

```
clawjournal events export "claude:my-app:abc-123" --out bundle.json
clawjournal events import bundle.json
clawjournal events import bundle.json --rebuild-derived
```

## Read agent-consumable docs

```
clawjournal events features --json
clawjournal events docs commands
clawjournal events docs schemas --json
clawjournal events docs errors --json
```

## Capability overlay (when adding additive drift)

```
clawjournal events capabilities | jq '.claude'
# … upstream client adds a new known-supported event type …
$EDITOR {HOME}/.clawjournal/capability_overlay.yaml
clawjournal events doctor
```

The overlay merges over the shipped matrix on next read.
