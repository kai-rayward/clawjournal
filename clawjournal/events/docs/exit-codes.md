# Exit codes

The new agent-consumable commands (`events doctor`, `events features`,
`events docs`) follow the schedule below. Other `events` subcommands
keep their current 0/1/2 behavior until a follow-up ticket retrofits
them.

| Code | Meaning                                  | Where it surfaces |
|------|------------------------------------------|-------------------|
| 0    | Success                                  | All three commands |
| 1    | Health check failed (partial compatibility) | `events doctor` |
| 2    | Usage error                              | All three (bad flag, unknown topic) |
| 3    | Index/DB missing                         | `events doctor` |
| 5    | Data corruption (`index.db` unreadable)  | `events doctor` |
| 6    | Incompatible version                     | `events doctor` (client ahead of matrix, or unknown client) |
| 9    | Unknown / unspecified                    | All three (catch-all) |

Codes 4 (network), 7 (lock/busy), 8 (partial result) are reserved for
the broader retrofit and are not emitted by these commands today.

## events doctor exit-code mapping

- `compatible` install or `events-empty` install or fresh install → 0
- `partially-compatible` (known client, missing fields) or
  `workbench-only` state (events tables missing) → 1
- `unknown-schema` (client emits an event type not in `EVENT_TYPES`)
  → 6
- `~/.clawjournal/` exists but `index.db` missing → 3
- `index.db` exists but unreadable → 5
- bad flag → 2
- anything unclassified → 9

## events docs exit-code mapping

- known topic → 0
- missing topic argument → 2
- unknown topic → 9 (with a hint listing valid topics)

## events features exit-code mapping

- success → 0
- bad flag → 2
