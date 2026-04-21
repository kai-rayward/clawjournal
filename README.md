# ClawJournal

Review and curate your coding agent conversation traces — 100% locally. ClawJournal scans session logs from Claude Code, Claude Desktop, Codex, Gemini CLI, OpenCode, OpenClaw, Kimi CLI, and Cline, automatically anonymizes secrets and personal information, and gives you a browser workbench to review everything before it ever leaves your machine.

## Your data stays local

Everything in the default workflow runs on your own computer:

- `scan`, `serve`, `inbox`, `search`, `score`, `export`, and `bundle-export` all run locally.
- The review UI opens on `localhost:8384` in your own browser — no account, no cloud service.
- `scan` auto-runs a secrets + PII findings pipeline per session. Findings are stored as hashed references in your local SQLite DB — plaintext is never persisted.
- `bundle-export` writes redacted files to your disk. It does not upload them.
- Uploading is a separate, opt-in flow. If you never configure an ingest endpoint and never run a share command, nothing is sent anywhere.

## If you decide to share

Sharing is fully opt-in and separate from local review. When you do choose to export or upload, ClawJournal re-applies regex redaction (paths, usernames, emails, API keys, tokens, private keys, and similar) on top of the scan-time findings, and the workbench Share flow adds an AI-assisted PII review on top of that.

The AI-assisted PII review uses the same backend as `score` — your current coding agent's automation CLI (e.g. `codex exec`, the Claude CLI). Home-dir paths and usernames are anonymized locally before anything is sent to the agent; if your agent routes to a cloud provider, that's where the PII review happens. Override with `--backend` to keep the call on a local model.

See [PRIVACY.md](PRIVACY.md) for the full redaction list and the two sharing paths (local file vs. self-configured upload).

---

## Quickstart

Inside any compatible coding agent (Claude Code, Codex, Cursor, Gemini CLI, OpenCode, [many more](https://github.com/nicepkg/skills)):

```bash
npx skills add kai-rayward/clawjournal
```

Then tell the agent: *"setup clawjournal"*. It installs the PyPI package, scans your local sessions with default settings, and opens the workbench at `http://localhost:8384`. Nothing is uploaded.

Prefer the terminal? See Stage 1 in the flow below — every stage shows both skills and shell commands.

---

## End-to-end flow

Six stages from a blank machine to a shared bundle. Each stage shows the skills-first way (inside your coding agent) and the shell-direct way.

```
 Install ──► Configure ──► Scan ──► Triage ──► Score ──► Package & Share
    1            2           3          4          5              6
```

**Three skills are installed by `npx skills add`:**

| Skill | Covers stages |
|-------|---------------|
| **clawjournal-setup** | 1 Install · 3 Scan · workbench launch |
| **clawjournal** | 4 Triage · 6 Package & Share |
| **clawjournal-score** | 5 Score |

Day-to-day, prompts like *"triage my new sessions"*, *"score everything unscored"*, or *"package my approved sessions for export"* route to the right skill automatically.

### 1. Install

**Skills — in your coding agent (Claude Code, Codex, Cursor, …):**

```bash
npx skills add kai-rayward/clawjournal
```

Adds the three ClawJournal skills to your agent. The PyPI package itself is installed in Stage 2 as part of `setup clawjournal`.

**Shell — in any bash/zsh terminal:**

```bash
pipx install clawjournal        # or: pip install clawjournal
```

Requires Python 3.10+. `pipx` is preferred because it isolates the CLI in its own environment and puts `clawjournal` on your `PATH`. The PyPI wheel already includes the pre-built browser workbench — no frontend build required.

**TruffleHog (required for sharing):**

```bash
brew install trufflehog      # macOS; Linux/Windows: see upstream installer
```

Every `bundle-export` and `share` runs an independent secrets scan on the redacted output before the export is considered complete. Exports are blocked if TruffleHog is missing or finds anything. See [PRIVACY.md](PRIVACY.md) for the full gate semantics.

### 2. Configure

Tell ClawJournal which agents' sessions to scan and what to exclude or redact.

**Skills — in your coding agent (Claude Code, Codex, Cursor, …):**

For a first-time run with defaults (all sources, no exclusions), say:

> *"setup clawjournal"*

The skill installs the PyPI package, runs a first scan, and opens the workbench. Later, to narrow scope or add redactions:

> *"Configure clawjournal to scan only claude and codex, exclude the `scratch` project, and always redact the string `acme-internal`."*

Subsequent scans pick up the new settings automatically.

**Shell — in any bash/zsh terminal:**

```bash
clawjournal config --source all                   # claude | codex | gemini | opencode | openclaw | kimi | custom | all
clawjournal list                                  # see discovered projects
clawjournal config --exclude "project1,project2"  # optional: exclude projects
clawjournal config --redact "string1,string2"     # optional: custom redactions (appends)
clawjournal config --redact-usernames "handle1"   # optional: anonymize usernames (appends)
clawjournal config --confirm-projects             # lock in project selection
```

`--exclude`, `--redact`, and `--redact-usernames` all append; they never overwrite. Safe to call repeatedly.

### 3. Scan

Reads your local session files into a SQLite DB and runs a per-session findings pipeline (secrets engine + PII engine). Findings are stored as hashed references — plaintext is never persisted.

**Skills — in your coding agent (Claude Code, Codex, Cursor, …):**

Your agent runs scan as part of `setup clawjournal`. Re-scan any time with: *"scan my sessions again."*

**Shell — in any bash/zsh terminal:**

```bash
clawjournal scan
```

The workbench daemon (`clawjournal serve`) also scans continuously in the background.

### 4. Triage

Approve sessions worth keeping, block the rest. Happens in the workbench (Sessions page) or the CLI.

**Skills — in your coding agent (Claude Code, Codex, Cursor, …):**

> *"Open clawjournal and help me triage the unreviewed sessions."*

**Shell — in any bash/zsh terminal:**

```bash
clawjournal serve                                    # workbench UI — the primary review surface
# or directly in the terminal:
clawjournal inbox --json --limit 20                  # list sessions
clawjournal search "refactor auth" --json            # full-text search
clawjournal approve <session_id> --reason "clean"    # approve
clawjournal block <session_id> --reason "private"    # block
clawjournal shortlist <session_id>                   # mark for deeper review
```

Optional hold-state controls — useful when you want to quarantine a session without blocking it (CLI only):

```bash
clawjournal hold <id> --reason "pending legal review"
clawjournal release <id>
clawjournal embargo <id> --until 2026-06-01
clawjournal hold-history <id>
```

### 5. Score

AI-assisted quality scoring on a 1–5 scale (1 = noise, 5 = excellent). Home-dir paths and usernames are anonymized before anything is sent to the judge.

**Skills — in your coding agent (Claude Code, Codex, Cursor, …):**

> *"Score my unscored sessions."*

This runs through the `clawjournal-score` skill and uses your current agent's automation CLI.

**Shell — in any bash/zsh terminal:**

```bash
clawjournal score --batch --auto-triage              # batch-score; auto-blocks noise (score 1) sessions
clawjournal score-view <id>                          # show score details
clawjournal set-score <id> --quality 4               # manual override
```

`--auto-triage` moves sessions with quality score 1 to `blocked`. Sessions scored 2–5 stay visible for you to decide.

By default scoring uses the current agent's automation CLI (e.g. `codex exec` inside Codex, the Claude CLI inside Claude Code). Use `--backend` to override. For Codex specifically, `codex exec` reuses saved CLI authentication by default; for automation the recommended explicit credential is `CODEX_API_KEY`.

### 6. Package & Share

Bundle approved sessions into a redacted export on disk. Uploading that bundle is a separate, opt-in step.

**Skills — in your coding agent (Claude Code, Codex, Cursor, …):**

> *"Package my approved sessions and export them locally."*
>
> *(optional)* *"Then share the bundle through the ingest service."*

**Workbench — in your browser:**

Open the workbench (`clawjournal serve`) and walk the Share page: **Queue → Redact → Review → Package → Done**. The Redact step layers AI-assisted PII detection on top of the scan-time findings.

**Shell — in any bash/zsh terminal:**

```bash
clawjournal bundle-create --status approved          # bundle all approved sessions
clawjournal bundle-list
clawjournal bundle-view <bundle_id>                  # inspect before exporting
clawjournal bundle-export <bundle_id>                # write sessions.jsonl + manifest.json to disk
```

Optional upload:

```bash
clawjournal verify-email you@university.edu          # one-time email verification
clawjournal share --preview --status approved        # dry-run
clawjournal bundle-share <bundle_id>                 # upload through the configured ingest service
```

Upload is gated on hold-state: only sessions in `auto_redacted` or `released` can leave the machine.

---

## Build from source (contributors)

You only need this path if you're developing ClawJournal itself — the PyPI wheel is the right choice for everyone else.

> Commands below assume a POSIX shell (bash/zsh). On Windows, run them inside WSL or Git Bash. Native PowerShell users: replace `source .venv/bin/activate` with `.venv\Scripts\Activate.ps1`.

```bash
git clone https://github.com/kai-rayward/clawjournal.git
cd clawjournal
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e .

# One-time frontend build for the browser workbench
cd clawjournal/web/frontend
npm install
npm run build
cd ../../..

clawjournal scan
clawjournal serve
```

<details>
<summary><b>Python not installed?</b></summary>

ClawJournal requires Python 3.10+.

| Platform | Install command |
|----------|----------------|
| **macOS** | `brew install python` |
| **Windows** | Download from [python.org/downloads](https://python.org/downloads) — check "Add to PATH" |
| **Linux** | `sudo apt install python3-full` (includes venv support) |

</details>

<details>
<summary><b>Using a virtual environment (recommended)</b></summary>

Modern Linux distributions (Debian 12+, Ubuntu 23.04+) and some macOS setups block system-wide pip installs ([PEP 668](https://peps.python.org/pep-0668/)).

From the repo root:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e .
```

> If you see `externally-managed-environment`, make sure the venv is activated before running `python -m pip`.

</details>

<details>
<summary><b>Node.js required only when building from source</b></summary>

The PyPI wheel ships the pre-built workbench. You only need Node if you're building from source.

| Platform | Install command |
|----------|----------------|
| **macOS** | `brew install node` |
| **Windows** | Download from [nodejs.org](https://nodejs.org) |
| **Linux** | `sudo apt install nodejs npm` |

```bash
cd clawjournal/web/frontend
npm install
npm run build
```

</details>

## Supported agents

ClawJournal can parse session data from: Claude Code, Claude Desktop, Codex, Gemini CLI, OpenCode, OpenClaw, Kimi CLI, and Cline.

## Project docs

- [PRIVACY.md](PRIVACY.md) — what stays local, what gets redacted, and how optional sharing works
- [ARCHITECTURE.md](ARCHITECTURE.md) — public architecture overview
- [CONTRIBUTING.md](CONTRIBUTING.md) — contribution guidelines
- [SECURITY.md](SECURITY.md) — security reporting and threat-model scope

---

## Command reference

<details>
<summary><b>All commands</b></summary>

### Essential

| Command | Description |
|---------|-------------|
| `clawjournal scan` | Index local sessions + run findings pipeline |
| `clawjournal serve` | Open workbench UI at localhost:8384 |
| `clawjournal config --source all` | Select source scope (required) |
| `clawjournal config --confirm-projects` | Confirm project selection (required before export) |
| `clawjournal score --batch --auto-triage` | AI-score sessions; auto-block noise (score 1) |
| `clawjournal bundle-create --status approved` | Bundle approved sessions |
| `clawjournal bundle-export <bundle_id>` | Export bundle to disk as `sessions.jsonl` + `manifest.json` |

### Triage & review

| Command | Description |
|---------|-------------|
| `clawjournal inbox --json --limit 20` | List sessions as JSON |
| `clawjournal search <query> --json` | Full-text search |
| `clawjournal approve <id> [id ...]` | Approve sessions |
| `clawjournal block <id> [id ...]` | Block sessions |
| `clawjournal shortlist <id> [id ...]` | Shortlist sessions |
| `clawjournal score --batch --limit 20` | AI-score up to 20 sessions |
| `clawjournal score-view <id>` | View score details |
| `clawjournal set-score <id> --quality <1-5>` | Manually set a quality score |

### Hold-state gate

| Command | Description |
|---------|-------------|
| `clawjournal hold <id>` | Move session to `pending_review` (blocks upload) |
| `clawjournal release <id>` | Release a held session for share |
| `clawjournal embargo <id> --until <ISO>` | Time-lock a session (auto-releases on expiry) |
| `clawjournal hold-history <id>` | Show the full hold-state timeline |

### Findings & allowlist

| Command | Description |
|---------|-------------|
| `clawjournal findings <id>` | List findings (hashed entities) for a session |
| `clawjournal findings <id> --accept <ref>` | Accept a finding (will be redacted at export) |
| `clawjournal findings <id> --ignore <ref>` | Ignore a finding |
| `clawjournal findings <id> --accept-all` / `--ignore-all` | Bulk decision on open findings |
| `clawjournal allowlist list` | Show global allowlist |
| `clawjournal allowlist add ...` | Allowlist an entity (hashed locally) |
| `clawjournal allowlist remove <id>` | Remove an allowlist entry |

### Bundles

| Command | Description |
|---------|-------------|
| `clawjournal bundle-create --status approved` | Create bundle from all approved sessions |
| `clawjournal bundle-list` | List bundles |
| `clawjournal bundle-view <bundle_id>` | View bundle details |
| `clawjournal bundle-export <bundle_id>` | Export bundle to disk |
| `clawjournal bundle-share <bundle_id>` | Upload via configured ingest service |

### Quick share

| Command | Description |
|---------|-------------|
| `clawjournal recent` | Show recent sessions (auto-scans if stale) |
| `clawjournal recent --source openclaw --since today` | Filter by source and time |
| `clawjournal card <id>` | Generate a share card for a session |
| `clawjournal card <id> --depth workflow` | Workflow-only card (safe for public channels) |
| `clawjournal card <id> --depth full` | Full card with redacted content |

### Optional upload

| Command | Description |
|---------|-------------|
| `clawjournal verify-email you@university.edu` | Verify a `.edu` email for upload authorization |
| `clawjournal share --preview --status approved` | Preview what would be shared without uploading |
| `clawjournal share --status approved` | Create a bundle and upload through the ingest service |

### Configuration

| Command | Description |
|---------|-------------|
| `clawjournal config --exclude "a,b"` | Add excluded projects (appends) |
| `clawjournal config --redact "str1,str2"` | Add strings to always redact (appends) |
| `clawjournal config --redact-usernames "u1,u2"` | Add usernames to anonymize (appends) |
| `clawjournal list` | List all projects with exclusion status |
| `clawjournal status` | Show current stage and next steps (JSON) |
| `clawjournal update-skill <agent>` | Install/update the clawjournal skill for an agent |
| `clawjournal serve --remote` | Print SSH tunnel command for remote VM access |

### Export & sanitize (advanced)

| Command | Description |
|---------|-------------|
| `clawjournal export` | Export to local JSONL |
| `clawjournal export --no-thinking` | Exclude extended thinking blocks |
| `clawjournal export --pii-review --pii-apply` | Legacy LLM-PII path — export + AI-PII review + sanitize |
| `clawjournal pii-review --file <file> --output <findings.json>` | Legacy — run PII detection on an exported file |
| `clawjournal pii-apply --file <file> --findings <findings.json> --output <sanitized.jsonl>` | Legacy — apply PII redactions to an exported file |
| `clawjournal pii-rubric` | Show PII entity types and detection rules |

**Legacy note:** `pii-review` and `pii-apply` remain for AI-based PII review of already-exported files, but deterministic secrets/PII detection has moved to the `findings` + `bundle-export` flow above. Prefer the new path.

</details>

<details>
<summary><b>What gets exported & data schema</b></summary>

| Data | Included | Notes |
|------|----------|-------|
| User messages | Yes | Full text (including voice transcripts) |
| Assistant responses | Yes | Full text output |
| Extended thinking | Yes | Claude's reasoning (opt out with `--no-thinking`) |
| Tool calls | Yes | Tool name + inputs + outputs |
| Token usage | Yes | Input/output tokens per session |
| Model & metadata | Yes | Model name, git branch, timestamps |

Each line in the exported JSONL is one session:

```json
{
  "session_id": "abc-123",
  "project": "my-project",
  "model": "claude-opus-4-6",
  "git_branch": "main",
  "start_time": "2025-06-15T10:00:00+00:00",
  "end_time": "2025-06-15T10:00:00+00:00",
  "messages": [
    {"role": "user", "content": "Fix the login bug", "timestamp": "..."},
    {
      "role": "assistant",
      "content": "I'll investigate the login flow.",
      "thinking": "The user wants me to look at...",
      "tool_uses": [
          {
            "tool": "bash",
            "input": {"command": "grep -r 'login' src/"},
            "output": {"text": "src/auth.py:42: def login(user, password):"},
            "status": "success"
          }
        ],
      "timestamp": "..."
    }
  ],
  "stats": {
    "user_messages": 5, "assistant_messages": 8,
    "tool_uses": 20, "input_tokens": 50000, "output_tokens": 3000
  }
}
```

</details>

<details>
<summary><b>Gotchas</b></summary>

- **`--exclude`, `--redact`, `--redact-usernames` APPEND** — they never overwrite. Safe to call repeatedly.
- **Source and project confirmation are required** — the CLI blocks export until both are set.
- **`scan` already redacts.** Secrets and PII findings are computed and stored as hashed references at scan time. For additional LLM-PII review, use the workbench Share page. The legacy `--pii-review` / `--pii-apply` CLI path still works for sanitizing already-exported files.
- **Hold-state gates uploads.** Sessions in `pending_review` or active `embargoed` cannot be shared; `auto_redacted` (default) and `released` can.
- **Large exports take time** — 500+ sessions may take 1–3 minutes.
- **Virtual environment recommended** — modern Linux (and some macOS setups) block system-wide pip installs. Use a venv to avoid issues.

</details>

## Acknowledgments

ClawJournal builds on early work from [dataclaw](https://github.com/peteromallet/dataclaw) by [@peteromallet](https://github.com/peteromallet).

## License

Apache-2.0
