# ClawJournal

Review and curate your coding agent conversation traces — 100% locally. ClawJournal scans session logs from Claude Code, Claude Desktop, Codex, Gemini CLI, OpenCode, OpenClaw, Kimi CLI, and Cline, automatically anonymizes secrets and personal information, and gives you a browser workbench to review everything before it ever leaves your machine.

## Your data stays local

Everything in the default workflow runs on your own computer:

- `scan`, `serve`, `inbox`, `search`, `score`, `export`, and `bundle-export` all run locally.
- The review UI opens on `localhost:8384` in your own browser — no account, no cloud service.
- `bundle-export` writes redacted files to your disk. It does not upload them.
- Uploading is a separate, opt-in flow. If you never configure an ingest endpoint and never run a share command, nothing is sent anywhere.

## If you decide to share

Sharing is fully opt-in and separate from local review. When you do choose to export or upload, ClawJournal runs automatic redaction first (paths, usernames, emails, API keys, tokens, private keys, and similar) and can layer AI-assisted PII review on top.

See [PRIVACY.md](PRIVACY.md) for the full redaction list and the two sharing paths (local file vs. self-configured upload).

---

## Quickstart

### Install from PyPI (recommended)

One line, no Node toolchain, no cloning — the published wheel includes the pre-built browser workbench.

```bash
pipx install clawjournal     # or: pip install clawjournal
clawjournal scan             # Index local sessions from Claude Code, Codex, etc.
clawjournal serve            # Open review UI at http://localhost:8384 (your machine only)
```

Requires Python 3.10+. `pipx` is preferred because it isolates the CLI in its own environment and puts `clawjournal` on your `PATH`.

### Let your coding agent set it up

Prefer to skip the terminal? Paste this into any coding agent (Claude Code, Codex, Cursor, etc.):

```
Install ClawJournal from PyPI (`pipx install clawjournal`), run `clawjournal scan`, and start `clawjournal serve`.
Keep everything local unless I explicitly ask to share data.
```

The agent will install the package, scan your sessions, and open the workbench at `http://localhost:8384`. Nothing leaves your machine.

### Install as a skill for repeat use

If you plan to use ClawJournal more than once, install it as a skill. Works with Claude Code, Codex, Cursor, Gemini CLI, OpenCode, and [many more](https://github.com/nicepkg/skills). Requires Node.js for the one-time `npx` command.

```bash
npx skills add kai-rayward/clawjournal
```

Then inside your coding agent, say:

```text
setup clawjournal
```

The setup skill installs ClawJournal on your machine, scans your local sessions, and launches the workbench in your browser. Nothing is uploaded. It adds:
- **clawjournal-setup** — interactive wizard to install, scan sessions, and launch the workbench
- **clawjournal** — review, triage, and share traces
- **clawjournal-score** — AI-assisted quality scoring

### Build from source (contributors)

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
<summary><b>Node.js required for the browser UI</b></summary>

`clawjournal serve` uses a frontend built from `clawjournal/web/frontend`. Build it once before your first browser launch.

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

If you only use terminal commands such as `scan`, `inbox`, `search`, or `export`, you can skip the frontend build.

</details>

### Supported agents

ClawJournal can parse session data from: Claude Code, Claude Desktop, Codex, Gemini CLI, OpenCode, OpenClaw, Kimi CLI, and Cline.

### Project docs

- [PRIVACY.md](PRIVACY.md) — what stays local, what gets redacted, and how optional sharing works
- [ARCHITECTURE.md](ARCHITECTURE.md) — public architecture overview
- [CONTRIBUTING.md](CONTRIBUTING.md) — contribution guidelines
- [SECURITY.md](SECURITY.md) — security reporting and threat-model scope

---

## Workflow

The core local-review flow runs entirely on your machine. Steps 2-3 are required before anything else; Step 4 is where you actually review traces in the workbench.

```
 Scan ──> Configure ──> Confirm ──> Triage
  1           2            3           4
           required ──────┘
```

### Step 1 — Scan

```bash
clawjournal scan
```

Reads your local session files and indexes them into a SQLite DB in your home directory. Run this first so later steps have data to work with.

### Step 2 — Configure source (required)

```bash
clawjournal config --source all
# Options: claude, codex, gemini, opencode, openclaw, kimi, custom, all
```

### Step 3 — Review and confirm projects (required)

```bash
clawjournal list                                     # See all discovered projects
clawjournal config --exclude "project1,project2"     # Optional: exclude projects
clawjournal config --confirm-projects                # Required: lock in project selection
```

### Step 4 — Triage sessions (recommended)

```bash
# Browse and search
clawjournal inbox --json --limit 20
clawjournal search "refactor auth" --json

# Approve or block
clawjournal approve <session_id> --reason "clean trace"
clawjournal block <session_id> --reason "proprietary code"

# Optional: AI-assisted scoring with the current agent's automation CLI (auto-approves 4-5, blocks 1-2)
clawjournal score --batch --auto-triage
```

By default, `clawjournal score` uses the current agent's automation CLI. For example, inside Codex it uses `codex exec`; inside Claude Code it uses the Claude CLI; inside OpenClaw it uses `openclaw agent --json`. It does not attach to the live interactive session or use Codex subagents. Use `--backend` to override when needed.

For Codex specifically, this follows Codex non-interactive mode: `codex exec` reuses saved CLI authentication by default, and for automation the recommended explicit credential is `CODEX_API_KEY`.

For a visual review experience: `clawjournal serve` opens the workbench on `localhost:8384` on your own machine. `clawjournal serve --remote` prints an SSH tunnel command so you can reach the same local server on a remote VM. Build the frontend once first if you installed from source.

If you later decide to export or share traces, see [PRIVACY.md](PRIVACY.md) and the `export`, `bundle-*`, and `share` commands in the reference below.

---

## Command reference

<details>
<summary><b>All commands</b></summary>

### Essential

| Command | Description |
|---------|-------------|
| `clawjournal scan` | Index local sessions into workbench DB |
| `clawjournal serve` | Open workbench UI at localhost:8384 (after the one-time frontend build) |
| `clawjournal config --source all` | Select source scope — `claude`, `codex`, `gemini`, `opencode`, `openclaw`, `kimi`, `custom`, or `all` (required) |
| `clawjournal config --confirm-projects` | Confirm project selection (required before export) |
| `clawjournal score --batch --auto-triage` | AI-score sessions with the current agent's automation CLI, auto-approve 4-5 and block 1-2 |
| `clawjournal export --pii-review --pii-apply` | Export with PII redaction (recommended before any sharing) |
| `clawjournal bundle-export <bundle_id>` | Export a redacted bundle to disk as `sessions.jsonl` + `manifest.json` |

### Quick share

| Command | Description |
|---------|-------------|
| `clawjournal recent` | Show recent sessions (auto-scans if stale) |
| `clawjournal recent --source openclaw --since today` | Filter by source and time |
| `clawjournal card <id>` | Generate a share card for a session |
| `clawjournal card <id> --depth workflow` | Workflow-only card (safe for public channels) |
| `clawjournal card <id> --depth full` | Full card with redacted content |

### Review & triage

| Command | Description |
|---------|-------------|
| `clawjournal inbox --json --limit 20` | List sessions as JSON (for agent parsing) |
| `clawjournal search <query> --json` | Full-text search across sessions |
| `clawjournal approve <id> [id ...]` | Approve sessions by ID |
| `clawjournal block <id> [id ...]` | Block sessions by ID |
| `clawjournal shortlist <id> [id ...]` | Shortlist sessions for review |
| `clawjournal score --batch --limit 20` | AI-score up to 20 sessions with the current agent's automation CLI |
| `clawjournal score-view <id>` | View score details for a session |
| `clawjournal set-score <id> --quality <1-5>` | Manually set a quality score |

### Bundles

| Command | Description |
|---------|-------------|
| `clawjournal bundle-create --status approved` | Create bundle from all approved sessions |
| `clawjournal bundle-list` | List all bundles |
| `clawjournal bundle-view <bundle_id>` | View bundle details and sessions |
| `clawjournal bundle-export <bundle_id>` | Export bundle to disk (JSONL + manifest) |
| `clawjournal bundle-share <bundle_id>` | Upload bundle to an ingest service (optional) |

### Optional upload

| Command | Description |
|---------|-------------|
| `clawjournal verify-email you@university.edu` | Verify a `.edu` email for upload authorization |
| `clawjournal share --preview --status approved` | Preview what would be shared without uploading |
| `clawjournal share --status approved` | Create a bundle and upload it through the configured ingest service |

### Export & PII

| Command | Description |
|---------|-------------|
| `clawjournal export` | Export to local JSONL |
| `clawjournal export --no-thinking` | Exclude extended thinking blocks |
| `clawjournal export --pii-review --pii-apply` | Export, generate PII findings, and produce sanitized JSONL |
| `clawjournal pii-review --file <file> --output <findings.json>` | Run PII detection on an exported file |
| `clawjournal pii-apply --file <file> --findings <findings.json> --output <sanitized.jsonl>` | Apply PII redactions to an exported file |
| `clawjournal pii-rubric` | Show PII entity types and detection rules |

### Configuration

| Command | Description |
|---------|-------------|
| `clawjournal config --exclude "a,b"` | Add excluded projects (appends) |
| `clawjournal config --redact "str1,str2"` | Add strings to always redact (appends) |
| `clawjournal config --redact-usernames "u1,u2"` | Add usernames to anonymize (appends) |
| `clawjournal list` | List all projects with exclusion status |
| `clawjournal status` | Show current stage and next steps (JSON) |
| `clawjournal update-skill claude` | Install/update the clawjournal skill for a specific agent |
| `clawjournal serve --remote` | Print SSH tunnel command for remote VM access |

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
- **Source and project confirmation are required** — the CLI will block export until both are set.
- **Run PII review before sharing** — automated redaction is good but not foolproof. `--pii-review --pii-apply` adds AI-assisted detection.
- **Large exports take time** — 500+ sessions may take 1-3 minutes.
- **Virtual environment recommended** — modern Linux (and some macOS setups) block system-wide pip installs. Use a venv to avoid issues.

</details>

## Acknowledgments

ClawJournal builds on early work from [dataclaw](https://github.com/peteromallet/dataclaw) by [@peteromallet](https://github.com/peteromallet).

## License

Apache-2.0
