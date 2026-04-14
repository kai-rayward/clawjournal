---
name: clawjournal-setup
description: Install ClawJournal, scan coding agent sessions, and launch the review workbench. Use when user wants to set up clawjournal, review their traces, get started with trace curation, or says "setup clawjournal". Triggers on "setup clawjournal", "install clawjournal", "review my traces", or first-time clawjournal requests.
---

# ClawJournal Setup

Interactive setup wizard. Walk the user through each step. Only pause when user input is required. Fix problems yourself when possible.

**Principle:** When something is broken or missing, fix it. Don't tell the user to go fix it themselves unless it genuinely requires their action. If a dependency is missing, install it. If a command fails, diagnose and repair.

## 0. Preflight

Check if `clawjournal` is already installed:
- `which clawjournal && clawjournal --version`
- `test -x ~/.clawjournal-venv/bin/clawjournal && ~/.clawjournal-venv/bin/clawjournal --version`

**If found:** Skip to Step 2. If only `~/.clawjournal-venv/bin/clawjournal` exists, use that full path for commands below.
**If not found:** Continue to Step 1.

## 1. Install

Check Python environment:
- `python3 --version`

If Python not found or < 3.10:
- Ask: "Python 3.10+ is required. Would you like me to install it?"
- macOS: `brew install python`
- Linux: `sudo apt-get install -y python3 python3-full`
- Windows: direct user to python.org/downloads

Install clawjournal from GitHub. The snippets below assume a POSIX shell. On native Windows (no WSL / Git Bash), translate each step to the equivalent PowerShell commands before running them.

1. Clone or update the repo:
   ```bash
   if [ -d ~/clawjournal/.git ]; then
     git -C ~/clawjournal pull --ff-only
   else
     git clone https://github.com/kai-rayward/clawjournal.git ~/clawjournal
   fi
   ```
2. Create a venv and install editable:
   ```bash
   python3 -m venv ~/.clawjournal-venv
   ~/.clawjournal-venv/bin/python -m pip install -e ~/clawjournal
   ```
3. If the user wants the browser UI, ensure Node.js/npm is available and build the frontend once:
   ```bash
   cd ~/clawjournal/clawjournal/web/frontend
   npm install
   npm run build
   cd ~/clawjournal
   ```

If `node` / `npm` is missing and the user wants the browser UI:
- Ask: "Node.js is required for the browser workbench. Would you like me to install it?"
- macOS: `brew install node`
- Linux: `sudo apt-get install -y nodejs npm`
- Windows: direct user to nodejs.org

Verify:
- `~/.clawjournal-venv/bin/clawjournal --version`

If verification fails, read the error and fix. Common issues:
- `clawjournal` not on PATH — use `~/.clawjournal-venv/bin/clawjournal` directly
- repo clone missing or stale — verify `~/clawjournal` exists and retry `python -m pip install -e ~/clawjournal`

## 2. Scan Sessions

Discover all local coding agent sessions:

```bash
~/.clawjournal-venv/bin/clawjournal scan
```

This indexes sessions from Claude Code, Codex, Gemini CLI, OpenCode, OpenClaw, Kimi CLI, and Cline — whatever is present on the machine.

Show the user a summary: "Found N sessions across M sources."

If zero sessions found:
- Check if the user has any supported coding agents installed
- Common issue: sessions live in non-default paths — ask the user

## 3. Score & Auto-Triage (optional but recommended)

Ask: "Would you like me to auto-score your sessions? This uses AI to rate quality 1-5 and auto-approve high-quality traces."

If yes:

```bash
~/.clawjournal-venv/bin/clawjournal score --batch --auto-triage
```

Show summary: "N auto-approved (quality 4-5), M auto-blocked (1-2), K need review."

If no: skip to Step 4.

## 4. Launch Workbench

Ask: "How would you like to review your traces?"

**Option A — Browser UI (recommended for local machines):**

Build the frontend first if `~/clawjournal/clawjournal/web/frontend/dist/index.html` is missing:

```bash
cd ~/clawjournal/clawjournal/web/frontend
npm install
npm run build
cd ~/clawjournal
```

```bash
~/.clawjournal-venv/bin/clawjournal serve
```

Tell the user: "Your workbench is open at localhost:8384. Everything is 100% local. Use the Inbox to triage traces, Search to find sessions, and Bundles to assemble exports."

**Option B — Terminal review (for remote VMs or headless environments):**

```bash
~/.clawjournal-venv/bin/clawjournal inbox --json --limit 15
```

Parse the JSON and present traces as a numbered list. Then guide triage interactively.

**For remote VMs:** `clawjournal serve --remote` prints the SSH tunnel command.

## 5. Done

Show summary:
- ClawJournal version installed
- Number of sessions indexed
- Number scored/triaged (if applicable)
- How to access the workbench

Tell the user:
- "You can review and share traces anytime with `/clawjournal`"
- "Score sessions with `/clawjournal-score`"
- "Everything stays 100% local until you explicitly choose to share"

## Troubleshooting

**clawjournal command not found after install:** Use `~/.clawjournal-venv/bin/clawjournal` directly, or add the venv bin directory to your shell PATH.

**No sessions found:** Make sure you've used a supported coding agent (Claude Code, Codex, Gemini CLI, etc.) on this machine. Sessions are stored in agent-specific directories under your home folder.

**Permission errors on scan:** ClawJournal reads session files from `~/.claude/`, `~/.codex/`, etc. Ensure these directories are readable.

**Browser UI shows a placeholder page:** The frontend has not been built yet. Run `cd ~/clawjournal/clawjournal/web/frontend && npm install && npm run build`.

**venv issues on Linux:** If you see `externally-managed-environment`, make sure you're installing into the venv: `python3 -m venv ~/.clawjournal-venv && ~/.clawjournal-venv/bin/python -m pip install -e ~/clawjournal`.
