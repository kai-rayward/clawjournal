---
name: clawjournal-score
description: Score coding agent sessions for quality. Use when the user asks to score a session, score sessions, re-score, auto-score, batch score, or evaluate traces. Triggers on "score sessions", "rate my traces", "quality check", or scoring requests.
metadata:
  argument-hint: "<session-id>"
---

# Score Sessions

## Quick Path: Batch Auto-Score

If no session ID was provided, suggest the automated approach first:

```bash
# Score all unscored sessions automatically (recommended)
clawjournal score --batch --auto-triage --limit 20

# Or without auto-triage:
clawjournal score --batch --limit 20
```

For hands-on scoring of a specific session, continue below.

## Session Data

Run this to view the session:

```bash
clawjournal score-view <session-id>
```

If no session ID was provided, list available sessions:

```bash
clawjournal score --batch --limit 10
```

## Scoring Rubric (1-5)

**5 = Excellent** — Clear non-trivial coding task. Successful verified outcome (tests pass, code compiles). Rich tool usage with multi-step problem-solving. Demonstrates patterns worth learning from.

**4 = Good** — Clear task with useful outcome. Some tool usage and verification. Reasonable conversation quality.

**3 = Average** — Understandable but routine task. Partial or unverified outcome. Basic conversation with limited tool usage.

**2 = Low** — Vague or trivial task. Failed/unclear outcome. Minimal meaningful interaction.

**1 = Poor** — No discernible coding task. Trivially short or broken session. Zero training data value.

### Evaluation dimensions
- **INTENT**: Is there a clear coding task? Would a reader understand the goal?
- **OUTCOME**: Did the task succeed? Were results verified (tests, build, manual check)?
- **SUBSTANCE**: Enough back-and-forth? Meaningful tool usage? Not trivial?
- **AGENT QUALITY**: Reasonable approaches? Good tool choices? Handles errors well?

### Detailed rubric

See `RUBRIC.md` in this directory for the full scoring rubric with examples. In the repo, this file is generated from `clawjournal/prompts/agents/scoring/rubric.md`; edit the canonical prompt copy first, then run `python -m clawjournal.prompt_sync`.
- How to read user feedback signals (positive, negative, process criticism, redirects)
- Discovered vs caused failures — distinguishing pre-existing bugs from agent-introduced ones
- Outcome and Intent sub-scores (1-5 each)
- Taste detection (option selection, style feedback)
- Five worked examples covering common scoring scenarios

## Store the Score

After scoring, save it:

```bash
clawjournal set-score <session-id> --quality <score> --reason "<1-2 sentence explanation>"
```
