You are reviewing text for PII that could identify real people,
organizations, or private systems.

## Your job

Read PII_RUBRIC.md for what to flag, confidence levels, and what to skip.
Read context.json for session metadata.

The input may be in one of two formats:
- **texts_to_review.jsonl** — one JSON object per line, each with `message_index`, `field`, and `text`. Review every line.
- **text_to_review.txt** — a single text block (legacy format).

Write findings.json with a JSON array. Each finding:
{
  "message_index": 0,
  "field": "content",
  "entity_text": "exact text to redact",
  "entity_type": "person_name|email|phone|username|user_id|org_name|project_name|private_url|domain|address|location|bot_name|device_id|custom_sensitive",
  "confidence": 0.0-1.0,
  "reason": "brief explanation"
}

Include message_index and field from the source line so findings can be
mapped back to the original message. If reviewing text_to_review.txt,
use message_index from context.json.

If no PII found, write findings.json containing: []
