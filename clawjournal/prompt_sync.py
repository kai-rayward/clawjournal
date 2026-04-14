"""Developer helpers for keeping mirrored prompt assets in sync."""

from __future__ import annotations

from pathlib import Path

from .scoring.backends import PROMPTS_DIR


REPO_ROOT = Path(__file__).resolve().parent.parent
SCORING_PROMPT_RUBRIC_FILE = PROMPTS_DIR / "scoring" / "rubric.md"
SCORING_SKILL_RUBRIC_FILE = REPO_ROOT / "skills" / "clawjournal-score" / "RUBRIC.md"


def build_scoring_skill_rubric() -> str:
    """Return the generated skill copy of the scoring rubric."""
    return SCORING_PROMPT_RUBRIC_FILE.read_text(encoding="utf-8")


def sync_scoring_skill_rubric() -> None:
    """Regenerate the scoring skill rubric from the canonical prompt copy."""
    SCORING_SKILL_RUBRIC_FILE.write_text(
        build_scoring_skill_rubric(),
        encoding="utf-8",
    )


def main() -> None:
    """Sync mirrored prompt assets used by distributed skills."""
    sync_scoring_skill_rubric()
    print(
        "Synced "
        f"{SCORING_SKILL_RUBRIC_FILE.relative_to(REPO_ROOT)} "
        "from "
        f"{SCORING_PROMPT_RUBRIC_FILE.relative_to(REPO_ROOT)}"
    )


if __name__ == "__main__":
    main()
