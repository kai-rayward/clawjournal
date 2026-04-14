# Contributing

Thanks for your interest in contributing to ClawJournal.

## Before You Start

- Open an issue first for larger changes, feature work, or behavior changes.
- Keep changes focused. Small, reviewable pull requests are preferred.
- Do not include real user traces, secrets, or private datasets in tests, fixtures, screenshots, or docs.

## Development

```bash
python -m pip install -e ".[dev]"
pytest
```

If you touch the browser workbench, also build the frontend locally:

```bash
cd clawjournal/web/frontend
npm install
npm run build
```

## Tests And Docs

- Add or update tests for behavior changes.
- Keep public docs aligned with the actual supported install path.
- `docs/` in this repo is local-only planning material and is not part of the public source tree.

## Pull Requests

- Describe the user-visible change and the main implementation decision.
- Call out any follow-up work or known limitations directly in the PR description.
- Avoid mixing unrelated refactors into the same change.
