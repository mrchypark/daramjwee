## Pre-PR Review Preflight

Before opening a pull request or requesting any remote review, run a proactive
review-prevention pass. The goal is to catch issues that this repository's past
reviewers repeatedly raise before external reviewers spend a pass on them.

Required sequence:

1. Update the remote main reference with `git fetch origin main`.
2. Search Yeoul for prior PR review outcomes, constraints, and repeated
   reviewer patterns for this repository.
3. Run the repo-local historical review report:
   `python3 scripts/pr_review_history_report.py`.
   If the current branch already has a PR, pass `--exclude-pr <number>` so the
   report focuses on past PRs instead of the active review loop.
4. Read the report as two pattern layers:
   - Common language/project patterns from `$CODEX_HOME/review-patterns`, such
     as `$HOME/.codex/review-patterns/common-go.json`.
   - Repository-specific patterns from `.codex/review-patterns`, especially
     `.codex/review-patterns/daramjwee.json`.
   Common patterns capture reusable Go review risks across projects; repository
   patterns capture daramjwee cache/store invariants and reviewer history.
5. Compare the active diff against `origin/main` and proactively address
   recurring review categories that apply locally. Treat the report as a source
   of likely risks, not as authority; verify every finding against the code.
6. Run the smallest meaningful verification for the touched area, plus broader
   tests when the change affects shared behavior.
7. Only after the preflight is clean should the agent create/update a PR or
   request remote review such as `/gemini review`.

The preflight should especially check for recurring themes from past reviews:
context-rich error wrapping, rollback/close/cleanup handling, context
cancellation propagation, race and stale-write behavior, security-sensitive file
permissions, timestamp and path portability, schema migration compatibility,
documentation of operational caveats, and tests that cover real edge behavior.

When a repeated review theme is useful beyond this repository, add it to the
common language pack. When it depends on daramjwee-specific cache/store
contracts, add it to the repo-specific pack. Keep pack entries as review risks
and verification prompts, not as automatic edit instructions.

If a PR already exists, continue to use thread-aware GitHub review state before
declaring review work complete. Flat PR comments alone are not sufficient.
