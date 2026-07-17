# 0051 — Outcomes v2: immutable commits, checks, reviews, and correction lineage

GitHub mirror: https://github.com/CryptoJym/plimsoll/issues/99

## TL;DR
- Preserve the revision timeline that current-state upserts erase: PR commits, every check attempt, review outcomes, merge, reopen, revert, and linked issues.
- Derive first-pass success, same-SHA retry, correction loops, time-to-green, review rounds, and true rework timestamps.
- Keep collection explicit/incremental and coverage-aware rather than a daemon-wide GitHub polling loop.

## Scope
GitHub backfill/recovery command, immutable local/shared contracts, ETag/cursor state, and derivations. Hosted persistence is paired separately.

## Acceptance Criteria
- [ ] Stable external IDs exist for commit, check attempt, review, PR, issue, merge, reopen, and revert facts; duplicate/out-of-order replay is idempotent.
- [ ] Failed SHA → same-SHA pass is a retry/flaky episode; failed SHA → newer SHA → pass is one correction loop.
- [ ] Changes-requested → newer revision → approval forms a review correction cycle.
- [ ] Green multi-commit work is not mislabeled rework merely because commit count is high.
- [ ] Revert uses the actual revert timestamp and structured/full-SHA evidence; inside/outside-window fixtures classify correctly.
- [ ] Pagination, GitHub 500, rate exhaustion, and missing required-check policy emit explicit incomplete/unknown coverage.
- [ ] One incremental run processes bounded changed PRs, persists cursor/ETag/rate receipt, and resumes deterministically.
- [ ] No PR title/body/diff/path is required for canonical metrics or uploaded by default.

## Operational Boundaries
- GitHub token stays operator/provider-side; never stored in the telemetry ledger.
- Background collection is not added to the local collector request path.
