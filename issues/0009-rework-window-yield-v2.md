# 0009 — Outcomes: rework-window detection → Validated Delivery Yield v2

## TL;DR
- Yield v1 = merged + non-failing checks. v2 adds the "no short-horizon rework" clause from the original metric definition: reverts, reopens, hotfix-follow-ups within a stability window (default 14 days).
- Data sources: GitHub commits (revert message convention + revert PRs), issue/PR reopen events, follow-up PRs touching the same files within the window.

## Scope
Report-side only (GitHub REST). No new capture. Window configurable.

## Context
- v1 lives in `scripts/efficiency-report.ts` (`validatedDeliveryYieldV1`).
- Revert detection precedent: the private importer hashes commit messages and flags `Revert "..."` patterns.

## Acceptance Criteria
- [ ] `--yield-window-days N` flag; yield v2 reported alongside v1 with the delta.
- [ ] A known-reverted PR in a test fixture (or recorded live case) drops out of the v2 numerator.
- [ ] Report names which PRs were excluded and why (revert sha / reopen event link).
