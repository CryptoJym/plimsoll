# 0008 — Linkage: per-event repo attribution for multi-repo sessions

## TL;DR
- Sessions that touch multiple repos currently attribute everything to one repo (`max(repo_hash)` in session rollups).
- Fix: efficiency report groups by (session, repo_hash) using per-event linkage columns that already exist.

## Scope
`scripts/efficiency-report.ts` aggregation + join only. Capture already stores per-event repo_hash/branch_hash/head_sha.

## Context
- Observed in production on day one: a single orchestrating session carried linkage rows for three different repos within minutes (hooks fire with whatever cwd the session is in).
- Token-bearing events (api_request) carry NO linkage (no cwd on those records) — attribution within a session must apportion token events to the temporally-nearest linked segment, or split pro-rata by linked tool-event counts. Pick one, document the bias.

## Acceptance Criteria
- [ ] Report shows a multi-repo session split across repos with the apportioning rule named in output.
- [ ] Join evidence includes per-repo segment counts so the apportionment is auditable.

## Open Questions
- Apportion by wall-clock segments between linkage changes, or by tool-event proximity? (Segments likely less gameable.)
