# 0061 — Maintenance deadline starves repo-context enrichment; join denominator is 13/2,423 sessions

## TL;DR
- Session→PR economics are blocked by the **denominator**, not the join: only 13 of 2,423 ledger sessions carry any repo/branch/sha linkage (2 for this repo).
- Cause: the maintenance child is deadline-killed at a hardcoded 30s (`cli.ts:1116`) — `maintenance_deadline_exceeded` × 652 in `collector.err.log` — so `git_context` enrichment and the reprice drain starve.
- The quarantine misattributes: it blames the candidate on stage at kill time, holds only one candidate, and expires in 15 minutes, so the loop repeats.
- The join itself is proven good: 2 real joined rows (PR #178 ↔ session, $774.68 priced; PR #138 ↔ session) via merge-sha against the production ledger.

## Scope
Make the maintenance cycle complete often enough that repo-context enrichment and repricing drain, so session linkage coverage grows and fleet cost-per-merged-PR becomes publishable with an honest denominator. Explicitly not covered: the learning-fact tables program (unwired writers, see docs/agent-economics-join-audit-2026-08-20.md §b), cloud sync, pricing catalog (fixed separately).

## Context
- Production: launchd `com.plimsoll.collector` runs `pnpm collector start` from the repo checkout; ledger `~/Library/Application Support/plimsoll/work-ledger.sqlite` (7.6 GB, WAL).
- Deadline: `deadlineMs: 30_000` hardcoded at `packages/collector-cli/src/cli.ts:1116`; boundary clamps to ≤60s (`maintenance-boundary.ts:847`).
- Child cycle: `runRecent()` (capture scan + cursor/event commits) → child repo-context fills → `resolveMaintenanceRepoContexts` → result frame (`maintenance-worker.ts:224–270`).
- Quarantine: single candidate, set on timeout (`maintenance-boundary.ts:751–752`), cleared after `escalatedCircuitMs` (`:319–321`); skip check in `resolveMaintenanceRepoContexts` (`maintenance-worker.ts:52`).
- Audit trail: docs/agent-economics-join-audit-2026-08-20.md.

## Problem / Task
The dashboard's repo/branch surfaces and any cost-per-merged-PR number are hollow while 99.5% of sessions have no linkage. Enrichment backlog at audit time: 48,280 `repo_context_event_links` rows `fill_pending=1`, 709 sessions in `repo_enrichment_dirty`, 0 inflight, 93 `repo_context_results` ever. Reprice queue 46,457 (drains only inside completed maintenance cycles).

## Evidence
- `grep -c maintenance_deadline_exceeded collector.err.log` → 652.
- `/status` at 2026-08-20 ~11:47 MDT: `circuit_open`, `skippedJobs: 31`, `lastResult: {rawEventWrites: 0, rolloutFilesRead: 2}`, `lastDurationMs: 30001`, quarantine `{source: codex, stage: git_context, candidateHash: sha256:e03c5460…}`.
- Candidate cwd identified by brute-force hash match: `~/Documents/Codex/Projects/new-reward/namaste-poodles-visibility-report` — but stat/listdir/read `.git/HEAD` on that path all complete in milliseconds, so the stage label is misattribution, not the hang.
- Linkage coverage (read-only SQL): claude_code 32/306,147 events with repo_hash; codex 184/1,891,989; sessions 13/2,423.
- Recent codex rollouts include 55–325 MB files; last cycle read 2 rollout files and wrote 0 events before the kill.

## Acceptance Criteria
- [ ] A maintenance receipt (or boundary status) reports per-stage elapsed time, so the actual 30s consumer inside `runRecent()` is named with numbers, not inferred.
- [ ] Maintenance cycles complete on this machine's real backlog: `maintenance_deadline_exceeded` stops accruing over a 24h window (allow isolated kills, no circuit-open steady state).
- [ ] `repo_context_event_links` `fill_pending=1` count falls monotonically across a day and `repo_enrichment_dirty` drains below 100.
- [ ] Session linkage coverage rises materially (target: >50% of sessions active after the fix date carry repo linkage; historical sessions best-effort).
- [ ] `pnpm proof:maintenance` and `pnpm proof:maintenance-boundary` stay green.

## Operational Boundaries
- Privacy invariants hold: raw paths never cross the child boundary; candidate identity stays hashed.
- No unbounded per-cycle work: whatever ends the starvation must keep the bounded-duty-cycle guarantees (#152/#160/#178 lineage).
- The production collector is not stopped/reconfigured for diagnosis; changes land via PR + owner-scheduled restart.
