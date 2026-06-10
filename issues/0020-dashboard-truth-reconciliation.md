# 0020 — Dashboard truth: reconcile views against ledger + vendor records, fix what falls out

## TL;DR
- Phase A.1 reconciliation (evidence/2026-06-10T21-25-26-000Z-reconciliation.md): cross-view
  totals agree, codex capture is bit-exact when connected, claude capture is complete since
  the v2 cutover — but `/api/sessions` 500s on every request and session→account attribution
  follows lexicographic `max(hash)`, parking ~$486 of real spend on a retired v1-era identity.
- Fix: sessions query aggregates-then-joins; sessions/repos/accounts attribute by event-dominant
  repo and cost-dominant account; proof gains 5 dashboard-soundness checks that fail on the old
  shapes (fail-demo artifact in evidence/).

## Context
- `/api/sessions` broken since `987a771` ("misuse of aggregate function max()" — correlated
  label subquery). Four commits and two PRs merged green because no proof executed dashboard reads.
  `collector.err.log` shows the rejection repeating while the dashboard lamp stayed green.
- v1→v2 swap at 2026-06-10T16:00Z split one human across hash forms (`d007d3…` backfill,
  `406685…` live); 5 straddle sessions carried $486.27.

## Acceptance Criteria
- [x] `pnpm proof` green with new checks: `dashboard_sessions_query_executes`,
      `session_attribution_follows_dominant_cost`, `session_list_resolves_dominant_repo`,
      `dashboard_detail_queries_execute`, `cross_view_costs_reconcile`.
- [x] Same checks fail against the pre-fix queries (evidence/2026-06-10T21-22-54-290Z).
- [x] Live `/api/accounts` matches event-level truth (406685 ≈ $594 vs d007d3 ≈ $67 at 21:25Z).
- [x] Reconciliation note committed under evidence/.

## Operational Boundaries
- `pnpm proof` stays green; no schema changes; no upload-body changes (privacy checks untouched).

## Notes For Future Agents
- Cost rides api_request rows which carry NO repo columns — session-grain repo dominance can
  only weight by event count. Account dominance weights by cost (cost rows carry the account).
- The sanitizer re-hash is unsalted `sha256(value)[:16]` (policy.ts) — proof fixtures can rely
  on stored-hash sort order being stable.
- Spun out: 0021 capture-health alarm, 0022 codex rollout tailer, 0023 account continuity,
  0024 cache-write tokens.
