## TL;DR

- The release proof now owns a deterministic, injectable clock for every dashboard-window fixture.
- The actual dashboard summary, session, repository, and priority-bucket queries are exercised at dates two years apart in UTC and America/Denver.
- Production time, retention, and sync semantics remain untouched; the injected `Date.now()` is scoped and restored inside the proof.

## Scope

Test-only changes in `scripts/signal-fidelity-proof.ts`. No collector production behavior, package configuration, live data, or service state changes.

## Context

- GitHub: https://github.com/CryptoJym/plimsoll/issues/82
- Parent: #75
- Resource gate: #81
- Baseline: `origin/main@9fc0af4cb59b01245f7a1862ba1647a152c8b537`
- Trace: `46be3ad1-514a-42d2-9f14-2212fdab14dc`

## Problem / Task

Fixed June 2026 telemetry rows aged out of the dashboard's 30-day queries. A clean `pnpm proof` therefore changed from green to red as wall-clock time advanced, even though production behavior had not regressed.

Centralize a proof clock, derive window-sensitive fixtures from it, and make calendar and timezone invariance part of the release proof.

## Evidence

Baseline failure on 2026-07-15 included:

```text
dashboard_summary_reads_ledger
priority_buckets_computed
dashboard_sessions_query_executes
cross_view_costs_reconcile
```

## Acceptance Criteria

- [x] Window-sensitive fixtures derive from one injected proof clock.
- [x] Dashboard window queries pass at `2026-06-15T12:00:00.000Z` and `2028-06-15T12:00:00.000Z`.
- [x] The same query signatures pass in `UTC` and `America/Denver`.
- [x] The proof restores the real clock before retention and session-sync gates.
- [ ] Draft PR checks pass on GitHub.

## Operational Boundaries

No production collector code, runtime clock, live HOME, database, service, provider, or deployment changes. `pnpm proof` must remain green and no raw content may persist in metadata mode.

## Notes For Future Agents

Do not globally fake `Date.now()` for the whole proof. Retention and session-sync checks intentionally use real process time; the proof clock belongs only around window-sensitive dashboard checks and the isolated regression fixture.
