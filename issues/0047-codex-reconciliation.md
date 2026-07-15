# 0047 — Bounded Codex usage reconciliation

Status: Repair implemented and locally verified on draft PR #92; independent re-verification pending

## TL;DR

Codex OTLP capture no longer performs a synchronous full-ledger reconciliation
before replying. New writes maintain compact unresolved usage, prioritized
candidates, and revisioned pending-window invalidations in the same SQLite
transaction. Session/model context is not duplicated: reconciliation performs
bounded plus-or-minus-ten-minute searches against raw truth through the existing
`idx_events_observed` index. The existing single in-flight scheduler drains
hard-bounded units under a soft deadline and exposes constant-work backlog,
high-water, visit/change, last/max duration, budget-exhaustion, success, and
degraded status.

Tracker: [#91](https://github.com/CryptoJym/plimsoll/issues/91). Parent: #75.
Baseline: `origin/main@efe1b02da0ec433aed8dfe0885e7f26ce4229ff8`.

## Architecture

```text
OTLP / tailer append
       |
       +-- deterministic event insert (duplicate id = no-op)
       +-- unresolved pending index
       +-- priority-0 fresh candidate/window receipts
       +-- priority-1 legacy candidate/window receipts
                         |
                         v
        one serialized hard-unit / soft-time slice
          fresh candidates -> windows -> remaining candidates
                         -> legacy rowid high-water
                         |
              bounded raw observed-time lookup
                (no context mirror or full scan)
                         |
                         v
         one promoted-column + payload UPDATE per changed event
            (outbox/projection triggers see one old/new pair)
```

Repeated context in an active bucket increments its revision without resetting
the live cursor. The current revision drains to a captured pending-row
high-water first; one later pass then covers the newest revision. This prevents
an active bucket from continually restarting at its head and starving tail
rows.

The constructor snapshots the pre-existing `max(rowid)` before later appends.
Legacy discovery reads at most a 500-row hard chunk per query and seeds only
rows that are genuinely unresolved candidates; it neither mirrors historical
context nor creates legacy context-window invalidations. New rows beyond the
high-water enter priority-zero queues through triggers. Legacy discoveries and
repair-generated windows are priority one, so a large historical backlog cannot
delay current usage plus later context.

## Evidence

Focused proof:

`pnpm proof:codex-reconciliation`

The temporary-database proof verifies:

- a real loopback Codex OTLP request over 50,000 irrelevant legacy rows visits
  zero reconciliation rows and only appends/enqueues;
- separate 300,000-row scan shapes: sparse history completed in four productive
  cycles and projected the surveyed 4,810,030-row ledger to 65 cycles; dense
  context with only 33 genuine candidates completed in three cycles and
  projected to 49. These projections are shape-specific, not universal;
- the verifier-equivalent mixed fixture contains 200,000 context-bearing rows
  and 200,000 genuine candidates. Its five observed default slices were
  bounded by 63.41 ms in the latest checkpoint run (61.94–63.41 ms maximum
  across corrected repeats). Fresh usage first failed closed without context,
  then resolved in the first cycle after later context while the legacy
  high-water remained incomplete. Because this
  workload performs real candidate repairs, no scan-only completion projection
  is claimed;
- that mixed fixture used 6.15–6.77% as many side-table/index page bytes as the
  raw table across corrected checkpoint runs. Its side rows were only
  genuine unresolved pending/candidate/window state; no context table or
  context indexes existed;
- all four nearest-context `EXPLAIN QUERY PLAN` variants report `SEARCH
  buffered_events USING INDEX idx_events_observed (observed_at>? AND
  observed_at<?)` and no raw-table scan. SQLite uses a bounded temporary sort
  for the event-ID tie term inside that time range;
- four equal-distance context rows inserted forward and in exact reverse order
  converge to identical session/model/cost after duplicate replay and reopen.
  Before-time ties use `observed_at desc, id desc`; after-time ties use
  `observed_at asc, id asc`; equal-distance side winners use the smaller event
  ID;
- backlog greater than one slice, later context, same-bucket repeated context,
  duplicate append, transaction rollback, reopen, scheduler overlap, and
  unchanged rerun;
- all seven prior unresolved rows converge with exactly one event update each;
  and
- compact reconciliation tables contain no payload/content/path/URL/email/token
  columns, while singleton status reports the measured last/max internal slice
  duration and whether the soft deadline was exhausted.

Resource proof adds `reconciliationRowsVisited` and a wired
`bounded_codex_reconciliation` scenario. Current local receipt: eight pass,
zero fail, four sibling-dependent `not_wired`, one optional skipped. This lane
does not claim the integrated #81 gate ready.

## Acceptance criteria

- [x] OTLP request path performs no reconciliation history query.
- [x] Candidate, pending, and revisioned window state is durable, idempotent,
  crash-resumable, priority-fair, and hard-unit bounded.
- [x] Historical context is not mirrored; nearest context uses bounded raw
  observed-time index searches.
- [x] Later session/model context revisits prior unresolved usage within the
  existing plus-or-minus-ten-minute rule.
- [x] Legacy discovery seeds only genuine candidates through a bounded rowid
  high-water walk. Sparse, dense-context, and candidate-dense measurements are
  reported separately without a universal cadence claim.
- [x] Each changed event uses one SQL update so downstream transaction triggers
  observe one old/new pair; duplicates are no-ops.
- [x] Status is a singleton read with backlog, cursor/high-water, visits,
  changes, last/max slice duration, deadline exhaustion, last success, and
  enumerated degraded reason.
- [x] Focused, full, maintenance, resource, typecheck, and CLI build gates pass
  under Node 22 on temporary resources.
- [ ] Freeze the repaired PR head and obtain a fresh independent exact-head
  verification before merge. #80 integration remains a later serial rebase
  gate owned by the parent orchestrator.

## Operational boundaries

No live ledger, installed collector, LaunchAgent, provider, cloud, credential,
or deployment mutation occurred. ADR-0001 remains proposed pending James's
acceptance; this implementation does not accept that owner decision.
