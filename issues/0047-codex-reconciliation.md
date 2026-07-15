# 0047 — Bounded Codex usage reconciliation

Status: Source implemented and locally verified; rebase/PR pending parent merge order

## TL;DR

Codex OTLP capture no longer performs a synchronous full-ledger reconciliation
before replying. New writes maintain compact unresolved-candidate and
session/model-context indexes in the same SQLite transaction. The existing
single in-flight maintenance scheduler drains fixed row/time slices and exposes
constant-work backlog, high-water, visit/change, success, and degraded status.

Tracker: [#91](https://github.com/CryptoJym/plimsoll/issues/91). Parent: #75.
Baseline: `origin/main@41fe678f5ee3163a1996f4f7663fe3b21c768405`.

## Architecture

```text
OTLP / tailer append
       |
       +-- deterministic event insert (duplicate id = no-op)
       +-- unresolved pending index + candidate queue
       +-- compact session/model context index
       +-- coalesced 10-minute context-window revision
                         |
                         v
        one serialized fixed row/time maintenance slice
          legacy rowid high-water -> window cursor -> candidates
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

Legacy discovery snapshots `max(rowid)` and reads bounded rowid chunks. It does
not build a new raw-ledger index in the constructor or run a filtered query
whose `LIMIT` can hide millions of irrelevant rows. New rows arriving beyond
the high-water already enter the compact queues through triggers.

## Evidence

Focused proof:

`pnpm proof:codex-reconciliation`

The temporary-database proof verifies:

- a real loopback Codex OTLP request over 50,000 irrelevant legacy rows visits
  zero reconciliation rows and only appends/enqueues;
- 300,000 legacy irrelevant rows complete in three default slices (100,000 per
  one-minute cycle, under the 50 ms target on repeated measured runs), projecting the surveyed
  4,810,030-row ledger to 49 cycles;
- backlog greater than one slice, later context, same-bucket repeated context,
  duplicate append, transaction rollback, reopen, scheduler overlap, and
  unchanged rerun;
- all seven prior unresolved rows converge with exactly one event update each;
  and
- compact reconciliation tables contain no payload/content/path/URL/email/token
  columns.

Resource proof adds `reconciliationRowsVisited` and a wired
`bounded_codex_reconciliation` scenario. Current local receipt: eight pass,
zero fail, four sibling-dependent `not_wired`, one optional skipped. This lane
does not claim the integrated #81 gate ready.

## Acceptance criteria

- [x] OTLP request path performs no reconciliation history query.
- [x] Candidate, pending, context, and revisioned window state is durable,
  idempotent, crash-resumable, and fixed-slice.
- [x] Later session/model context revisits prior unresolved usage within the
  existing plus-or-minus-ten-minute rule.
- [x] Legacy discovery is a bounded rowid high-water walk and has a measured
  useful completion cadence.
- [x] Each changed event uses one SQL update so downstream transaction triggers
  observe one old/new pair; duplicates are no-ops.
- [x] Status is a singleton read with backlog, cursor/high-water, visits,
  changes, last success, and enumerated degraded reason.
- [x] Focused, full, maintenance, resource, typecheck, and CLI build gates pass
  under Node 22 on temporary resources.
- [ ] Rebase after #79/#80 merge order is known, resolve shared buffer,
  maintenance, server, resource-counter, and signal-proof seams, then obtain an
  independent exact-head verification before merge.

## Operational boundaries

No live ledger, installed collector, LaunchAgent, provider, cloud, credential,
or deployment mutation occurred. ADR-0001 remains proposed pending James's
acceptance; this implementation does not accept that owner decision.
