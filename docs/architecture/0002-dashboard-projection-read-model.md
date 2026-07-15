# ADR-0002: Hashed fact layer and coherent dashboard read model

## Status

Proposed — pending owner acceptance

Issue #80 implements this as a reversible additive source change. It does not accept the architecture for James, activate the collector, mutate the live ledger, or enable raw retention.

## Context

The loopback dashboard regrouped millions of `buffered_events` rows across five requests every 30 seconds. Capture health also recursively walked local session trees on a request-triggered cadence. The measured bundle consumed 12.26 CPU-seconds and 13.15 seconds wall time on the live ledger.

The raw ledger remains useful evidence, but it is the wrong request model. A cache around the existing queries would move the spike rather than remove history-proportional work, and a second process/database would weaken the one-transaction local design.

## Decision

Keep one collector process and one SQLite database. Add an internal CQRS read model with four layers:

```text
promoted raw columns
       |
       v
hashed dashboard_event_facts + compressed generic-span segments
                  -- bounded raw-row repair/backfill
       |
       v
fixed-window numeric + session/repo/account read models
       |
       v
atomic dashboard_snapshots generation (30/90/182/365/1825 days)
       |
       v
one loopback /api/snapshot request + compatibility slices
```

Facts derive a safe projection event ID and safe session/machine/account/head hashes. Repository and branch linkage is accepted only in canonical `sha256:<64 lowercase hex>` form. Facts contain no payload, content, raw ID, hostname, path, URL, email, label, or credential. Local labels, emails, subscriptions, and priority configuration remain separate presentation dimensions and are applied only to the loopback response; durable snapshot JSON does not duplicate them.

Producer strings are not trusted merely because they arrived in promoted
columns. Source, event type, and action class pass closed allowlists; unsafe
values fall back to `unknown`/`other`. Model identifiers pass a conservative
identifier grammar and otherwise become a derived SHA-256 identity. Session,
machine, account, and head values are always hashed; repo/branch values that are
not already canonical hashes are rejected. Multibyte credential and path
sentinels cover facts, snapshots, activity/source state, and repair receipts.

Integer nano-USD is the additive cost representation. The issue proof pins its allowed difference from legacy SQLite REAL sums to per-event rounding tolerance.

Simple totals use old-minus/new-plus deltas. Window-relative dominance and distinct membership use the smallest exact repair grain: one affected session in one fixed window. A direct raw SQL trigger durably queues any relevant insert/update that does not pass through the application seam. Projection failure rolls back its savepoint, leaves raw capture committed, and retains a repair receipt.

The fact layer is intentionally selective. Sessionless events with no model,
tokens, cost, or linkage do not become one row across six indexes. Bounded
backfill batches encode those safe promoted fields in immutable gzip segments
(at most 1,000 events each) while applying exact source/day/action/window and
lifetime rollups. Rare corrections and deletes write safe old-contribution
receipts plus exact-version cancellations in the same raw transaction. The
100,000-row release fixture must produce zero indexed facts, at most 1,000
segments, no more than 128 logical projection bytes per raw event, and no more
than 32 bytes/event of actual SQLite file growth (including table and index
pages). The measured fixture is materially below both bounds.

The measured fixture produces 100 segments, 5.65 logical projection bytes per
raw event, and 7.95 bytes/event of actual SQLite file growth.

Active generic-span capture does not compress one event at a time. The raw
transaction leaves its trigger-authored repair receipt durable and returns;
maintenance drains at most 250 repairs, groups them by day, and coalesces each
day into tail segments capped at 1,000 events. Publication stays on the prior
coherent generation while any repair is pending. In the 5,000-row active
fixture this produces five segments across 20 maintenance slices, with at most
one segment write per slice and no garbage-collection work during intake.

Repair reasons also encode whether a row has ever entered the projection.

- `raw_insert` and `projection_apply_failed` are never-projected states. Later
  updates preserve that state and maintenance applies only the latest current
  raw value once; a delete may discard it without a cancellation.
- `raw_update` is an already-projected state. An irrelevant compact update may
  drain as a no-op, but the first later meaningful update or delete records one
  old compact contribution before the repair coalesces further changes.
- `raw_delete` settles the previously projected fact or compact contribution
  to zero.

The mutation and repair queues have an explicit dependency rule. Maintenance
must not consume or delete an already-projected `raw_update` repair while a
compact mutation for the same raw row still exists. The mutation drains first,
materializes its old-state cancellation, and thereby makes the repair eligible
to add the current row exactly once. Eligibility is filtered before the
250-row limit, and eligible repairs are read through the `(queued_at,
raw_rowid)` index while the dependency check uses the mutation table's integer
primary key. A blocked low rowid therefore cannot hide independent eligible
work or turn the bounded query into a queue scan.

This ordering prevents both the missing-row failure from overwriting a pending
insert and the inverse failure where blindly replaying every compact update
duplicates an already-projected item. The update/delete triggers are recreated
on open so databases created by an earlier draft receive the corrected state
transition rules without a ledger rewrite.

Compact corrections and deletes have a separate physical garbage-collection
state machine. Each affected day has one fair-scheduled job with a revision,
frozen processing revision, segment high-water mark, and durable cursor. A
slice visits one whole segment and at most 1,000 items, then atomically rewrites
or removes its payload, settles matching cancellations, and advances per-source
scratch summaries. New segments and cancellations increment the day revision
without moving the in-flight cursor; the frozen pass reaches its high-water
before one restart. Garbage collection waits until compact mutation and repair
admission queues drain, so a bulk delete does not repeatedly rescan unstable
days. Snapshot publication remains blocked until the garbage-collection queue
settles.

Day/source count and min/max summaries are updated in the same transactions as
segment insert, rewrite, deletion, and final garbage-collection settlement.
Lifetime and source-latest bounds therefore use indexed summaries rather than
decompressing history; a moving window may decode only the single day that
straddles its cutoff. The 20,000-row delete fixture removes all 20 segments,
payload items, cancellations, day jobs, and day/source summaries in 99 bounded
maintenance slices. Freed SQLite pages appear on the freelist; file truncation
is deliberately not promised without a separate vacuum policy.

The compact summary schema has its own one-time additive migration marker.
When that marker is absent or false, one transaction inserts one day job for
every distinct indexed day in the union of compact segments and cancellations,
marks the prior coherent generation stale, recomputes the job backlog, and
commits the marker with those jobs. This includes unaffected segment days:
rebuilding only cancellation days would leave lifetime, source-latest, and
window-bound summaries incomplete. A crash before commit retains neither the
marker nor partial jobs; a crash after commit resumes the durable day cursors.
The marker remains set after completion, so later opens do not rescan or reseed.
An unchanged migration segment is decoded into summary scratch but is not
rewritten or recompressed.

Session repair is also row-bounded, not merely session-bounded. Each
session/window job persists a fact cursor, append high-water mark, restart
revision, and normalized repo/branch/account/source/machine accumulators.
Maintenance reads at most 1,000 facts or 50 ms of reducer-loop work per turn.
Corrections, expiry, and alias changes restart only that accumulator; append-only
growth extends its high-water mark. Existing read-model rows remain untouched
until one transaction atomically swaps the finalized replacement. This matters
on the observed ledger, where a single session exceeds 230,000 rows.

Legacy migration captures a raw rowid high-water without `COUNT(*)`, dual-writes new events, copies at most 1,000 rows per slice, then performs an independent bounded reference pass for all five windows. Reads switch only after reference totals, dirty sessions, and repair queues settle. Retention consumes the persisted parity marker but activation remains explicit and proof-gated.

The daemon accelerates only this initial migration with a cooperative duty
cycle: up to 40 slices per 60-second scheduler run, a `setImmediate` yield
between slices, and at most 2,000 ms of active projection work per run. Status
reports slice/yield counts, active milliseconds, remaining rowid upper bound,
and an ETA. At the observed 4.81 million raw rows, the raw pass plus independent
parity pass is at most 9.62 million row visits; sustained 40,000 rows/minute is
about 241 minutes. This is a conservative four-hour migration rather than an
80-hour one-slice-per-minute rollout, while capping worst-case active duty near
3.3% of the minute. No request performs or accelerates migration work.

Tailers persist path-free activity aggregates. Snapshot/status health joins those aggregates to projected ledger activity; no request discovers filesystem entries.

## Consequences

### Positive

- Main dashboard and status refresh become constant-row reads with deterministic zero raw/filesystem request scans.
- The dominant generic-span class is compressed by bounded batches instead of duplicating millions of indexed facts.
- Raw TTL can later become independent of analytics because compact facts and lifetime receipts survive raw deletion.
- Failure is honest: initial migration returns `projection_backfilling`; later failures serve the last coherent generation as stale/degraded.
- Exact dominant repo/account, multi-repo fallback, distinct branch/session, unpriced, cache, subscription, and tail semantics remain testable.

### Negative

- Additive tables and migration temporarily increase database size.
- Window expiry and historical corrections require explicit bounded repair state.
- Compressed segments remain lifetime evidence after window expiry. Correction/delete cancellations remain only until bounded garbage collection proves and atomically settles their physical removal.
- Presentation is composed after reading the durable snapshot, so the ETag includes a small settings/activity version as well as projection generation.
- Session drill-down identifiers become safe projection hashes rather than raw producer IDs.

### Neutral

- Raw reference query helpers remain available to proof code but are not imported by production request routes.
- Snapshot publication may run on the coalesced maintenance lane when the clock advances; browser refresh itself never rebuilds.

## Alternatives Considered

### TTL cache around raw queries

Rejected. A cache miss still performs the multi-second synchronous history scan and can freeze capture on the shared event loop.

### Query the compact fact table directly on every request

Rejected. It removes payload bloat but request work still grows with event history.

### Separate analytics process or database

Rejected for the 80/20 migration. It adds supervision, recovery ordering, IPC, and consistency failure modes before bounded work has been tried in the existing transaction boundary.

### Naive additive session/repository totals

Rejected. Distinct sessions/branches and dominant repo/account can change when one event is corrected, expires, or arrives late; targeted session recomputation is the smallest exact grain.

## Failure Modes

| Failure | Behavior |
|---|---|
| Projection delta fails | Raw capture commits; repair row and degraded reason persist. |
| Raw row is deleted | The raw transaction persists a safe compact old-contribution receipt or a fact tombstone; bounded maintenance subtracts before publishing. |
| Compact insert changes before its first repair | The never-projected receipt survives every update; maintenance reads current raw state and applies it exactly once, or settles a pending delete to zero. |
| Compact update repair is queued with its old-state mutation | The repair remains blocked by raw rowid until the mutation drains and creates its cancellation; the current value is then added exactly once, including after reopen. |
| Upgrade predates compact day summaries and GC triggers | One atomic indexed day-union migration seeds bounded rebuild jobs for all segment/cancellation days, keeps the prior generation stale, and resumes after reopen without reseeding. |
| Compact garbage collection crashes after payload rewrite | The enclosing SQLite transaction rolls back payload, receipts, summaries, and cursor together; the same frozen segment replays after reopen. |
| Compact day changes during garbage collection | The frozen pass reaches its segment high-water, then restarts once against the newer revision; fair day scheduling prevents another day from starving. |
| Giant session repair crashes | Durable cursor and normalized accumulators resume; the prior snapshot remains stale/coherent until atomic finalize. |
| Legacy backfill crashes | Rowid cursor resumes; deterministic fact IDs make replay a no-op. |
| Snapshot assembly fails | Prior generation remains; request reports stale/degraded and never runs a raw fallback. |
| Clock rolls backward | Prior generation remains available; parity becomes false and an explicit rebuild/degraded state blocks silent subtraction errors. |
| Tailer discovery truncates/errors | Path-free activity state becomes amber; quiet is never reported green from stale evidence. |
| Unsupported window | API returns 400 with the five supported windows; it does not scan raw history. |

## Known residual

Tailer discovery still recursively inventories transcript roots on the
background maintenance cadence. The read-only live measurement was 3,391
directory entries, 1,190 JSONL stats, 15 recent files, and 100.48 ms for
discover-plus-stat. At a one-minute cadence that is roughly 0.17% of one CPU,
secondary to the eliminated multi-second request bundle. It is no longer
reachable from snapshot, status, or detail requests; a durable directory-change
index is a later optimization, not a blocking subsystem.

## References

- [Parent architecture issue #75](https://github.com/CryptoJym/plimsoll/issues/75)
- [Dashboard projection issue #80](https://github.com/CryptoJym/plimsoll/issues/80)
- [ADR-0001](./0001-resource-bounded-local-collector.md)
- [Resource budget gates](./resource-budget-gates.md)
