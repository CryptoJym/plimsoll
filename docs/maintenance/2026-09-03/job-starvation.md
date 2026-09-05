# Lane 3 report — STOPPED_NEEDS_SPLIT

Status: `STOPPED_NEEDS_SPLIT`

## Root cause in plain words

The maintenance worker reaches ready, but the work inside one job is not
scheduled against one shared deadline. Capture runs first. Link filling,
enrichment, and Git-context attribution run later. The last of those can hold
the worker until the parent reaches its fixed 30-second deadline and kills the
process. The parent treats every such kill as a failed run, opens the circuit,
and prevents the next maintenance attempt. Work that could return space or
reduce the oldest queues therefore does not get priority.

Commit `7fbbb2d` does not resume `git_context` across a deadline kill. It keeps
the deferred requests in the child-only `carriedRepoContexts` array. A deadline
kill destroys that array. The parent failure path then deletes unresolved
child-owned rows from `repo_context_inflight` and counts them as
`worker_crash`. Consequently the next worker cannot continue from that
in-memory cursor. Per-context results completed before the kill remain durable,
but the deferred cursor does not.

## Measurements

The following values are source-authoritative live receipts or read-only
observations made on 2026-09-03. No live-ledger write was performed.

| Stage | Measured time | Result |
| --- | ---: | --- |
| Worker initialization before job work | 308 ms | Completed |
| `git_context` last observed hold | 29,583 ms | Broke the 30,000 ms parent deadline |
| `fillPendingEventLinks` | `UNKNOWN` | 158,457 rows remained; no completed-job timing receipt exists |
| Enrichment | `UNKNOWN` | 1,281 dirty sessions remained; no completed-job timing receipt exists |
| Retention prune | `UNKNOWN` | Runs in the parent on a separate timer, not as a measured worker-job stage |
| WAL checkpoint | `UNKNOWN` | Startup checkpoint is separate from the maintenance job; no job-stage timing exists |

The read-only live database open/page census completed in 17 ms and reported a
4,096-byte page size, 5,291,131 pages, and zero freelist pages. At observation
time the main database was approximately 20 GiB and its WAL approximately
6.2 MiB. This does not supply missing per-stage job timings; those timings
remain `UNKNOWN` rather than being inferred.

`git_context` holds for roughly the full deadline because its budget is
computed from the original 30-second request rather than the time remaining
after initialization and capture. More importantly, the budget is checked only
between repository resolutions. One slow synchronous Git resolution cannot be
preempted by that check. The parent's hard timer is therefore the first guard
that fires.

## Why this lane must be split

The requested behavior is not one isolated patch. A correct implementation
requires coordinated changes to all of these contracts:

1. A protocol-level `maintenance_job_progress` frame and acknowledgement, so
   the parent can prove durable rows before classifying a deadline.
2. A durable stage/cursor schema, including the actual deferred Git-context
   request identity rather than the current child-memory-only array.
3. Bounded implementations for retention deletion, WAL checkpoint, link fill,
   enrichment, and Git context, all driven by the same remaining-time budget.
4. Worker orchestration in the required priority order, with a reserved
   teardown/receipt margin.
5. Boundary semantics that return `PARTIAL_OK` only after an acknowledged
   durable-progress frame, while preserving failure/circuit behavior for a
   crash or zero progress.
6. Parent scheduler/result changes so `PARTIAL_OK` is counted as a successful
   maintenance attempt without fabricating a normal full result.

Changing only the boundary would call a killed job successful without proof.
Changing only the worker would still make the parent open the circuit. Changing
only `carriedRepoContexts` would not survive process death. Those partial fixes
would violate the requested durability contract.

Recommended split:

- Split A: durable cursor schema plus independently tested bounded stage
  primitives and live-copy timing harness.
- Split B: protocol progress receipt plus parent `PARTIAL_OK` arbitration and
  real-crash circuit tests.
- Split C: priority-ordered worker integration, drain-rate measurement on a
  ledger copy, typecheck/build, and operator report.

## Expected drain time

`UNKNOWN`. No new bounded implementation was shipped and no safe new pace was
measured, so an estimate for 158,457 links would be invented. Split C must
calculate it as `remaining links / measured committed links per completed
minute`, including the configured cadence and partial jobs.

## Verification

| Status | Check | Command | Evidence |
| --- | --- | --- | --- |
| PASS | Checkout boundary | `pwd && git branch --show-current` | Lane path; branch `maint-orphan-wal-20260903` |
| PASS | Read-only ledger page census | `sqlite3 "file:$LEDGER?mode=ro"` with `pragma query_only=on` and page PRAGMAs | 17 ms; page size 4,096; 5,291,131 pages; freelist 0 |
| PASS | Resume semantics source trace | `sed` over `maintenance-worker.ts` and `buffer.ts` | Deferred requests are child memory; kill recovery deletes unresolved child inflight rows |
| NOT_RUN | Focused behavioral proofs | `pnpm proof:maintenance-starvation` | No implementation change was made |
| NOT_RUN | Typecheck | `pnpm exec tsc -p tsconfig.json --noEmit` | Stop condition reached before implementation |
| NOT_RUN | Build | `pnpm --dir packages/collector-cli build` | Stop condition reached before implementation |

## Operator step

None for this stopped lane. Do not restart the collector for this report-only
commit. After the split implementation is integrated and the live checkout
pulls that verified branch head, the operator action remains exactly one
collector restart.

No live checkout was touched, no process was signaled or restarted, no live
ledger write was made, and nothing was pushed.
