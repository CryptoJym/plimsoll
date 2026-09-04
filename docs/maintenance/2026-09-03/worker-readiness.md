# Plimsoll maintenance worker readiness lane report

Date: 2026-09-03 MDT
Branch: `maint-orphan-wal-20260903`
Lane 2 commits: `3498294`, `b14244a`, `062cc8b`

## Root cause

Plain words: the child process was alive, but it did not announce that fact
until after it had opened and initialized the entire 20 GB ledger. That ledger
work took longer than the parent's fixed 10-second ready timer, so the parent
killed a healthy-but-busy child before the actual maintenance job was allowed
to own the long-work deadline.

Old code path:

1. `cli.ts` entered `__maintenance_worker`.
2. It loaded config and checked privacy.
3. It called `openBuffer`, which constructs `LocalEventBuffer` and runs schema,
   index, trigger, and projection initialization against the ledger.
4. It constructed `CollectorMaintenance`, `RolloutTailer`, and
   `TranscriptTailer`.
5. Only then did `runMaintenanceWorkerService` install the IPC listener and
   send `ready`.
6. `MaintenanceProcessBoundary` expired `readyDeadlineMs: 10_000` first.

The WAL was not the cause: the operator had already reduced it from 27,201 MB
to approximately zero, and a freshly restarted worker still followed the same
timeout/orphan/circuit pattern.

## Measured pre-ready work

Measurement method: SQLite backup/copy work and constructor timing were done
against a private copied ledger only. The live file was never opened for write.
The timed copy represented the live approximately 20 GB, 5,270,140-page ledger
with approximately 6.4 million buffered rows after WAL truncation.

| Pre-ready step in the old design | Time |
| --- | ---: |
| `LocalEventBuffer` construction (`openBuffer`, workspace selection excluded) | **11,264 ms** |
| Workspace selection | 160 ms |
| Maintenance and two tailer constructors | 2,503 ms |
| Total before old `ready` signal | **13,927 ms** |

The step exceeding 10 seconds was `LocalEventBuffer` construction inside
`openBuffer`.

## Change

- `packages/collector-cli/src/maintenance-worker.ts` now installs IPC first,
  reports `process_up`, sends `ready`, and lazily initializes the ledger/runtime
  on the first `run` request. The parent starts the existing job deadline before
  sending that request, so initialization and scanning remain bounded by
  `maintenance_deadline_exceeded`.
- `packages/collector-cli/src/cli.ts` supplies the lazy initializer and emits
  content-free `maintenance_worker_stage` JSON for `config_loaded`,
  `privacy_checked`, `ledger_opened`, and `maintenance_constructed` with elapsed
  milliseconds.
- `packages/collector-cli/src/maintenance-boundary.ts` inherits child stderr so
  those stages reach collector logs. After TERM and KILL grace it also performs
  one final nonce-bound fingerprint check; a confirmed-gone original process is
  detached and counted as reaped even if the Node child object never delivered
  `close`. A still-live or identity-ambiguous process keeps the existing fence.
- Focused proofs cover ready-before-initialize, stage receipts, the unchanged
  job deadline, confirmed-gone busy-worker KILL, lane-1 orphan recovery, and
  source/bundled workers. The real-worker proof uses production's 10-second
  process-start readiness contract; ledger initialization is inside the job.

## Verification

| Status | Command | Evidence |
| --- | --- | --- |
| PASS | `pnpm proof:maintenance-worker-ready` | 4/4; ready before initialization; process, ready, and initialization stages emitted |
| PASS | `PLIMSOLL_MAINTENANCE_PROOF_FOCUS=job_deadline pnpm proof:maintenance-boundary` | seven timeout-stage cases; each timed out and reaped |
| PASS | `PLIMSOLL_MAINTENANCE_PROOF_FOCUS=busy_worker_kill pnpm proof:maintenance-boundary` | TERM then KILL; confirmed gone; `childPresent:false`, `orphanRisk:false` |
| PASS | `PLIMSOLL_MAINTENANCE_PROOF_FOCUS=orphan_recovery pnpm proof:maintenance-boundary` | recoverable orphan allowed a fresh worker; unkillable child stayed fenced and loudly reported |
| PASS | `PLIMSOLL_MAINTENANCE_PROOF_FOCUS=real_worker pnpm proof:maintenance-boundary` | source and bundled hidden workers ran and shut down; structured stage lines visible |
| PASS | `pnpm exec tsc -p tsconfig.json --noEmit` | exit 0, no output |
| PASS | `pnpm --dir packages/collector-cli build` | `dist/cli.mjs 1.9mb`; `Done in 1953ms`; exit 0 |
| NOT_RUN | `pnpm proof:maintenance-boundary` | Full suite not run in Lane 2; Lane 1 already recorded an unrelated existing HTTP-timeout failure. |
| NOT_RUN | package test/lint | No test or lint script exists in `packages/collector-cli/package.json`. |

The real-worker focus intermittently hit its old proof-only 3-second ready limit
under concurrent I/O. Commit `062cc8b` aligned that integration proof with the
production 10-second process-start contract. The table does not claim a
full-suite result.

## Live recovery after deployment

The WAL is already truncated, so no additional manual checkpoint is part of
this recovery.

1. Deploy this branch through the normal reviewed release path and verify the
   exact deployed head includes `3498294`, `b14244a`, and `062cc8b` (or their
   reviewed descendants).
2. James approves one controlled collector restart. It is required to load the
   new maintenance child entry; this lane did not perform it.
3. Read collector logs for `maintenance_worker_stage`. `process_up` and
   `ready_sent` should precede `initialization_start`, `ledger_opened`, and
   `maintenance_constructed`.
4. Confirm `maintenance_worker_ready_timeout` stops. Heavy initialization or
   scanning may still reach `maintenance_deadline_exceeded`; that is the correct
   job-level guard. If killed, confirm the next cycle is not fenced by repeated
   `maintenance_child_not_reaped`.
5. Confirm later maintenance completes, retention pruning resumes, and WAL size
   stays controlled. Row deletion and checkpointing do not shrink the 20 GB main
   database. Schedule offline `VACUUM` only with separate approval, collector
   stopped, a verified backup/integrity plan, and free space for at least one
   additional full database copy plus operating reserve.

## Boundaries and unknowns

- No claim is made that the full maintenance-boundary suite is green in this
  lane; it was not run.
- Actual post-deployment live stage times and successful maintenance completion
  remain `UNKNOWN` until the approved restart and log readback.
- No live process was signaled or restarted, the live ledger was not written,
  and nothing was pushed, deployed, published, or opened as a PR.
