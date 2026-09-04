# Split A lane report — STOPPED_NEEDS_SPLIT

Date: 2026-09-04 MDT
Branch: `maint-split-a-20260903`
Base: `2ae3249ed1db52b68685bce7bcb2c6f2f0f1b766`
Status: `STOPPED_NEEDS_SPLIT`

## Outcome

Durable stage state, bounded row-count slices, a copy-only timing harness, and
focused durability/batch/zero-budget proofs were implemented. The live-copy
timing run disproved the stronger positive-budget wall-time contract: a single
synchronous SQLite enrichment slice of 64 rows took about 31 minutes despite a
2,000 ms remaining-time budget. This lane therefore stops honestly instead of
claiming that all delivered primitives respect a wall deadline.

Local commits:

- `755bfd3 feat: add bounded durable maintenance stages`
- `3a309d2 fix: bound retention cursor scans`

## Implemented

- `maintenance_stage_cursors` is created idempotently during normal ledger
  initialization and by the standalone primitive module. It has one durable
  row for retention deletion, WAL checkpoint, pending event-link fill,
  enrichment, and Git context.
- `maintenance_git_context_queue` stores the actual deferred request identity:
  `context_id`, `source`, and `cwd`. Requests are inserted before resolution;
  each resolved result, queue removal, cumulative cursor update, and next
  request selection share one transaction. A process death during resolution
  therefore leaves that exact request queued.
- All five primitives accept `remainingMs` and `batchSize`, commit progress,
  update their cursor, and return `{rows, ms, remaining}`. Checkpoint additionally
  reports both PASSIVE and conditional TRUNCATE `{busy, log, checkpointed}`.
- Retention is capped at 1,000 visited rows per call and uses durable rowid
  cursors over raw events and metric samples. It does not issue the prior
  qualifying-row scan/sort across the whole ledger.
- `scripts/maintenance-stage-timing.ts` checks free space, opens the live ledger
  read-only with `query_only`, holds a short read snapshot while `cp` copies the
  main database and WAL, closes that reader immediately, runs only against the
  private copy, and deletes the copy/WAL/SHM in `finally`.
- No IPC protocol or parent-boundary code was changed. Split B ownership was
  preserved.

## Copy-only timing

Command:

`PLIMSOLL_STAGE_BATCH_SIZE=64 PLIMSOLL_STAGE_BUDGET_MS=2000 pnpm timing:maintenance-stages -- '/Users/utlyze/Library/Application Support/Plimsoll/work-ledger.sqlite'`

The first two attempts correctly failed: main-file-only `cp` produced
`SQLITE_CORRUPT`, then the integrity-result parser rejected the copy. The
harness was corrected to copy the WAL under a short read snapshot and to avoid
an unbounded full-ledger `quick_check`. Two later runs exited 0 and cleaned up
their copies. Representative final receipt:

| Stage | Rows | Milliseconds | Budget remaining | Extra |
| --- | ---: | ---: | ---: | --- |
| retention deletion | 0 | 78,738 | 0 | 64-row batch |
| pending event-link fill | 0 | 860 | 1,140 | no eligible resolved links in selected slice |
| enrichment | 64 | 1,834,719 | 0 | wall budget exceeded inside synchronous SQL |
| git context | 0 | 0 | 2,000 | copied pre-migration ledger had no durable queued requests |
| WAL checkpoint | 0 | 73 | 1,927 | PASSIVE `busy=0 log=9 checkpointed=9`; TRUNCATE `busy=0 log=0 checkpointed=0` |

The other completed receipt measured retention 98,282 ms, pending links 4,768
ms, enrichment 1,861,801 ms for 64 rows, Git 6 ms with no queued work, and
checkpoint 203 ms with the same `9/9` PASSIVE result. Split C must not derive a
drain estimate from a nominal two-second cadence until the synchronous
enrichment query is further decomposed.

## Verification

| Status | Check | Exact command | Evidence |
| --- | --- | --- | --- |
| PASS | Focused stage proof | `pnpm proof:maintenance-stages` | `checks:5`, `passed:5`; schema idempotence, file-backed close/reopen Git identity, row caps, and zero-budget no-work |
| PASS | Typecheck | `pnpm exec tsc -p tsconfig.json --noEmit` | exit 0, no output |
| PASS | Collector build | `pnpm --dir packages/collector-cli build` | `dist/cli.mjs 1.9mb`; exit 0 |
| PASS | Copy-only harness execution and cleanup | timing command above | two exit-0 JSON receipts; no remaining `plimsoll-maintenance-timing-*` file or harness process |
| PASS | Whitespace/error markers | `git diff --check` | exit 0, no output before implementation commits |
| FAIL | Positive-budget wall-time respect | timing command above with `PLIMSOLL_STAGE_BUDGET_MS=2000` | enrichment took 1,834,719 ms for 64 rows; another run took 1,861,801 ms |
| NOT_RUN | Full maintenance-boundary proof | `pnpm proof:maintenance-boundary` | Split A did not change the IPC or parent boundary; earlier lane recorded an unrelated existing HTTP timeout |

## Required next split

Split A is still too large at the SQLite cancellation boundary. The remaining
work should be split at `runRepoEnrichmentMaintenance`: replace its correlated
per-session candidate query with smaller durable sub-cursors or run each SQL
slice in a disposable child that the existing job deadline can terminate.
Pending-link and retention cold-copy timings should also be repeated after that
change; a row-count cap alone does not prove a millisecond cap on a cold 20 GB
ledger. Git resolution similarly remains cooperative only between requests; a
single synchronous Git call is not preemptible in this split.

No live checkout was touched, no process was signaled or restarted, no live
ledger write was made, nothing was pushed, and every live read-only connection
was closed. All private ledger copies and temporary timing bundles were
deleted. `SEAT-RULES.md` was not present under this clone or the searched
Plimsoll lanes path, so the explicit lane rules and prior lane reports were
followed directly.
