# Plimsoll resource budget gates

This document turns ADR-0001 into release evidence. The primary assertions are deterministic work counters. Timing is retained as a secondary regression alarm because CI hardware and cold caches vary.

## Measurement contract

Every integrated run uses a fresh temporary `HOME`, `PLIMSOLL_HOME`, SQLite database, synthetic session trees, and an operating-system-assigned loopback port held by a live reservation socket until explicit handoff/cleanup. A challenger bind must receive `EADDRINUSE` before the receipt calls the port held. The duplicate-start scenario uses a second operating-system-assigned loopback port whose reservation is explicitly handed off immediately before the two real CLI candidates race; the harness reservation remains held independently. Child processes receive a fixed minimal environment allowlist; copying and subtracting from `process.env` is forbidden. Sentinel fixtures must prove credential-like names and values do not cross that boundary. The run must not read or write the operator's collector directory, call a hosted provider, use credentials, or control the installed LaunchAgent.

Required counter values come from observed production seams, never empty defaults or expected constants. The no-change scenario temporarily observes `fs.readdirSync` and counts every returned directory entry, restores the original function in `finally`, and takes `maintenanceRuns` from the scheduler's `runCount`. The duplicate-start scenario builds `packages/collector-cli/dist/cli.mjs`, verifies each child executes that exact packaged path under Node 22, counts `listenersCreated` from actual `active` start outputs, and counts `restartRequests` from start outcomes that did not converge to a verified `active` or successful `already_running` result.

The resource receipt has four scenario states:

- `pass`: the scenario ran and every required assertion passed;
- `fail`: the scenario ran and a required assertion failed;
- `not_wired`: the contract exists, but a sibling production lane has not exposed the required test seam yet; and
- `skipped`: an optional check was deliberately not requested.

`not_wired` is never a release pass. During scaffolding, the command exits successfully when current checks pass but emits `gateReady: false`. `--require-integrated` converts any required `not_wired` scenario into a non-zero release-gate result.

## Budget matrix

| Surface / phase | Primary deterministic gate | Secondary observation | Why this budget exists |
|---|---|---|---|
| Harness environment boundary | child env keys are an exact allowlisted subset; credential-like names `0`; secret sentinel values `0` | not applicable | Subtractive scrubbing misses unknown credentials and injected variables. |
| Harness loopback reservation | reservation listener held `1`; assigned port matches listener; challenger bind result `EADDRINUSE` | reservation setup <= 1 s | Reading an ephemeral port and immediately closing it does not reserve it. |
| Idle/no-change interval | `fullHistoryFileReads=0`; `rawEventWrites=0`; `repriceRowsVisited=0`; `enrichmentRowsVisited=0`; `overlappingJobs=0` | Five-minute CPU <= 1 CPU-second on the controlled fixture | Current minute loops replay growing files and run history work even when nothing changes. |
| One complete appended JSONL line | `filesOpened<=1`; `fileBytesRead<=newSuffixBytes+4096`; `rawEventWrites=1`; committed offset equals the complete-line boundary | Capture visible <= 500 ms | A file's age must not determine the cost of its next append. |
| Partial line plus restart | first phase `rawEventWrites=0`; second phase adds exactly one deterministic event; duplicate inserts `0` | Completion visible <= 500 ms | Crash-safe framing must neither lose nor double count usage. |
| Duplicate collector start | `listenersCreated=1`; candidate `restartRequests=0`; owner PID mutation `0` | candidate exits <= 1 s | 16,079 port conflicts must not become a supervisor storm. |
| Generic zero-value OTLP span | `eventsAdmitted=0`; `rawEventWrites=0`; `droppedByReason.generic_zero_value=1`; retained payload bytes `0` | Admission <= 100 ms | About 86% of surveyed rows carried no token/cost value. |
| Valuable OTLP usage/error/action/linkage fixture | exactly one sanitized admission per deterministic ID; projection delta and outbox enqueue each <=1 | Admission <= 100 ms per fixture | Filtering must not trade CPU for silent data loss. |
| Projection update | work proportional to the admitted delta; `rawRowsScanned=0` on steady-state append | <= 100 ms for fixture delta | Dashboard state should be maintained once, not recomputed on every view. |
| Five-surface dashboard snapshot | `rawRowsScanned=0`; `filesystemEntriesScanned=0`; projection rows visited <= fixture projection cardinality | warm p95 <= 500 ms | Surveyed five-endpoint refresh used 12.26 CPU-seconds. |
| Transient upload failure | raw-event rewrites `0`; one bounded attempt increment; later attempt scheduled; overlaps `0` | request timeout/backoff follows configured cap | Delivery failure must not churn evidence or block capture. |
| Deterministic poison followed by valid rows | poison dead-letter inserts `1`; valid acknowledgements equal valid fixtures; head-of-line blocks `0` | valid continuation <= one sync cycle | Five invalid legacy rows caused repeated deterministic failure. |
| Retention/backlog pressure | work limited to configured batch; exact age/row/byte gauges present; health degraded at budget | one maintenance slice <= 500 ms | 4,007,788 unuploaded rows bypassed the stated retention behavior. |
| Entire proof | required failures `0`; required unwired `0`; receipt schema valid; privacy scan violations `0` | wall/CPU/RSS reported, not sole pass criteria | The gate must be machine-readable and stable across runners. |

Timing targets apply to the repository's controlled fixtures after a warm-up where specified. They are not claims about every operator's historical ledger.

## Required work counters

The integrated harness records, at minimum:

```text
eventsObserved, eventsAdmitted, eventsDropped
rawEventWrites, rawEventRewrites, rawRowsScanned
filesOpened, fileBytesRead, fullHistoryFileReads
projectionRowsVisited, projectionRowsWritten
outboxRowsEnqueued, outboxAttempts, deadLettersWritten
repriceRowsVisited, enrichmentRowsVisited
maintenanceRuns, overlappingJobs
listenersCreated, restartRequests
filesystemEntriesScanned
```

Counters are per scenario and reset before each action phase. Production lanes may add counters, but changing a counter's meaning requires a receipt schema version change.

## Adversarial scenarios

1. **Concurrent ownership:** synchronize two starts against the same fresh home and port. Delay the winner after ownership acquisition to maximize the race.
2. **PID reuse:** write a stale record naming a live unrelated PID. The collector must reject identity without signaling that process.
3. **Append framing:** split a valid JSONL record at every byte boundary selected by the fixture and restart between halves.
4. **Rotation and shrink:** replace an inode, truncate in place, and append after each. Exactly the intended generation is ingested.
5. **Trigger storm:** fire timer, append, and manual triggers together while the lane is deliberately paused. Concurrency remains one and one coalesced follow-up runs.
6. **Admission bypass attempts:** unknown spans with large nested attributes, error markers, linkage only, usage only, and tool arguments prove value classification occurs after sanitization.
7. **Projection crash boundary:** fail after raw insert, during projection delta, and before commit. Transactional replay reaches one exact state.
8. **Poison positions:** invalid row first, middle, and last, including output limit one and a two-probe cycle; repeated sync never duplicates the dead letter, later valid rows deliver, a global 400/422 quarantines zero, and a crash after sibling acknowledgement replays settlement idempotently.
9. **Retry storm:** repeated 429/500/timeouts prove bounded backoff, one in-flight request, and no raw-event rewrite.
10. **Scale-shape fixture:** multiply raw history cardinality while holding projection cardinality constant. Dashboard work counters remain constant.
11. **Privacy sentinels:** seed prompt, response, tool arguments, absolute path, repository URL, email, token, and cookie sentinels. None may appear in DB rows, upload bodies, logs, or receipts in metadata mode.
12. **Clock drift:** run the proof at dates far beyond fixture timestamps. Window-sensitive assertions must use an injected fixture clock rather than the wall clock (#82).
13. **Environment inheritance:** seed the parent environment with credential-like names and unique value sentinels. The constructed child environment contains neither, and its key set is a subset of the fixed allowlist.
14. **Port theft:** hold the port-0 listener, verify its assigned port, and challenge the exact address. The challenger must fail with `EADDRINUSE` before any receipt claims a reservation.

## Gate sequence

1. Run architecture/static contract and isolated empty-ledger checks.
2. Run deterministic proof with the injected fixture time delivered by #82.
3. Run each sibling's focused resource scenarios on its own PR.
4. Integrate #76–#80 heads on a clean tree and run the required resource proof with `--require-integrated`.
5. Run `pnpm proof`, TypeScript no-emit check, CLI build, privacy sentinel scan, and receipt-schema validation.
6. Review the receipt adversarially: no `not_wired`, no skipped required checks, no work-counter omission, and no wall-clock-only pass.
7. Merge serially and re-run from current `main`. Deployment or LaunchAgent activation remains a separate owner-approved action.

## Current partial-wiring boundary

The issue #81 harness validates the proposed architecture artifact, creates an isolated temporary ledger and a held/challenged loopback reservation, proves a minimal child-environment allowlist against credential sentinels, and can invoke the existing signal-fidelity proof. It now also executes two production paths:

- **#77 no-change maintenance:** real rollout/transcript fixtures run through `LocalEventBuffer`, both incremental tailers, `CollectorMaintenance`, and `CoalescingMaintenanceScheduler`. One bounded first migration is allowed; a coalesced full run and a third unchanged run must both report zero file bytes/opens, raw-event mutations, repricing/enrichment visits, and overlap. Filesystem enumeration is not claimed as zero: the receipt exposes the observed directory-entry count.
- **#76 duplicate ownership:** two real packaged Node 22 CLI processes race against one temporary home and port. Exactly one active owner and one successful `already_running` candidate must agree on the same versioned runtime identity; the PID record remains byte-identical until the same packaged CLI stop path terminates only that owner.

Poison continuation now runs through the production delivery seam. The integrated capture/projection/outbox, dashboard-projection budget, and integrated metadata-privacy scenarios remain explicitly `not_wired` on #79/#80 integration. The receipt therefore remains `gateReady: false`, and `--require-integrated` remains a required non-zero result.

The original `origin/main@9fc0af4` baseline failed `pnpm proof` because fixed May fixtures aged outside 30-day windows. #82 delivered the injected fixture clock on `origin/main@196d35f`; the rebased issue #81 lane now verifies the full proof green. This history remains documented because wall-clock-dependent fixtures are a release-gate failure mode, not a reason to weaken assertions.
