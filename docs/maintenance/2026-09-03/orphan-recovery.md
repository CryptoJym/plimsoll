# Plimsoll collector maintenance-orphan lane report

Date: 2026-09-03 MDT
Branch: `maint-orphan-wal-20260903`
Base: `7fbbb2d9e27a781c4e11cd31c1d830bec6b5d6bf`

## Root cause

Plain words: the collector gave up permanently after one maintenance child did
not exit within its TERM/KILL grace windows. It kept the child and its SQLite
handles referenced, then rejected every later maintenance run. With no later
checkpoint or retention pruning, the live WAL continued growing.

Code path before this lane:

1. `MaintenanceProcessBoundary.terminateChild()` sent `SIGTERM`, waited, sent
   `SIGKILL`, waited, and set `orphanRisk=true` if the child still had not
   emitted `close`.
2. `MaintenanceProcessBoundary.run()` immediately threw
   `maintenance_child_not_reaped` whenever `orphanRisk` was true.
3. The scheduler in `cli.ts` classified that as `boundary_unavailable`, logged
   it, and did not run maintenance. Nothing ever retried reaping the child.

The corrected path is at `packages/collector-cli/src/maintenance-boundary.ts`
lines 356-363 and 937-1034: a later run enters one shared recovery promise,
rechecks the nonce-bound process fingerprint before every signal, retries the
TERM-to-KILL sequence with bounded exponential backoff, confirms close or that
the original fingerprint is gone, then clears the fence. If the original child
is still alive, it retains the fence, increments failure counters, and emits a
structured receipt containing PID, age, attempt, signals sent, and outcome.

## What changed

- `packages/collector-cli/src/maintenance-boundary.ts`
  - added bounded orphan recovery, retry/backoff, PID-reuse-safe checks,
    recovery counters, and a no-double-worker recovery fence.
- `packages/collector-cli/src/cli.ts`
  - emits `maintenance_orphan_recovery` receipts;
  - invokes one WAL self-heal attempt after start ownership is acquired and
    before the HTTP listener or ordinary collector database connection opens.
- `packages/collector-cli/src/startup-wal-self-heal.ts`
  - checks `<ledger>-wal` against a threshold (default 1 GiB), runs
    `wal_checkpoint(TRUNCATE)` in a disposable child, bounds it to five seconds,
    and returns bytes-before/after plus busy/log/checkpointed counts.
- `packages/collector-cli/src/config.ts`
  - added `startupWalCheckpointBytes`, default `1073741824`.
- `packages/collector-cli/src/rejection-diagnostics.ts` and `server.ts`
  - classify rejection identity into the closed set `codex`, `claude_code`,
    `otlp_exporter`, or `unknown`; aggregation is bounded by reason/client pair;
    raw headers and request content are never retained; HTTP response bodies are
    unchanged.
- `packages/collector-cli/README.md`
  - documents the threshold, bounded startup attempt, and the requirement that
    no conflicting SQLite process hold the WAL.
- `scripts/maintenance-boundary-proof.ts`,
  `scripts/startup-wal-self-heal-proof.ts`, and
  `scripts/rejection-aggregation-proof.ts`
  - added regression proofs for all requested cases.

Local commits:

- `8701192 fix: recover orphaned maintenance workers`
- `fb4cfa3 fix: bound startup WAL checkpoint recovery`
- `2fe5d11 feat: classify bounded collector rejections`

## Verification

| Status | Check | Exact command | Last output |
| --- | --- | --- | --- |
| PASS | Orphan recovery focused proof | `PLIMSOLL_MAINTENANCE_PROOF_FOCUS=orphan_recovery pnpm proof:maintenance-boundary` | `maintenance_boundary_orphan_recovery`; recovered spawns `2`; stuck spawns `1`; stuck failures `1`; six TERM/KILL signals recorded |
| PASS | WAL threshold/once/timeout proof | `pnpm proof:startup-wal-self-heal` | `{"proof":"startup_wal_self_heal","checks":3,"passed":3,"calls":1,...}` |
| PASS | Fixed-cardinality rejection proof (scaled) | `REJECTION_PROOF_SCALE=0.0001 pnpm proof:rejection-aggregation` | `{"checks":20,"passed":20,"failed":0,"intervalMs":60000,"digest":"f5ca38f7"}` |
| PASS | Typecheck | `pnpm exec tsc -p tsconfig.json` | no output; exit 0 |
| PASS | Collector package build | `pnpm --dir packages/collector-cli build` | `dist/cli.mjs  1.9mb`; `Done in 1455ms`; exit 0 |
| PASS | Whitespace/error markers | `git diff --check` | no output; exit 0 |
| FAIL | Full maintenance-boundary suite | `pnpm proof:maintenance-boundary` | `{"proof":"maintenance_boundary","ok":false,"reason":"proof_http_timeout"}`; exit 1, reproduced twice in the existing FIFO/HTTP availability phase before the new focused check |
| FAIL | Full 108k rejection suite | `pnpm proof:rejection-aggregation` | completed all rejection phases, then `{"error":"rejection_aggregation_proof_failed","message":"read ECONNRESET"}`; exit 1 during the late control phase under current disk/I/O pressure |
| NOT_RUN | Package `test` script | `pnpm --dir packages/collector-cli test` | package has no `test` script |
| NOT_RUN | Package lint script | `pnpm --dir packages/collector-cli lint` | package has no `lint` script |

The two full-suite failures mean this lane is not wholly green. The focused
regressions, typecheck, and bundle build are green; no claim is made that the
unscaled suite passes on this machine in its current state.

## Oversize sender finding

Verdict: **UNKNOWN for the historical/live rejections captured today.** The
deployed log schema does not contain a source header, user-agent, remote port,
content-length, or observed byte count. Naming either tool from those lines
would be guesswork.

What is proved:

- Live `collector.err.log` lines 329157-329160 show continuing
  `compressed_body_too_large` windows (13 and 15 requests) adjacent to
  `maintenance_child_not_reaped`, but contain no identity or size field.
- `/Users/utlyze/.claude/settings.json` lines 3-8 configure Claude Code OTLP to
  port 48271 with `x-plimsoll-source=claude_code`.
- `/Users/utlyze/.codex/config.toml` lines 370-389 configure Codex logs,
  metrics, and traces to the same port with `x-plimsoll-source=codex`.
- A read-only `lsof -nP -iTCP:48271` observation showed the collector listener
  and two established loopback server sockets, but the short-lived client ends
  were already gone and could not be attributed.
- The collector limit is exactly 262,144 compressed bytes
  (`http-boundary.ts` lines 4-6). Rejection occurs when declared content length
  or streamed bytes are greater than that limit (lines 167-174 and 199-203).
  Therefore the only defensible size finding is **greater than 262,144 bytes**;
  the exact body size is not present in available logs.

After deployment, the new first/summarized rejection lines will include the
bounded `clientClass`, which will identify the next recurrence without logging
payload content. Recommendation: use that receipt to change the identified
client's OTLP batching/export size. Do not raise the collector limit without a
separate availability test: the existing 256 KiB cap is deliberate and this
lane found no evidence that it is plainly wrong.

## Operator recovery steps requiring James's approval

1. Deploy these local branch commits through the normal reviewed release path.
   Do not operate from the live checkout until the exact deployed head is
   verified.
2. Approve one controlled collector restart after that deployment. A restart
   is required to remove the existing three-day-old collector/maintenance
   process generation and load the new boundary behavior. This lane did not
   restart or signal either live PID.
3. On startup, read the single `startup_wal_checkpoint` receipt. Expected when
   no other process retains the WAL: `attempted:true`, `outcome:"completed"`,
   `busy:0`, and `bytesAfter` near zero. The roughly 27 GB WAL should be
   checkpointed into the main database as needed and then truncated, returning
   most WAL space to the filesystem. If the receipt is `busy`, `timed_out`, or
   `failed`, stop and identify the remaining holder before another approved
   recovery action; do not assume truncation occurred.
4. Confirm later maintenance runs no longer repeat
   `maintenance_child_not_reaped`, and confirm retention pruning resumes.
   During checkpointing and pruning the main database may grow before free
   pages accumulate; deleting rows does not itself shrink the SQLite file.
5. Consider an offline `VACUUM` only after WAL truncation and retention pruning,
   during an approved maintenance window. Preconditions: collector stopped,
   integrity/backup plan confirmed, and free disk comfortably greater than the
   current database size plus temporary-copy and safety headroom (practically,
   at least one additional full database copy plus operational reserve). With
   only 47 GB free before recovering a 27 GB WAL, do not start VACUUM first.

## Could not prove

- Which of Codex or Claude Code emitted the historical oversize requests.
- The exact rejected compressed body size beyond the strict `>262144` bound.
- That the existing live orphan exits during the approved restart; the startup
  receipt is the authoritative checkpoint result.
- A green unscaled collector proof suite on the currently disk-constrained
  machine: the literal failures are recorded above.
- `SEAT-RULES.md` was not present in this clone or elsewhere under the searched
  user/project paths; the explicit lane instructions were followed directly.

No live ledger was opened by this lane, no process was signaled or restarted,
and nothing was pushed or published.

## Lane 2 — maintenance worker readiness on the 20 GB ledger

### Root cause and measurement

The worker said “ready” only after it had loaded configuration, checked privacy,
opened and initialized the SQLite ledger, and constructed maintenance plus both
tailers. The parent allowed 10 seconds for all of that. On a copy of the live
post-checkpoint ledger (approximately 20 GB), the measured
pre-ready steps were:

| Step | Measured time |
| --- | ---: |
| `LocalEventBuffer` construction (`openBuffer`, without workspace selection) | 11,264 ms |
| Workspace selection | 160 ms |
| `CollectorMaintenance` and tailer construction | 2,503 ms |
| Total before the old ready signal (excluding close) | 13,927 ms |

`LocalEventBuffer` construction was the individual step that exceeded the
10-second ready deadline. This measurement used an APFS copy of the live ledger;
all SQLite writes were confined to that copy. The same failure after the
operator reduced the live WAL from 27,201 MB to approximately zero independently
shows that WAL size was not the readiness blocker.

### Lane 2 changes

- Commit `3498294` (`fix: decouple maintenance readiness from ledger open`)
  installs the worker IPC handler and sends `ready` before configuration,
  privacy checks, SQLite open/schema work, or tailer construction. Lazy
  initialization begins only after the parent sends the job, so the existing
  job deadline remains the guard for heavy work.
- Worker stderr is inherited by the collector and emits content-free JSON lines
  with `warning:"maintenance_worker_stage"`, `stage`, and `ms`. Stages include
  process/ready, initialization, config, privacy, ledger open, and maintenance
  construction.
- Commit `b14244a` (`fix: reap maintenance workers confirmed gone`) rechecks the
  nonce-bound process fingerprint after TERM grace, KILL, and KILL grace. If the
  original PID is confirmed gone even though Node did not emit `close`, it
  detaches the stale child and clears `orphanRisk` immediately. PID reuse and
  missing initial fingerprint remain fail-closed.
- Commit `062cc8b` (`test: match maintenance worker ready deadline`) makes the
  real-worker integration proof use the same 10-second process-start deadline
  as production. Ledger work is no longer inside that window.

### Lane 2 verification

| Status | Check | Exact command | Last output |
| --- | --- | --- | --- |
| PASS | Ready independent of initialization and stage receipts | `pnpm proof:maintenance-worker-ready` | `checks:4`, `passed:4`, `readyBeforeInitialization:true` |
| PASS | Job deadline still fires and reaps | `PLIMSOLL_MAINTENANCE_PROOF_FOCUS=job_deadline pnpm proof:maintenance-boundary` | all seven progress stages `timedOut:true`, `reapedChildren:1` |
| PASS | Busy worker TERM/KILL confirmed-gone path clears fence | `PLIMSOLL_MAINTENANCE_PROOF_FOCUS=busy_worker_kill pnpm proof:maintenance-boundary` | signals `SIGTERM,SIGKILL`; `childPresent:false`; `orphanRisk:false` |
| PASS | Lane-1 orphan recovery remains intact | `PLIMSOLL_MAINTENANCE_PROOF_FOCUS=orphan_recovery pnpm proof:maintenance-boundary` | recovered spawns `2`; stuck spawns `1`; stuck failure receipt `1` |
| PASS | Source and bundled real worker | `PLIMSOLL_MAINTENANCE_PROOF_FOCUS=real_worker pnpm proof:maintenance-boundary` | source and dist worker passed; stage lines visible; both reaped cleanly |
| PASS | Typecheck | `pnpm exec tsc -p tsconfig.json --noEmit` | no output; exit 0 |
| PASS | Collector package build | `pnpm --dir packages/collector-cli build` | `dist/cli.mjs 1.9mb`; `Done in 1953ms`; exit 0 |
| NOT_RUN | Full maintenance-boundary suite | `pnpm proof:maintenance-boundary` | Not run in Lane 2; focused proofs cover the changed paths. Lane 1 recorded an existing HTTP-timeout failure. |
| NOT_RUN | Package test/lint scripts | package scripts | `packages/collector-cli/package.json` defines only `build`; no test or lint script exists. |

Proof attempts using the older proof-only 3-second real-worker ready limit
intermittently returned `maintenance_worker_ready_timeout` under concurrent I/O.
That proof limit was aligned with production's 10-second process-start contract
in `062cc8b`; this is not represented as a green full-suite run.

### Updated operator recovery after deployment

The WAL is already truncated; do not repeat the out-of-band checkpoint merely
because of this fix. After these commits are deployed through the reviewed
release path and the exact deployed head is verified, James must approve one
collector restart so the process loads the new worker entry. Then confirm:

1. `maintenance_worker_stage` shows `process_up` and `ready_sent` immediately,
   followed by the measured initialization stages inside the job.
2. `maintenance_worker_ready_timeout` does not recur.
3. Any initialization or scan overrun is reported as the job-level
   `maintenance_deadline_exceeded`, followed by TERM/KILL and either a close or
   confirmed-gone reap with no persistent `maintenance_child_not_reaped` fence.
4. Later scheduled maintenance completes and retention/checkpoint activity
   resumes. The 20 GB main database does not shrink merely because the WAL was
   truncated or rows are pruned. Consider offline `VACUUM` only in a separately
   approved maintenance window with the collector stopped, backup/integrity
   plan confirmed, and free disk greater than one full database copy plus
   operational reserve.

Lane 2 did not signal or restart any live process, write to the live ledger,
push, deploy, or open a PR.
