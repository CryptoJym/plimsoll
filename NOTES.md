# Issue #181 — maintenance deadline starvation and quarantine blame

## What happened first: this lane was not empty

The task brief said an Ox lane "produced nothing". That was wrong. When this
session started, an `opencode` process (PID 56627) was **live in this working
directory** and mid-flight; it had already modified `cli.ts`,
`maintenance-boundary.ts`, `package.json` and created
`maintenance-starvation.ts` and `maintenance-starvation-proof.ts`, and its
final prompt instructed it to `git commit` everything.

Actions taken, in order:

1. Copied every file it had touched to `.p181-preserved/ox-lane/` before doing
   anything else (never throw work away).
2. Backed out the one edit I had already made to `maintenance-boundary.ts`, so
   I was not corrupting a file another process was writing.
3. Waited. The lane exited; it landed commit `222739d` at 19:32:38 MDT.
4. Took ownership of the directory and audited what it had left.

Its runtime work for **defect 2 (blame attribution)** and **defect 3
(starvation visibility)** is real and I kept it, extended. Its work for
**defect 1 (resumability)** did not exist, and its proof was a false green.
This commit finishes the job on top of `222739d`.

## The false green (the finding that mattered most)

`pnpm run proof:maintenance-starvation` exited **0 while printing nothing at
all**. `quarantineProvableBlameProof()` awaited a promise that never settled —
the fixture `FakeChild` never closed on a `shutdown` message, so
`waitForClose()` blocked on a `ManualClock` timer nobody would ever advance.
With no pending handles Node drained the loop and exited 0. Three of five
checks had never run, and none of the output was ever printed.

That is exactly why the caller's `REWORK-BRIEF.md` could plant the
quarantine defect and the `starving: false` defect on the committed code and
still see exit 0: **nothing was being evaluated.** The controls had never been
seen to fail, so they were not controls.

Fixed three ways:

- `FakeChild.send` now closes on `shutdown`, matching the proven fixture in
  `scripts/maintenance-boundary-proof.ts`.
- The two recovery children now answer with a result receipt instead of
  stalling, so the recovery assertions can be reached.
- A `process.on("exit")` guard fails the run if `main()` did not complete, and
  `main()` asserts an exact expected check count. A silent exit can never read
  as green again.

I verified the caller's two exact plants against the fixed proof; both now
bite (see **Bite proof**, plants 3 and 5).

## What the fix does

### 1. Work makes progress under a deadline — `maintenance-worker.ts`

The all-or-nothing unit was `git_context` resolution. It ran **after**
`runRecent()` resolved, outside every stage timing and outside the
`CaptureWorkBudget`: the whole batch was resolved into an in-memory array and
`applyRepoContextResults` committed it **once, at the end**. A deadline kill
anywhere inside that loop discarded every context the run had already
resolved, and the next cycle started the same batch from zero. That is the
measured shape of the field data — 48,280 links `fill_pending` against 93
contexts ever resolved, over 652 kills.

`resolveRepoContextBatch()` replaces it:

- **commit what completed** — each result is committed as it resolves, so a
  kill keeps every context that finished;
- **bounded** — the pass stops at `gitContextBudgetMs(deadlineMs)`, half the
  job deadline capped at 10s;
- **resumable cursor** — the unresolved remainder is *deferred*, never burned
  as an UNKNOWN result. Deferred contexts keep their durable
  `repo_context_inflight` rows, and the worker carries them into the next job
  (`carriedRepoContexts`), processed ahead of fresh ones so they cannot starve.
  `boundRepoContextCarryOver()` bounds and de-duplicates that carry-over and
  returns any overflow for exact retirement, so no inflight row leaks.

**The 30s deadline is unchanged.** I did not raise it and I have no measured
evidence from this sandbox that would justify raising it. The budget is a
*share* of the existing deadline. The proof asserts `deadlineMs: 30_000` is
still present in `cli.ts` so a later change cannot quietly swap resumability
for a bigger constant.

`resolveMaintenanceRepoContexts()` is kept as a thin wrapper with its original
array return, so `scripts/repo-context-proof.ts` needed no edits.

### 2. Quarantine blames only what it can prove — `maintenance-boundary.ts`

Kept and extended the Ox lane's design, which matches the one I had derived
independently: the only evidence this parent can measure without touching the
filesystem (`staticParentFilesystemIsolationProof` forbids that) is the
candidate's **own time on stage** before the timer fired. A candidate that
held the stage for at least half the deadline window is the measured dominant
consumer — no other stage could have held more. Below that, being on stage
when the kill landed is not evidence, and the blame is recorded
`attribution: "unknown"` and **never applied**.

My addition: a deadline kill with **no** progress frame at all — a child that
died before naming any candidate — previously fired no `onDeadline` callback,
so the kill rate silently undercounted exactly the worst hangs. Every deadline
kill now reports, candidate or not.

Existing assertions in `scripts/maintenance-boundary-proof.ts` (which require
the on-stage candidate to be quarantined) still pass unchanged, because in
both of those fixtures the candidate holds the stage for the entire window and
is therefore proven.

### 3. No silent starvation — `maintenance-starvation.ts`, `cli.ts`

Durable ledger receipt: deadline-kill count and timestamp, the last blame
checkpoint with its attribution, the pending-enrichment backlog
(`fill_pending` links and dirty enrichment sessions), and a `starving` flag.
Surfaced on `/status` via `maintenanceStatus()` and warned to the log on a
kill over a live backlog.

Added for defect 1: `gitContext.committedTotal` (cumulative) and
`gitContext.lastDeferred`. A rising committed total under a live deferral is
the number that separates *resuming* from *restarting from zero*.

## Commands and exit codes

Every command run from
`/Users/utlyze/Documents/Codex/2026-08-22/plimsoll-lanes/p181`, Node v22.22.0.

| Command | Exit |
| --- | --- |
| `npx tsc --noEmit` | 0 |
| `pnpm run proof:maintenance-starvation` | 0 |
| `pnpm run proof:maintenance` | 0 |
| `pnpm run proof:lane-receipts` | 0 |
| `pnpm run proof:maintenance-boundary` | 0 |
| `pnpm run proof:repo-context` | 0 |
| `pnpm run proof:git-context` | 0 |

```
npx tsc --noEmit -> EXIT=0
pnpm run proof:maintenance-starvation -> EXIT=0
pnpm run proof:maintenance -> EXIT=0
pnpm run proof:lane-receipts -> EXIT=0
pnpm run proof:maintenance-boundary -> EXIT=0
pnpm run proof:repo-context -> EXIT=0
pnpm run proof:git-context -> EXIT=0
```

`proof:maintenance-boundary`, `proof:repo-context` and `proof:git-context` are
not in the finish bar but cover code I changed, so they were run too. No
pre-existing or environmental failures were encountered; nothing was skipped.

Green receipt, all five checks:

```
{
  "status": "pass",
  "checks": [
    { "name": "fixture_batch_larger_than_one_window_makes_disjoint_progress_across_runs",
      "detail": { "seededSessions": 10, "stitchedPerRun": [4,4,2,0], "firstRunCount": 4,
                  "totalStitched": 10, "overlap": 0, "dirtyQueueAfter": 0 } },
    { "name": "git_context_batch_is_bounded_committed_per_context_and_resumable",
      "detail": { "seededContexts": 6, "firstWindowResolved": 3, "firstWindowDeferred": 3,
                  "durableResultsWhileResolving": [0,1,2,3,4,5], "resolverCalls": 6,
                  "committedTotal": 6, "budgetMsForProductionDeadline": 10000,
                  "productionDeadlineUnchangedMs": 30000 } },
    { "name": "deadline_kill_records_partial_progress_checkpoint_and_kill_rate",
      "detail": { "deadlineKills": 1, "checkpointStage": "jsonl_open",
                  "attribution": "proven", "heldMs": 100, "pathFree": true } },
    { "name": "quarantine_blames_only_provable_slow_candidates",
      "detail": { "slowCandidateQuarantined": true, "slowAttribution": "proven",
                  "provenQuarantineCrossedRecoveryOnce": true,
                  "fastCandidateQuarantined": false, "fastAttribution": "unknown",
                  "fastHeldMs": 5, "unknownBlames": 1, "pathFree": true } },
    { "name": "starvation_receipt_reflects_seeded_backlog_and_kill_rate",
      "detail": { "fillPendingSeeded": 6, "dirtySessionsSeeded": 3, "kills": 2,
                  "starvingDuringBacklog": true, "starvingAfterDrain": false } }
  ]
}
```

## Bite proof

Each defect was planted back into the production source, the proof was run and
its RED output captured verbatim, the plant was reverted, and the proof was
re-run green. Every `error` string below is copied from the proof's own JSON.

### Plant 1 — the batch is committed once at the end (the #181 defect proper)

`resolveRepoContextBatch`: removed the per-context `options.commit?.([result])`
and committed the whole array before returning, keeping the budget.

```
RED_EXIT(commit-at-end)=1
{
  "status": "fail",
  "error": "each resolved context must be durable before the next one starts\n+ actual - expected\n\n  [\n    0,\n+   0,\n+   0\n-   1,\n-   2\n  ]\n"
}
```

`[0, 0, 0]`: nothing is durable while the batch is resolving, so a kill
discards it. Fixed, the same observation reads `[0, 1, 2, 3, 4, 5]`.
`GREEN_AFTER_REVERT=0`.

### Plant 2 — the batch is unbounded (no budget, no deferral)

Also disabled the budget check and the deferral.

```
RED_EXIT(resumability)=1
{
  "status": "fail",
  "error": "one budget window must resolve only part of the batch\n\n6 !== 3\n"
}
```

`GREEN_AFTER_REVERT(resumability)=0`.

### Plant 3 — blame whoever is on stage (the caller's exact plant)

`maintenance-boundary.ts`, replacing the evidence test with
`const proven = timedOut && timedOutProgress !== null;`

```
RED_EXIT(proven-always)=1
{
  "status": "fail",
  "error": "the fast candidate must NOT be quarantined merely for being on stage\n+ actual - expected\n\n+ 'sha256:3714f75c29a77ea47f7d5f2107318085e4eb439d4a4110bff64ec7054b40a161'\n- null\n"
}
```

The innocent candidate held the stage for a measured 5ms of a 100ms window and
is named as the quarantine subject. The variant plant `if (timedOutProgress)`
on the apply site produces the identical RED. `GREEN_AFTER_REVERT=0`.

### Plant 4 — the backlog counter returns zeros

`maintenanceBacklogSnapshot` hardcoded to `{ 0, 0 }`.

```
RED_EXIT(counter)=1
{
  "status": "fail",
  "error": "receipt must count the seeded fill_pending backlog\n\n0 !== 6\n"
}
```

`GREEN_AFTER_REVERT(counter)=0`.

### Plant 5 — the starvation flag is hardcoded false (the caller's exact plant)

`starving: deadlineKills > 0 && (...)` replaced with `starving: false`.

```
RED_EXIT(starving-flag)=1
{
  "status": "fail",
  "error": "kills over a live backlog must surface as starvation\n\nfalse !== true\n"
}
```

`GREEN_AFTER_REVERT=0`.

### Plant 6 — the silent green returns

`await new Promise<void>(() => undefined);` at the top of
`quarantineProvableBlameProof`, reproducing the original hang.

```
RED_EXIT(falsegreen)=1
{
  "status": "fail",
  "error": "proof_exited_before_completion",
  "checksRun": 3,
  "expected": 5
}
```

Before the guard this exact condition exited **0 with no output**.
`GREEN_AFTER_REVERT(falsegreen)=0`.

## Weakest part

**The blame threshold is a heuristic, and it is the weakest thing here.**
"Held the stage for at least half the deadline window" is a real measurement of
that candidate, and it is the strongest evidence available to a parent that is
statically forbidden from touching the filesystem and only ever sees a
`sha256:` candidate hash. But it is not a probe of the candidate's own work. A
candidate that is genuinely slow but yields a progress frame just before the
kill lands is recorded UNKNOWN and escapes quarantine — the fix deliberately
prefers that miss to the false accusation the issue is about, but it is a miss.
A true per-candidate probe would have to run in the child and be reported
before the kill; that is a protocol change I did not make.

Second weakest: **the carry-over cursor is in the child's memory, not the
ledger.** Contexts deferred by the budget survive across *jobs* because the
child is persistent, but if the child is killed the carry-over dies with it and
those contexts wait for the parent's `recoverRepoContextState()` to release
their inflight rows. The results already committed are durable — that is the
part the issue was actually about — but the *remainder* is not resumable across
a child restart. Making it so needs a durable queue keyed by something other
than the raw `cwd`, which is deliberately never persisted; that is a design
question I did not want to answer unilaterally.

Third: **`resumableBatchProgressProof` (the enrichment-cursor check inherited
from the Ox lane) does not bite.** It characterises resumability that
`runRepoEnrichmentMaintenance` already had before this issue, so no plant makes
it red. I left it because it is a true and useful regression guard, but it
proves nothing about this change. The check that carries requirement 1 is
`git_context_batch_is_bounded_committed_per_context_and_resumable`.

Fourth: the budget share (0.5) and the 10s cap are **chosen, not measured**. I
have no ledger in this sandbox to measure a real `git_context` pass against.

## Files

Changed on top of commit `222739d`:

- `packages/collector-cli/src/maintenance-worker.ts` — bounded, per-context
  committed, resumable `git_context` batch; carry-over cursor and its bound.
- `packages/collector-cli/src/maintenance-boundary.ts` — every deadline kill
  reports, including the no-candidate case.
- `packages/collector-cli/src/maintenance-starvation.ts` — `gitContext`
  committed/deferred receipt.
- `scripts/maintenance-starvation-proof.ts` — hang fixed, completion guard
  added, requirement-1 check added, backlog fixture arithmetic corrected.
- `.gitignore` — ignore the Ox lane's session data and the preservation copy.

Untracked from commit `222739d` (kept on disk, removed from the repo): 4.1MB of
`.oc-data/opencode/*.db` session binaries, `.p181-preserved/ox-lane/*`
(duplicate copies of source files), and `opencode.json` — the repo already has
precedent for this in commit `2f871ea`, "chore: remove stray Ox guard file from
repo root".

No existing assertion was weakened or deleted. The one existing-fixture change
(`FakeChild.send` closing on `shutdown`) makes a hung fixture able to assert at
all; it is in the proof file the previous lane created, not in a pre-existing
proof.
