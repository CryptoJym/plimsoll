# SINGLE-SHOT BUILD — no follow-up messages will arrive; finish the ENTIRE job before ending your turn.

You are fixing plimsoll issue #181 in a sandboxed clone. `node_modules` is pre-installed — do NOT run install (no network). Do NOT push, do NOT use `gh`, do NOT touch anything outside this directory. Commit your work locally.

## The defect (root-caused already — do not re-derive, verify then fix)

The maintenance child is deadline-killed at a hardcoded 30s (`packages/collector-cli/src/cli.ts`, near line 1116). Measured consequences on a real ledger: `maintenance_deadline_exceeded` × 652 in the error log; `git_context` enrichment starves (48,280 event-links stuck `fill_pending`, 709 dirty sessions, only 93 contexts ever resolved); the reprice drain starves too. Net: only 13 of 2,423 sessions carry repo/branch/sha linkage, so cost-per-merged-PR has no honest denominator.

Second defect in the same loop: the quarantine MISATTRIBUTES. It blames whichever candidate is on stage when the kill fires — probes of the blamed cwd return in milliseconds — then holds that candidate, expires in 15 minutes, and the loop repeats.

Read the issue text in ISSUE.md for the full sounding.

## What the fix must achieve

1. **Work must make progress under a deadline.** A deadline-kill must not discard the whole batch's progress. Prefer bounded, resumable work (commit what completed; record a cursor) over an all-or-nothing child that dies at 30s. Do not simply raise 30s to a bigger constant and call it fixed — if a larger budget is part of the answer, justify it in NOTES.md with measured evidence, and still make progress resumable.
2. **The quarantine must blame only what it can prove.** A candidate may be quarantined only on evidence that IT was slow/failing (e.g. its own measured probe), never merely because it was on stage at kill time. An unproven blame must be recorded as UNKNOWN rather than applied.
3. **No silent starvation.** The system must surface the starvation state (a counter/receipt showing pending-enrichment backlog and deadline-kill rate) so this cannot recur invisibly.

## Proof required (this is the deliverable, not the code alone)

Write a proof script under `scripts/` (follow the existing proof conventions in that directory — read 2-3 of them first) wired into `package.json`, that FAILS on the pre-fix behavior and PASSES after, covering:
- a fixture batch larger than one deadline window makes measurable progress across consecutive runs (resumability), rather than repeating from zero;
- a slow candidate is quarantined; a fast candidate that merely happened to be on stage at kill time is NOT quarantined (this is the misattribution case — assert on the quarantine record's subject);
- the starvation/backlog counter reflects reality on a seeded backlog.
PROVE IT BITES: plant each defect back in turn, capture the RED output verbatim into NOTES.md under "## Bite proof", then revert and show green.

## Finish bar
`npx tsc --noEmit` exit 0, your new proof exit 0, and `pnpm run proof:lane-receipts` + `pnpm run proof:maintenance` still exit 0 (run them; if a pre-existing failure is environmental, say so with its output rather than editing the test). Paste every command + exit code into NOTES.md. Never weaken or delete an existing assertion. Create `P181.DONE` when green.
