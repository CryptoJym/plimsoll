# Split B lane report — protocol progress and PARTIAL_OK arbitration

Date: 2026-09-03 MDT
Branch: `maint-split-b-20260903`
Implementation commit: `c0cc35b`
Status: `PASS_WITH_KNOWN_PROOF_FAILURE`

## Delivered

- Protocol schema 4 adds strict, bounded `maintenance_job_progress` frames with
  `stage`, `rows`, `ms`, and `remaining`. The parent acknowledges each frame by
  generation, nonce, and sequence.
- The Split A integration contract is the typed `onDurableCommit` callback on
  `CollectorMaintenance.runRecent`. Split A owns invoking it from its bounded
  stage primitives after their transactions commit. This lane does not add
  those primitives or any cursor schema.
- The current worker also reports its existing durable `git_context` batch
  commit through the same contract.
- Production keeps the 30,000 ms outer deadline and reserves 1,000 ms for the
  last IPC acknowledgement and fingerprint-safe child teardown. The worker is
  given the resulting 29,000 ms work budget.
- A deadline after an acknowledged durable frame resolves as the distinct
  `PARTIAL_OK` outcome. It resets failure state, opens no circuit, increments no
  scheduler failure, and logs `maintenance_partial_ok` without incrementing the
  starvation deadline-kill ledger.
- A zero-progress deadline still rejects with
  `maintenance_deadline_exceeded`, records the timeout, reaps the child, and
  opens the circuit. A real child disconnect/crash remains a failure and opens
  the circuit.
- A partial attempt does not fabricate a normal maintenance result. Scheduler
  and cadence types accept the discriminated partial outcome without adding
  its rows to full-run aggregates. Any unresolved parent repo-context handoff
  is returned to recovery as `boundary_unavailable`.

## Verification

| Status | Check | Exact command | Evidence |
| --- | --- | --- | --- |
| PASS | PARTIAL_OK, zero-progress deadline, scheduler success accounting, real crash circuit | `PLIMSOLL_MAINTENANCE_PROOF_FOCUS=partial_ok pnpm proof:maintenance-boundary` | 2/2 checks; `PARTIAL_OK`, circuit failures 0, scheduler failed runs 0; zero-progress circuit failures 1; real crash reaped with circuit failure |
| PASS | Busy worker durable-progress frame | `pnpm proof:maintenance-worker-ready` | 5/5; frame with 100 committed rows emitted while the worker promise remained busy |
| PASS | Lane 1 orphan recovery rules | `PLIMSOLL_MAINTENANCE_PROOF_FOCUS=orphan_recovery pnpm proof:maintenance-boundary` | recovery spawns 2; stuck spawns 1; stuck failure 1; no double worker |
| PASS | Lane 2 confirmed-gone busy-child reap | `PLIMSOLL_MAINTENANCE_PROOF_FOCUS=busy_worker_kill pnpm proof:maintenance-boundary` | TERM then KILL; `childPresent:false`; `orphanRisk:false` |
| PASS | Existing zero-progress stage deadlines | `PLIMSOLL_MAINTENANCE_PROOF_FOCUS=job_deadline pnpm proof:maintenance-boundary` | all seven diagnostic stages timed out, reaped, and opened one circuit |
| PASS | Typecheck | `pnpm exec tsc -p tsconfig.json --noEmit` | exit 0; no output |
| PASS | Collector build | `pnpm --dir packages/collector-cli build` | `dist/cli.mjs 1.9mb`; done in 4390 ms; exit 0 |
| PASS | Whitespace/error markers | `git diff --check` | no output; exit 0 before implementation commit |
| FAIL | Source and bundled real-worker focus | `PLIMSOLL_MAINTENANCE_PROOF_FOCUS=real_worker pnpm proof:maintenance-boundary` | `maintenance_worker_ready_timeout`; reproduced twice under current I/O pressure. The dedicated real-crash proof inside the PARTIAL_OK focus passed. |
| NOT_RUN | Full maintenance-boundary suite | `pnpm proof:maintenance-boundary` | Not run. Earlier lanes already record an unrelated FIFO/HTTP availability timeout, and this lane used focused proofs for every changed boundary path. |
| NOT_RUN | Package test/lint scripts | package scripts | `packages/collector-cli/package.json` defines no test or lint script. |

## Boundaries and unknowns

- `SEAT-RULES.md` was absent from this clone and the searched adjacent project
  paths; the explicit seat rules in the lane brief were followed directly.
- Split A still must connect each new bounded stage primitive to
  `onDurableCommit`. Until that integration lands, only the existing
  `git_context` durable batch emits this new frame.
- The source/bundled-worker focus is not green on this machine. Its observed
  failure remains literal; no readiness limit was relaxed.
- No stage primitives or cursor schema were implemented. No live checkout was
  touched, no process was signaled or restarted, no live ledger connection was
  opened, and nothing was pushed, deployed, published, or opened as a PR.
