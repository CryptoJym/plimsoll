## Summary

This change keeps automatic maintenance moving when the collector is under
disk pressure. Work is split into bounded, resumable stages; completed slices
stay committed; and a slow or failed child cannot silently block later runs.

Failures are now diagnosable without exposing local paths. A
`maintenance_failed` line carries a bounded error class and message, the stage,
elapsed milliseconds, and whether the parent acknowledged progress.

## Root causes

- Maintenance work had all-or-nothing sections. A deadline could discard useful
  progress and restart the same work on the next cadence.
- Worker readiness included ledger initialization, so disk contention could
  exhaust the startup deadline before the worker announced that it was alive.
- Enrichment shared the primary maintenance child and could monopolize its
  deadline.
- The worker reduced every internal failure to `maintenance_failed`, removing
  details such as `SQLITE_BUSY` and `database is locked` from operator logs.

## What changed

- Recover and reap orphaned maintenance workers, including bounded startup WAL
  checkpoint recovery.
- Announce worker readiness before lazy ledger initialization; allow 45 seconds
  for process startup.
- Commit retention, WAL checkpoint, pending-link, enrichment, and git-context
  slices under explicit row and wall-clock limits.
- Acknowledge durable progress frames and return `PARTIAL_OK` only after the
  acknowledgement is confirmed.
- Integrate the bounded stages and cursor handoffs into the automatic cadence.
- Run enrichment in its own bounded disposable child job.
- Emit fixed-cardinality rejection summaries and bounded, path-free maintenance
  failure diagnostics.

## Proofs

| Check | Command | Result |
|---|---|---|
| Failure diagnostic boundary | `PLIMSOLL_MAINTENANCE_PROOF_FOCUS=failure_diagnostics pnpm proof:maintenance-boundary` | PASS |
| Worker readiness and failure sanitization | `pnpm proof:maintenance-worker-ready` | PASS |
| Orphan recovery | `PLIMSOLL_MAINTENANCE_PROOF_FOCUS=orphan_recovery pnpm proof:maintenance-boundary` | PASS, 1/1 |
| Bounded stages | `pnpm proof:maintenance-stages` | PASS, 5/5 |
| Stage integration | `pnpm proof:maintenance-stage-integration` | PASS, 3/3 |
| Partial progress protocol | `PLIMSOLL_MAINTENANCE_PROOF_FOCUS=partial_ok pnpm proof:maintenance-boundary` | PASS, 2/2 |
| Enrichment child | `pnpm proof:enrichment-job` | PASS, 5/5 |
| Rejection diagnostics | `REJECTION_PROOF_SCALE=0.0001 pnpm proof:rejection-aggregation` | PASS, 20/20 |
| Typecheck | `pnpm exec tsc --noEmit` | PASS |
| Build | `pnpm exec esbuild packages/collector-cli/src/cli.ts --bundle --platform=node --format=esm --packages=external --outfile=/tmp/plimsoll-collector-build.mjs` | PASS, 1.3 MB bundle |

The full maintenance-boundary proof retains a pre-existing FIFO/HTTP
availability timeout: `timed_out_waiting_for_fifo_child_block_marker`. This PR
does not relabel that failure as passing; the focused maintenance proofs above
are the acceptance evidence for this change.

## Operator steps

After release, restart the collector through the normal installer or service
manager. Watch the collector log for `maintenance_failed`,
`maintenance_partial_ok`, and maintenance stage receipts. If a failure occurs,
use `errorClass`, `message`, `stage`, `elapsedMs`, and
`progressAcknowledged` together; the receipt intentionally omits local paths.

## Rollback

Revert this PR and restart the collector through the same service manager. No
schema downgrade or data rewrite is required. Already committed maintenance
slices remain valid; the older collector can continue from its existing
durable cursors.

## Maintainer commands (James only)

From this checkout, after reviewing the seven commits and final lane report:

```bash
git push --set-upstream origin maint-pr-prep-20260904
gh pr create --repo CryptoJym/plimsoll --base main --head maint-pr-prep-20260904 --title "Fix bounded maintenance recovery and failure diagnostics" --body-file PR-BODY.md
```

These protected actions were not run in this lane.
