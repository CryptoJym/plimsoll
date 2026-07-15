# 0043 — Sync isolation: bounded outbox, poison quarantine, honest retention

## TL;DR

- Source implementation adds a durable sanitized outbox, compact terminal receipts, bounded migration, finite leases, poison continuation, and exact constant-work status gauges.
- Configured capture commits new raw evidence and its delivery projection atomically; unconfigured local capture does not duplicate event storage.
- Raw TTL remains explicitly blocked on `projection_parity`; this change does not delete pending/dead raw evidence or claim #80 is complete.

## Scope

Tracked by [GitHub issue #79](https://github.com/CryptoJym/plimsoll/issues/79). This slice changes the local one-process/one-SQLite delivery path, fake-HTTP proof, CLI/status surfaces, and bounded daemon migration cadence. It does not change the hosted ingest contract, dashboard projections, the live ledger, a LaunchAgent, or any deployed service.

## Context

Implementation branch: `agent/plimsoll-79-outbox`, originally stacked on the #77 scheduler head and rebased onto merged `main` before review. The existing `buffered_events.uploaded_at` remains a compatibility marker; `remainingUnuploaded` retains its legacy meaning while `remainingDelivery` reports active outbox work.

## Acceptance Evidence

- `pnpm proof:outbox`: 27 deterministic checks pass using temporary SQLite files and fake `fetch`; provider network and live state are not touched.
- `pnpm proof`: existing signal-fidelity suite passes, including upload watermark, privacy, join handshake, upload-history, session sync, and outcome sync.
- `pnpm proof:maintenance`: 13/13 checks pass on the merged #77 scheduler/data path.
- `pnpm exec tsc --noEmit -p tsconfig.json`: passes under Node 22.
- `pnpm --dir packages/collector-cli build`: passes under Node 22.
- `git diff --check`: passes.

The focused proof covers atomic rollback/replay, duplicate zero rewrite, configured versus unconfigured capture, bounded migration/reopen/uploaded skip, local invalid rows, poison first/middle/last, global 422, auth, 429, network failure, deterministic backoff, crash after remote success, exact retry bytes, linkage before/after seal, raw-retention compatibility, `--no-mark` zero mutation, row/byte/age pressure, oversize rejection, privacy sentinels, acknowledged-only partial batches, and singleton status gauges.

## Operational Boundaries

- No live ledger, hosted provider, installed collector, LaunchAgent, or cloud schema was read or mutated by the focused proof.
- Terminal receipts contain only a safe deterministic delivery UUID, enumerated terminal reason/status class, timestamps, and attempt count.
- Failure response bodies, validation paths, credentials, raw event IDs, local labels, paths, URLs, emails, prompts, outputs, and tool arguments do not enter active envelopes, receipts, status, or runtime upload errors.
- Pending envelopes are never auto-evicted. Pressure pauses only legacy migration; atomic new configured capture continues and reports degraded state.

## Notes For Future Agents

- `/status.delivery.work` is a deterministic constant-work receipt: one singleton control row, zero active/receipt/raw history rows scanned. Transactional triggers maintain its gauges; deleting the oldest active item uses `idx_upload_outbox_created` to repair only the minimum.
- Migration uses a resumable raw `rowid` watermark and reports last-slice rows/bytes without an exact million-row remaining count. Defaults are bounded at 5,000 rows / 32 MiB per slice and 20 upload batches per daemon cycle.
- `upload --no-mark` deliberately stays on the stateless legacy snapshot builder; it acquires no lease and mutates no attempt, receipt, outbox, or `uploaded_at` state.
- #80 must prove projection parity before changing `compatibility_uploaded_only` retention behavior.
