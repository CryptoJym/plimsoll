# Changelog

## 0.7.5 — 2026-09-08

- Stable and finitely growing oversized Codex and Claude JSONL records can
  resume across bounded capture jobs and collector restarts. Partial parsing
  stays separate from committed cursors, with generation checks before reads
  and atomic completion. Existing byte, record, event and time limits remain.
- An explicitly provisioned local Codex conductor can report adjacent observed
  usage intervals through authenticated HTTP and durable producer ownership.
  Retries preserve one event and one delivery. The first observation after an
  attachment or producer restart establishes a baseline without charging usage.
- Observed intervals survive raw retention, remain unpriced and unallocated
  when attribution is missing, and stay excluded from qualified Finance totals.
  Terminal privacy receipts also withdraw retained interval usage.

Continuous file growth can still prevent full-prefix verification from finishing.
The optional live observer covers connected conductor intervals; it does not
establish complete history, Desktop or Claude live coverage, or a native latency
guarantee. Producer activation requires coordinated provisioning and an idle seat.

Before downgrading to 0.7.4, stop capture or keep affected roots disabled for the
entire downgrade: older binaries ignore continuation and retirement state. Keep
the ledger, source files and session authority intact. Disabling the live observer
does not transfer its sessions to a tailer.

## Unreleased

### Fixed

- Automatic maintenance now keeps committed progress across deadlines, bounds
  cursor and enrichment work, and reaps disposable workers before replacement.
- Maintenance worker startup is separated from ledger initialization and has a
  45-second process-readiness deadline for hosts under heavy disk load.
- `maintenance_failed` log receipts now include a path-free error class and
  message (at most 200 characters), failure stage, elapsed milliseconds, and
  whether a progress frame was acknowledged.
