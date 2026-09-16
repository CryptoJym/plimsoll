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

### Added

- Producer-to-ledger parity instrumentation (eco-6hoxj.29): managed Codex/Grok
  curl hooks fail on HTTP errors and retry inside the hook timeout; Claude HTTP
  hooks carry the matching timeout/retry contract; each post mints a stable
  event id; `plimsoll producer-parity` joins those ids to the ledger; collector
  `/status` exposes producer counters and circuit-open transition timestamps.

### Changed

- Local `GET /status` stays closed without the management credential.
  Unauthenticated liveness remains `GET /healthz` (`{"ok":true}` only). Fleet
  readers that used raw `/status` migrate to `/healthz` or `plimsoll status`
  (`docs/runbooks/local-status-http.md`, `scripts/native-status-read.py`).

### Fixed

- A managed-config reconcile that loses the state-file lock still writes its
  apply/refuse receipt; the stamp, backoff map and backup record retry on the
  next tick. The event-loop chunk proof uses a CI-safe bound so a cold runner
  cannot fail a yield that already holds.
- Daemon session sync now converges without `upload-history --sessions`. A
  failed or interrupted 5-minute refresh survives restart, and a ledger
  catch-up covers sessions whose events were already uploaded so they never
  re-entered the touched window.
- Automatic maintenance now keeps committed progress across deadlines, bounds
  cursor and enrichment work, and reaps disposable workers before replacement.
- Maintenance worker startup is separated from ledger initialization and has a
  45-second process-readiness deadline for hosts under heavy disk load.
- `maintenance_failed` log receipts now include a path-free error class and
  message (at most 200 characters), failure stage, elapsed milliseconds, and
  whether a progress frame was acknowledged.
