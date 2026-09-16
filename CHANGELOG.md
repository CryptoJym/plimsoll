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

- `doctor --read-only --json` reports `producerProcesses`: the local Codex,
  Claude Code, Gemini CLI and Grok processes, each with its config home, start
  time, managed surface, managed-apply time, `staleConfig`, owner (launchd
  label, conductor seat, desktop app) and a restart hint. When any producer
  started before its managed config was last applied, doctor adds one
  `summary` line naming how to restart them. Readiness is unchanged, a process
  whose environment cannot be read is `unknown` and never stale, and no token,
  command line or path outside `$HOME` is printed.
- While `source_required` or `producer_token_required` rejections are open,
  `status` names the stale-producer count in the capture source's reason and
  in `captureHealth.staleProducers`, from a background scan cached for at most
  60 seconds. When the environment or launchd read fails the scan is
  `partial`, and doctor and status name the failed read and how many producers
  were inspected instead of a zero count. A daemon admission body whose rows do
  not have the expected shape skips the scan and says so; `status` still exits 0.

### Fixed

- Automatic maintenance now keeps committed progress across deadlines, bounds
  cursor and enrichment work, and reaps disposable workers before replacement.
- Maintenance worker startup is separated from ledger initialization and has a
  45-second process-readiness deadline for hosts under heavy disk load.
- `maintenance_failed` log receipts now include a path-free error class and
  message (at most 200 characters), failure stage, elapsed milliseconds, and
  whether a progress frame was acknowledged.
