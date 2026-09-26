# Changelog

## Unreleased

- Active Codex sessions keep their summary rebuild cursor when reconciliation
  edits a row the summary has not read yet. Ordinary delivery acknowledgements
  no longer dirty a session through an outbox delete; privacy-changing lineage
  and terminal receipt changes still do. A worker slice is retried if an edit
  lands while it is being read, and scanned edits or erasures still rebuild
  only their affected session (`.163.87`).
- The scanned-aware summary triggers now use distinct names so a 0.7.41
  downgrade installs its own revision-marking triggers before sync. A CI
  rehearsal rejects a terminal privacy receipt raced with the old worker;
  0.7.39 still reads the full ledger on rollback (`.163.87` round 2).
- A terminal receipt retarget now also advances the old delivery's session
  revision. A 0.7.41 worker rejects a stale read after this edit, and the
  re-upgraded collector rebuilds before sending (`.163.87` round 3).

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

## 0.7.41 — 2026-09-25

- An unfinished session-summary rebuild now resumes across daemon cycles as
  the sync horizon moves (`.163.86`). In 0.7.40, a long session that received
  a raw update restarted its rebuild on every cycle, and session sync on that
  Mac stopped advancing; no events were lost. Rows appended during a rebuild
  are applied after the frozen historical scan, and a raw edit or erasure of
  the session still restarts its rebuild. Partial sync logs name why summaries
  remain pending and how many rebuilds began.
- New events for a session are recorded promptly while its summary uploads
  (`.163.80`). The upload keeps the snapshot it started with, and a later pass
  sends any newly eligible events. Erasures still wait for the active send.
- A busy ledger no longer silently skips the daemon's session-sync carry
  write; it retries that durable write for a bounded time and reports
  exhaustion (`.163.80`).
- Learning results and status show the effective start when local retention
  or the fact cap shortens the requested window (`.163.73`).
- Snapshot retention is safe to interrupt (`.163.30`). A removal is recorded
  before any file moves, and the next 0.7.41 or later prune or completed
  update finishes it or restores what it moved. Releases 0.7.38 to 0.7.40
  cannot read these records, so they skip retention. `plimsoll lifecycle
  snapshots reconcile` (a dry run unless `--apply`) shows why retention is
  blocked and repairs it.
- An update refuses to start while any other process has the ledger, its WAL
  or its shared-memory file open, or when it cannot prove that none does
  (`ledger_in_use`, `quiescence_unproven`). A refused update changes nothing;
  start it again once the ledger is free. Stop every ledger user, including
  SQLite clients and other `plimsoll` commands, during an update window
  (`.163.30`). A rollback checks the restored ledger with
  `PRAGMA integrity_check` and records `restore.integrity` in its receipt.
- `--retention keep-all` still removes no snapshot, runtime version, receipt
  or trash entry; an update can delete a leftover
  `work-ledger.sqlite.restore-*` file from an earlier crashed restore.

## 0.7.40 — 2026-09-24

- Busy-session indexing records session context at capture time and validates
  it once per reopen (`.163.21`).
- On a busy host, uploads claim and acknowledge in 125-row slices and yield to
  the event loop between batches; only ledger WAL checkpoints move to a worker
  thread (`.163.24`).
- Session sync keeps incremental session summaries in the ledger and catches up
  on a bounded cadence from an indexed cursor instead of rescanning history
  (`.163.41`).
- Resource observation now covers the supported directory APIs and proves the
  stable fixture sweep and ownership isolation (`.163.31`).
- Learning-facts maintenance is bounded and capacity-tested, and busy-host Grok
  capture keeps progress durable through worker replacement (`.163.38`,
  `.163.42`).
- Loopback transport is direct and bounded, while lifecycle updates stage runtime
  files transactionally and preserve recovery evidence (`.163.48`, `.163.49`,
  `.163.50`).
- System-e2e path fields are normalized for the CI layout, and the coverage gate
  runs the newly covered proofs with host-only scopes recorded (`.163.59`,
  `.163.45`).
- Session summary uploads use short database write transactions and
  per-session send leases, leaving unrelated ledger writes free during the
  network request. A live lease defers conflicting mutations, and an erasure
  waits for the send so it is never overtaken. Intake for a session that is
  being uploaded waits for that send; past the 750 ms busy budget (a slow cloud
  round trip) its events are spooled and delivered later, never lost.
  Automatic maintenance retries a lease-deferred attempt after a bounded 1-5
  second backoff without counting it as a failure or opening the worker
  circuit; repricing keeps its pending row until a retry succeeds. Multi-batch
  catch-ups converge, and under steady intake the daemon still runs session
  sync once the event backlog fits in one cycle, or, with a larger backlog, at
  the end of the first upload cycle that finishes 60 s or more after the
  previous session pass (`.163.75`).
- The session context index keeps its checksum exact past 2^53, so very large
  ledgers no longer rebuild the index on every open (`.163.75`).
- When a learning-fact table is full, a fact that would be refused anyway
  (a retry with no target, an episode fact with no parent) is dropped before
  anything is evicted (`.163.75`).

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
  When a stale producer's config home is the default because its environment
  names no home variable, doctor's summary and status say how many were
  attributed that way, and status adds `captureHealth.staleProducerAttribution`.
- Producer-to-ledger parity instrumentation (eco-6hoxj.29): managed Codex/Grok
  curl hooks fail on HTTP errors and retry inside the hook timeout; Claude HTTP
  hooks carry the matching timeout/retry contract; each post mints a stable
  event id; `plimsoll producer-parity` joins those ids to the ledger; collector
  `/status` exposes producer counters and circuit-open transition timestamps.

### Changed

- When the OTLP intake spool cannot hold a refused request (full, or the disk
  refuses), a deadline refusal is answered `503` with `Retry-After: 1` instead
  of `408`, which OTLP exporters do not retry. A body that never finished
  arriving is still `408`.
- Local `GET /status` stays closed without the management credential.
  Unauthenticated liveness remains `GET /healthz` (`{"ok":true}` only). Fleet
  readers that used raw `/status` migrate to `/healthz` or `plimsoll status`
  (`docs/runbooks/local-status-http.md`, `scripts/native-status-read.py`).

### Fixed

- OTLP exports the ledger cannot commit in time are kept, not lost
  (`eco-6hoxj.163.17`). An authenticated, validated request that meets a busy
  ledger or runs out of its 1.5 s deadline — including one whose body arrived
  while the event loop was blocked — is answered `202 otlp_spooled` after its
  uncommitted rows are flushed to `otlp-spool/` (normalized ledger rows, never
  the body), and a 2 s drain replays them exactly once through the live
  commit path. Bounded at 5,000 files, 256 MiB and 7 days; reported under
  `/status` `otlpSpool`; `PLIMSOLL_OTLP_SPOOL=off` restores the old answers.
- Local producer admission restores two Studio0 rejection classes
  (`eco-6hoxj.25`): identity-encoded bodies between 2 MiB and 4 MiB are no
  longer `compressed_body_too_large`, Claude HTTP hooks always send
  `x-plimsoll-source` (and the producer token when provisioned), and first-line
  token/body rejections name the closed route so hook vs OTLP is visible
  without a collector restart.
- `forward-hook-http` mints a stable event id before the first attempt so a
  spool replay after a connection reset, a closed socket, or a request timeout
  cannot double-count the event. The client now spools those unknown-outcome
  classes as well as 503, 408, and ECONNREFUSED. A body that already carries a
  UUID is unchanged; a body with no id still gets a fresh UUID on the
  collector's own intake.
- Capture-health names sub-minute future skew in seconds, never renders `NaNm`
  for an unparseable `last_event_at` (fail-safe amber), and keeps future-amber
  rather than lag-red when the ledger watermark is ahead of the clock
  (REVIEW-73-r2 F5–F7).
- Capture health `overall` is `no_events` when every configured source has
  captured nothing yet, so a green lamp cannot hide an empty host. Mixed
  unused sources still leave a capturing host green.
- Activity-scan `error` / `lastErrorCode` is no longer a capture-health scan
  state. Tailers never published it; `last_error_code` remains a finance
  column only.
- A managed-config reconcile that loses the state-file lock still writes its
  apply/refuse receipt; the stamp, backoff map and backup record retry on the
  next tick. The event-loop chunk proof uses a CI-safe bound so a cold runner
  cannot fail a yield that already holds.
- Automatic capture on many-root hosts sizes each cadence's directory-entry
  allowance from the observed root/corpus size (256-entry floor, 16384 cap)
  while the 50 ms discovery wall and 200 ms fairness budget still bound the
  tick. A finished or limited sweep persists its origin so the next generation
  does not restart enumeration at root 0 (eco-6hoxj.73.1).
- Capture-health receipts never publish `0` entries this sweep beside a leftover
  previous-cadence `entriesThisTick`. Native status reads treat that pair as a
  transient unless it persists across two reads at least 10 s apart
  (eco-6hoxj.155).
- Capture-health receipts count the same directory entries on both explicit
  discover walks, name a drained cadence as finished with no cursor to resume,
  and never pair `converging` with `limitReached`. Retry class follows
  `discoveryEntries` visited this cadence, not pending files behind the
  metadata gate (REVIEW-78).
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
