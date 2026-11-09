# Changelog

## 0.7.60 — 2026-11-09

- A retention expiry is not an edit (plimsoll N89, N105). The session-summary
  delete triggers skip dirty marks, ledger revision bumps and segment repairs
  for a raw row whose retention receipt is written, and an expired checkpoint
  on a complete summary stays valid instead of walking a predecessor search
  into a from-scratch recompute. Hosts stop re-sending thousands of old
  sessions a day, and a rollout gate's targets complete inside their bound.
- Session summary reads: the append read orders by the queue rowid and the
  repair read seeks its bounded rowid window without the session index, which
  removes two temporary sorts per read.

## 0.7.59 — 2026-11-07

- Dashboard window adds, replacements, deletions and expiry use the same
  cutoff and cursor membership (plimsoll N89). A late historical event ahead
  of a partial expiry cursor is counted before expiry removes it, preserving
  its stored model contribution instead of subtracting usage never added.
- Existing projections rebuild their window, source, daily, model and action
  aggregates privately from retained facts and uncanceled compact history,
  checking one settled window per maintenance pass and atomically replacing
  only drifted aggregates. The last valid published snapshot and full `/status`
  keep serving during repair, including after a downgrade and re-upgrade.
  Negative totals and parity drift schedule the
  affected window for automatic repair; invalid source evidence still refuses
  publication. No usage, lineage, receipt or delivery retention changes.
- Derived schema version 4 fences older projection writers while repair is
  pending. Rolling back the collector preserves the ledger; older readers
  report `projection_schema_newer` until this version returns.

## 0.7.58 — 2026-11-07

- One retention scheduler (plimsoll N50, N87). The daemon's cadence is the
  only pruner: the maintenance child no longer prunes outside the daemon's
  WAL-pressure gate. Raw, secondary and disposable-telemetry stages take one
  bounded writer turn each and rotate, so an unfinished raw sweep no longer
  postpones the other tables; finished stages rest six hours. Busy and failed
  passes back off from 200 ms (large backlog) or 5 s, doubling to a 60 s cap,
  instead of retrying every follow-up. `/status` `maintenance.retentionCadence`
  shows the stage, pending stages, streaks and counters.
- Disposable footprint telemetry keeps its existing limits as an explicit
  allowlist: `budget_samples` the newest 1,440 rows and 24 hours,
  `budget_daily` the current UTC day and seven before it. No new window or
  cap applies to usage, metric samples, lineage, receipts, pending delivery,
  audit, privacy or account evidence.
- `ledger compact` opens the source read-only, always verifies
  `buffered_events` and `upload_receipts` rowid identities (also under
  `--verify none`), refuses unknown destination free space, fsyncs the copy,
  never overwrites a receipt, and moves each main/WAL/SHM family together in
  its swap and undo steps.

## 0.7.57 — 2026-11-06

- The daemon reads Claude plan limits for enrolled seats without interactive
  sessions or status-line setup (eco-6hoxj.165.99, issue #172). It reads the
  provider's OAuth usage endpoint every ten minutes, verifies the token's
  profile against the seat's account UUID, email and organization, and emits
  `claude_code` observations through the existing plan-limit emission state.
  Five-hour and seven-day windows retain their provider names, percentages,
  reset times and credential-reported plan type, with the same account hash
  as Claude events. The OAuth heartbeat leaves room for polling jitter.
- Missing accounts, unavailable/expired credentials, identity mismatches and
  failed reads are counted in `/status`. Keychain reads refuse permission UI;
  tokens stay in memory, and only allowlisted plan-limit facts enter uploads.
  The reader keeps private spacing and backoff state under `claude-plan-limits`
  in the collector home, independently of the manual provider adapters; it
  produces no adapter receipt. Attempts reserve ten-minute spacing per account
  across seats and restarts before reading credentials. Retry-After and
  single-flight polling bound failures; shutdown drains the in-flight read.

## 0.7.56 — 2026-11-05

- Codex forked rollouts keep their own filename session id when inherited
  parent metadata appears later (plimsoll N61). Existing cursors repair the id
  on resume while preserving their byte offset, counter index and totals.
- Conflicting usage leaves the first event intact and commits a durable
  conflict observation and token-counted loss, allowing the cursor to advance.
  Other repeated slice failures park at their offset until the file metadata
  or collector version changes; diagnostics name the known commit errors.

## 0.7.55 — 2026-11-05

- Claude Code telemetry keeps its producer token out of `~/.claude/settings.json`
  and every `~/.claude-seats/<seat>/settings.json` (plimsoll N60, eco-754vw6.46).
  The token lives in one private `~/.claude/plimsoll-claude.headers` (0600); the three
  managed hooks are command hooks that read it at run time and write the
  producer-parity receipt, and OTLP headers come from a collector-owned
  `otelHeadersHelper` script (`<collector home>/bin/claude-otel-headers.sh`,
  0700). `setup --yes`, the managed-config reconcile and
  `rotate-producer-token --source claude_code` write both files before any
  settings file, migrate the inline http-hook form and the legacy inline curl
  hooks in place with backups, and `doctor` reads `telemetry.claude` valid only
  when settings, header and helper all match. A host updated from an older
  collector heals on its next reconcile cadence or with `plimsoll setup --yes`.

## 0.7.54 — 2026-11-05

- Fresh-ledger cutover skips checkpoints the tailer would rebuild and reports
  `invalidCursorsNotCarried`. Present files without carried cursors retain byte
  fences, so legacy rows no longer block the plan or replay archived history.
- `upload --tool-stats-weeks` delivers completed-week reports without starting
  capture, using the daemon's bounded reader and existing acknowledgements.
  Busy hosts can stop before Monday 00:00 UTC, deliver the previous week after
  the boundary, then plan and switch without recording current-week attempts.

The 2026-11-01 lifecycle incident applied a 12,555,559,952-byte WAL during
quiescence for about eight minutes. The synchronous checkpoint prevented
fence renewal; completion then lost its fence after the runtime had switched.

- Quiescence and its TRUNCATE checkpoint run on a worker while the lifecycle
  lease renews. The worker holds the exclusive SQLite lock through the clone
  and exits before the operation proceeds. A lost fence still refuses further
  mutation and leaves a resumable journal.
- An interrupted update prints the normal JSON receipt envelope with
  `status: interrupted`, phase, operation ID, reason and the exact resume
  command, persists a uniquely named interruption receipt, and exits 1.
  Retrying the original command with the same operation ID resumes from
  `switched` or `verified` without snapshotting or switching again.
- `--snapshot-database-max-bytes N` (config `snapshotDatabaseMaxBytes`) skips
  the ledger snapshot above the measured ledger plus WAL size. Config and
  service stay in the snapshot; its receipt and inventory report
  `skipped_by_operator` and `cannotRestoreReason`. Automatic rollback restores
  runtime, config and service while preserving the live ledger. No bound is
  the default. A database file moved out of an ordinary snapshot is an explicit
  restore refusal: move it back before resuming; no live ledger is removed.
  Skipped snapshots retain the metadata flag that a ledger existed: even the
  actual 0.7.53 reader refuses their missing file before it can remove a live
  ledger; 0.7.54 understands the limited restore.
- Preflight explains that clones share blocks and cost new space for every
  page rewritten until removed or moved to another volume. The large-ledger
  runbook distinguishes explicit runtime rollback from an operation recovering
  its own snapshot.
- `lifecycle captures redact` rewrites existing `capture-config-*.json`
  diagnostic captures using credential field names from the config schema,
  with a durable receipt of file names and field names only. The legacy
  diagnostic capture writer is absent from this release. Restorable config
  backups retain their existing credential-preservation guarantee.
- Quiescence handle probes disable address and service-name resolution in
  `lsof`; they retain the all-process file search and fail-closed timeouts.
- The worker-close proof inventories only its own process (workers share
  its file-descriptor table), avoiding a host-wide `lsof` scan on a shared
  runner while retaining its handle-count and exclusive-barrier assertions.
- `proof:lifecycle-update-recovery` exercises the long checkpoint, nonzero
  interruption process exit, both resume phases, clone bound, safe restore,
  preflight and value-blind diagnostic redaction. Pairing-index warnings and
  the stopped-service upgrade remain unchanged.

## 0.7.53 — 2026-11-01

Incident: on the same `studio0_scale` host, eighteen minutes into the 0.7.52
catch-up at 871 rows a second, the WAL grew from 1.4 GB to 12.6 GB and free
disk fell toward zero. The checkpoint worker ran every 5 s and copied nothing
back: the retention hold count (one `count(*)` over the 33 million rows past
the cutoff, on a read-only worker with no deadline) held a read snapshot from
the first status refresh on, and a snapshot pins every WAL frame written after
it. The lifecycle update's APFS clone of the ledger would then have turned
every checkpointed page into lost space.

- The hold count reads in pages of 5,000 rows by `(created_at, id)` through
  `idx_events_retention`, so no statement holds a snapshot for long, and gives
  up after 30 s with `retention_hold_count_interrupted`; the next attempt
  waits 10 minutes. The bounded 513-row sample stays the answer in between
  (`heldForUploadExact: false`), as it did before an exact count arrived;
  `retention.holdCount` in `/status` names the last interruption and the
  remaining back-off.
- Retention passes defer while a reader pins a WAL of 1 GiB or more
  (`walCheckpoint.heldByReader` with `walBytes` at the bound): the cadence
  counts them as `deferred`, logs `retention_deferred_wal_pressure` once and
  `retention_resumed_wal_pressure_cleared` when the frames are copied back.
  The prune can no longer outrun the checkpoint on a pinned WAL, whatever
  holds it.
- Operators of a large ledger: `lifecycle update` keeps an APFS clone of the
  ledger as its rollback snapshot and `snapshots prune` keeps at least one.
  Under heavy deletion the clone costs disk page by page; move the snapshot's
  `database` file to another volume right after the update (runbook
  `docs/runbooks/ledger-compact.md`, "Large ledgers and lifecycle snapshots").
  A lifecycle option to skip the database clone above a size bound is a
  follow-up.
- Proof: `proof:collector-reliability` adds
  `retention_hold_count_pages_its_read_and_stops_at_its_deadline` and
  `retention_cadence_defers_while_a_reader_pins_a_large_wal` (20 checks).

## 0.7.52 — 2026-11-01

Incident: on a `studio0_scale` host the ledger grew to 171 GB with raw
retention set to 14 days. The daemon's retention pass ran 34 times (128 rows
each) and then never again: its gate waited for the maintenance and enrichment
schedulers to be idle, and on a busy host they rarely were. Enrichment
children were left unreaped because one slow `ps` identity check folded into
"not ours", so nothing was ever signalled and every later tick failed with
`enrichment_child_not_reaped`.

- Retention passes yield to the ledger, not to other schedulers. A writer held
  by another connection past a 25 ms busy timeout defers the pass (a `busy`
  receipt); the in-flight gate is gone. Each pass is bounded by rows and by a
  250 ms writer budget, and a pass cut short moves its cursor to the last row
  it visited, so nothing is skipped. The pass size doubles from 128 up to
  4,096 rows while a large backlog (50,000 or more rows past the cutoff)
  clears inside half the budget, halves after an overrun and resets once the
  backlog is gone. While a large backlog clears, the next pass follows after
  a wait equal to the last pass's own duration, at least 200 ms and at most
  5 s, overrun or not (the overrun already halved the page): the prune takes
  at most half of the collector's thread, and a pass stalled by cold reads
  on a contended disk is followed by an equal pause for the intake queued
  behind it. A held writer is retried after 200 ms, doubling on each
  consecutive hold up to the 5 s follow-up; without a large backlog the
  follow-up is 5 s. On the incident host the first cut of this release
  waited 5 s after every pass cut short by cold reads and drained under a
  hundred rows a second, days of catch-up. Proof: a 500,000-row synthetic
  backlog clears in bounded passes with no 5 s wait while the backlog is
  large, a 1 s stall is followed by a 1 s wait, and a held writer backs off
  200, 400, 800 ms (`proof:collector-reliability`).
- Enrichment children settle by their handle. The `ps` identity check now has
  three answers (ours, another process, unknown) and a bounded cap; only a
  positive mismatch refuses to signal, an unanswered check never does, and a
  child Node already reports as exited is not signalled. A child whose parent
  never sends `run` leaves on its own after 30 s. Proof: hung and throwing
  verifiers settle within the deadline (`proof:enrichment-job`).
- The default retention follows the ledger budget on a `studio0_scale` host.
  `retentionDays` other than 90, or `retentionDaysPinned: true`, always wins;
  otherwise the host keeps as many whole days as 16 GB holds at the measured
  admission rate (2,400 bytes a row, an estimate), floored at 7 and capped at
  90, once seven complete days are measured. `/status`, `plimsoll status` and
  the maintenance worker use the same effective days; `retentionPolicy` names
  the days, their source and basis. Proof: `proof:retention-policy`.
- Secondary retention with receipts. Acknowledged upload receipts older than
  30 days whose raw row is gone, and summary rows older than the effective
  retention whose raw row is gone, expire into `secondary_retention_receipts`
  with totals in `secondary_retention_control` (`secondaryRetention` in
  `plimsoll status`). Dead receipts, receipts whose raw row still exists and
  legacy receipts without lineage never expire. A finished sweep rests six
  hours. Proof: `proof:collector-reliability`.
- `plimsoll ledger compact --into ABSOLUTE-PATH [--plan] [--verify ...]`
  copies the stopped ledger with `VACUUM INTO`, verifies every table's row
  count against the source and prints the swap steps with their undo. It
  refuses while a collector connection is open, when the destination exists or
  its directory is not private, and when the destination lacks free space.
  Runbook: `docs/runbooks/ledger-compact.md`. Proof: `proof:ledger-compact`.
- The weekly tool-statistics read runs on a reader thread, through the
  `observed_at` index, in one-hour chunks, under a 30-minute deadline; a
  failed read backs off up to an hour. Incident, 2026-11-01, same host: the
  week's coverage query, planned without statistics through the workspace
  index, read every row of a 43-million-row table on the collector's main
  thread for 3.5 hours, and intake, `/status` and retention waited. Proof:
  the read is planned through the observed index, the reader thread's result
  equals the in-process aggregate, a passed deadline ends the read, and the
  frozen report is the same from either read
  (`proof:weekly-tool-stats-reader`).

## 0.7.51 — 2026-10-05 (unpublished)

- #453 adds the dispatch binding index, bounded dispatch history and explicit
  adoption qualification as a rollback bridge. Adoption remains off and is
  refused against the released 0.7.48 reader; activation requires a later
  reviewed build and qualification of the actual installed pair.
- #453 adds test-only join-proof diagnostics without changing proof limits.

## 0.7.50 — 2026-10-05 (unpublished)

- #458 keeps source-recorded Codex processing-tier metadata readable before
  enabling any tier writer, and preserves reported cached-input counts,
  including zero, while absent counts remain unknown.
- #460 reads distinct queued session IDs and checks for each session's first
  ready event, preventing a blocked session's backlog from forcing full
  session re-sends.

## 0.7.48 — 2026-10-03 (unpublished)

- Skip plan-limit observations without an account binding. Rollout commit
  exceptions now produce content-free diagnostics, preserving the existing
  usage arithmetic, deterministic IDs and session authority.
- Classify Codex OTLP resource service names as Codex even with another valid
  producer credential, and warn once per process about that mismatch. A
  Codex Desktop service with a valid local Claude credential is accepted and
  stored as Codex, preserving Studio0 Desktop usage. Invalid, unknown,
  malformed or header-mismatched credentials remain refused. The dispatch
  integration proof checks both admission and token rejection, including
  deduplication and one warning containing source names only.
- Attach complete, bounded dead-letter censuses to capture gaps. Unknown
  amounts stay null; untyped or oversized intervals remain known gaps. An
  census-schema or generic claim refusal permits one signed resend without
  the census. Named signature, clock or admission refusals retain their guard.
- **Release prerequisite:** cloud PR #279 must be merged and live before the
  lead authorizes the 0.7.48 GO file. The lead owns that production check.
  The studio3 live-tail exception remains unconfirmed by scratch replay;
  this release does not claim that outage resolved.

## 0.7.43 — 2026-09-26

- `plimsoll dispatch bind` assigns a bounded work item, role, and optional
  technique to a session before capture. Codex OTLP logs and Codex and Claude
  tailer rows carry the binding; a late bind needs an explicit restamp of
  unsent local rows. GitHub repository names are hashed in outbound work IDs
  (`eco-6hoxj.165.3`).
- A bound Codex SSE usage log keeps its dispatch fields when it pairs with a
  response span; the neutralised span carries none (`eco-6hoxj.163.99`).
- A loaded LaunchAgent with zero runs gets one kickstart and a fresh status
  check. Capture-root discovery identifies homes already covered by a local
  producer and warns before adding a second file path (`eco-6hoxj.163.91`,
  `eco-6hoxj.163.93`).
- A plain lifecycle update builds the existing Codex pairing indexes after
  readiness and records the result in its durable completion receipt. A busy
  ledger yields a bounded, nonfatal skip (`eco-6hoxj.163.100`).
- Codex OTLP service names are classified as Codex, and a response span keeps
  its conversation link (`eco-6hoxj.163.98`). A Codex service that sends with
  a Claude credential now gets `401 source_mismatch` and must use the Codex
  source and token.
- Claude capture health stays amber while session counts reconcile after a
  collector restart; a real local token transcript missing from the ledger
  still reads red (`eco-6hoxj.163.101`).

## 0.7.42 — 2026-09-26

- Busy-session summaries preserve rebuild progress while unread rows change.
  A terminal receipt retarget advances the old session revision so a 0.7.41
  worker rejects stale data during a downgrade (`eco-6hoxj.163.87`).
- Codex and Claude tailers keep skipped usage visible as a possible loss when
  a JSON discriminator probe saturates. A revisit queue resumes partial files
  without losing the capture claim gap (`eco-6hoxj.163.81`).
- Coverage walks page saved partial cursors and record a gap when a known
  partial file vanishes before its directory entry is reached. A file created
  and removed entirely between walks remains outside this claim
  (`eco-6hoxj.163.82`).
- B1 observe-only budget sampling records ledger size, process memory and CPU,
  outbox age, and summary lag. Status and CSV show advisory targets and local
  history; no capture budget is enforced. A stopped collector purge also
  removes ledger WAL and SHM files (`eco-6hoxj.164.3`).
- Codex SSE logs and matching response spans pair in the local ledger before
  usage upload, so a response counts once while the duplicate span remains
  available as raw evidence. The stopped-service pairing index upgrade can be
  repeated after a 0.7.41 rollback to refresh the historical cursor and target
  (`eco-6hoxj.163.95`).
- HTTP deadline proofs cover file-backed status probes, and maintenance FIFO
  proofs bound deadline-to-reap and absolute hook latency. The runtime keeps
  the bounded HTTP request behavior (`eco-6hoxj.163.83`).

The replaced dashboard and session-summary triggers remain safe for a 0.7.41
downgrade (F14).

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

- Capture coverage reads large directories a few entries at a time and keeps
  each source within 4,096 work units per turn. A large Codex day folder can
  keep growing while coverage works through it; the check resumes its open
  directory cursor and finishes, and later files are picked up by this check
  or the next one. The queue stays at 4,096 files. A folder removed or renamed
  makes the check incomplete. If a listed Codex or Claude file disappears,
  coverage uses its saved capture progress to mark it finished or record a
  gap; repeated file removals no longer hold the source frontier indefinitely.
  On a source's first coverage walk, a changed folder records an uncertainty
  gap for any file removed before its open cursor could list it. Later walks
  compare saved partial capture cursors with the files checked, so a known
  partial file deleted before listing becomes a gap. Small changed folders get
  one verifying restart. Small unchanged folders reuse a bounded
  list of names on later checks; every file is still checked for changes.
  Files created and removed between walks without ever being listed cannot be
  detected without a filesystem journal; coverage does not attest those files.
  Ignored names use work but do not count toward the 200,000 relevant-entry
  ceiling. Codex, Claude Code, and Grok each get a share of every turn, with
  the first source rotating and unused time returning to active sources.
  Legacy symlinked Codex or Claude roots now make coverage incomplete, like
  configured roots.
  The 250 ms coverage turn is a target: a synchronous directory read, file
  stat, or database write can run past it.
- When the OTLP intake spool cannot hold a refused request (full, or the disk
  refuses), a deadline refusal is answered `503` with `Retry-After: 1` instead
  of `408`, which OTLP exporters do not retry. A body that never finished
  arriving is still `408`.
- Local `GET /status` stays closed without the management credential.
  Unauthenticated liveness remains `GET /healthz` (`{"ok":true}` only). Fleet
  readers that used raw `/status` migrate to `/healthz` or `plimsoll status`
  (`docs/runbooks/local-status-http.md`, `scripts/native-status-read.py`).

### Fixed

- Codex rollout and Claude transcript capture now revisits files left unread in
  older folders, after partial reads, and after post-enrollment growth. Records
  beyond the reader ceiling are passed in bounded chunks; skipped kinds and
  bytes are counted, and any possible usage loss remains a capture gap after
  the file reaches its end. Same-session rewrites with unchanged prior usage
  events replay through stable identities so tokens are not counted again.
  Skipped-record kinds use JSON nesting. The bounded reader also checks the
  whole skipped record for duplicate or escaped discriminator keys before it
  calls a skip known non-usage. An uncertain skip stays a possible usage loss
  in the cloud claim, even after its file reaches EOF. Each key counter keeps
  scanning independently; a saturated or incomplete counter cannot certify
  non-usage even across a saved continuation. An older saved skip is
  reclassified on resume. Receipt identity includes a hash of the first 2 KiB:
  same-offset, same-size rewrites sharing that prefix still share one receipt.
  Earlier pre-enrollment bytes remain excluded. Once post-enrollment growth
  advances the shared cursor to EOF, those earlier bytes cannot be imported
  through that cursor; a separate historical cursor and identity policy is needed.
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
