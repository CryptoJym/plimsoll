# Giving ledger space back: `plimsoll ledger compact`

Since 0.7.52. Raw retention deletes rows, but SQLite keeps the freed pages
inside `work-ledger.sqlite`: the file never shrinks on its own. This runbook
copies the stopped ledger into a fresh, compact file with `VACUUM INTO`,
verifies the copy and swaps it in. Every step has an undo. Nothing here
deletes the old ledger.

## Before you start

- Retention can catch up first. In `/status`, `retention.lastPass.hasMore` is
  `false`, and in `plimsoll status`, `secondaryRetention.lastHasMore` is
  `false`. Compacting earlier gives back the space already freed; a pending
  upload or a continuous raw backlog does not prevent offline compaction.
- Pick a destination directory on a volume with free space of at least the
  live bytes plus 5% (`--plan` prints `requiredFreeBytes`). The directory must
  be private: owned by you, mode 0700, reached through no symlink and no
  group- or world-writable ancestor. Another volume is fine; the copy is copied
  back, not hard-linked.
- The collector must be stopped. An open collector connection refuses the run
  with `ledger_in_use: stop the collector first`.

## Steps

1. Stop the collector: `plimsoll unload-launch-agent`.
2. Plan (reads only):
   `plimsoll ledger compact --into /volume/private-dir/work-ledger.compact.sqlite --plan`.
   Check `status: "ledger_compact_plan"`, `freelistPages`, `liveBytesEstimate`
   (the expected size of the copy) and `destinationFreeBytes`.
3. Compact and verify: the same command without `--plan`. The default
   verification compares every table's row count and the number of schema
   objects between the ledger and the copy. Every verification mode also
   checks the `buffered_events` rowid/id and `upload_receipts` rowid/delivery-id
   mappings: raw lineage and receipt cursors must keep their identities.
   A mismatch refuses the copy for use. `--verify quick-check` runs
   SQLite's `quick_check` on the copy instead; `--verify none` skips counts
   and integrity checks but still checks lineage.
   The receipt is printed and written next to the copy as
   `<copy>.compaction-receipt.json` (mode 0600). Preflight refusals write no
   copy; a copy/verification failure can leave a partial or rejected copy.
   Use only a `compacted` receipt with both verification and lineage passed.
   If the process was killed mid-write, remove `<copy>` and `<copy>-journal`
   before trying again.
4. Swap, following the receipt's `swap` lines exactly: move the old ledger and
   its `-wal` and `-shm` sidecars aside as `<ledger>.before-compaction`, copy
   the compacted file to the ledger path, `chmod 600` it. When the ledger's own
   volume is the one short of space, move `<ledger>.before-compaction` and its
   sidecars into the destination directory instead; the undo moves them back.
5. Start the collector: `plimsoll load-launch-agent`. Check `/status`:
   `retention`, `delivery` and the weekly report fields answer as before.
6. Keep `<ledger>.before-compaction` until the next weekly report is
   acknowledged. The opener barrier sidecar
   (`work-ledger.sqlite.connections.lock.sqlite`) is never moved or deleted.

## Undo (one step)

With the collector stopped, follow the receipt's `undo` lines: move the
compacted ledger to `<ledger>.compacted-rejected`, move
`<ledger>.before-compaction` and its sidecars back to the ledger path, start
the collector.

The receipt moves each main file with its own WAL/SHM family, including the
rejected compacted ledger during undo. It refuses an existing destination or
backup instead of overwriting it. Keep all three files together; never pair
the old main file with the compacted copy's WAL.

## What the copy carries

`VACUUM INTO` copies every row of every table, so the current week's rows,
upload holds, migration protection and all receipts travel with it; the week
gate needs no exception. The collector sets the copy's journal mode back to
WAL on first open.

The source connections are read-only, including when committed rows remain
in its WAL. Compaction reads those rows into the copy without checkpointing
or consuming the source WAL. The verified copy is fsynced before success is
reported. This is an offline copy and explicit swap, not an online `VACUUM`
or an automatic rewrite of a large ledger; an existing freelist needs no
extra deletion before it can be reclaimed. The opener barrier covers
Plimsoll connections; keep any external SQLite writers stopped too.

## Bounded local retention

The daemon rotates one stage per turn: disposable footprint measurements,
raw retention, then secondary cleanup. A raw `hasMore=true` never blocks the
other stages. Each turn starts at 128 rows and may grow to 4,096, with a
250 ms cooperative writer budget and a 25 ms SQLite busy wait. Statements
and the current row may overrun that budget; it is not a hard I/O deadline.
Completed stages rest six hours. Cursors/aggregate receipts survive restart;
the stage rotation itself restarts with the daemon.

Disposable retention is an explicit two-table allowlist, using the existing
footprint contract, even when sampling has stopped:

| Table | Policy |
|---|---|
| `budget_samples` | At most the newest 1,440 IDs; expire samples strictly older than 24 hours. |
| `budget_daily` | Current UTC day and seven prior days; expire strictly earlier days. |

Catch-up deletes at most the turn's row budget; one aggregate record in
`maintenance_state` records totals, completion time and whether more remains.
No per-row retention log is added. The existing sampling path enforces the
same policy on insertion. No new window applies to `metric_samples`, billable
usage, raw lineage, raw/upload/secondary receipts, pending delivery, audit,
privacy, or client/account evidence. Existing guarded raw and secondary
retention semantics remain in force.

All stages defer while the checkpoint worker reports a reader pinning a WAL
of at least 1 GiB. The maintenance child leaves retention to this cadence,
so it cannot bypass that guard. Busy retries start at 200 ms under a known
large raw backlog, otherwise five seconds, then double up to 60 seconds;
other repeated failures start at five seconds and double to the same cap.
Each attempt yields to the timer; no synchronous retry loop runs.
`maintenance.retentionCadence` in `/status` exposes the
last stage/receipt, due stages, counts, busy/failure streaks and `nextRetryAt`.
Raw/secondary cursors and the telemetry aggregate remain in the ledger.
Retention deferral only bounds maintenance writes: it cannot bound WAL
growth from ongoing intake while an external reader pins it.

## Relation to the fresh-ledger cutover

`capture-roots epoch-switch` (0.7.46) starts a new ledger and archives the old
one; it changes the installation epoch and is the tool when the old ledger must
be retired. Compaction keeps the epoch, the rows and the receipts; it only gives
back the pages retention has already freed.

## Large ledgers and lifecycle snapshots

`lifecycle update` and `lifecycle rollback` normally keep an APFS clone of
the ledger as their operation's recovery snapshot
(`lifecycle/snapshots/<operation-id>/database`). Retention keeps the two
newest completed snapshots and a usable way back. A clone shares blocks
with the live ledger, so every page rewritten afterward costs new space
until the clone is removed or moved to another volume. During retention
catch-up most pages can change (2026-11-01: a 171 GB ledger, free disk
falling about 1 GB a minute).

Before stopping the collector, run `lifecycle update --preflight` to see
the clone cost and space plan. From 0.7.54, pass
`--snapshot-database-max-bytes N` or set `snapshotDatabaseMaxBytes` in config
to omit the database snapshot above the measured ledger plus WAL size.
Config and service remain backed up. The receipt and `snapshots list`
report `skipped_by_operator` and `cannotRestoreReason`. If the operation
fails, its automatic rollback restores runtime, config and service only;
it leaves the live ledger untouched and says so in `restore`. No bound is
the default. Recover such a journal with 0.7.54 or later. The metadata still
says a database existed, so older readers refuse its missing file before
touching the live ledger.

When retaining a database snapshot, move its `database` file to an archive
volume right after a healthy update on a large host. Keep `config`,
`service` and `snapshot.json` in place and leave a `database.moved.json`
note containing the destination and time. Keep a move receipt before the
move; undo is moving that same file back. While it is away, `snapshots list`
reports `its database copy is missing` and the snapshot cannot restore the
ledger.

The rollback behavior is precise: a successful explicit `lifecycle rollback
--artifact ...` installs the chosen runtime through a new transaction and
keeps the live ledger; it does not select an earlier operation's snapshot.
If an update or rollback fails and recovers its own snapshot (including a
resumed `rollback_required` journal), metadata still says the moved database
was present. Restoration checks that file before touching any live ledger,
config, runtime pointer or service. A missing file refuses with
`snapshot_database_missing` and leaves them alone; it never follows the
`none` path that removes a ledger originally absent at snapshot time. Move
the database file back to its original snapshot path **before** resuming a
rollback that should restore the ledger, then rerun the original command
with its original operation ID. Moving the file alone never changes the
metadata to say the ledger was absent.
