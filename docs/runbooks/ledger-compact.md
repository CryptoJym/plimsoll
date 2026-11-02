# Giving ledger space back: `plimsoll ledger compact`

Since 0.7.52. Raw retention deletes rows, but SQLite keeps the freed pages
inside `work-ledger.sqlite`: the file never shrinks on its own. This runbook
copies the stopped ledger into a fresh, compact file with `VACUUM INTO`,
verifies the copy and swaps it in. Every step has an undo. Nothing here
deletes the old ledger.

## Before you start

- Let retention catch up first. In `/status`, `retention.lastPass.hasMore` is
  `false`, and in `plimsoll status`, `secondaryRetention.lastHasMore` is
  `false`. Compacting earlier gives back only the space already freed.
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
   objects between the ledger and the copy. `--verify quick-check` runs
   SQLite's `quick_check` on the copy instead; `--verify none` only copies.
   The receipt is printed and written next to the copy as
   `<copy>.compaction-receipt.json` (mode 0600). A refused run writes no copy.
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

## What the copy carries

`VACUUM INTO` copies every row of every table, so the current week's rows,
upload holds, migration protection and all receipts travel with it; the week
gate needs no exception. The collector sets the copy's journal mode back to
WAL on first open.

## Relation to the fresh-ledger cutover

`capture-roots epoch-switch` (0.7.46) starts a new ledger and archives the old
one; it changes the installation epoch and is the tool when the old ledger must
be retired. Compaction keeps the epoch, the rows and the receipts; it only gives
back the pages retention has already freed.

## Large ledgers and lifecycle snapshots

`lifecycle update` and `lifecycle rollback` keep an APFS clone of the whole ledger as the operation's rollback snapshot (`lifecycle/snapshots/<operation-id>/database`); retention keeps the two newest and `snapshots prune` keeps at least one. A clone costs nothing until the live file's pages change, then one block per changed page. Under a retention catch-up most pages change, so on a ledger of this size the clone can take the whole free disk (2026-11-01: a 171 GB ledger, free disk falling about 1 GB a minute). Right after an update or rollback on such a host, move the snapshot's `database` file to another volume (keep `config`, `service` and `snapshot.json` in place and leave a `database.moved.json` note with the destination and the time). Undo: move it back. While the file is away, `lifecycle snapshots list` shows the snapshot as not restorable and a rollback cannot restore the ledger from it.
