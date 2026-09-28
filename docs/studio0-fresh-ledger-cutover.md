# Studio0 replacement ledger cutover and rollback

This procedure applies to the collector **0.7.46 cutover build** and a stopped
collector. The old ledger remains archived. Keep its archive path and manifest
under the rollout lead's existing copy inventory. The device identity, upload
keys, and capture-root configuration stay in place.

## Release order and safe window

1. Deploy the cloud capture-watermark **expand** migration and ingest code.
   Then deploy the separate **contract** migration that makes
   `(tenant_id, device_install_id, installation_epoch_id, epoch_started_at, source)`
   the primary key. Confirm that contract has applied **before any replacement
   ledger window**. An expanded-only schema still has the four-column primary
   key, so it rejects the second same-epoch ledger watermark row.
2. Schedule this cutover early in a UTC week. Wait until the preceding UTC
   week's tool-stats report is acknowledged, and before this UTC week has any
   tool attempt. `epoch-plan --archive` refuses an unacknowledged report or a
   current-week attempt. It reports `nextSafeWindowAt` and whether an
   acknowledgement is still required. A current-week attempt means waiting
   until the next Monday 00:00 UTC **and** for that week's report to be
   acknowledged. Do not migrate a partial weekly aggregate.
3. Drain delivery and hook/OTLP spools using the rollout plan's three stable
   zero readings. Stop the collector using the plan's supported unload. Check
   its outbox, receipt, session-sync and spool gates with the old ledger still
   present. If any gate fails, restart the old runtime without changing files.

## Switch

4. Run the lifecycle update to the 0.7.46 cutover build **while the old ledger
   is still in place**. Keep the snapshot and its receipts. Leave the daemon
   stopped. This preserves the B2 rollback ordering.
5. Create a private (0700) archive directory on the same volume, outside the
   collector home. Choose a unique absolute path inside it, for example
   `/absolute/Plimsoll-archive/old-ledger.sqlite`. With the old ledger still
   at its normal pathname, run:

   ```sh
   plimsoll capture-roots epoch-plan --archive "$archive" --json
   ```

   Require `status: capture_roots_epoch_plan`, the configured root count and
   epoch, a nonzero `cursorRows` when cursors exist, and the listed
   `carriedRows` for Codex live authorization. The plan checks the archived
   binding against the configured tenant, device and root epoch.
   The plan reads the old ledger; it does not write a replacement. It refuses
   invalid or mixed root epochs, malformed or unreadable archive cursors/live
   rows, a regressed host clock, and the weekly conditions above. Resolve the
   stated refusal before proceeding. Do not edit `collector.config.json`.
6. The rollout tool's exact cutover action is:

   ```sh
   plimsoll capture-roots epoch-switch --archive "$archive" --json
   ```

   The command rechecks the plan under the lifecycle mutation authority,
   takes SQLite exclusive ownership of the old inode, checkpoints its WAL,
   creates and binds a staged replacement to the agreed root epoch, and
   carries every valid per-file committed cursor plus Codex live authorization
   rows. It creates a hard link for the archive and atomically swaps the
   already-bound stage into the active pathname. The archive is never
   deleted. The replacement's durable marker records the archive identity,
   archive path, and minimum collector version 0.7.45. Its capture baseline
   still applies to files absent from the archive's cursor set; preexisting
   files with carried cursors resume at their committed offsets.

   If the process stops after linking the archive but before the swap, the
   active and archive paths refer to the same old inode. Keep both paths and
   rerun `epoch-plan --archive` followed by `epoch-switch --archive`; the
   command resumes that state. If the response is lost after the swap, inspect
   the active ledger's `collector_replacement_ledger` row and archive inode
   before retrying. Do not unlink, rename, or overwrite either ledger to
   guess which step completed.
7. Start the 0.7.46 collector. Check readiness, queue/spool gates, session
   sync, all 23 roots, zero `epoch_mismatch`, Claude/Codex forward appends,
   Codex live producer authentication, and Grok's historical
   `before_enrollment` refusals. A replaced ledger retains the old epoch for
   owner-page machine continuity; the new cloud watermark row is separated
   by `epoch_started_at`. Record the archive and new ledger in the copy
   inventory. Hash the archive in the background after the swap.

## Rollback or downgrade

Stop the 0.7.46 collector first. **Never start 0.7.44 or an older runtime on
the replacement ledger**, even to inspect it. The supported 0.7.45+ lifecycle
update/rollback path checks the marker under the same mutation authority and
refuses a runtime below 0.7.45 until the old ledger is restored.

Choose a new absolute `freshAttempt` path in the private archive directory,
and run this with the 0.7.46 cutover CLI while the daemon is stopped:

```sh
plimsoll capture-roots epoch-restore --archive "$archive" --save-fresh "$freshAttempt" --json
```

The command checks the marker and archive identity, clones the archived old
ledger into an atomic restore stage, retains the replacement at `freshAttempt`,
and swaps the old image into the active pathname. The archive and fresh
attempt are both retained. Confirm the active ledger has the old epoch and no
replacement marker. **Only then** run the rollout's pinned `lifecycle
rollback` to its pre-update runtime, keeping snapshots/receipts, and start
that runtime. Its snapshot may restore the old ledger again; it must never
see the replacement file. Check the old outbox and cloud history after it
starts. Events already accepted from the replacement remain in the cloud;
retain the fresh attempt for reconciliation rather than replaying it through
the old runtime.

If the restore command refuses, keep the daemon stopped and inspect its reason
and the three paths. Do not hand the replacement pathname to an old runtime.
