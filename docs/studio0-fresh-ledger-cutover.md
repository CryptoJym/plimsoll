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
2. Schedule this cutover early in a UTC week. Run the weekly tool-stats upload
   first and verify acknowledgement for **every due week with attempts**, from
   `first_week` through the last completed UTC week. There must be no current
   week tool attempt and no unacknowledged report row. `epoch-plan --archive`
   lists `dueWeeksWithoutAcknowledgement` and an `operatorAction` on refusal.
   A current-week attempt means waiting until the next Monday 00:00 UTC and
   for that week's report to be acknowledged. No weekly aggregate is carried
   into the replacement.
3. Drain delivery and hook/OTLP spools using the rollout plan's three stable
   zero readings. Stop the collector using the plan's supported unload. Check
   its outbox, receipt, session-sync and spool gates with the old ledger still
   present. If any gate fails, restart the old runtime without changing files.

## Switch

4. Run the lifecycle update to the 0.7.46 cutover build **while the old ledger
   is still in place**. Keep the snapshot and its receipts. Leave the daemon
   stopped. This preserves the B2 rollback ordering.
5. Create an archive directory **owned by the collector user, mode 0700** on
   the same volume, outside the collector home. Every ancestor must be owned
   by that user or root, and must not be writable by group or others. The
   command enforces these conditions before any link and sets retained ledger
   files to 0600. Choose a unique absolute path inside it, for example
   `/absolute/Plimsoll-archive/old-ledger.sqlite`. With the old ledger still
   at its normal pathname, run:

   ```sh
   plimsoll capture-roots epoch-plan --archive "$archive" --json
   ```

   Before scheduling the 90-second window, run the same read-only command
   with `--sizes` on a private Studio0 **ledger and root copy**. Require
   `totalCarriedBytes <= carryBudgetBytes` (8 MiB). The plan reports exact
   row counts and SQLite value-payload bytes per carried table; the budget
   includes cursor and authorization rows, not the 88.7 GB archive. A fixture
   with 20,000 retained rows had 948,890 carried value bytes and took 4.6 to
   7.0 seconds to plan and switch in two runs. Scaling both measured totals
   to 8 MiB gives about 41 to 62 seconds within the 90-second window.
   Actual Studio0 latency is still unverified. A refused budget requires a new plan, not an
   override. On the final stopped collector, require
   `status: capture_roots_epoch_plan`, `sidecarsMayAppear: false`, the root
   count and epoch, the listed `carriedRows` and `carriedBytes`, and the count
   of `untrackedFileFences`. The plan checks the archived
   binding against the configured tenant, device and root epoch.
   A closed ledger uses an immutable SQLite read and leaves no new files. If
   the ledger is open or closure cannot be proved, the result explicitly says
   `sidecarsMayAppear: true`; stop the owner and rerun. Never delete sidecars.
   The plan reads the old ledger; it does not write a replacement. It refuses
   invalid or mixed root epochs, malformed or unreadable archive cursors/live
   rows, an unsafe archive path, a regressed host clock, unreadable root
   inventory, and the weekly conditions above. Resolve the
   stated refusal before proceeding. Do not edit `collector.config.json`.
6. The rollout tool's exact cutover action is:

   ```sh
   plimsoll capture-roots epoch-switch --archive "$archive" --json
   ```

   The command rechecks the plan under the lifecycle mutation authority,
   takes SQLite exclusive ownership of the old inode, checkpoints its WAL,
   creates and binds a staged replacement to the agreed root epoch, and
   carries each valid per-file committed cursor whose file generation still
   matches, plus Codex live authorization rows. It creates a hard link for
   the archive and atomically swaps the
   already-bound stage into the active pathname. The archive is never
   deleted. The replacement's durable marker records the archive identity,
   archive path, and minimum collector version 0.7.46. Files with carried
   cursors resume at their committed byte offsets, even when appended records
   have timestamps before the switch. Every physically present file without
   a carried cursor is fenced at its observed switch size, independently of
   host or filesystem clocks; later growth starts at that byte boundary.
   A new file generation at an old pathname is treated as a file without a
   carried cursor: its obsolete archive cursor is validated but not copied.

   If the process stops after linking the archive but before the swap, the
   active and archive paths refer to the same old inode. Keep both paths. A
   handled error removes its owned stage; rerun `epoch-plan --archive` and
   `epoch-switch --archive`. A hard kill can leave
   `work-ledger.sqlite.replacement-stage` and its sidecars. Plan recognizes
   this as `recoveryStagePresent: true` while the active old ledger remains
   valid. Rerun switch after the killed owner's fenced lease expires (up to
   ten minutes); switch removes only its owned incomplete stage under the
   mutation lease and exclusive old-ledger lock. No manual stage move is
   needed. Never move
   the active ledger or its archive in this recovery. If the response is lost
   after the swap, inspect the active
   ledger's `collector_replacement_ledger` row and archive inode before
   retrying. Do not unlink or overwrite either ledger to guess which step
   completed.
7. Start the 0.7.46 collector. Check readiness, queue/spool gates, session
   sync, all 23 roots, zero `epoch_mismatch`, Claude/Codex forward appends,
   Codex live producer authentication, and Grok's historical
   `before_enrollment` refusals. A replaced ledger retains the old epoch for
   owner-page machine continuity; the new cloud watermark row is separated
   by `epoch_started_at`. Record the archive and new ledger in the copy
   inventory. Hash the archive in the background after the swap.

## Rollback or downgrade

Stop the 0.7.46 collector first. **Never start 0.7.45 or an older runtime on
the replacement ledger**, even to inspect it. The supported 0.7.46 lifecycle
update/rollback path checks the marker under the same mutation authority and
refuses a runtime below 0.7.46 until the old ledger is restored.

Choose a new absolute `freshAttempt` path in a collector-owned 0700 directory
whose ancestors meet the same path rule as the archive,
and run this with the 0.7.46 cutover CLI while the daemon is stopped:

```sh
plimsoll capture-roots epoch-restore --archive "$archive" --save-fresh "$freshAttempt" --json
```

The command checks the marker and archive identity, clones the archived old
ledger into a restore stage, and folds the replacement's weekly event, tool
attempt, and dimension rows from the cutover UTC week onward in one SQLite
transaction. It commits and checkpoints that fold before atomically swapping
the clone into the active pathname. The replacement is retained at
`freshAttempt`; the archive is unchanged. If a replacement weekly report is
already frozen, restore refuses with `restore_weekly_report_reconcile_required`
and leaves the replacement active, so the archived image cannot publish a
partial week. Reconcile that report with the rollout lead before retrying
restore. Confirm the active ledger has the old epoch and no
replacement marker. **Only then** run the rollout's pinned `lifecycle
rollback` to its pre-update runtime, keeping snapshots/receipts, and start
that runtime. Its snapshot may restore the old ledger again; it must never
see the replacement file. Check the old outbox and cloud history after it
starts. Events already accepted from the replacement remain in the cloud;
retain the fresh attempt for reconciliation rather than replaying it through
the old runtime.

If the restore command refuses, keep the daemon stopped and inspect its reason
and the three paths. Do not hand the replacement pathname to an old runtime.
