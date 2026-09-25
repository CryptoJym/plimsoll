# Local lifecycle transaction contract

Status: **operator command shipped in the packaged CLI; npm publication,
release signing, and live-fleet rollout remain open under GitHub issue #103.**

The lifecycle core is a transaction coordinator for the canonical packaged
Mac installer. It deliberately has no filesystem, process, service-manager,
network, registry, or credential access of its own; the installed
`plimsoll` command supplies real filesystem, SQLite ledger snapshot (APFS
clone or online backup), and LaunchAgent-manifest adapters
(`src/lifecycle-adapters.ts`).

## Operator commands

The packaged CLI exposes exactly these operations:

```text
plimsoll lifecycle update   --operation-id ID --artifact self|BUNDLE.mjs [--artifact-version V] [--retention keep-all] [--readiness-timeout-ms MS]
plimsoll lifecycle rollback --operation-id ID --artifact self|BUNDLE.mjs [--artifact-version V] [--retention keep-all]
plimsoll lifecycle uninstall --operation-id ID [--apply]
plimsoll lifecycle purge     --operation-id ID [--apply --confirm-exact "PURGE PLIMSOLL LOCAL DATA"]
plimsoll lifecycle support-bundle --operation-id ID
plimsoll lifecycle update --preflight
plimsoll lifecycle snapshots list  [--keep N] [--json]
plimsoll lifecycle snapshots prune [--keep N] [--apply] [--operation-id ID]
plimsoll lifecycle snapshots reconcile [--keep-snapshots ID[,ID...]] [--apply] [--force] [--operation-id ID]
```

`--artifact self` pins the currently running packaged bundle (a source
checkout or shell shim is refused). The resolver stages the bundle plus its
vendored native dependency closure (`better-sqlite3`, `bindings`,
`file-uri-to-path`) into a digest-verified artifact, so the immutable runtime
executes without the original `node_modules`. Explicit artifact paths are
absolute built bundles and require `--artifact-version`.

Every operation prints one JSON receipt plus a boundary statement. Update and
rollback never invoke `launchctl`: they publish the desired manifest and the
operator restarts explicitly with `plimsoll load-launch-agent`. An update
window is therefore three steps: stop the collector
(`plimsoll unload-launch-agent`), run `lifecycle update`, then start the new
runtime (`plimsoll load-launch-agent`). An update run while the collector is
live is refused before any change (`ledger_in_use`, see "The ledger must be
quiet"); retry the same operation ID after the stop.

Once a host has run 0.7.38 or later, run updates and prunes with 0.7.38 or
later. Releases up to 0.7.37 write receipts without a completion sequence:
an update or rollback run by one of them (for example `npx -y
@plimsoll/cli@0.7.37 lifecycle update`) leaves a receipt whose place in the
order cannot be proved, and retention stops until `snapshots reconcile`
(0.7.41 and later) repairs it.

To return to an older runtime explicitly, run that release's own
`lifecycle rollback --operation-id <id> --artifact self`. A CLI installs
only a bundle from its own install tree, so a newer CLI cannot install an
older release's bundle (it refuses with "artifact source must be a child of
the ownership root" and changes nothing).

Once a host has been sealed (`snapshots reconcile --keep-snapshots`), run
updates, prunes and reconciles with 0.7.41 or later. 0.7.38 and 0.7.39 read a
sealed order record as invalid: they remove nothing, which is safe, but an
update or rollback they run writes a receipt without a completion sequence.
0.7.41 keeps that snapshot as `receipt_without_sequence`, and `snapshots
list` says to run `snapshots reconcile`, whose keep-set then decides it. An
older release's own rollback is therefore safe on a sealed host and costs
one reconcile afterwards. 0.7.38 likewise keeps, as unknown, the snapshots of
rollbacks whose receipts record `restore.integrity`, and it does not
recognize `snapshots_reconcile` receipts.

An update or rollback:

1. obtains one exclusive lifecycle lock;
2. opens or resumes the operation journal;
3. snapshots compatible config, the ledger (an APFS clone of the quiesced
   ledger, otherwise the SQLite online backup; see "Snapshots, retention and
   disk"), and the owned service manifest, or refuses before any change when
   another process has the ledger open or a full ledger copy would not fit;
4. copies a digest-verified artifact to an immutable absolute
   `versions/VERSION/darwin-ARCH/bin/plimsoll.mjs` path, together with its
   vendored companion files (each digest-verified and renamed into place). A
   version that already exists is never changed: a different executable or
   companion for it fails before any of its files is touched, identical files
   are kept as they are, and a failed stage removes only what it created;
5. asks the injected service adapter to activate that exact executable and
   atomically moves the convenience `current` pointer;
6. accepts success only when runtime version, service, config compatibility,
   and database compatibility are all verified before a bounded readiness
   deadline; and
7. restores the prior runtime, config, database, and service manifest if any
   post-snapshot step fails (the database atomically, and only while no other
   process has it open); and
8. after a healthy completion, removes the snapshots and runtimes retention
   no longer keeps, recording each removal in the receipt (unless
   `--retention keep-all`; see "Keeping everything during an update").

The journal is `0600`; private directories and executable runtime files are
`0700`. Reopening the same interrupted operation is idempotent. If restore
itself fails, the durable journal and receipt remain `rollback_required`;
reopening that same operation retries rollback and cannot advance or verify
the target version. A different operation cannot cross its lock or journal.
After a terminal receipt, the operation ID is permanently consumed and must
not be reused.

## One fenced mutation authority

Issue #158: load, unload, install, uninstall, update, rollback, purge, and
owned-PID cleanup all serialize through one shared lifecycle mutation
authority (`src/lifecycle-authority.ts`) instead of separate lock domains.
A lease from this authority carries a durable monotonically increasing
fencing revision plus an expiring owner identity. Every mutating step
revalidates the exact lease immediately before acting; a superseded or
expired owner fails closed with `LifecycleInterruption`, leaves its journal
resumable for a retry under a fresh lease, and never runs an automatic
rollback on behalf of a successor. Busy, expired, superseded, released, and
ambiguous outcomes are literal path-free codes; an ambiguous domain state
(junk entry, malformed record, symlinked record) authorizes no destructive
cleanup. The canonical authority root is
`<collector home>/lifecycle-authority`; directories are `0700` and records
`0600`. Read-only observation, doctor, previews, dry runs, and exact no-op
installs never acquire the mutation lease. No daemon, watcher, polling loop,
credential movement, or hosted control surface is involved.

## Snapshots, retention and disk

Every update or rollback first snapshots the collector config, the ledger
and the service manifest into `lifecycle/snapshots/<operation-id>/`, and
stages the new runtime under `lifecycle/versions/<version>/`. Before
retention existed nothing ever removed either, so every update kept another
full copy of the ledger. A host with a large ledger eventually could not
update at all (Studio0 held 21 copies: 693,521,793,024 bytes, 645.9 GiB).

**The ledger must be quiet.** `lifecycle update` never stops the collector;
the managed update window stops it before running the command. The update
then opens the ledger in SQLite's exclusive locking mode, which SQLite
refuses while any other connection in any process has the ledger open. A
connection that was opened but has not run a statement yet holds no lock, so
the update also asks `lsof` whether any other process has the ledger, its
`-wal` or its `-shm` open. If another process has it open (the collector,
another `plimsoll` command, any SQLite client, even one that has not used it
yet), or either check cannot be completed, the update is refused
before anything changes: no journal remains, nothing is staged or switched,
the service is not touched, the receipt says `status: "refused"` with
`refusal.reason: "ledger_in_use"` (or `"quiescence_unproven"`), and the same
operation ID can be retried once that process has stopped. A rollback of an
update taken while a writer stayed attached would have to replace the file
under that writer, whose later writes would silently vanish.

**How the ledger is copied.** Holding the exclusive lock, the update runs a
TRUNCATE checkpoint so the database file alone is the complete ledger,
clones it with `clonefile(2)`, and only then releases the lock. A clone
costs no space when taken and shares every block with the ledger; it grows
only as the running collector later rewrites pages. The snapshot metadata
and the receipt record `method: "clone"`. If the ledger is not in WAL mode
or cannot be cloned (another volume, a file system without clones), the
snapshot is the SQLite online backup, a full copy, recorded as
`method: "online_backup"` with the reason in `cloneFallback`.

The checkpoint runs with SQLite's `checkpoint_fullfsync` on, so on macOS the
ledger file reaches stable storage (`F_FULLFSYNC`) before its WAL is
emptied; a plain `fsync` there reaches only the drive's cache, where a power
loss can lose the ledger's pages after the WAL is already gone. Every file
and directory `fsync` the lifecycle issues itself is already `F_FULLFSYNC`
on macOS (Node's libuv makes it so). Before it measures free space, an update
also removes a `<ledger>.restore-<nonce>` that a crashed byte-copy restore
left behind (it can be as large as the ledger).

**How a rollback restores it.** A restore never deletes the live ledger
first. It builds the restored ledger beside it (`<ledger>.restore-<nonce>`):
an APFS clone of the snapshot, or, only when the volume has room for a byte
copy while the live ledger still exists, a copy (a clone snapshot shares its
blocks with the live ledger, so removing the live name would free little).
The copy is fsynced and must pass `PRAGMA integrity_check`. If the ledger
was already damaged before the update, every snapshot carries that damage:
the copy may then fail the check only with complaints the live ledger also
has (both are checked), so the rollback is no worse than keeping the live
ledger. The receipt then records `restore.integrity: "preexisting_damage"`;
repair the ledger afterwards (for index damage, `REINDEX`). A copy with any
complaint the live ledger does not have is refused. Then, holding an
exclusive lock on the restored copy and on the live ledger (refused if any
other connection has the live ledger locked, or if `lsof` shows any other
process with the ledger, `-wal` or `-shm` open), a TRUNCATE checkpoint
(also with `checkpoint_fullfsync`) empties the live WAL, the operation's lease is checked one last time, one
atomic rename swaps the files, and the replaced file's `-wal`, `-shm` and
`-journal` names are removed before either lock is released. The only
process that can still hold the replaced file is one that opened it in the
fraction of a second between the `lsof` check and the rename. As a last
defense the replaced file's header is zeroed once no name reaches it: such
a process gets "not a database" if it first uses the ledger before anything
reopens the restored one. If it first uses it only after the restarted
collector has written (so the restored ledger has a WAL), SQLite pairs it
with that `-wal`/`-shm` by name and it can read and write stale pages. That
residual needs a process that opens the ledger during the swap itself and
then waits; the update window must stop every ledger user, and no Plimsoll
command behaves that way. The ledger is restored before the config, runtime pointer and
service; if it refuses (`ledger_in_use`, `insufficient_free_space`,
`integrity_check_failed`), nothing else changes, the journal stays
`rollback_required` and the receipt records `restoreRefusal`; retry the
same operation ID after fixing the cause. A completed rollback records how
the ledger was restored in `restore` (`clone` or `copy`, and why).
`integrity_check` reads the whole restored ledger (about 265 MiB/s measured,
so over 4 minutes for a 69 GB ledger). It runs in a helper process while the
operation renews its lease, and a byte copy renews it the same way; if the lease is still lost (a stalled process
superseded by another operation), the rollback stops before the swap with
`LIFECYCLE_INTERRUPTED` and the same operation ID finishes it later.

Node cannot clone on macOS itself (libuv answers `COPYFILE_FICLONE_FORCE`
with ENOSYS and turns `COPYFILE_FICLONE` into a byte copy), and `cp -c` and
`ditto --clone` silently fall back to full copies. The adapter therefore calls
`clonefile(2)` through JavaScript for Automation (`/usr/bin/osascript`) in a
child process with a PATH-only environment: it clones or fails, never
copies, and it never opens a descriptor on the ledger in the command's own
process (closing one would release SQLite's locks). The script binds one C
function and sends no Apple Events, so it needs no GUI session, no
automation (TCC) consent and no terminal; the ledger lives in the user's
home, outside TCC-protected folders. It therefore behaves the same in a
LaunchAgent, over SSH and in a terminal. The data-safety proof runs it from
a detached session with no terminal and a PATH-only environment; an actual
LaunchAgent or SSH run is not part of the proofs (they must not touch a
host's service manager). If the helper cannot run in some context, the
snapshot takes the space-checked full copy (or refuses) and a restore takes
the space-checked byte copy (or refuses); it never loses data.

**Disk an update needs.** With the collector stopped on an APFS volume: no
space for the ledger copy, and none for a rollback. When a full copy is
needed, the update requires free space of at least twice the ledger (plus
its WAL), one for the snapshot and one for a rollback's byte copy, and a
headroom of max(2 GiB, 5% of the ledger); otherwise it is refused before any
change with `refusal.reason: "insufficient_free_space"` and the numbers.
Staging a runtime needs a few tens of MB more.

Run `plimsoll lifecycle update --preflight` before stopping the service. It
is read-only: it creates, changes and removes nothing (not even the
lifecycle directory). It asks the volume whether it supports cloning and
checks that the ledger and the snapshot directory are on one volume, prints
the method and free space the update will need (`requiredFreeBytes`, and
`requiredFreeBytesIfCloneFails` for the full-copy case), and exits 1 if that
would not fit. It cannot tell whether the ledger will still be open when
the update runs: that is refused, not copied.

**What is kept.** After every update or rollback that completes with a
healthy runtime, retention keeps:

- the 2 newest completed update/rollback snapshots. The newest restores the
  runtime that ran before the current one; the second is a restore point for
  a problem found only after a further update, or after a same-version
  re-pin whose snapshot restores the current runtime itself;
- the newest completed snapshot that restores a runtime other than the
  installed one, even when it is older than those two; if that one cannot
  actually restore (see "Repairing blocked retention"), also the newest one
  that can;
- every snapshot an unfinished operation references (the journal, including
  a rollback that still needs recovery) and every snapshot whose operation
  is unknown;
- the installed runtime, the runtimes the `current` pointer and the service
  manifest reference, both runtimes of an unfinished operation, and every
  runtime a kept snapshot restores. If a kept snapshot has no readable
  metadata, no runtime is removed.

**Which operations are known.** An operation counts as completed only when
its marker under `completed-operations/` is the complete receipt it wrote:
exactly the receipt fields (the 13 every release from 0.7.0 to 0.7.37 wrote,
plus only the snapshot, retention, restore and completion-sequence records
newer releases add), `operationId` equal to the marker's file name and
snapshot ID, and the values its status implies (healthy readiness for a
completed one, the restored version for a rolled-back one). Its
`retainedTargets` and `purgeOnlyTargets` are exactly this release's lists or
exactly the pair every release from 0.7.0 to 0.7.39 wrote (without
`status_summary`), never a mix of the two. Any missing, extra, contradictory
or invalid field makes the operation unknown, and its snapshot is never
removed. A 0.7.39 or older collector does not know the newer lists, so after
a rollback to one of them it keeps every snapshot of an operation a newer
release completed.

**Which snapshots are newest.** Never file times, which a clock step can
reverse. Every completed or rolled-back update/rollback gets a
`completionSequence`, reserved under the mutation lease and fsynced in
`lifecycle/completion-order.json` before its receipt is written. The first
reservation also records every marker that already existed: those predate
sequencing and are older than every sequenced completion. Receipts written
before sequencing (every host upgraded from 0.7.37 or earlier) are ordered
only by their version chain: the latest completed operation installed the
version the next one started from (the installed version, or the starting
version of the oldest sequenced completion), and each step back is the one
operation whose `toVersion` is the next step's `fromVersion`. The chain stops
at the first missing or ambiguous step (same-version re-pins, a rollback to a
repeated version, a fresh install), and is used only when every operation is
visible: no unreadable marker, no snapshot without one, none recorded at
sequencing that has since gone. A completed snapshot is removed only when 2
(or `--keep N`) kept snapshots are provably newer. Duplicate sequences, a
sequence beyond the order record, a missing or damaged order record, or a
pre-sequencing receipt that appeared after sequencing began (an older
collector ran an update later) make the order unproven: retention removes
nothing (`completion_order_unproven`) until `snapshots reconcile` repairs it
(see "Repairing blocked retention").

Everything else is removed: older completed snapshots, snapshots of failed
updates whose automatic rollback already restored them (`rolled_back`), and
runtimes nothing references. Operation markers under `completed-operations/`
are never removed, so a pruned operation's ID still cannot be reused. If the
lifecycle state, the journal or a removal record cannot be read, or the
completion order cannot be proved, retention removes nothing. Retention runs
only after the receipt is committed; a failure there is recorded
(`retention.status: "skipped"`) and never fails or rolls back the update.

**Every removal is recorded before it happens.** Before anything moves, an
apply writes and fsyncs `lifecycle/removals/<operation-id>.json`, naming
every snapshot and runtime it will remove and every trash entry no record
accounts for. Each item is then renamed into `lifecycle/trash/` (atomic on
one volume; the directories are fsynced) and deleted. The record stays until
the operation's receipt is durable and names every removed item
(`retention.removed`, by name and apparent bytes; no path or content). If the
process stops at any point, or the receipt cannot be written, the next prune
or completed update finishes the recorded removals and reports them under
`retention.recovered` in its own receipt; items that never moved are decided
again.
If the trash holds the last usable way back, prune restores its recorded
items and reports them under `retention.restored`. A failed or incomplete
restore refuses prune and keeps the remaining trash and removal record.

**Operator commands.**

```sh
plimsoll lifecycle snapshots list            # id, created, size, method, operation state, keep/prune and why
plimsoll lifecycle snapshots list --json
plimsoll lifecycle snapshots prune           # dry run: exactly what would go, changes nothing
plimsoll lifecycle snapshots prune --apply   # holds the lifecycle lease; same protections as retention
plimsoll lifecycle snapshots prune --keep 1 --apply
```

`--keep N` (1 to 64, default 2) changes only how many newest completed
snapshots are kept; every other protection stays. Prune may run while the
collector is running and while an interrupted operation is in
`rollback_required`, because freeing space may be needed for its restore. It
never touches the ledger or what that operation references. A
`rollback_complete` journal blocks prune until the rollback receipt is durable.
Its output is
value-blind: operation IDs, runtime names, dates, sizes, methods and
decisions.

**Keeping everything during an update.** `--retention keep-all` on `lifecycle
update` or `lifecycle rollback` makes the operation remove nothing that
existed before it: no snapshot, runtime or trash entry (no lifecycle command
removes a display receipt; see "Support output"). Its receipt
records `retention.status: "skipped"` with `skippedReason:
"skipped_by_operator"` and `wouldRemove`, what retention would have removed at
that moment (absent when that read-only preview was blocked or failed). Managed rollout
windows use it so that an update never deletes a host's history; removing old
snapshots stays a separate, explicit `snapshots prune --apply`. The flag needs
0.7.39 or later; older versions ignore it and prune. It takes
exactly `keep-all`; a missing or other value, a repeated or `=`-joined flag,
or the flag on any other lifecycle command fails before any change. `update`
and `rollback` also refuse any option they do not take, so a misspelled flag
such as `--keep-all` or `--retension keep-all` fails instead of pruning.

To free space on a host that already holds many snapshots (for example before
an update window that refuses for disk), run the new release's command
without installing it, and review the dry run before applying:

```sh
npx -y @plimsoll/cli@<version> lifecycle snapshots list
npx -y @plimsoll/cli@<version> lifecycle snapshots prune          # review
npx -y @plimsoll/cli@<version> lifecycle snapshots prune --apply
```

On a host whose history predates sequencing, the dry run shows which
snapshots the version chain orders. A snapshot marked
`completion_order_unproven` stays; if the chain cannot place the two newest,
nothing is pruned until two sequenced updates have completed (or one has,
with `--keep 1`), or until the operator names what to keep with
`snapshots reconcile --keep-snapshots`.

**Repairing blocked retention.** `snapshots list` names the reason in
`blockedReason`. Two reasons need an operator: `completion_order_unproven`
(the order record is lost or damaged, two completions share a sequence, a
sequence is beyond the record, or a receipt without a sequence appeared
after sequencing began) and `removal_record_unreadable`. Snapshots kept as
`operation_unknown` or `receipt_without_sequence` (their receipt cannot be
read or ordered) also stay until an operator decides them. The repair is
`snapshots reconcile` (0.7.41 and later):

```sh
plimsoll lifecycle snapshots reconcile                    # dry run: findings and the repair it would make
plimsoll lifecycle snapshots reconcile --apply            # only when the order can be rebuilt from provable facts
plimsoll lifecycle snapshots reconcile --keep-snapshots ID[,ID...] --apply
```

The dry run changes nothing. It reports each finding and one `repair`:

- `none`: the order is proven and retention can decide every snapshot.
  Apply only moves unreadable removal records aside and removes what a
  crash left.
- `rebuilt`: the order record is missing or damaged, but every receipt is
  readable and carries a sequence, no two share one, every snapshot has a
  receipt, and the sequences follow the version chain: in sequence order
  each operation starts from the version the one before it left installed
  (its target, or its starting version when it rolled back), and the last
  one left the version installed now. The receipts' own sequences are then
  the whole order, and apply rewrites the record from them. A sequence that
  contradicts the chain (a damaged receipt claiming to be newest) makes it
  `needs_keep` instead.
- `needs_keep`: anything else: the order cannot be proved, or retention
  keeps a snapshot it cannot order or whose receipt it cannot read
  (`findings.undecidedSnapshots`). Apply without `--keep-snapshots` refuses
  and changes nothing.

The dry run also reports `neededRepair` (the repair without a keep-set) and
`newestWayBack`: the newest snapshot, by its recorded time, that restores a
version other than the installed one.

`--keep-snapshots` seals the order with the operator's decision. The named
snapshots are kept; every other snapshot present now is released
(`released_by_reconcile`) and goes at the next prune or completed update;
every receipt present now counts as older than every completion after the
seal. The named snapshots stay (`kept_by_reconcile`) until `--keep N` newer
completions exist; then retention treats them like any older snapshot. When
any snapshot can restore an earlier version, the keep-set must name at least
one of them, so a way back always remains; the first install's snapshot
restores no version and does not count. A way back must also be able to
restore: its own config, service and database copies are present (the
database copy at its recorded size), and the runtime it restores is still a
regular file under `versions/` that matches the digest the snapshot recorded.
Snapshots record that digest from 0.7.41 on; older ones recorded none, so for
them only the runtime's presence is checked. The dry run lists
`unusableWaysBack`, each with its reason, and a keep-set of only such
snapshots is refused with that reason. That rule holds even with `--force`,
and the check only reads: it restores and writes nothing.
Two more cases need `--force`, and the dry run shows both: sealing when
nothing needs a keep-set (`neededRepair` is `none` or `rebuilt`), and a
keep-set that releases `newestWayBack` (`newestWayBackReleased`). A forced
seal records `forced: true` in its receipt. Choose from `snapshots list`
(dates, versions, sizes).

Apply holds the lifecycle lease and refuses while an interrupted operation
awaits recovery. It moves each unreadable removal record, byte for byte, to
`lifecycle/removals-unreadable/` (kept there; only `purge` removes it; the
receipt names each with its size and SHA-256), removes temporaries a crash
left (`removals/*.json.tmp`, `completion-order.json.tmp`), writes the new
order record durably, and writes a `snapshots_reconcile` receipt with the
findings, the repair, the keep-set and the released snapshots. It removes no
snapshot or runtime itself: run `snapshots prune` (dry run first) afterwards.
A prune apply or completed update also removes those temporaries whenever
retention is not blocked.

## Uninstall, purge, leave, and revoke

Uninstall is a preview unless `--apply` is explicit. Apply removes the owned
service manifest, runtime pointer, and versioned runtimes. (Surgical removal
of embedded tool-config fragments is not wired yet: the real adapter reports
`tool_config_fragments` as owned but owns no fragment files until the
config-removal lane lands, and receipts say exactly that.) It preserves the
collector config, workspace credentials, ledger,
history, status summary, lifecycle snapshots, and workspace membership. Both
preview and apply receipts expose those under typed `retainedTargets`;
`lifecycle_snapshots` never appears in uninstall `ownedTargets`. The same
receipts classify the collector config, workspace credentials, ledger,
history, status summary (`status_summary`) and lifecycle snapshots under
`purgeOnlyTargets`, so an uninstall receipt cannot imply that purge-only data
was deleted.

Purging data is a different operation. It is a preview by default and lists
the live collector config, ledger, history, status summary, and lifecycle
snapshots. Apply requires both `--apply` and the exact confirmation shown
above, then deletes the live copies and secret-bearing lifecycle snapshot
copies, including any still awaiting removal in the lifecycle trash. The
status summary (`status-summary.json`: usage counters and the last run's
`/healthz` key) goes with any temp file an interrupted write left beside it
(`status-summary.json.<pid>.<16 hex>.tmp`); nothing else that shares the
name is touched. Leaving a
workspace and revoking a device are also distinct: neither is simulated or
reported complete by local uninstall or purge.

## Support output

The support bundle is reconstructed from an allowlist: package/runtime
versions, coarse health, four nonnegative counters, and at most 32 aggregate
log codes. Each returned object and log row is newly constructed from exact own
scalar data fields; unknown, inherited, accessor, nested, case-alias, and
Unicode-alias fields are stripped without invoking getters. It does not copy
log text or adapter objects. Absolute paths,
prompts/responses/tool content, repository or account identifiers, cookies,
tokens, signing material, install credentials, and workspace credentials have
no output field.

Lifecycle receipts are similarly symbolic. Every operation keeps its display
receipt in `lifecycle/receipts/` (`<operation-id>-<operation>.json`, a few KB
each; a retry of the same operation replaces its own). No lifecycle command
removes one, not even a prune: a refused or `rollback_required` receipt is the
only record of its operation. `ownedTargets` reports what the operation previews or applies,
`retainedTargets` reports what remains, and `purgeOnlyTargets` identifies data
that only the separate purge operation may remove. They report state
transitions and categories, never paths or secret values.

## Isolated proof

Two gates run on the repository's supported Node 22 environment:

```sh
pnpm proof:lifecycle            # transaction primitives with injected adapters
pnpm proof:lifecycle-operator   # real adapter composition + packaged CLI end to end
pnpm proof:lifecycle-retention  # bounded retention, clone snapshots, disk refusal, prune
pnpm proof:lifecycle-data-safety  # open writers, atomic restore, strict receipts, clock steps, removal records
pnpm proof:lifecycle-preservation  # no command removes a receipt; staging never changes an existing runtime
```

The primitive proof uses a fresh temporary ownership root and injected
service/database adapters. It covers arm64/x64 metadata, supported and
unsupported Node majors, permissions, health and disk-full rollback,
interruption/reopen, lock races, completed-ID reuse, failed-restore recovery,
readiness cancellation/deadline, malformed state, lifecycle/snapshot ancestor
and leaf symlink swaps, preview/apply/purge snapshot deletion, and
support-bundle privacy.

The operator proof builds the actual packaged bundle and drives it through
temporary sandbox homes: self-pin enrollment through the real CLI process,
immutable-runtime execution of the staged copy (the daemon's exact entry
point), manifest decisions that never reference dist/npx-cache/repo paths,
production durable-readiness including automatic rollback when the collector
config is incompatible, refusal while another connection holds the ledger,
a WAL-only commit surviving the snapshot/restore cycle,
interruption/reopen, lock races, completed-ID reuse, companion-digest tamper,
architecture-directory symlink swaps, preview/apply uninstall and purge
separation, support-bundle sanitization, truthful blank-sandbox doctor with
the shared version source, and a stub `launchctl` asserting zero service-
manager invocations. Neither proof touches a browser, a provider, the npm
registry, or an installed Plimsoll service; the operator proof reads only its
own sandbox config and ledger.

The retention proof drives the production composition over a disposable home
with a few-hundred-MB WAL ledger: N+3 sequential updates leave exactly the
retained snapshots and runtimes; an unfinished operation's snapshot and
runtimes survive a `--keep 1` prune; a rollback after pruning restores the
exact ledger; a quiesced snapshot is a clone that consumes about zero free
space (measured with `statfs`), passes `integrity_check`, and keeps other
processes out while it is taken; a forced clone failure is a full online
backup; a ledger another process has open, and a full copy without room,
are refused before any change; a crash right after the first rename into
the trash is finished by the next prune; dry runs and listings change
nothing; and the real CLI update path clones. Keep-all updates and rollbacks,
through the manager and the real CLI, leave every earlier entry, the trash
and a full receipts directory in place, record exactly what a prune would
remove, and a later prune removes exactly that; a misused or misspelled
`--retention` changes nothing, a keep-all update that fails readiness
rolls back without trimming receipts or touching the trash, and a keep-all
update whose preview is blocked (an unreadable removal record) records no
`wouldRemove` and leaves that record as it was.

The data-safety proof covers the worst cases: a writer that stays attached
through an update, or attaches after the snapshot, never ends up writing to
a replaced file; a restore without room, whose copy fails, or whose copy
fails `integrity_check` leaves the live ledger untouched; damaged completion
markers keep their snapshots; a backward clock step, with sequenced or with
pre-sequencing receipts, never removes the newest rollback points, and an
unprovable order removes nothing; a process lost right after an unlink, or a
receipt that cannot be written, still ends in a durable receipt naming the
removal; preflight writes nothing; and the clone helper works from a
detached session with no terminal.

The preservation proof starts from more than 32 receipts, including a
refused one, and runs a support bundle, uninstall and purge previews (also
through the real CLI), a keep-all update, an update, a rollback, an update
that rolls back, a prune while a `rollback_required` rollback is pending, and
uninstall and purge applies: every receipt that existed before each command
is still there, unchanged. It then installs a version with vendored
companions and tries a rebuilt executable and a rebuilt native module under
the same version: each fails and rolls back with every file of the installed
version unchanged (content, mode, mtime and inode), an identical bundle
re-stages without rewriting anything, a companion that fails its digest
leaves no file behind, and a temp file an interrupted copy left is replaced.
