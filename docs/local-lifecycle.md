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
```

`--artifact self` pins the currently running packaged bundle (a source
checkout or shell shim is refused). The resolver stages the bundle plus its
vendored native dependency closure (`better-sqlite3`, `bindings`,
`file-uri-to-path`) into a digest-verified artifact, so the immutable runtime
executes without the original `node_modules`. Explicit artifact paths are
absolute built bundles and require `--artifact-version`.

Every operation prints one JSON receipt plus a boundary statement. Update and
rollback never invoke `launchctl`: they publish the desired manifest and the
operator restarts explicitly with `plimsoll load-launch-agent`.

An update or rollback:

1. obtains one exclusive lifecycle lock;
2. opens or resumes the operation journal;
3. snapshots compatible config, the ledger (an APFS clone of the quiesced
   ledger, otherwise the SQLite online backup; see "Snapshots, retention and
   disk"), and the owned service manifest, or refuses before any change when
   another process has the ledger open or a full ledger copy would not fit;
4. copies a digest-verified artifact to an immutable absolute
   `versions/VERSION/darwin-ARCH/bin/plimsoll.mjs` path, together with its
   vendored companion files (each digest-verified);
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
refuses while any other connection in any process has the ledger open. If
another process has it open (the collector, another `plimsoll` command, any
SQLite client), or the ledger cannot be locked at all, the update is refused
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

**How a rollback restores it.** A restore never deletes the live ledger
first. It builds the restored ledger beside it (`<ledger>.restore-<nonce>`):
an APFS clone of the snapshot, or, only when the volume has room for a byte
copy while the live ledger still exists, a copy (a clone snapshot shares its
blocks with the live ledger, so removing the live name would free little).
The copy is fsynced and must pass `PRAGMA integrity_check`. Then, holding an
exclusive lock on the restored copy and on the live ledger (refused if any
other connection has the live ledger open), a TRUNCATE checkpoint empties
the live WAL, one atomic rename swaps the files, the replaced file's header
is zeroed once no name reaches it (a process that opened it an instant
earlier gets "not a database" instead of writing into an unlinked file),
and its `-wal`, `-shm` and `-journal` names are removed before either lock
is released. The ledger is restored before the config, runtime pointer and
service; if it refuses (`ledger_in_use`, `insufficient_free_space`,
`integrity_check_failed`), nothing else changes, the journal stays
`rollback_required` and the receipt records `restoreRefusal`; retry the
same operation ID after fixing the cause. A completed rollback records how
the ledger was restored in `restore` (`clone` or `copy`, and why).
`integrity_check` reads the whole restored ledger, so a rollback of a very
large ledger takes minutes.

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
  installed one, even when it is older than those two;
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
exactly the pair every release from 0.7.0 to 0.7.38 wrote (without
`status_summary`), never a mix of the two. Any missing, extra, contradictory
or invalid field makes the operation unknown, and its snapshot is never
removed. A 0.7.38 collector does not know the newer lists, so after a
rollback to 0.7.38 it keeps every snapshot of an operation a newer release
completed.

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
nothing (`completion_order_unproven`) until it is repaired.

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
collector is running and while an interrupted operation awaits recovery (it
never touches the ledger or what that operation references). Its output is
value-blind: operation IDs, runtime names, dates, sizes, methods and
decisions.

**Keeping everything during an update.** `--retention keep-all` on `lifecycle
update` or `lifecycle rollback` makes the operation remove nothing that
existed before it: no snapshot, runtime, trash entry or display receipt (the
`receipts/` directory is otherwise trimmed to its newest 32). Its receipt
records `retention.status: "skipped"` with `skippedReason:
"skipped_by_operator"` and `wouldRemove`, what retention would have removed at
that moment (absent when that read-only preview was blocked or failed). Managed rollout
windows use it so that an update never deletes a host's history; removing old
snapshots stays a separate, explicit `snapshots prune --apply`. The flag takes
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
with `--keep 1`).

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

Lifecycle receipts are similarly symbolic and bounded to the newest 32 local
records. `ownedTargets` reports what the operation previews or applies,
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
`--retention` changes nothing, and a keep-all update that fails readiness
rolls back without trimming receipts or touching the trash.

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
