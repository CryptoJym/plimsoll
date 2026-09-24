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
plimsoll lifecycle update   --operation-id ID --artifact self|BUNDLE.mjs [--artifact-version V] [--readiness-timeout-ms MS]
plimsoll lifecycle rollback --operation-id ID --artifact self|BUNDLE.mjs [--artifact-version V]
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
3. snapshots compatible config, the ledger (an APFS clone when the ledger is
   quiesced, otherwise the SQLite online backup; see "Snapshots, retention
   and disk"), and the owned service manifest, or refuses before any change
   when a full ledger copy would not fit;
4. copies a digest-verified artifact to an immutable absolute
   `versions/VERSION/darwin-ARCH/bin/plimsoll.mjs` path, together with its
   vendored companion files (each digest-verified);
5. asks the injected service adapter to activate that exact executable and
   atomically moves the convenience `current` pointer;
6. accepts success only when runtime version, service, config compatibility,
   and database compatibility are all verified before a bounded readiness
   deadline; and
7. restores the prior runtime, config, database, and service manifest if any
   post-snapshot step fails; and
8. after a healthy completion, removes the snapshots and runtimes retention
   no longer keeps, recording each removal in the receipt.

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
update at all (one held 21 copies, about 646 GiB).

**How the ledger is copied.** `lifecycle update` never stops the collector;
the managed update window stops it before running the command. When no other
connection has the ledger open, the snapshot is an APFS clone:
the lifecycle command opens the ledger in SQLite's exclusive locking mode,
which SQLite refuses while any other connection in any process has it open;
holding that lock it runs a TRUNCATE checkpoint so the database file alone is
the complete ledger, clones it with `clonefile(2)`, and only then releases the
lock. A clone costs no space when taken and shares every block with the
ledger; it grows only as the running collector later rewrites pages. The
snapshot metadata and the receipt record `method: "clone"`. If the ledger is
in use, is not in WAL mode, or cannot be cloned (another volume, a file
system without clones), the snapshot is the SQLite online backup, a full
copy, recorded as `method: "online_backup"` with the reason in
`cloneFallback`. Restores clone the snapshot back when the volume allows it,
so a rollback needs no free space either; the restored bytes are identical.

Node cannot clone on macOS itself (libuv answers `COPYFILE_FICLONE_FORCE`
with ENOSYS and turns `COPYFILE_FICLONE` into a byte copy), and `cp -c` and
`ditto --clone` silently fall back to full copies. The adapter therefore calls
`clonefile(2)` through JavaScript for Automation (`/usr/bin/osascript`) in a
child process: it clones or fails, never copies, and it never opens a
descriptor on the ledger in the command's own process (closing one would
release SQLite's locks).

**Disk an update needs.** With the collector stopped on an APFS volume: no
space for the ledger copy. When a full copy is needed, the update requires
free space of at least the ledger (plus its WAL) and a headroom of
max(2 GiB, 5% of the ledger); otherwise it stops before anything changes:
no journal remains, nothing is staged or switched, the service is not
touched, the receipt says `status: "refused"` with
`refusal.reason: "insufficient_free_space"` and the numbers, and the same
operation ID can be retried. Run `plimsoll lifecycle update --preflight`
before stopping the service: it predicts the method (it clones the ledger
into the lifecycle trash once to prove the volume can, then removes the
probe), prints the free space needed, and exits 1 if a full copy would not
fit. Staging a runtime needs a few tens of MB more.

**What is kept.** After every update or rollback that completes with a
healthy runtime, retention keeps:

- the 2 newest completed update/rollback snapshots. The newest restores the
  runtime that ran before the current one; the second is a restore point for
  a problem found only after a further update, or after a same-version
  re-pin whose snapshot restores the current runtime itself;
- the newest completed snapshot that restores a runtime other than the
  installed one, even when it is older than those two;
- every snapshot an unfinished operation references (the journal, including
  a rollback that still needs recovery) and every snapshot without a
  readable completion record (unknown provenance);
- the installed runtime, the runtimes the `current` pointer and the service
  manifest reference, both runtimes of an unfinished operation, and every
  runtime a kept snapshot restores. If a kept snapshot has no readable
  metadata, no runtime is removed.

Everything else is removed: older completed snapshots, snapshots of failed
updates whose automatic rollback already restored them (`rolled_back`),
incomplete copies of finished operations, and runtimes nothing references.
Operation markers under `completed-operations/` are never removed, so a
pruned operation's ID still cannot be reused. If the lifecycle state or the
journal cannot be read, retention removes nothing. Retention runs only after
the receipt is committed; a failure there is recorded
(`retention.status: "skipped"`) and never fails or rolls back the update.

**Removal is crash-safe.** Each removed snapshot or runtime is renamed into
`lifecycle/trash/` (atomic on one volume; the directories are fsynced) and
only then deleted. If the process stops in between, the next prune or
completed update deletes what is left and records it under
`retention.recovered`. Every removal is recorded in that operation's receipt
by name and apparent bytes only (`retention.removed`); no path or content.

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
never touches what that operation references). Its output is value-blind:
operation IDs, runtime names, dates, sizes, methods and decisions.

To free space on a host that already holds many snapshots (for example before
an update window that refuses for disk), run the new release's command
without installing it:

```sh
npx -y @plimsoll/cli@<version> lifecycle snapshots list
npx -y @plimsoll/cli@<version> lifecycle snapshots prune          # review
npx -y @plimsoll/cli@<version> lifecycle snapshots prune --apply
```

## Uninstall, purge, leave, and revoke

Uninstall is a preview unless `--apply` is explicit. Apply removes the owned
service manifest, runtime pointer, and versioned runtimes. (Surgical removal
of embedded tool-config fragments is not wired yet: the real adapter reports
`tool_config_fragments` as owned but owns no fragment files until the
config-removal lane lands, and receipts say exactly that.) It preserves the
collector config, workspace credentials, ledger,
history, lifecycle snapshots, and workspace membership. Both preview and apply
receipts expose those under typed `retainedTargets`; `lifecycle_snapshots`
never appears in uninstall `ownedTargets`. The same receipts classify the
collector config, workspace credentials, ledger, history, and lifecycle
snapshots under `purgeOnlyTargets`, so an uninstall receipt cannot imply that
purge-only data was deleted.

Purging data is a different operation. It is a preview by default and lists
the live collector config, ledger, history, and lifecycle snapshots. Apply
requires both `--apply` and the exact confirmation shown above, then deletes
the live copies and secret-bearing lifecycle snapshot copies, including any
still awaiting removal in the lifecycle trash. Leaving a
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
config is incompatible, live-ledger online backup across an upgrade,
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
processes out while it is taken; forced and live-writer fallbacks are full
online backups; a full copy without room is refused before any change; a
crash right after the first rename into the trash is finished by the next
prune; dry runs and listings change nothing; and the real CLI update path
clones.
