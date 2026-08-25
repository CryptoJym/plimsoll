# Local lifecycle transaction contract

Status: **operator command shipped in the packaged CLI; npm publication,
release signing, and live-fleet rollout remain open under GitHub issue #103.**

The lifecycle core is a transaction coordinator for the canonical packaged
Mac installer. It deliberately has no filesystem, process, service-manager,
network, registry, or credential access of its own; the installed
`plimsoll` command supplies real filesystem, SQLite-online-backup, and
LaunchAgent-manifest adapters (`src/lifecycle-adapters.ts`).

## Operator commands

The packaged CLI exposes exactly these operations:

```text
plimsoll lifecycle update   --operation-id ID --artifact self|BUNDLE.mjs [--artifact-version V] [--readiness-timeout-ms MS]
plimsoll lifecycle rollback --operation-id ID --artifact self|BUNDLE.mjs [--artifact-version V]
plimsoll lifecycle uninstall --operation-id ID [--apply]
plimsoll lifecycle purge     --operation-id ID [--apply --confirm-exact "PURGE PLIMSOLL LOCAL DATA"]
plimsoll lifecycle support-bundle --operation-id ID
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
3. snapshots compatible config, the database through an injected online
   backup adapter, and the owned service manifest;
4. copies a digest-verified artifact to an immutable absolute
   `versions/VERSION/darwin-ARCH/bin/plimsoll.mjs` path, together with its
   vendored companion files (each digest-verified);
5. asks the injected service adapter to activate that exact executable and
   atomically moves the convenience `current` pointer;
6. accepts success only when runtime version, service, config compatibility,
   and database compatibility are all verified before a bounded readiness
   deadline; and
7. restores the prior runtime, config, database, and service manifest if any
   post-snapshot step fails.

The journal is `0600`; private directories and executable runtime files are
`0700`. Reopening the same interrupted operation is idempotent. If restore
itself fails, the durable journal and receipt remain `rollback_required`;
reopening that same operation retries rollback and cannot advance or verify
the target version. A different operation cannot cross its lock or journal.
After a terminal receipt, the operation ID is permanently consumed and must
not be reused.

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
the live copies and secret-bearing lifecycle snapshot copies. Leaving a
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
