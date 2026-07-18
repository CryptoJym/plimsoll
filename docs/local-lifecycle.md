# Local lifecycle transaction contract

Status: **source-validated primitive; not a published installer or live fleet
rollout**. GitHub issue #103 remains open.

The lifecycle core is a transaction coordinator for the canonical packaged
Mac installer. It deliberately has no filesystem, process, service-manager,
network, registry, or credential access. The installer supplies explicit
filesystem, SQLite-backup, service, health, and artifact adapters.

## Update and rollback

`runLifecycleCommand` accepts these exact operations at the injected command
boundary:

```text
update --operation-id ID --artifact REF
rollback --operation-id ID --artifact REF
uninstall --operation-id ID [--apply]
purge --operation-id ID [--apply --confirm-exact "PURGE PLIMSOLL LOCAL DATA"]
support-bundle --operation-id ID
```

This is not yet exposed as the installed `plimsoll lifecycle` command. The
package/release adapter, release signing, trusted npm publication, and real-Mac
service integration remain work under #103. Until those gates land, these are
source APIs and isolated proof fixtures, not operator instructions.

An update or rollback:

1. obtains one exclusive lifecycle lock;
2. opens or resumes the operation journal;
3. snapshots compatible config, the database through an injected online
   backup adapter, and the owned service manifest;
4. copies a digest-verified artifact to an immutable absolute
   `versions/VERSION/darwin-ARCH/bin/plimsoll` path;
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
service manifest, exact tool-config fragments, runtime pointer, and versioned
runtimes. It preserves the collector config, workspace credentials, ledger,
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

Run on the repository's supported Node 22 environment:

```sh
pnpm proof:lifecycle
```

The proof uses a fresh temporary ownership root and injected service/database
adapters. It covers arm64/x64 metadata, supported and unsupported Node majors,
permissions, health and disk-full rollback, interruption/reopen, lock races,
completed-ID reuse, failed-restore recovery, readiness cancellation/deadline,
malformed state, lifecycle/snapshot ancestor and leaf symlink swaps,
preview/apply/purge snapshot deletion, and support-bundle privacy. It never
invokes `launchctl`, a browser, a provider, the npm registry, or an installed
Plimsoll service and never reads the operator's config or ledger.
