# Exact artifact qualification and rollout

`pnpm proof:install-artifact` builds and packs twice, compares exact bytes,
installs that tarball in a disposable account home, and loads its own native
SQLite dependency. The receipt binds source commit/dirty state, source-input
and lock hashes, runtime/tarball/manifest hashes, Node version/ABI, native
binding hash and the focused acceptance checks. `SOURCE_DATE_EPOCH` defaults
to the checked-out commit's timestamp; set it explicitly when reproducing an
export. Build using the same pinned lock, Node and esbuild versions.

The qualification runs a private child daemon on a newly allocated loopback
port. It submits synthetic authenticated Codex OTLP tokens, restarts, writes
a synthetic Codex rollout while offline, and replays it twice through the
existing tailer. This proves the local protocol and capture path. It does not
attest a real provider session, remote upload replay, a loaded service, or a
seven-host rollout.

The installed artifact is also passed to the existing lifecycle operator
proof. It exercises immutable staging/native closure, a process exit after
the durable `switched` journal, refusal before mutation lease expiry, recovery
of the same operation after an injected child clock crosses that expiry,
failed-readiness rollback with an open WAL ledger, explicit rollback,
metadata-only support bundles, retained-data uninstall and preview-gated
purge. The fault/clock hooks only exist in the disposable proof home. The
runtime source and real lease files are never edited to force recovery.

## Run and inspect

```sh
pnpm install --frozen-lockfile
pnpm exec tsc --noEmit
pnpm proof:completion
pnpm proof:startup-wal-self-heal
pnpm proof:authenticated-ingestion
pnpm proof:retention
pnpm proof:packaged-runtime
pnpm proof:install-artifact
pnpm proof
```

Proof package scripts invoke `scripts/run-proof.ts`, which creates private
HOME, PLIMSOLL_HOME, provider, XDG and temporary roots using an environment
allowlist. The wrapper checks process exit, an unpredictable run ID, the
completion boundary and counts, and executable/sentinel hashes. The WAL
proof is bundled and run directly in Node. A pending promise, early exit 0,
missing receipt, wrong count or failed assertion cannot pass. Scripts that
use the completion guard must run through this wrapper.

Receipts are in `evidence/completion/` and `evidence/install-artifact/`.
Require `status: passed`, all counts passed, no skipped qualification checks,
matching artifact hashes and a clean integrated source commit before release.
Keep the CI HTTP latency and other existing required gates. The short process
tree sample reports RSS and CPU observations; it does not replace the settled
30-minute and active 15-minute pilot measurements.

The handoff includes `qualified-package.tgz`, `runtime-manifest.json`,
`install-package.json` and `install-lock.json`. To replay dependency resolution
in another disposable directory, copy the last two as `package.json` and
`package-lock.json`, copy the tarball under its unchanged name, and run
`npm ci --omit=dev --no-audit --no-fund` with an isolated home/cache and the
same supported Node runtime. Native binding qualification must run on each
target OS/architecture/Node ABI; a version string is insufficient.

## Parent-owned rollout and recovery

1. Apply the binary patch to the clean integration tree, merge the other
   bounded lanes, and rerun the required CI and exact artifact qualification.
   Review the final diff and store the final commit, tarball/runtime hashes,
   dependency closure and host runtime/ABI in the release record. Select a new
   package version when needed; this proof never publishes or overwrites npm.
2. Select an eligible pilot host under current fleet ownership and holds.
   Preserve the prior immutable artifact and record its hash/version. Use the
   existing lifecycle snapshot/SQLite online backup and private config/identity
   recovery path. Confirm the backup is readable and binds to this installation
   before activation. Keep credentials in their existing private store, out of
   logs, ordinary support receipts and public artifacts.
3. Invoke the packaged lifecycle update with a unique recorded operation ID
   and the selected exact artifact. The current lifecycle command stages files,
   snapshots and updates the manifest; loading/unloading the live service is a
   separate owner action. Inspect the operation receipt and readiness before
   that action, then attest the live PID, runtime digest, home identity and a
   real admitted provider token. Do not substitute the source `install.sh`
   sequence for the lifecycle transaction.
4. If interrupted, preserve the journal and operation ID. The mutation lease
   remains authoritative until release or its recorded expiry (currently ten
   minutes by default). Retry the same operation/artifact after expiry; do not
   delete a lease/journal or start a replacement operation. A different pending
   operation correctly fails closed.
5. If readiness fails, inspect the persisted rollback receipt and verify the
   prior manifest/version, config bytes, ledger and identity before restarting
   the prior service. Explicit rollback uses the retained exact prior artifact
   and its recorded version. A failed restore keeps the journal for recovery;
   preserve it and the snapshots. Uninstall and purge are separate actions,
   never a substitute for rollback.
6. After the pilot passes restart, first real token, offline capture/remote
   replay, support privacy and backup restore, proceed host by host with fresh
   fit evidence. Keep unsupported or untested hosts explicitly unqualified.
   Parent owns publication, domains, service operations and final acceptance.
