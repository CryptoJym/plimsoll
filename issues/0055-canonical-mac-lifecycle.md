# 0055 — Distribution: canonical Mac install, truthful doctor, update, rollback, and uninstall

GitHub mirror: https://github.com/CryptoJym/plimsoll/issues/103

## TL;DR
- Replace divergent npx/source paths with one pinned packaged-runtime lifecycle suitable for teammates.
- Make doctor read-only and truthful; missing daemon/config/signal makes the gate fail with a nonzero exit.
- Stage upgrades and automatically roll back runtime/config/database when readiness fails.

## Scope
macOS arm64/x64, packaged CLI/runtime, release workflow/provenance, LaunchAgent, config ownership, and lifecycle docs. Linux/Windows remain issue 0007.

## Evidence
- `install.sh` invokes source LaunchAgent install without required `--dev` and fails on accepted main.
- npm `latest` is `@plimsoll/cli@0.6.0`, published before the accepted resource architecture.
- `doctor` currently prints `ok:true` when no LaunchAgent exists and the collector is unreachable.
- No trusted-publish workflow, rollback command, surgical config removal, or full uninstall exists.

## Acceptance Criteria
- [ ] One canonical command performs preflight, version-pinned runtime install, config preview/apply, LaunchAgent install, readiness wait, and enrollment receipt.
- [ ] Permanent daemon executes an immutable absolute packaged runtime, never git checkout, npx cache, shell cwd, or mutable global path.
- [ ] One version source feeds package, CLI, join/upload receipts, doctor, runtime path, and fleet inventory.
- [ ] `doctor --read-only --json` does not create config/ledger; computes `ok`; returns nonzero on conflicts, missing service, unreachable endpoints, wrong version, or missing token signal.
- [ ] Update stages version N+1, snapshots compatible state, switches atomically, verifies, and restores N on failure.
- [ ] Uninstall removes service and owned tool-config fragments while preserving ledger by default; purge requires separate confirmation; leave/revoke is distinct and explicit.
- [ ] Tagged release produces signed/provenance-attested artifacts and cold registry install smoke on supported architectures/Node versions.
- [ ] Runtime uses minimal absolute PATH, bounded logs, `0700/0600` permissions, sanitized support bundle, and no secrets in argv/stdout/log/plist.
- [ ] README, package README, Product Gate, and issues match the executable commands byte-for-byte.

## Operational Boundaries
- No publish, tag, npm credential change, or LaunchAgent activation without separate release/rollout authorization.

## 2026-07-17 source slice
- Adapter-driven lifecycle transaction primitives and an isolated proof now
  cover immutable version paths, staged update/rollback, compatible snapshot
  adapters, automatic restore, interruption/reopen, preview-default uninstall,
  separate purge, and sanitized bounded support output.
- This slice does not expose a published operator command, sign or publish a
  release, run `launchctl`, validate real arm64/x64 Macs, leave a workspace, or
  revoke a device. The GitHub issue remains open for those release/fleet gates.

## 2026-08-25 operator-command slice
- `plimsoll lifecycle update|rollback|uninstall|purge|support-bundle` is now a
  real packaged-CLI command (`src/lifecycle-adapters.ts` composes the
  FilesystemLifecycleAdapter with a SQLite online-backup adapter and a
  LaunchAgent-manifest service adapter). It never invokes `launchctl`; load /
  unload stay explicit (`load-launch-agent`).
- `--artifact self` pins the running packaged bundle plus its vendored native
  dependency closure (better-sqlite3, bindings, file-uri-to-path) as a
  digest-verified immutable runtime under
  `versions/VERSION/darwin-ARCH/bin/plimsoll.mjs`, so the daemon manifest
  never references npx caches, dist folders, or source trees.
- Production readiness verifies durable state (runtime bytes, manifest
  decision, config validity, ledger compatibility) and failed readiness
  auto-restores runtime/config/database/manifest; proven including a live WAL
  ledger upgrade and interruption/reopen through the real CLI process.
- `doctor --read-only --json` now reports the shared `PLIMSOLL_VERSION`
  (package.json → version.ts → receipts → doctor), verified equal across
  surfaces by the new proof. Read-only truthfulness itself landed in #107.
- New gate: `pnpm proof:lifecycle-operator` (50 checks; stub launchctl asserts
  zero service-manager invocations). Wired into CI after `proof:lifecycle`.
- Still open under #103: npm trusted publication + provenance signing, cold
  registry install smoke on real arm64/x64 Macs, surgical removal of embedded
  tool-config fragments (real adapter owns none yet), live LaunchAgent
  activation validation, leave/revoke hosted operations.
