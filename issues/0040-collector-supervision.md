# 0040 — Collector supervision: one owner, bounded restart, stable executable

GitHub: [#76](https://github.com/CryptoJym/plimsoll/issues/76)

## TL;DR

- Concurrent collector starts converge on one listener; the follower returns a successful `already_running` receipt.
- The listener owns the PID file only after binding the configured port, and stale records never authorize killing an unrelated process.
- LaunchAgent retries are crash-only and throttled; packaged installs run the stable Node + CLI paths directly while source-tree installs require explicit `--dev`.

## Scope

Collector start ownership, PID safety, LaunchAgent rendering, and a source-safe focused proof. This does not load, unload, install, or otherwise mutate a live LaunchAgent.

## Context

Baseline: `origin/main@9fc0af4`. Shared trace: `46be3ad1-514a-42d2-9f14-2212fdab14dc`.

Production surfaces:

- `packages/collector-cli/src/runtime-ownership.ts`
- `packages/collector-cli/src/cli.ts`
- `packages/collector-cli/src/launch-agent.ts`

Proof surface: `scripts/collector-supervision-proof.ts`.

## Problem / Task

Starting a second collector must be an idempotent success, not a PID overwrite followed by `EADDRINUSE`. Genuine failures may be retried at a bounded rate, but successful convergence must not create a launchd retry loop.

## Evidence

The system audit found 16,079 port-in-use failures and 16,087 failed lifecycle launches versus 23 active starts. The installed service remained disabled throughout this source change.

## Acceptance Criteria

- [x] Two starts against one temporary home and port produce one `active` listener and one successful `already_running` receipt with the same owner PID.
- [x] A stale PID record is replaced without signaling the unrelated live process it names.
- [x] Graceful owner shutdown removes only the PID file it owns.
- [x] LaunchAgent uses `KeepAlive.SuccessfulExit=false` plus `ThrottleInterval=30`.
- [x] Packaged arguments contain the stable CLI executable and no `pnpm` or `tsx`; development mode is explicit.
- [x] Focused E2E, CLI build, TypeScript check, and the repository proof gate pass.

## Operational Boundaries

- Temporary homes and ephemeral loopback ports only.
- No `launchctl`, live label, live ledger, credentials, provider, or cloud mutation.
- `pnpm proof` remains the repository-wide gate; issue #82 owns the pre-existing calendar-relative fixture failure.

## Notes For Future Agents

The start lock is a short-lived arbitration record, not a daemon lock. It is released after the winner binds and writes its PID. Health validation uses the existing loopback `/status` contract so a stale PID alone is never treated as ownership.

## Verification

Verified on Node `v22.22.0` after rebasing onto `origin/main@196d35f`:

- `pnpm exec tsc --noEmit`
- `pnpm --dir packages/collector-cli build`
- `pnpm exec tsx scripts/collector-supervision-proof.ts`
- `pnpm proof`
- `git diff --check`

The focused proof uses only temporary collector homes and ephemeral loopback ports. It also rejects a foreign listener whose `/status` payload is not the Plimsoll contract, and validates the rendered plist with `plutil` on macOS.
