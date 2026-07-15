# 0040 — Collector supervision: one owner, bounded restart, stable executable

GitHub: [#76](https://github.com/CryptoJym/plimsoll/issues/76)

## TL;DR

- Concurrent collector starts converge on one listener; the follower returns a successful `already_running` receipt.
- PID record, start lock, `/status`, duplicate-start receipt, and stop authorization share one exact runtime identity.
- Stale records never authorize killing an unrelated process; PID reuse is checked by a process-start fingerprint and a finite lock lease.
- LaunchAgent retries are crash-only and throttled; packaged installs run the stable Node + CLI paths directly while source-tree installs require explicit `--dev`.

## Scope

Collector start ownership, PID safety, LaunchAgent rendering, and a source-safe focused proof. This does not load, unload, install, or otherwise mutate a live LaunchAgent.

## Context

Baseline: `origin/main@9fc0af4`. Shared trace: `46be3ad1-514a-42d2-9f14-2212fdab14dc`.

Production surfaces:

- `packages/collector-cli/src/runtime-ownership.ts`
- `packages/collector-cli/src/cli.ts`
- `packages/collector-cli/src/launch-agent.ts`
- `packages/collector-cli/src/server.ts`

Proof surface: `scripts/collector-supervision-proof.ts`.

## Problem / Task

Starting a second collector must be an idempotent success, not a PID overwrite followed by `EADDRINUSE`. Genuine failures may be retried at a bounded rate, but successful convergence must not create a launchd retry loop.

## Evidence

The system audit found 16,079 port-in-use failures and 16,087 failed lifecycle launches versus 23 active starts. The installed service remained disabled throughout this source change.

Adversarial verification failed PR #87 at `1f42227`: shape-only status could pair with an unrelated PID, owner death after the first probe could yield false `already_running`, argv substrings could authorize `SIGTERM`, and PID reuse could pin a stale lock. Those findings define the version-2 runtime identity contract below.

## Acceptance Criteria

- [x] Two starts against one temporary home and port produce one `active` listener and one successful `already_running` receipt with the exact same PID, random instance ID, and process-start fingerprint.
- [x] A stale PID record is replaced without signaling the unrelated live process it names.
- [x] Graceful owner shutdown removes only the PID file it owns.
- [x] A candidate rechecks liveness, fingerprint, and the exact `/status` identity twice before returning `already_running`; owner death after probe one recovers instead.
- [x] `stop` probes the exact runtime identity and rechecks the process fingerprint immediately before `SIGTERM`; CLI-shaped and legacy processes remain untouched.
- [x] Start locks carry instance ID, fingerprint, and creation time; mismatched reused PIDs and expired two-minute leases recover.
- [x] LaunchAgent uses `KeepAlive.SuccessfulExit=false` plus `ThrottleInterval=30`.
- [x] Packaged arguments contain the stable CLI executable and no `pnpm` or `tsx`; development mode is explicit.
- [x] Focused E2E, CLI build, TypeScript check, and the repository proof gate pass.

## Operational Boundaries

- Temporary homes and ephemeral loopback ports only.
- No `launchctl`, live label, live ledger, credentials, provider, or cloud mutation.
- `pnpm proof` remains the repository-wide gate; issue #82 owns the pre-existing calendar-relative fixture failure.

## Notes For Future Agents

The start lock is a short-lived arbitration record, not a daemon lock. It is released after the winner binds and writes its PID. Its two-minute lease covers the current synchronous ledger open/prune path; a crash leaves a finite delay, and launchd's throttled retry recovers it.

Runtime identity version 2 is `pid + instanceId + processStartFingerprint`. The instance ID is a random UUID generated per start. The fingerprint hashes the PID plus the operating system's process start readback. PID liveness, argv, a PID file alone, or a shape-valid status response never authorize convergence or signaling.

## Verification

Verified on Node `v22.22.0` after rebasing onto `origin/main@44c3571`:

- `pnpm exec tsc --noEmit`
- `pnpm --dir packages/collector-cli build`
- `pnpm exec tsx scripts/collector-supervision-proof.ts`
- `pnpm proof`
- `git diff --check`

The focused proof uses only temporary collector homes and ephemeral loopback ports. It covers a foreign exact-shape status, owner death after the first valid status, an inert Node process carrying the CLI path and `start` argv, a reused-live-PID lock, an expired lease, legacy PID blocking, normal concurrent convergence, `EADDRINUSE`, packaged/dev plist separation, and `plutil` validation.
