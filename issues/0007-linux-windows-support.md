# 0007 — Platform: Linux first, Windows after

## TL;DR
- Collector is macOS-shaped: `~/Library/Application Support/Plimsoll`, LaunchAgent plists, `launchctl`.
- Linux needs: XDG data dir, systemd user unit generation, install.sh branches. Windows is a separate later lane.

## Scope
Linux: full parity (capture, ledger, daemon, proof in CI ubuntu matrix). Windows: out of scope here — file a child issue when Linux lands.

## Context
- Path logic: `packages/collector-cli/src/config.ts` (`collectorHome`).
- Daemon lifecycle: `packages/collector-cli/src/launch-agent.ts` — abstract to a `daemon/` module with platform backends.
- Proof already avoids macOS-isms except the LaunchAgent checks in `doctor`.

## Acceptance Criteria
- [ ] `collectorHome` → `$XDG_DATA_HOME/plimsoll` (fallback `~/.local/share/plimsoll`) on Linux.
- [ ] `pnpm collector install-daemon` writes + enables a systemd user unit on Linux, LaunchAgent on macOS.
- [ ] CI matrix: proof green on ubuntu-latest + macos-14.
