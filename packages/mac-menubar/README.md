# Plimsoll Menubar

The Plimsoll Menubar is a small macOS status-bar companion for the local
collector. It shows whether the collector is running, the event count, token
coverage, and aggregate input/output token totals, and it can open the local
dashboard. It is read-only: it never starts, stops or reconfigures the
collector.

## Build and run

Requires macOS 13 or newer and Swift 5.9 or newer.

From this directory:

```bash
swift build
swift run plimsoll-menubar
```

The app invokes the collector without a shell. Configure one of these modes
before launching it:

```bash
# Use a packaged collector executable.
PLIMSOLL_COLLECTOR_BIN=/absolute/path/to/plimsoll swift run plimsoll-menubar

# Use the collector from a git checkout.
PLIMSOLL_COLLECTOR_REPO=/absolute/path/to/plimsoll \
  PLIMSOLL_PNPM_BIN=/absolute/path/to/pnpm \
  swift run plimsoll-menubar
```

In checkout mode, `PLIMSOLL_PNPM_BIN` is optional. When omitted, the app runs
`/usr/bin/env pnpm --silent --dir <repo> collector status` with fixed argument
boundaries (`--silent` keeps pnpm's script banner out of the status JSON).
`status` is the only collector command the app runs.

Status reads the collector's local `status` JSON. Running/stopped is a separate
loopback-only `GET /healthz` probe; the probe does not expose or send ledger
data. Token coverage is `tokenAttributedEvents / count * 100`. It is shown as
unavailable when stats are absent or the event denominator is zero.

## Test

```bash
scripts/check.sh
```

This runs `swift build` and then `swift test`. The tests use Swift Testing, so
they need a Swift 6 toolchain: Xcode 16 or newer, or the Command Line Tools
alone (which ship no XCTest; the script adds the Swift Testing paths SwiftPM
leaves out there).

## Permission doctor

Run the built executable's read-only doctor:

```bash
swift run plimsoll-menubar --doctor
```

The doctor reports `false` for accessibility, camera, input monitoring,
microphone, and screen recording. This package requests no additional macOS
permissions: it has no entitlements, usage-description keys, screen-recording
or accessibility APIs, and does not install a helper or LaunchAgent.

The app is a local source/build lane. App Store packaging, signing, and
notarization are separate release work.
