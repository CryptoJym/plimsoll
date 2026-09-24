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

## What it shows

The status item is titled `PL`. Its menu shows two lines, re-read each time
the menu opens:

```text
Running · 1234 events · 87.5% token coverage
Tokens: 5200000 in · 310000 out
```

`Stopped` means nothing answered `/healthz`; the counts still come from the
local ledger. If `status` fails, the first line reads `Collector unavailable`
and the second gives the reason. The menu also has Refresh, Open Dashboard
(`http://127.0.0.1:<port>`) and Quit.

`swift run plimsoll-menubar --status` prints the same two lines once as JSON
and exits (1 when the collector is unavailable), without starting the app.

## Safety

- Read-only. The only collector command it runs is `status`: on launch, when
  the menu opens and on Refresh, never overlapping. It cannot start, stop or
  reconfigure the collector. `plimsoll status` itself stamps the device's
  last-seen time and, on a Mac where the collector was never set up, creates
  its home. Point the app at the same `plimsoll` that runs your collector.
- No credential. It never reads `local-ingest-auth.json`; `plimsoll status`
  presents the management credential to its own daemon, and the `/healthz`
  probe sends none. Collector errors appear as one line, with anything shaped
  like a Plimsoll credential replaced by `[redacted]`. Open Dashboard never
  puts the credential in the URL, so a browser that has not been given the
  management credential shows the dashboard without data.
- Bounded. A `status` run that takes over 60 seconds is stopped. The probe
  allows 3 seconds, stays on 127.0.0.1 without a proxy, and counts only the
  collector's `{"ok":true}` reply as running.
- No shell, no helper, no LaunchAgent, no extra macOS permissions (below).

## Install

Build a release binary and start it from a shell where `plimsoll status`
works, so it sees the same collector home:

```bash
swift build -c release
PLIMSOLL_COLLECTOR_BIN="$(command -v plimsoll)" .build/release/plimsoll-menubar &
```

For a source install (`install.sh`), use checkout mode with
`PLIMSOLL_COLLECTOR_REPO` instead. To stop it, choose Quit Plimsoll Menubar.
There is no `.app` bundle or login item yet; that ships with the signed
release work below.

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
or accessibility APIs, and does not install a helper or LaunchAgent. The tests
check this in the source: they fail if `Sources/` calls one of those APIs or
the package gains an entitlements file or plist.

CI builds and tests this package on the macOS proof runner
(`pnpm proof:mac-menubar`).

The app is a local source/build lane. App Store packaging, signing, and
notarization are separate release work.
