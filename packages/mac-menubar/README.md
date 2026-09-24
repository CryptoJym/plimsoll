# Plimsoll Menubar

The Plimsoll Menubar is a small macOS status-bar companion for the local
collector. It shows whether the collector is running, the event count, token
coverage, and aggregate input/output token totals, and it can open the local
dashboard. It is read-only: it never starts, stops or reconfigures the
collector.

## Build and run

Requires macOS 13 or newer and Swift 5.9 or newer, and a collector new enough
to write `status-summary.json` (the release after 0.7.37).

From this directory:

```bash
swift build
swift run plimsoll-menubar
```

The app needs no configuration. It reads the collector's private summary
file from the collector home: `PLIMSOLL_HOME` if set (an absolute path, as
the collector requires), otherwise `~/Library/Application Support/Plimsoll`.

## How it reads status

The running collector rewrites `status-summary.json` in its home every 15 s:
four lifetime counters from its own `/status` cache, its port, a random
per-run `instanceId`, its version and the write time (collector runbook
`docs/runbooks/local-status-http.md`). The app opens that file without
following a symlink, requires it to be the user's own private (0600) regular
file of at most 16 KB, and parses it strictly. It then sends one
`GET /healthz` to `127.0.0.1:<port>` from the file to see whether the
collector is running.

It runs no collector command and starts no process. It never opens the
ledger or the credential file, and never talks to the authenticated
`/status` route.

Token coverage is `tokenAttributedEvents / count * 100`. It is shown as
unavailable when the counters are absent or the event count is zero.

## What it shows

The status item is titled `PL`. Its menu shows two lines, re-read each time
the menu opens:

```text
Running · 1234 events · 87.5% token coverage
Tokens: 5200000 in · 310000 out
```

- `Stopped · … · as of 2 h ago`: the collector does not answer; the counts
  are from its last summary.
- `Collector unavailable · summary not updated for 7 min`: it answers, but
  has stopped rewriting its summary.
- `Collector unavailable` with a fixed reason: there is no usable summary
  (missing, not private, unreadable, or `PLIMSOLL_HOME` is not absolute).

The menu also has Refresh, Open Dashboard (`http://127.0.0.1:<port>/`,
offered only while the collector is running) and Quit.

`swift run plimsoll-menubar --status` prints the same lines once as JSON,
with the dashboard URL or `null`, and exits without starting the app (exit
1 when there is no usable summary).

## Safety

- Read-only and cheap. A refresh reads one small file and makes one loopback
  request, on launch, when the menu opens and on Refresh, never
  overlapping. It costs the same on any ledger size, and the app cannot
  start, stop or reconfigure the collector.
- No credential. The app never reads the credential file, and the
  `/healthz` probe sends no credential, cookie or proxy header. Every line
  it shows is fixed text around numbers; nothing from the file, the
  environment or the network is displayed as text. Open Dashboard never
  puts the credential in the URL.
- Bounded. The probe allows 3 seconds and stays on 127.0.0.1 with no proxy
  or cache.
- No shell, no child process, no helper, no LaunchAgent, no extra macOS
  permissions (below).

## Install

Build a release binary and start it as the user who runs the collector:

```bash
swift build -c release
.build/release/plimsoll-menubar &
```

If the collector uses a custom home, start the app with the same
`PLIMSOLL_HOME`. To stop it, choose Quit Plimsoll Menubar. There is no `.app`
bundle or login item yet; that ships with the signed release work below.

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
