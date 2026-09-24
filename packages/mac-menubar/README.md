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
per-run `instanceId`, a random per-run `healthzKey`, its version and the write
time (collector runbook `docs/runbooks/local-status-http.md`). The app opens
that file without following a symlink, requires it to be the user's own
private (0600) regular file of at most 16 KB, and parses it exactly: those
seven keys and no others, each of the right form. It then sends one
`GET /healthz?challenge=<32 fresh random bytes, base64url>` to
`127.0.0.1:<port>` from the file.

The collector counts as running only if that reply is exactly:

```http
HTTP/1.1 200
{"ok":true,"instanceId":"<the instanceId in status-summary.json>","proof":"<HMAC>"}
```

That means HTTP 200 and a JSON object with those three keys and no others,
`ok` the boolean `true`, `instanceId` equal to the id of the run that wrote
the summary, and `proof` the base64url HMAC-SHA256, under the summary's
`healthzKey`, of `plimsoll.healthz-proof/v1`, the port, the `instanceId` and
this challenge (one per line), checked in constant time. Only the collector
run and a reader of the private file hold the key, so anything else is not
the collector: another service answering the common `{"ok":true}`, an older
or newer collector run, a process that replays the public `instanceId` or an
earlier answer after taking the port, or one relaying the challenge from
another port. The summary stays on disk after the collector stops, and it
can outlive a collector that no longer owns the port.

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

The menu also has Refresh, Open Dashboard and Quit. Open Dashboard
(`http://127.0.0.1:<port>/`) is offered only while the collector is verified
running, and is checked again when clicked. The dashboard keeps its
credential in that origin's browser storage, so no other service on the
port may be handed it.

`swift run plimsoll-menubar --status` prints the same lines once as JSON,
with the dashboard URL or `null`, and exits without starting the app (exit
1 when there is no usable summary).

## Safety

- Read-only and cheap. A refresh reads one small file and makes one loopback
  request, on launch, when the menu opens and on Refresh, never
  overlapping. It costs the same on any ledger size, and the app cannot
  start, stop or reconfigure the collector.
- No credential. The app never reads the credential file, and the
  `/healthz` probe sends no credential, cookie or proxy header. The
  summary's `healthzKey` is used only to check the proof; it is never sent,
  shown or printed. Every line it shows is fixed text around numbers;
  nothing from the file, the environment or the network is displayed as
  text. Open Dashboard never puts the credential in the URL.
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
