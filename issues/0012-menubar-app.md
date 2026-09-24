# 0012 — Menubar: open-source the macOS status app

Status: `packages/mac-menubar` landed from PR #253 and verified locally against collector 0.7.37 (eco-6hoxj.163.34); the proof workflow builds and tests it (`pnpm proof:mac-menubar`).

## TL;DR
- A Swift menubar wrapper exists in the private repo (collector start/stop, status glance, no invasive permissions). Extract, rename, open-source it here.
- Value: visibility — the dock-inspector experience. A user should *see* the line.

## Scope
Extract + rename + build instructions. App Store / notarized distribution is separate (pairs with the binary lane).

The open-source app is read-only: it shows status and never starts, stops or reconfigures the collector (start/stop was left out).

## Context
- Source: private repo `packages/mac-menubar` (Swift Package Manager; builds with `swift build`).
- It shells to the collector CLI — keep that contract; point it at either git-checkout or packaged bin (0011).

## Acceptance Criteria
- [x] `packages/mac-menubar` here builds with `swift build` and shows collector status (running/stopped, event count, token coverage).
  `pnpm proof:mac-menubar` runs `swift build` then `swift test`; `plimsoll-menubar --status` prints the menu's lines, checked against a disposable 0.7.37 collector running, stopped and without its credential file.
- [x] No additional macOS permissions requested (no screen recording, accessibility, etc.) — assert in README + doctor.
  The README and `--doctor` assert it, and the tests fail if the sources call a permission-prompting API or the package gains an entitlements file or plist.
