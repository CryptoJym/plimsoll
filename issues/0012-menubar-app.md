# 0012 — Menubar: open-source the macOS status app

## TL;DR
- A Swift menubar wrapper exists in the private repo (collector start/stop, status glance, no invasive permissions). Extract, rename, open-source it here.
- Value: visibility — the dock-inspector experience. A user should *see* the line.

## Scope
Extract + rename + build instructions. App Store / notarized distribution is separate (pairs with the binary lane).

## Context
- Source: private repo `packages/mac-menubar` (Swift Package Manager; builds with `swift build`).
- It shells to the collector CLI — keep that contract; point it at either git-checkout or packaged bin (0011).

## Acceptance Criteria
- [ ] `packages/mac-menubar` here builds with `swift build` and shows collector status (running/stopped, event count, token coverage).
- [ ] No additional macOS permissions requested (no screen recording, accessibility, etc.) — assert in README + doctor.
