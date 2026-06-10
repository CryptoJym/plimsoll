# 0011 — Packaging: @plimsoll/cli on npm, npx quickstart

## TL;DR
- `npx @plimsoll/cli install` should be the one-liner. Core npm name `plimsoll` is squatted (dead 0.1.x experiment) — scope everything under `@plimsoll/*`.
- Requires building the TS → dist (tsx is a dev crutch), bin entry, and the LaunchAgent pointing at the installed package instead of a git checkout.

## Scope
Publish pipeline + bin UX. Signed standalone binary (no Node at all) is a separate later lane (bun/SEA) — file when this closes.

## Context
- Native dep better-sqlite3 ships prebuilds for mac arm64/x64 + Node LTS — keep Node engine range tight (>=20 <25) to stay on prebuilds and avoid the ABI class of failures that killed the v1 daemon for 2.5 weeks.
- LaunchAgent currently execs `pnpm --dir <repo> collector start`; packaged mode should exec the installed `plimsoll` bin directly.

## Acceptance Criteria
- [ ] `npm i -g @plimsoll/cli && plimsoll install` reaches collecting state on a clean Mac.
- [ ] `npx @plimsoll/cli doctor` works without global install.
- [ ] CI publishes on tag with provenance; README quickstart switches to npx as primary, git clone as contributor path.
