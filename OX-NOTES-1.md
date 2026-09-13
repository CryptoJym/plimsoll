# OX-NOTES — Issue #1 (0001): v0.1 open release readiness — child-lane ledger + quickstart contract

Lane: `eco-wyxy2` (single-shot, read-only outside lane dir, no push/gh/railway).
Date: 2026-08-25. Repo state: main @ 7fbbb2d, clean tree before this lane.

## What this lane did

Issue 0001 is the v0.1 release-readiness parent. Its three acceptance criteria were
audited against the real tree; two could be closed with evidence, one is blocked
inside this lane and is documented as such instead of being faked.

1. **Child-lane ledger added to `issues/0001-v0.1-release-readiness.md`** — per-child
   status (closed / closed-in-substance / open-deferred) each tied to file-and-line or
   registry evidence. AC-1 ticked on that basis.
2. **README quickstart contract check** (AC-3) — every command in the README's
   Quickstart + contributors blocks was validated against source-derived facts:
   - subcommands must appear in `packages/collector-cli/src/cli.ts` (`command === "…"`);
   - flags must be accepted by the parsed flag sets in cli.ts (`setup`: --yes/--dry-run/
     --claude-settings/--codex-config; `doctor`: --read-only/--json; `install-launch-agent`:
     --dev/--repo-root/--pnpm/--load/--dry-run);
   - pnpm script names must exist in root package.json; install.sh args must match its
     case arms; `pnpm report` must pass `--repository owner/repo`.
   Checker: Python 3.14, run from the lane (not committed — see "deliberately not done").
   Result: **PASS — 2 bash blocks, 12 distinct commands all match source**.
   Adversarial self-test: 11 mutated commands (typo'd subcommands `steup`, removed lane
   `apply-config`, unknown flags --force/--write/--raw-mode/--fix, nonexistent script
   `repot`, bare/short-flagged `report`, `install.sh --apply`) — **11/11 rejected**, and
   the checker provably restores README byte-for-byte after probing.

## Registry verification for 0011 (read-only GET)

`https://registry.npmjs.org/@plimsoll%2fcli` → HTTP 200:
`dist-tags.latest = "0.6.0"`, `versions["0.6.0"].bin = {plimsoll: "dist/cli.mjs"}`,
engines `>=20 <25`, published `2026-06-13T01:54:25.552Z`. In-tree
`packages/collector-cli/package.json` version is exactly `0.6.0` → the published npx
quickstart corresponds to this tree. Remaining 0011 gaps (tag-publish workflow with
provenance, clean-Mac global-install smoke) are recorded as deferred in the ledger.

## Blocked: fresh-Mac walkthrough transcript (AC-2) — NOT_RUN, with cause

The lane host's Node-family runtime is stubbed; verbatim observations:

```
$ /Users/utlyze/.nvm/current/bin/node --version
22.0.0            # a real Node prints vX.Y.Z; a ~200-byte shim file sits at bin/node

$ cd repo && pnpm install ; echo EXIT=$?
22.0.0
EXIT=0            # exit 0 but node_modules is never created
```

Same output for `npx -y pnpm@10.25.0 install`. With no executable Node there is no way to
run the collector, doctor, report, or any proof script in this lane, and running the real
installer would write outside the lane (`~/.plimsoll`, LaunchAgents, tool configs)
regardless. The transcript requirement therefore stays OPEN in issue 0001 with a note
pointing directors at `proof:system-e2e` (+verify/tamper) as the standing isolated
equivalents until a clean Mac run is recorded.

Consequently these checks are NOT_RUN in this lane (not assumed green):
`tsc --noEmit`, `pnpm proof`, every other proof:* target. They remain CI-enforced via
`.github/workflows/proof.yml`.

## Files changed

- `issues/0001-v0.1-release-readiness.md` — AC ticks (two), W-2 blocker note, child-lane ledger.
- `OX-NOTES-1.md` (this file).

No product code, config, workflow, or proof files were touched.

## Deliberately not done (scope discipline)

- No permanent checker committed to the repo: the repo's own gates are TS/pnpm-based;
  committing a Python side-tool would add an unmaintained second stack. If maintainers
  want the gate permanently, port it to a TS proof (`proof:readme-contract`) — the lane's
  implementation documents the exact rule set.
- No edits to README prose beyond what the checker validates (the quickstart already
  matches reality; the historical `$48.46` example block is labeled as a past measurement
  and cannot be re-derived without a ledger).
