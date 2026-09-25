# Lean Plimsoll pending contract tests (eco-6hoxj.164.4, B0)

These tests bind the lean-Plimsoll contracts (`docs/lean/CONTRACTS.md`, with the round-7 ARCHITECTURE, BUDGETS, MIGRATION,
PROOF and BEADS documents beside it) to this repository's surfaces. **Every pending test fails today by design** and is declared
`pending("<bead>")`, which node:test reports under `# todo`: the suite prints each failure and still exits 0, so it never
blocks a build. The bead named in a test's title implements the surface and removes the marker in the same change; from then
on the test is blocking. `b22-documents.contract.ts` is a guard (green today) that keeps the three documents agreeing.

Run: `pnpm contracts:lean` (also a CI step; Node 22 as in CI). This directory is outside `tsconfig` `include` on purpose: a
missing surface must fail as a test, never as a typecheck error. Files: `schema` (B2a/B10a/B10b/B22: DDL v3, C3), `retention-hold`
(B10a), `actor-stamp` (B2a: C1/C4), `membership` (B2a), `runway` (B1/B10a: C2), `day-key` (B5/B9a), `converter` (B2a: C3),
`capture-gaps` (B22: C5), `receipts-and-ladder` (B2a/B10b/B13). A pending test that unexpectedly passes is reported as a passing
todo: remove its marker.
