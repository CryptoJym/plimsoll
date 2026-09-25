# Lean Plimsoll pending contract tests (eco-6hoxj.164.4, B0)

These tests bind the lean-Plimsoll contracts (`docs/lean/CONTRACTS.md`, round 9 = B0 round 3, with the ARCHITECTURE, BUDGETS,
MIGRATION, PROOF and BEADS documents beside it) to this repository's surfaces. **Every pending test fails today by design** and is declared
`pending("<bead>")`, which node:test reports under `# todo`: the suite prints each failure and still exits 0, so it never
blocks a build. The bead named in a test's title implements the surface and removes the marker in the same change; from then
on the test is blocking. `b22-documents.contract.ts` is a guard (green today) that keeps the three documents agreeing.

Run: `pnpm contracts:lean` (also a CI step; Node 22 as in CI). This directory is outside `tsconfig` `include` on purpose: a
missing surface must fail as a test, never as a typecheck error; product modules that exist today are loaded through
`loadSurface` as well, so a rename stays a pending failure. Files: `schema` (B2a/B10a/B10b/B22: DDL v3, C3), `retention-hold`
(B10a), `actor-stamp` (B2a: the (install, version) pair, the joined install and its scope, C1/C4), `membership` (B2a), `runway`
(B1/B10a/B2a: G owed, the census gate and the measured C_conv, C2), `day-key` (B5/B9a), `converter` (B2a: C3), `conversion-rejects`
(B2a/B10b: retention and the live pointer, C3), `capture-gaps` (B22: C5), `receipts-and-ladder` (B2a/B10b/B13), `usage-record-pin`
(three green guards and one B2a test, C8). Guards (green today): `b22-documents`, the foreign-keys pragma in `schema`, `helper`
(every fixture append lands: the temporary ledger's enrollment epoch is pinned at 2026-01-01, before the fixtures' fixed
timestamps; the ladder test's old rows keep their outbox lineage under a mocked clock; the retention test's reject row is leased and
acknowledged), the three pin guards. A pending test that unexpectedly passes is reported as a passing todo: remove its marker.
