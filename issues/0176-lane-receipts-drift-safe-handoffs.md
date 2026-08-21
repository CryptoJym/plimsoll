# 0176 — Deterministic build/audit lane receipts and drift-safe handoffs

## TL;DR
- Parallel build/audit lanes get five reusable read-only controls: liveness preflight, bounded phased fan-out, worktree census, deterministic handoff receipts, and immutable attempt history.
- Shipped as `packages/shared/src/lane-receipts.ts` + hostile fixtures + `pnpm proof:lane-receipts` (57 checks).
- Workflow instrumentation only — no merge, install, deploy, process-stop, credential, or machine authority.

## Scope
In: preflight records (lane ID, owner role, branch, base/head SHA, clean/dirty, last activity, literal liveness), phase/budget declaration, census findings, deterministic `HANDOFF.md` + `ops-receipt.json`, append-only attempt history, privacy guard, hostile fixtures, proof.
Out: automatic termination, broad cleanup, auto-merge, deployment, remote shell, employee/performance scoring; #133 product CLI receipts; #171 artifact approval gates.

## Acceptance Criteria
- `pnpm proof:lane-receipts` passes with checks covering every acceptance bullet.
- Preflight parse fails closed on missing fields, bad SHAs, unknown enums (`missing-fields.json`).
- Contradictory states flagged (`contradictory-states.json`); ACTIVE past staleness budget reported stale (`stale-liveness.json`).
- Census detects duplicate heads, duplicate lane ownership (`duplicate-lane-ownership.json`), dirty-unowned lanes, base drift, missing worktrees (`dirty-drift.json`), stale handoffs — without mutating anything.
- Receipts are byte-deterministic regardless of input order; HANDOFF.md derived from receipt only.
- Retry keeps original attempt ID/source head; erased prior failures detected via `retry-overwrite.json`; changed source head on retry fails closed.
- Privacy guard rejects prompts, transcripts, credentials/env values, home paths, provider payloads, productivity judgments (`secret-like-strings.json`).
- Integration gate requires typescript-exact-head / privacy / tamper / adversarial-review evidence at the exact head, with reviewer independence from the builder.

## Operational Boundaries
- `pnpm proof` stays green. All functions are pure data-in/data-out: no fs writes, no processes, no state mutation of inputs.
- Findings/receipts carry bounded subjects only — never raw content, never home paths.

## Notes For Future Agents
- Library: `packages/shared/src/lane-receipts.ts`. Proof: `scripts/lane-receipt-proof.ts`. Fixtures: `scripts/fixtures/lane-receipts/hostile/` (all synthetic — token-shaped strings are fake sentinels).
- Census and preflight are observation layers: a finding must never trigger repair or stoppage in calling code.

## Open Questions
- Should census consume live `git worktree list --porcelain` output via a future adapter, or stay snapshot-driven?
