# 0174 — Matched-outcome capacity research guardrails

GitHub source: https://github.com/CryptoJym/plimsoll/issues/174 (parent #167; reuses #157 evidence packets and plimsoll-cloud#28 D3/D4 versioning).

## TL;DR
- Capacity can be studied only as a supporting context variable under a separate, pre-registered scientific protocol enforced fail-closed in code.
- Shipped as `packages/shared/src/capacity-research.ts` + `pnpm proof:capacity-research` (32 checks) + reachability-gate integration.
- No second matched-outcome engine: the protocol computes no effects and joins no data; it gates designs and findings only.

## Scope
In: protocol pre-registration (work type + time-window matching cell, six required evidence dimensions, declared minimum sample, UNKNOWN missing-evidence policy, six separate fact-stream join keys, three-machine-compatibility exclusion, human review), readiness gating to literal UNKNOWN, disclosure-complete finding envelope with forbidden-decision enforcement, static doctrine integration.
Out: any effect computation, live joins, rankings/coaching/compensation/discipline/interventions/D3-D4/verdict surfaces, changes to `capacity.ts` planning behavior or `learning-evidence.ts`.

## Acceptance Criteria
- [x] Capacity cannot feed rankings, coaching scores, compensation, discipline, interventions, D3/D4, or individual verdicts: forbidden decisions throw in `buildCapacityResearchFinding`; the reachability gate treats every capacity-research symbol consumer as a capacity consumer (`falsification_*` checks).
- [x] Three-machine compatibility data is excluded from statistical team conclusions: explicit exclusion required at pre-registration; compatibility observations subtracted from all inference denominators; compatibility-only studies are UNKNOWN.
- [x] Insufficient evidence remains `UNKNOWN`: below-minimum samples, zero matched pairs, and unknown-evidence pairs yield status-UNKNOWN findings with null claim/confidence/sample.
- [x] Every output states scope, source dates, confidence, limitations, and supported/unsupported decisions.
- [x] Work type AND time window match before comparison; six evidence dimensions individually required; minimum sample + UNKNOWN policy declared before inference.

## Operational Boundaries
- `pnpm proof:capacity` and `pnpm proof:capacity-research` stay green; repo-wide typecheck gains no new errors.
- Pure data-in/data-out module; no fs writes, no fetches, no collector state changes.

## Notes For Future Agents
- Library: `packages/shared/src/capacity-research.ts`. Proof: `scripts/capacity-research-proof.ts`.
- The proof script is on the reachability gate's exact-path enforcement-tooling allowlist because enforcing the doctrine requires exercising the API. The allowlist stays a closed path set; nothing can join by renaming.
- Matched-outcome evidence itself still comes from #157 packets; this layer never replaces them.

## Open Questions
- Should hosted D3/D4 (plimsoll-cloud#28) adopt this protocol schema as its intake contract for any capacity-context study? Owner decision.
