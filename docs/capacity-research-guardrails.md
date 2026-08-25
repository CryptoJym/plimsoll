# Matched-outcome capacity research guardrails

`packages/shared/src/capacity-research.ts` (issue #174) is the versioned
contract under which capacity may be STUDIED as a supporting context
variable in outcome research. It is a pre-registration and disclosure
gate, not a second matched-outcome engine: it computes no effects, joins
no live data, and never replaces the bounded work-unit evidence packets
(`learning-evidence.ts`) as the evidence source.

## What must be declared before any inference

`validateCapacityResearchProtocol` fails closed unless every item below is
declared up front:

- **Matching cell** — one work type, one explicit time window
  (`since` < `until`), and a population event (`submitted` or `merged`).
  Comparisons across mismatched work types or windows are structurally
  impossible.
- **All six required evidence dimensions** — `stable_outcome_evidence`,
  `quality`, `rework`, `complexity`, `declared_confounders`,
  `human_review`. Dropping any one dimension fails pre-registration.
- **Minimum sample and missing-evidence rule** — `minMatchedPairs >= 1`
  and `missingEvidencePolicy: "unknown"`. There is no other policy:
  dropping, zeroing, or imputing missing evidence is rejected.
- **Six separate fact-stream joins** — identity, activity, time, cost,
  capacity, and outcomes each carry their own join key. A missing stream,
  duplicate stream, or two streams sharing one key (a collapse) fails.
- **Explicit exclusion of three-machine compatibility data**
  (`three_machine_compatibility`) from statistical team conclusions.
- **Recorded human review** — approver identity and timestamp; a
  future-dated approval fails closed.

## Readiness gates

`evaluateCapacityResearchReadiness` subtracts machine-compatibility
observations entirely and treats matched pairs with unknown required
evidence as unusable-but-visible. Below the protocol's declared minimum,
or without any matched pairs, the result is literal `UNKNOWN` with the
failing gate named — never a small effect estimate, never a zero.

## Research output envelope

`buildCapacityResearchFinding` emits findings only through a
disclosure-complete envelope: schema version, study id, scope (work type,
window, population event), source dates (window plus generation instant),
confidence level and basis, fixed doctrine limitations, sample counts
including excluded compatibility observations, and the decisions the
output can and cannot support.

Fail-closed rules on estimable findings:

- Claim classes cap at observational association (`observed` /
  `suggestive`). Causality requires a separate pre-registered experiment;
  requesting it throws.
- Confidence levels cap at `low` / `moderate`.
- A supported decision intersecting the forbidden set — rankings,
  coaching scores, compensation, discipline, interventions, D3/D4 hosted
  analytics, individual performance verdicts — throws. Capacity is a
  supporting context variable; it never feeds those surfaces.
- Insufficient evidence produces a status-`UNKNOWN` finding with null
  claim/confidence/sample, empty supported decisions, and the full
  unsupported-decision list intact.

## Static doctrine integration

The module is deliberately capacity-named: everything it exports joins
the capacity symbol set of the reachability gate
(`scripts/capacity-dependency-reachability.ts`), so any decision surface
that consumes these symbols turns the gate red exactly like a consumer of
the planning module. The only offense exemption remains the gate's own
enforcement tooling keyed to exact repo-relative paths; issue #174 adds
`scripts/capacity-research-proof.ts` to that closed allowlist.

Run the adversarial fixtures with:

```sh
pnpm proof:capacity-research
```
