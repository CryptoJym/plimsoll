# Learning metric truth contract

`packages/shared/src/metric-registry.ts` is the only versioned definition surface for the first learning metrics. It is pure: callers provide a bounded manifest, and the module returns deterministic descriptive results without fetching, recommending, or changing collector state.

## Versions

- Analysis manifest: `1.0.0`
- Metric definitions: `1.0.0`

Both versions are required in every input. An unknown version fails closed instead of silently changing a formula.

Every registry entry also carries:

- lifecycle status: `experimental`, `active`, or `deprecated`
- validation status: `unvalidated`, `adversarial_fixture_validated`, `externally_validated`, or `blocked`
- the population event used for cohort membership: `submitted` or `merged`

Persisted results must pass `assertComparableMetricResults` before comparison. Different or unsupported formula versions, registry-mismatched lifecycle/validation/population claims, incompatible formula configuration, malformed status values, and blocked/unvalidated results fail closed. The result identity includes the closed `formulaConfig` object; stability and rework results therefore carry their exact `stabilityHorizonDays`, while metrics whose formula does not use that parameter carry `null`.

## Registry

| Metric | Numerator | Denominator / censoring |
| --- | --- | --- |
| Project allocation coverage | Deliveries with an exact or apportioned project allocation and attribution | All submitted-cohort deliveries; explicit unallocated and unknown allocation remain in the denominator |
| First-pass yield | Verified explicit pass on the first required-check observation | All cohort deliveries; `none`, `unknown`, missing pages, and failed fetches cannot pass |
| Correction loop | Verified first failures that later become green by `asOf` | All verified first failures |
| Time/tokens to first green | Separate elapsed-time and input/output/cache-read/cache-write sums | A separate known-observation count for every dimension |
| Mature stable delivery | Mature, check-passing deliveries with complete rework evidence and no rework in the horizon | Only deliveries merged in the declared window whose full stability horizon has elapsed |
| Post-merge rework | Mature deliveries with observed rework in the horizon | All deliveries merged in the declared window and mature; incomplete evidence makes the rate a floor |
| Known-cost coverage | Deliveries with reported or explicitly estimated cost | All cohort deliveries; missing cost is unknown |
| Technique exposure | Deliveries with one or more attributed technique ids | All cohort deliveries; exposure is descriptive, not an effect estimate |

## Required result envelope

Every metric result names:

- `definitionVersion`, closed `formulaConfig`, `window`, and `asOf`
- lifecycle status, validation status, and population event
- keyed `numerator`, `denominator`, and calculated `measures`
- `sample` counts: eligible, sampled, excluded, censored, unknown, and exclusion reasons
- verified/inferred/partial/blocked/excluded source `coverage`
- configured horizon and mature/censored counts under `maturity`
- attribution methods, attribution coverage, and exact/apportioned/unallocated/unknown allocation mix
- evidence state and analytical claim class
- limitations and any deterministic breakdown rows

Evidence states are `verified`, `partial`, `inferred`, `blocked`, and `excluded`. Claim classes are `observed`, `suggestive`, `associated`, `causal`, and `not_estimable`. This single-cohort registry emits only observed or suggestive descriptions; blocked, excluded, and fully censored results are `not_estimable`. Association and causality require a separate comparative design and are never inferred from technique exposure.

## Fail-closed rules

- Identity strings are accepted only in canonical trimmed NFC form. Analysis, delivery, project, attempt, revision, supersession, technique, and event-kind identifiers with leading/trailing whitespace or Unicode aliases are rejected before grouping, uniqueness checks, or lineage comparisons.
- Check attempts require canonical attempt and revision ids, unique attempt ids and sequences, an increasing sequence, and a linear `supersedesAttemptId` chain. This is the deterministic check lineage; repeated canonical revision ids remain visible and do not close a correction.
- Check success requires `checks.evidenceState = verified`, an explicit `passed` observation, and unambiguous lineage. `none`, `unknown`, or conflicting states at the same timestamp make the outcome incomplete. A visible pass on an incomplete page does not count.
- A correction closes only when the final passing attempt follows the failed attempt through the lineage and carries a different revision id.
- Events after `asOf` are invisible. Stability and rework also ignore events after the configured horizon.
- Submitted-cohort metrics use `submittedAt`; stability and rework use `mergedAt`. A delivery submitted before the window but merged inside it remains in merge-based populations.
- Right-censored deliveries remain visible in `eligibleCount` but do not enter stability numerators or denominators until mature.
- Missing cost yields `null`/`unknown`; a subtotal with incomplete coverage is a `floor`. An explicitly reported `$0` remains distinguishable from missing cost.
- Input, output, cache-read, cache-write, time, and USD are distinct units. `sumLikeQuantities` rejects unlike units, so token dimensions cannot be collapsed or mixed with dollars.
- Runtime enum values are validated even for plain JavaScript/JSON callers. Verified evidence cannot carry null token dimensions, missing cost, null technique ids, or an unknown project allocation.
- Persisted result status and population claims must exactly match the current registry entry. Formula comparison also checks the complete closed runtime configuration, so results calculated with different stability horizons are incompatible even when their definition version matches.

## Project allocation

Each delivery declares one allocation kind:

- `exact`: one project share of `1.0`, matching the legacy singular project id
- `apportioned`: two or more unique project shares that sum to `1.0`
- `unallocated`: explicitly no project allocation
- `unknown`: allocation evidence is incomplete; it cannot be marked verified

The result envelope reports all four counts so exact attribution, apportioned attribution, explicit non-allocation, and missing evidence never collapse into one number.

Run the adversarial golden fixtures with:

```sh
pnpm proof:metric-truth
```
