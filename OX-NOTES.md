# OX-NOTES — Issue #194: Lane-receipts integration gate fail-open fix

## Root cause

`evaluateIntegrationGate` (packages/shared/src/lane-receipts.ts) computed
`passing = evidence.filter(passed && headSha === context.headSha)` and refused
only when a gate had **no evidence** or **no passing rows** at the head. An
explicit `{ passed: false }` record at the exact context head was silently
outvoted by any coexisting passing row (duplicate lane, rerun, or adversarial
re-submission) → `allowed: true`. Classic unratcheted-refusal fail-open.

Same-battery defects verified and fixed in the same lane:

1. **Gate fail-open** (main): explicit failure at head ignored when passing duplicates exist.
2. **Dead-coded contradiction**: `preflightContradictions` had an `&& false`
   clause that dead-coded the ACTIVE + headSha===baseSha ("unreachable")
   contradiction check.
3. **Unenforced bounds**: `isBoundedStringList` / `MAX_LIST_ITEMS` / `MAX_ITEM_LENGTH`
   were declared but enforced nowhere; `buildOpsReceipt` accepted arbitrarily
   large lists/items.
4. **Top-level-only privacy scan**: `findReceiptPrivacyViolations` checked the
   allowlist/forbidden-concept rules on top-level key names only; structured
   payloads smuggled inside nested objects under allowed fields (e.g.
   `resumeCommand: { prompt: ... }`) passed. Prose words inside allowed string
   values are still not flagged as fields (keys are scanned, not string contents).

## Files changed

- `packages/shared/src/lane-receipts.ts`
  - `evaluateIntegrationGate`: new `gate_failed_at_head:<gate>` reason fired whenever any
    `passed:false` row exists at `context.headSha`, regardless of passing duplicates;
    failures recorded at *other* heads still do not block; adversarial-reviewer
    distinctness check unchanged.
  - `preflightContradictions`: removed `&& false`; ACTIVE + head==base now reports `"unreachable"`.
  - `buildOpsReceipt`: fails closed (`receipt_list_out_of_bounds:<field>:...`) via the previously-dead
    `isBoundedStringList` on changedFiles/exactTests/failures/blockers.
  - `findReceiptPrivacyViolations`: recursive key scan at all depths with path-qualified reasons
    (`forbidden_field:resumeCommand.prompt`); top-level reasons unchanged in format.
- `scripts/lane-receipt-proof.ts`: new adversarial proof functions
  `proveGateFailureCoexistence`, `proveNestedConceptSmuggling`,
  `proveReceiptListBounds`, plus the `unreachable` contradiction check and wiring into `main()`.
- `scripts/fixtures/lane-receipts/hostile/gate-failure-coexistence.json` (new hostile fixture:
  failing+passing coexistence at same head, failing-only, stale-failure-at-other-head).
- `scripts/fixtures/lane-receipts/hostile/unbounded-list.json` (new hostile fixture: 513 items /
  4097-char item beyond MAX_*).

## Adversarial test evidence (fail on current code → pass after fix)

Driven by a temporary harness asserting each behavior directly against
`packages/shared/src/lane-receipts.ts` (`npx tsx <harness>`); the repo proof
asserts the same behaviors permanently.

| Test | On pre-fix code | On post-fix code |
|---|---|---|
| T1 single `passed:false` at head → `allowed:false` + `gate_failed_at_head:privacy` | assertion fired (defect reproduced) | holds |
| T2 failing+passing coexistence at same head → refused with `gate_failed_at_head` | assertion fired (defect reproduced) | holds |
| T3 failure at *other* head + passing at context head → still allowed (regression guard) | held | held |
| T4 ACTIVE + head==base → contradiction `unreachable` reported | no contradiction (dead-coded) | reported |
| T5 513-item list to buildOpsReceipt → throws | **accepted silently** (bug present) | throws `receipt_list_out_of_bounds:changedFiles...` |
| T6 4097-char list item → throws | **accepted silently** (bug present) | throws `receipt_list_out_of_bounds:exactTests...` |
| T7 `resumeCommand: { prompt: ... }` → privacy violation | zero violations (smuggling passed) | violation reported |

Raw harness output captured pre-fix (T1/T2/T4/T7 "defect reproduced";
T5/T6 "bug present: hostile input was accepted") and post-fix (all flipped,
T3 guard unchanged). The repo proof halts on first failed assert pre-fix:

```
$ pnpm proof:lane-receipts          # pre-fix
AssertionError [ERR_ASSERTION]: active_lane_with_head_equal_to_base_is_contradictory: {"contradictions":[]}
exit=1
```

## Proof commands + results

```
$ pnpm proof:lane-receipts            # post-fix
status: "passed", checks: 69, exit=0

$ npx tsc --noEmit                    # repo-wide typecheck
exit=0

$ pnpm proof:signal-fidelity && pnpm proof:privacy-mode && pnpm proof:metric-truth \
  && pnpm proof:maintenance && pnpm proof:repo-context && pnpm proof:codex-reconciliation \
  && pnpm proof:usage-dedupe && pnpm proof:join-isolation && pnpm proof:outbox \
  && pnpm proof:fingerprint-versioning && pnpm proof:capacity && pnpm proof:allocation \
  && pnpm proof:learning-facts && pnpm proof:lifecycle
all exit=0
```

Pre-existing/environmental failures, verified identical on a clean tree
(`git stash` → rerun → identical failure → `git stash pop`), unrelated to this change:

- `proof:maintenance-boundary`: `hook p95 1290.9ms exceeded 750ms` (latency-budget proof, load-sensitive; also fails clean).
- `proof:dashboard`: undici fetch error at dashboard-projection-proof.ts:570 (network-dependent; also fails clean).

No packaged-CLI build was needed: the lane-receipts proof runs from source via tsx.

## Acceptance criteria status

- [x] A `passed:false` record for a gate at the context head makes `allowed:false`
      with distinct reason `gate_failed_at_head:<gate>`, regardless of passing duplicates. (T1/T2)
- [x] Hostile fixture: failing+passing coexistence at the same head → refused
      (`gate-failure-coexistence.json`, covered by `proveGateFailureCoexistence`). (T2)
- [x] Adversarial-reviewer distinctness check unchanged
      (`adversarial_reviewer_matches_builder_owner` logic untouched; existing proof checks still green).
- [x] Battery extras verified during fix: `&& false` reactivated; MAX_* bounds enforced
      fail-closed in `buildOpsReceipt`; forbidden-concept scan extended to keys at every depth.
