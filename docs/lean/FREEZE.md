# Freeze list after B0 round 3 (round 9, eco-6hoxj.164.4)

**As of:** 2026-09-25 MDT. Start point: `input/review-r2/VERDICT.json` (`freezeAgree`: nine items; `freezeDisagree`: C1/C4 "not yet",
with the conditions listed) and the round-8 `FREEZE.md`. A contract is **freezable** when its rule is stated once, its fixture is green
under the current rule, its pending test exists in the owning repository, and no open review finding touches it. **Freezing is the
lead's signature**, not B0's: this list says what is ready for that signature and what still needs a review round. Nothing is frozen by
this document.

## Freezable now (ready for the lead's signature)

| Contract | Where | Why it is ready |
|---|---|---|
| The pinned `<UR>` usage-record predicate and the finance/reconciliation identity fields as test inputs, with the lane-2 re-pin on merge (C8) | CONTRACTS.md C8; ARCHITECTURE.md §3.1, §10; `fixtures/usage_record_predicate.sql` in both repositories; cloud and collector `usage-record-pin` tests | Review r2 agreed on condition that the case folding be fixed: it now folds case outside quoted literals only, the pinned value is unchanged, and a third guard in each repository flips a literal (pin changes) and a keyword (pin unchanged). |
| Epoch-scoped day keys and the autoincrement `target_ref` | ARCHITECTURE.md §2.2, §3.3, §3.5; collector `schema.contract.ts`, `membership.contract.ts` | Review r2 agreed; nothing open. |
| The `day_summary` item and its receipt shape, and the collector's acknowledgement-only-through-receipts rule | ARCHITECTURE.md §5.2; cloud `activity-summary.contract.test.ts`; collector `receipts-and-ladder.contract.ts` | Review r2 agreed; nothing open. |
| **The S1b runway formula (C2 rules 2-3) and the census obligation (rule 1)** | CONTRACTS.md C2; ARCHITECTURE.md §2.4; BUDGETS.md §4.2-4.3; MIGRATION.md S1b, S3; `fixtures/s1b_runway_host_bound.py` (r9 25/25); collector `runway.contract.ts` | Review r2 agreed ("sign the formula now") and asked for two additions as text, both made: `C_conv` is a measurement (the converter's own page allocation per write transaction) and the re-census copy during the hold is gated on the runway. The formula itself is unchanged since round 8. **The numbers stay open** (below). |
| `conversion_rejects` DDL and retention (C3) | CONTRACTS.md C3; ARCHITECTURE.md §2.2, §2.3, §3.5; collector `schema.contract.ts`, `converter.contract.ts`, `conversion-rejects.contract.ts` | Review r2 agreed on condition that test 2 be fixed before B10b: it now appends its raw row through the buffer and acknowledges it, and a helper guard proves the premise. |
| The open-ended unresolved-file gap (`epoch_open`) in the bead, the drill and the architecture (C5) | BEADS.md B22; PROOF.md §8; ARCHITECTURE.md §3.6; collector `b22-documents.contract.ts` (guard), `capture-gaps.contract.ts`; cloud `capture-coverage.contract.test.ts` | Review r2 agreed; nothing open. |
| Token volume state and the G3 gate, including the `no_cache_columns` guard | DECISION-METRICS.md §0; ARCHITECTURE.md §3.1; cloud `token-volume.contract.test.ts` | Review r2 agreed; nothing open. |
| The abort rebuild bound | MIGRATION.md §4 step 8; PROOF.md §6 item 7; collector `receipts-and-ladder.contract.ts` | Review r2 agreed; nothing open. |

## Ready after one independent read of the round-9 text

| Contract | Where | State |
|---|---|---|
| **The actor-ownership predicate (C1) with the null-stamp exception and the joined-install scope (C4)** | CONTRACTS.md C1, C4; ARCHITECTURE.md §5.1-5.2, §7; `fixtures/b4_offline_rebind.py` (r9 44/44, red under r6, r7 and r8); cloud `actor-binding-stamp.contract.test.ts` (11 cases), `actor-binding-stamp-postgres.contract.test.ts` (5 cases), `schema-additions.contract.test.ts`; collector `actor-stamp.contract.ts` (7 cases) | Review r2 said "not yet" and named the conditions: accept only the joined install's responses (blocker 1), state the serialization of a sighting against the rebind (blocker 2), and in the same round scope the fact to the uploader, add the table to tenant erasure and state the retention of installs and audit rows (should-fixes 1-2). All five are done in this round, each with a fixture check that is red under round 8 and a pending test; the R5 interleaving was also reproduced in a real PostgreSQL cluster (`checks/r5-postgres-reproduction.log`). The design the reviewer accepted (the pair, the sticky first-sighting fact, clearing at a re-join) is unchanged. **Not listed as ready outright**, because this round changes the C4 rule (the joined install) and the fact's key (the uploader) beyond what the reviewer read; one independent read of C1/C4 round 9 is the remaining condition for signature. |

## Not freezable yet (needs a review round or a measurement)

| Contract | Why not |
|---|---|
| **The S1b runway numbers** as a gate that may start a live hold | The row widths (`F`, `T`, `S`, `SD`, `DR`, `RB`, `SEG`, `I`) are still column-width estimates and only Studio4's cardinalities are measured (its segment proxy is not: the census was not re-run in rounds 8 or 9). The fixture shows the rule overstates the runway when the true widths exceed 1.25 × the estimates (and does not self-correct), so no hold may start on any host before S2 has measured the widths on the Studio5 copy and B1's post-catch-up census has run on that host's copy (review r6 `firstWave`: "no live hold or S2"). |
| The deletion proof and the cloud residue proof as a whole | Its blockers were stated as conditions on freezing the actor and runway contracts, which the deletion proof's step 1 depends on through the day receipt; freeze after C1 and C2 are signed. |
| Claim v2, the fault marker and restart rule, the paged id exchange | Specified, not implemented, no fixture beyond `sf5` and `b22`; review r6 called them future contracts (R6-15). A fixture-backed round is needed before freezing. |
| The per-row width constants and the 1.25 factor | Estimates until S2 (BUDGETS.md §6); the fixture's width-sensitivity check is the reason. |
| B18's fleet half | Gated on an accepted native owner receipt (unchanged since round 6). |

## What the lead signs

1. C2: the formula (`G_owed`, one G) and the census obligation (after the catch-up, fresh, re-taken before S3, space-gated and, during the hold, runway-gated, segment proxy), with `C_conv` as a measurement: ready; the widths, the 1.25 factor and every host's numbers stay open until S2 and the per-host census.
2. C3 (`conversion_rejects`), C5 (open gap), C8 (the `<UR>` pin), the day receipt, the day keys and `target_ref`, the volume gates, the abort bound: ready.
3. C1 and C4 (the actor predicate as the pair fixed at its first sighting, serialized against the rebind and keyed by the uploader; the null-stamp exception; the joined-install scope): ready **after one independent read of the round-9 text**.

## First-wave conditions from reviews r1 and r2, and their state

- Review r2: "the first wave can continue"; B10a's hold tests append and fail only at `hold_reason`; B1 builds `G_owed` as in round 8 (the formula is unchanged in round 9) and reads `C_conv` from the converter's measured checkpoint.
- Review r1: "fix the pending-test helper before B10a relies on its tests": done in round 8 and completed in round 9 (the two remaining unpassable tests, both B10b's, are repaired with guards; neither is first wave).
