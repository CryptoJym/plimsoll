# Freeze list after B0 round 2 (round 8, eco-6hoxj.164.4)

**As of:** 2026-09-25 MDT. Start point: `input/review-r1/VERDICT.json` (`freezeAgree`: nine items; `freezeDisagree`: C1/C4 and the C2
formula) and the round-7 `FREEZE.md`. A contract is **freezable** when its rule is stated once, its fixture is green under the current
rule, its pending test exists in the owning repository, and no open review finding touches it. **Freezing is the lead's signature**,
not B0's: this list says what is ready for that signature and what still needs a review round. Nothing is frozen by this document.

## Freezable now (ready for the lead's signature)

| Contract | Where | Why it is ready |
|---|---|---|
| The pinned `<UR>` usage-record predicate and the finance/reconciliation identity fields as test inputs, with the lane-2 re-pin on merge | CONTRACTS.md C8; ARCHITECTURE.md §3.1, §10; `fixtures/usage_record_predicate.sql` in both repositories; cloud and collector `usage-record-pin` tests | Review r1 agreed, on condition of the promised pin test: it now exists in both repositories (two guards green each, one pending each). The text is unchanged at `loader.ts` since `067a8a4`. |
| Epoch-scoped day keys and the autoincrement `target_ref` | ARCHITECTURE.md §2.2, §3.3, §3.5; collector `schema.contract.ts`, `membership.contract.ts` | Review r1 agreed; membership test 2 now appends its rows (blocker 3 fixed, `helper.contract.ts`). |
| The `day_summary` item and its receipt shape, and the collector's acknowledgement-only-through-receipts rule | ARCHITECTURE.md §5.2; cloud `activity-summary.contract.test.ts`; collector `receipts-and-ladder.contract.ts` | Review r1 agreed; nothing open. |
| **The actor-ownership predicate (C1) with the null-stamp exception and the install scope (C4)** | CONTRACTS.md C1, C4; ARCHITECTURE.md §5.1-5.2; `fixtures/b4_offline_rebind.py` (r8 36/36); cloud `actor-binding-stamp.contract.test.ts`, `schema-additions.contract.test.ts`; collector `actor-stamp.contract.ts` | Review r1's blocker is closed: the stamp is the pair `(install, version)`, its answer is fixed at the first sighting and recorded, the collector clears the pair at a join, re-join or transition, and the claim states exactly the guarantee the fixture proves (four numbered statements, an over-time probe of every sighted pair, the reviewer's R1 and R2 orderings and the honest re-join as cases). **Recommended for signature after one independent read of C1 round 8**, because the repair changes the wire format (the pair) and adds a durable cloud fact (`stamp_not_issued`) beyond the reviewer's proposed repair (CHANGES.md §5). |
| **The S1b runway formula (C2 rules 2-3) and the census obligation (rule 1)** | CONTRACTS.md C2; ARCHITECTURE.md §2.4; BUDGETS.md §4.2-4.3; MIGRATION.md S1b, S3; `fixtures/s1b_runway_host_bound.py` (r8 19/19); collector `runway.contract.ts` | Review r1's blocker is closed: `G_owed` counts only the converter's own bytes against the census-era work and adds the rows admitted after the census; the fixture's truth is a simulation independent of the rule, and it shows the rule never overstates the runway for widths up to 1.25 × the estimates and that no rung fires late. The census obligation carries the reviewer's additions (after the catch-up, fresh at S1b, re-taken before S3, space-gated) and the segment proxy. **The numbers stay open** (below). |
| `conversion_rejects` DDL and retention (C3) | CONTRACTS.md C3; ARCHITECTURE.md §2.2, §2.3, §3.5; collector `schema.contract.ts`, `converter.contract.ts`, `conversion-rejects.contract.ts` | Review r1 agreed the DDL on three conditions, all met: §2.3 lists the table and open-reject raw rows, the retention rule has a pending test, and `raw_rowid` is nulled by the raw-delete trigger. |
| The open-ended unresolved-file gap (`epoch_open`) in the bead, the drill and the architecture (C5) | BEADS.md B22; PROOF.md §8; ARCHITECTURE.md §3.6; collector `b22-documents.contract.ts` (guard), `capture-gaps.contract.ts`; cloud `capture-coverage.contract.test.ts` | Review r1 agreed; the cloud tests now name their owning bead (B6, the cloud half of B22). |
| Token volume state and the G3 gate, including the `no_cache_columns` guard | DECISION-METRICS.md §0 (now in `docs/lean/`); ARCHITECTURE.md §3.1; cloud `token-volume.contract.test.ts` | Review r1 agreed; `DECISION-METRICS.md` is now carried beside the other documents. |
| The abort rebuild bound | MIGRATION.md §4 step 8; PROOF.md §6 item 7; collector `receipts-and-ladder.contract.ts` | Review r1 agreed; nothing open. |

## Not freezable yet (needs a review round or a measurement)

| Contract | Why not |
|---|---|
| **The S1b runway numbers** as a gate that may start a live hold | The row widths (`F`, `T`, `S`, `SD`, `DR`, `RB`, `SEG`, `I`) are still column-width estimates and only Studio4's cardinalities are measured (its segment proxy is not: the census was not re-run in round 8). The fixture shows the rule overstates the runway when the true widths exceed 1.25 × the estimates, so no hold may start on any host before S2 has measured the widths on the Studio5 copy and B1's post-catch-up census has run on that host's copy (review r6 `firstWave`: "no live hold or S2"). |
| The deletion proof and the cloud residue proof as a whole | Its blockers were stated as conditions on freezing the actor and runway contracts, which the deletion proof's step 1 depends on through the day receipt; freeze after C1 and C2 are signed. |
| Claim v2, the fault marker and restart rule, the paged id exchange | Specified, not implemented, no fixture beyond `sf5` and `b22`; review r6 called them future contracts (R6-15). A fixture-backed round is needed before freezing. |
| The per-row width constants and the 1.25 factor | Estimates until S2 (BUDGETS.md §6); the fixture's width-sensitivity check is the reason. |
| B18's fleet half | Gated on an accepted native owner receipt (unchanged since round 6). |

## What the lead signs

1. C1 and C4 (the actor predicate as the pair fixed at its first sighting, the null-stamp exception and the install scope): ready, after one independent read of the round-8 text.
2. C2: the formula (`G_owed`, one G) and the census obligation (after the catch-up, fresh, re-taken before S3, space-gated, segment proxy): ready; the widths, the 1.25 factor and every host's numbers stay open until S2 and the per-host census.
3. C3 (`conversion_rejects`), C5 (open gap), C8 (the `<UR>` pin), the day receipt, the day keys and `target_ref`, the volume gates, the abort bound: ready.

## First-wave conditions from review r1, and their state

- "Fix the pending-test helper before B10a relies on its tests": done (blocker 3, `helper.contract.ts` green); B10a's two hold tests now append their rows and fail only at `hold_reason`.
- "B1 implements the corrected G owed, not rule 3 as written": the corrected rule is C2 rule 3 (round 8) and `runway.contract.ts` test 3 binds it; B1's row in BEADS.md names it.
