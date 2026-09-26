# Freeze list after B0 round 4 (round 10, eco-6hoxj.164.4)

**As of:** 2026-09-25 MDT. Start point: the round-9 `FREEZE.md`, the lead's signatures (the eight round-9 "freezable now" items, and
**C4**), and the independent read of C1/C4 after round 9 (`input/read-c1c4/VERDICT.json`: **C4 passes; C1 NOT_YET** with two blocking
items and ten should-fixes). A contract is **freezable** when its rule is stated once, its fixture is green under the current rule, its
pending test exists in the owning repository, and no open review finding touches it. **Freezing is the lead's signature**, not B0's:
this list says what is signed, what is ready for that signature and what still needs a review round. Nothing is frozen by this
document.

## Signed by the lead (recorded here; nothing in this round changes their text)

| Contract | Where |
|---|---|
| The pinned `<UR>` usage-record predicate and the finance/reconciliation identity fields as test inputs, with the lane-2 re-pin on merge (C8) | CONTRACTS.md C8; ARCHITECTURE.md §3.1, §10; `fixtures/usage_record_predicate.sql` in both repositories; cloud and collector `usage-record-pin` tests |
| Epoch-scoped day keys and the autoincrement `target_ref` | ARCHITECTURE.md §2.2, §3.3, §3.5; collector `schema.contract.ts`, `membership.contract.ts` |
| The `day_summary` item and its receipt shape, and the collector's acknowledgement-only-through-receipts rule | ARCHITECTURE.md §5.2; cloud `activity-summary.contract.test.ts`; collector `receipts-and-ladder.contract.ts` |
| The S1b runway formula (C2 rules 2-3) and the census obligation (rule 1), with `C_conv` as a measurement; **the numbers stay open** (below) | CONTRACTS.md C2; ARCHITECTURE.md §2.4; BUDGETS.md §4.2-4.3; MIGRATION.md S1b, S3; `fixtures/s1b_runway_host_bound.py` (r9 25/25); collector `runway.contract.ts` |
| `conversion_rejects` DDL and retention (C3) | CONTRACTS.md C3; ARCHITECTURE.md §2.2, §2.3, §3.5; collector `schema.contract.ts`, `converter.contract.ts`, `conversion-rejects.contract.ts` |
| The open-ended unresolved-file gap (`epoch_open`) in the bead, the drill and the architecture (C5) | BEADS.md B22; PROOF.md §8; ARCHITECTURE.md §3.6; collector `b22-documents.contract.ts` (guard), `capture-gaps.contract.ts`; cloud `capture-coverage.contract.test.ts` |
| Token volume state and the G3 gate, including the `no_cache_columns` guard | DECISION-METRICS.md §0; ARCHITECTURE.md §3.1; cloud `token-volume.contract.test.ts` |
| The abort rebuild bound | MIGRATION.md §4 step 8; PROOF.md §6 item 7; collector `receipts-and-ladder.contract.ts` |
| **The joined-install scope of the pair and the null stamp before the first response (C4)** | CONTRACTS.md C4; ARCHITECTURE.md §5.2; `fixtures/b4_offline_rebind.py` case L; collector `actor-stamp.contract.ts`; cloud `actor-binding-stamp.contract.test.ts`. The read of C1/C4 passed it ("the joined-install rule is sound: a late answer from the old install is ignored and counted; no non-joined response can change the pair or create a fact; an honest re-join is never blocked"). Round 10 adds only what its should-fixes asked, as text and tests that narrow nothing the lead signed: the handshake's version is recorded only when the response names the grant's install, the pre-B2a seed has a test, the partial join is named in `/status`, and the join test runs through `join.ts` activation. |

## Ready after one more independent read of the round-10 text

| Contract | Where | State |
|---|---|---|
| **The actor-ownership predicate (C1) with the null-stamp exception** | CONTRACTS.md C1 (and the C4 sentences round 10 added, C6); ARCHITECTURE.md §5.1-5.2, §7; `fixtures/b4_offline_rebind.py` (r10 51/51; red under r6, r7, r8 and r9); cloud `actor-binding-stamp.contract.test.ts` (13 cases), `actor-binding-stamp-postgres.contract.test.ts` (7 cases), `schema-additions.contract.test.ts` (5 cases); collector `actor-stamp.contract.ts` (11 cases) | The read said **not yet** and named two blocking items, both closed in this round with a fixture check red under round 9 and a pending test: (1) a sighting takes the lock **first** and reads `binding_version`, the audit rows and the facts under it, and C6's ingest surface takes the view `loadSightingView` returns in the judging transaction; (2) the fact is keyed by the **ledger** (the chain of installs of one Mac, linked at join with proof of possession of the previous install's key), so the old and the new install of a re-joined Mac read one fact while another Mac never enters it. Every should-fix is done (`CHANGES.md`). The design the read accepted (the pair, the sticky first-sighting fact, `FOR SHARE` against the rebind, `ON CONFLICT DO NOTHING` with a re-read, the cap, the null-stamp exception, retention until erasure) is unchanged. **Not listed as ready outright**, exactly as the read asked ("then one more read of those two changes"): the lock order and the fact's key are new text no reviewer has read; one independent read of C1 round 10 is the remaining condition for signature. |

## Not freezable yet (needs a review round or a measurement)

| Contract | Why not |
|---|---|
| **The S1b runway numbers** as a gate that may start a live hold | The row widths (`F`, `T`, `S`, `SD`, `DR`, `RB`, `SEG`, `I`) are still column-width estimates and only Studio4's cardinalities are measured (its segment proxy is not: the census was not re-run in rounds 8-10). The fixture shows the rule overstates the runway when the true widths exceed 1.25 × the estimates (and does not self-correct), so no hold may start on any host before S2 has measured the widths on the Studio5 copy and B1's post-catch-up census has run on that host's copy (review r6 `firstWave`: "no live hold or S2"). |
| The deletion proof and the cloud residue proof as a whole | Its blockers were stated as conditions on freezing the actor and runway contracts, which the deletion proof's step 1 depends on through the day receipt; freeze after C1 is signed. |
| Claim v2, the fault marker and restart rule, the paged id exchange | Specified, not implemented, no fixture beyond `sf5` and `b22`; review r6 called them future contracts (R6-15). A fixture-backed round is needed before freezing. |
| The per-row width constants and the 1.25 factor | Estimates until S2 (BUDGETS.md §6); the fixture's width-sensitivity check is the reason. |
| B18's fleet half | Gated on an accepted native owner receipt (unchanged since round 6). |

## What the lead signs next

1. C1 (the actor predicate as the pair fixed at its first sighting, the sighting locking first and reading everything under the lock,
   the fact keyed by the ledger, lifecycle-independent resolution, the erasure-only guard; the null-stamp exception): ready **after one
   independent read of the round-10 text** (C1, the C4 sentences round 10 added, C6's cloud surfaces).
2. Nothing else changes hands in this round: C4 and the eight round-9 items are signed; the runway numbers and the rest stay open as above.

## First-wave conditions from reviews r1 and r2, and their state

- Review r2: "the first wave can continue"; B10a's hold tests append and fail only at `hold_reason`; B1 builds `G_owed` as in round 8 (the formula is unchanged since) and reads `C_conv` from the converter's measured checkpoint.
- Review r1: "fix the pending-test helper before B10a relies on its tests": done in round 8 and completed in round 9; round 10 adds one green guard (join.ts activation with a fake cloud) for the new collector test's premise.
- The read of C1/C4: "neither [Postgres-proof item] blocks signing" but both must be fixed "before B6 removes the markers": done in round 10 (`checks/postgres-harness-repaired-probe.log`); B6 inherits a harness that runs.
