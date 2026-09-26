# Freeze list after B0 round 6 (round 12, eco-6hoxj.164.4)

**As of:** 2026-09-26 MDT. Start point: the round-11 `FREEZE.md`, the lead's signatures (the eight round-9 "freezable now" items, and
**C4**), and the independent read of C1 after round 11 (`input/read-c1-r11/VERDICT.json`: **NOT_YET**, the two round-11 fixes
confirmed with zero crossings in a 576-tuple enumeration and every singleton link attack refused, one new blocking item and one
should-fix). A contract is **freezable** when its rule is stated once, its fixture is
green under the current rule, its pending test exists in the owning repository, and no open review finding touches it. **Freezing is
the lead's signature**, not B0's: this list says what is signed, what is ready for that signature and what still needs a review
round. Nothing is frozen by this document.

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
| **The joined-install scope of the pair and the null stamp before the first response (C4)** | CONTRACTS.md C4; ARCHITECTURE.md §5.2; `fixtures/b4_offline_rebind.py` case L; collector `actor-stamp.contract.ts`; cloud `actor-binding-stamp.contract.test.ts`. The read of C1/C4 passed it. Round 10 added only what its should-fixes asked. **Round 11 adds only a cross-reference** (a pre-re-join row delivered through a ledger the join could not link is held by C1 until the ledger is linked) **and one additive `/status` field** (`parkedRows`); the collector's promise the lead signed, keep the pair on rows already in the outbox at a re-join, is unchanged, and the judgment of such rows was always C1's. If the lead reads that cross-reference as a change to C4, the alternative is to strike it from C4 and let C1 alone carry the hold; every test binds C1. **Round 12 changes nothing in C4** (the read confirmed its round-11 changes as additive). |

## Ready after one more independent read of the round-12 text

| Contract | Where | State |
|---|---|---|
| **The actor-ownership predicate (C1) with the null-stamp exception** | CONTRACTS.md C1 (guarantee 5, "Serialization" and "Scope" round 11; "The link is the release" round 12; the C4 sentence and `/status` field round 11 added; C6); ARCHITECTURE.md §5.2, §11; MIGRATION.md; `fixtures/b4_offline_rebind.py` (r12 69/69; red under r6, r7, r8, r9, r10 and r11); cloud `actor-binding-stamp.contract.test.ts` (15 cases), `actor-binding-stamp-postgres.contract.test.ts` (9 cases, with `fixtures/join-route-child.ts`), `schema-additions.contract.test.ts` (5 cases); collector `actor-stamp.contract.ts` (12 cases) | The read of the round-11 text confirmed both round-11 fixes (S8 and the single-install S9: the r10 fixture 54/60, r11 60/60, a 576-tuple enumeration with zero crossings, every singleton link attack refused) and said **not yet** for one item: the admin link moved one install but re-keyed every fact of its ledger, so a proof-linked child of an unlinked root was left behind and its poisoned pair flipped (the read's `CHAIN_COUNTEREXAMPLE`), and the link checked nothing about its destination. Closed in this round with a fixture check red under round 11 and a pending test: **the link moves a ledger, never an install**, into the root of its destination in the same tenant, in one transaction under one set of locks with one audit row; the join's lineage step holds the previous install's row so a join and a merge serialize; the response names the ledger so a merged chain's member retries its parked rows. Both counterexamples were reproduced as written in round 11 and shown closed under round 12 on PostgreSQL 17.11, with the merge against a join in both orders (`checks/round12-sql-orderings.log`). The should-fix is done (`CHANGES.md`). The design the three reads accepted (the pair, the sticky first-sighting fact, lock then read, the ledger key with the join's proof, `ON CONFLICT DO NOTHING` with a re-read, the cap, the refusal across ledgers with the hold, the null-stamp exception, lifecycle-independent resolution, the erasure-only guard, retention until erasure) is unchanged. **Not listed as ready outright**: the whole-ledger merge, its destination rule, the join's lock and the ledger on the response are new text no reviewer has read; one independent read of C1 round 12 is the remaining condition for signature. |

## Not freezable yet (needs a review round or a measurement)

| Contract | Why not |
|---|---|
| **The S1b runway numbers** as a gate that may start a live hold | The row widths (`F`, `T`, `S`, `SD`, `DR`, `RB`, `SEG`, `I`) are still column-width estimates and only Studio4's cardinalities are measured (its segment proxy is not: the census was not re-run in rounds 8-12). The fixture shows the rule overstates the runway when the true widths exceed 1.25 × the estimates (and does not self-correct), so no hold may start on any host before S2 has measured the widths on the Studio5 copy and B1's post-catch-up census has run on that host's copy (review r6 `firstWave`: "no live hold or S2"). |
| The deletion proof and the cloud residue proof as a whole | Its blockers were stated as conditions on freezing the actor and runway contracts, which the deletion proof's step 1 depends on through the day receipt; freeze after C1 is signed. |
| Claim v2, the fault marker and restart rule, the paged id exchange | Specified, not implemented, no fixture beyond `sf5` and `b22`; review r6 called them future contracts (R6-15). A fixture-backed round is needed before freezing. |
| The per-row width constants and the 1.25 factor | Estimates until S2 (BUDGETS.md §6); the fixture's width-sensitivity check is the reason. |
| B18's fleet half | Gated on an accepted native owner receipt (unchanged since round 6). |

## What the lead signs next

1. C1 (the actor predicate as the pair fixed at its first sighting, the sighting locking first and reading everything under the lock,
   the fact keyed by the ledger, ownership never crossing a ledger with the hold and the whole-ledger link as its release,
   lifecycle-independent resolution, the erasure-only guard; the null-stamp exception): ready **after one independent read of the
   round-12 text** (C1 "The link is the release" round 12, the round-11 text the read confirmed, C6's surfaces).
2. Nothing else changes hands in this round: C4 and the eight round-9 items are signed; the runway numbers and the rest stay open as above.

## First-wave conditions from reviews r1 and r2 and the two reads of C1, and their state

- Review r2: "the first wave can continue"; B10a's hold tests append and fail only at `hold_reason`; B1 builds `G_owed` as in round 8 (the formula is unchanged since) and reads `C_conv` from the converter's measured checkpoint.
- Review r1: "fix the pending-test helper before B10a relies on its tests": done in round 8 and completed in round 9; round 10 added one green guard (join.ts activation with a fake cloud) for the new collector test's premise.
- The read of C1/C4: "neither [Postgres-proof item] blocks signing" but both must be fixed "before B6 removes the markers": done in round 10 (`checks/postgres-harness-repaired-probe.log`).
- The read of C1 after round 10: its two blocking items were closed in round 11 and confirmed by the read of C1 after round 11; its should-fixes are done: BEADS.md's B6 row says the fact is keyed by the ledger (it said "uploader"), `cluster()` stops its cluster on any set-up failure, the join route's token binding has integration cases through a child process (proved runnable against today's route, `checks/join-route-child-probe.log`), and "another Mac never enters the ledger" is stated with its condition, that the previous install's key stays secret, together with the first-sighting guarantee per ledger.
- The read of C1 after round 11: its one blocking item is closed above (the link moves a ledger, never an install, into a canonical destination; the join's lock; the ledger on the response); its should-fix is done: the Postgres proof seeds P server-bound to B, as the S8 ordering has it.
