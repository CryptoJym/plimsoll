# Freeze list after B0 (round 7, eco-6hoxj.164.4)

**As of:** 2026-09-25 MDT. Start point: `input/review-r6/VERDICT.json` `freezable` and BEADS.md §3 (round 6). A contract is
**freezable** when its rule is stated once, its fixture is green under the current rule, its pending test exists in the owning
repository, and no open review finding touches it. **Freezing is the lead's signature**, not B0's: this list says what is ready
for that signature and what still needs a review round. Nothing is frozen by this document.

## Freezable now (ready for the lead's signature)

| Contract | Where | Why it is ready |
|---|---|---|
| The pinned `<UR>` usage-record predicate and the finance/reconciliation identity fields as test inputs, with the lane-2 re-pin on merge | ARCHITECTURE.md §3.1, §10; cloud `loader.ts:72-79` at `4954995` (inline text unchanged since `067a8a4`, re-read this round) | Carried from round 6; unchanged. The re-pin obligation stays with B0's successor when lane 2 exports `USAGE_RECORD_PREDICATE_SQL`. |
| Epoch-scoped day keys and the autoincrement `target_ref` | ARCHITECTURE.md §2.2, §3.3, §3.5; collector `schema.contract.ts`, `membership.contract.ts` | Carried from round 6; the round-5 fixtures are still green; the DDL is now bound by a pending test. |
| The `day_summary` item and its receipt shape, and the collector's acknowledgement-only-through-receipts rule | ARCHITECTURE.md §5.2; cloud `activity-summary.contract.test.ts`; collector `receipts-and-ladder.contract.ts` | Round 6 allowed it as a provisional B0 fixture; nothing in review r6 touched it; both halves now have pending tests. |
| **The actor-ownership predicate (C1)** with the null-stamp exception (C4) and the corrected `heard_at` semantics | CONTRACTS.md C1, C4; ARCHITECTURE.md §5.1-5.2; `fixtures/b4_offline_rebind.py`; cloud `actor-binding-stamp.contract.test.ts`; collector `actor-stamp.contract.ts` | Review r6 blocker 1 is closed by one function that both paths call; the issued-but-unheard, invalid, pre-first-response and offline-rebind cases are covered; the reviewer's counterexample is a test. **Recommended for signature after one independent read of C1**, because it reverses a round-6 decision (the echo is no longer an ownership input). |
| `conversion_rejects` DDL and retention (C3) | CONTRACTS.md C3; ARCHITECTURE.md §3.5; collector `schema.contract.ts`, `converter.contract.ts` | Specified with identity, reason, gap link and retention; no open finding. |
| The open-ended unresolved-file gap (`epoch_open`) in the bead, the drill and the architecture (C5) | BEADS.md B22; PROOF.md §8; ARCHITECTURE.md §3.6; collector `b22-documents.contract.ts` (guard), `capture-gaps.contract.ts`; cloud `capture-coverage.contract.test.ts` | Round 6 closed the rule; B0 ported both halves with repository-relative paths. |
| Token volume state and the G3 gate, including the `no_cache_columns` guard | DECISION-METRICS.md §0; ARCHITECTURE.md §3.1; cloud `token-volume.contract.test.ts` | Round-5 and round-6 fixtures green; no open finding. |
| The abort rebuild bound | MIGRATION.md §4 step 8; PROOF.md §6 item 7; collector `receipts-and-ladder.contract.ts` | Round-6 should-fix closed; no open finding. |

## Not freezable yet (needs a review round or a measurement)

| Contract | Why not |
|---|---|
| **The S1b runway rule (C2)** as a **gate that may start a live hold** | The rule is stated and tested, and the Studio4 census proves the mix differs per host, but the row widths (`F`, `T`, `S`, `SD`, `DR`, `RB`, `SEG`, `I`) are still column-width estimates and only Studio4's cardinalities are measured. Freeze the **formula and the census obligation** after the reviewer reads C2; keep the **numbers** open until S2 measures the widths on the Studio5 copy and B1's census has run on each light host's copy. No hold may start on any host before then (review r6 `firstWave`: "no live hold or S2"). |
| The deletion proof and the cloud residue proof as a whole | Untouched by review r6, but its blockers ("no actor or runway sign-off") were stated as conditions on freezing the actor and runway contracts, which the deletion proof's step 1 depends on through the day receipt; freeze after C1 and C2 are signed. |
| Claim v2, the fault marker and restart rule, the paged id exchange | Specified, not implemented, no fixture beyond `sf5` and `b22`; review r6 called them future contracts (R6-15). A fixture-backed round is needed before freezing. |
| The per-row width constants and the 1.25 factor | Estimates until S2 (BUDGETS.md §6). |
| B18's fleet half | Gated on an accepted native owner receipt (unchanged since round 6). |

## What the lead signs

1. C1 and C4 (actor predicate and null stamps): ready.
2. C3 (`conversion_rejects`), C5 (open gap port), the day receipt, the day keys and `target_ref`, the volume gates, the abort bound: ready.
3. C2: sign the formula and the census obligation; leave the widths and the 1.25 factor open until S2 measures them.
