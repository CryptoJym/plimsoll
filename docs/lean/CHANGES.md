# Round 7 (B0) changes: each blocker and `b0Carries` item, where it changed, and which test binds it

**Bead:** eco-6hoxj.164.4 (B0) · **As of:** 2026-09-25 MDT · Inputs: `input/plan-r6/` (round-6 plan), `input/review-r6/`
(VERDICT.json: 2 blockers, 5 should-fixes, 4 `b0Carries`, 3 `freezable`). Documents and tests only; no product code; read-only
toward every hosted service and live collector; no push. Every round-6 sentence that changed is in `checks/docs-diff-r6-r7.patch`
(65 changed lines across five documents), produced by the recorded, uniqueness-checked replacements in
`checks/round7_doc_edits.py`. `CONTRACTS.md` holds the normative text (C1-C7); `FREEZE.md` the freeze list.

## 1. Blockers and `b0Carries` → section → fixture → pending test

| Item (VERDICT.json) | Contract | Sections changed | Fixture (red r6 / green r7) | Pending tests (bead) |
|---|---|---|---|---|
| **Blocker 1** / `b0Carries[0]`: delivered and undelivered validators disagree for an issued-but-unheard stamp; `heard_at` must be the first echo | **C1** one predicate `actor_for_stamp(install, stamp)`: a stamped row belongs to `actor_of(install, V)` iff `0 ≤ V ≤ binding_version` at judgment, on both paths; never-issued stamps fail closed on both; the echo and the rowid order are diagnostics; `heard_at` = first echoing request | ARCHITECTURE.md §5.1, §5.2 (three paragraphs), §11 | `fixtures/b4_offline_rebind.py` rewritten: explicit contact timeline (`heard_at(1) = 650`, v2 unheard), eight orderings A-H, replay, invalid stamp, null-stamp exception; r6 14/16 (case D splits null/B; `heard_at(1) = 300`), r7 16/16 | cloud `actor-binding-stamp.contract.test.ts` (B6: 8 tests, the reviewer's counterexample first); collector `actor-stamp.contract.ts` (B2a: 4 tests) |
| **Blocker 2** / `b0Carries[1]`: S1b G from Studio1's density; 333 sessions/1,000 usage rows gives 507 MB against a 457 MB gate and 58.06 real days at the 63-day threshold | **C2** G per host from that host's own counted cardinalities (light hosts: B1's one-time census on a `VACUUM INTO` copy; busy hosts and Studio0: the copy's measured peak); no gate value for scaled or missing counts; `G_gate` in the numerator **and** the multiplier | BUDGETS.md header, §3 (dual-write row), §4.2 (two bullets), §4.3, §6; ARCHITECTURE.md §2.4 (three bullets); MIGRATION.md S1b (d); PROOF.md §4 fixtures row, §6 item 8; BEADS.md B1 | `fixtures/s1b_runway_host_bound.py` (new): Studio1, the Studio4 census, the reviewer's 333 host and round 6's scaled Studio4; r6 3/10 (457.3 < 509.9 MB; 63.00 claimed vs 57.98 real; multiplier G ≠ numerator G; G double-counted), r7 10/10 | collector `runway.contract.ts` (B1, B10a: 4 tests, the reviewer's counterexample and the Studio4 census included) |
| **Should-fix 5** (G remaining vs full G) | **C2 rule 3**: `G_remaining = max(0, G_gate − new_table_bytes_now)`; `runway.g = {gate, remaining, basis}` in `/status` | BUDGETS.md §4.2, §4.3; ARCHITECTURE.md §2.4; PROOF.md §6 item 8 | `s1b_runway_host_bound.py` checks 8-10 (mid-conversion runway equals the true arithmetic; the < 5-day rung fires only when true; 0 after S3) | collector `runway.contract.ts` test 3 |
| `b0Carries[2]` (a): durable `conversion_rejects` DDL | **C3**: identity `(event_id, raw_generation)`, live `raw_rowid`, `reason` enum, value-blind `detail`, digest of the stored string, `gap_id` → the counted gap, seen/attempt counters, `resolved_at_ms`; never deleted by age or pressure | ARCHITECTURE.md §3.5; MIGRATION.md S3 certify (f) | `fixtures/b5_non_iso_day_facts.py` (unchanged; the reject list by id) | collector `schema.contract.ts` (B2a, DDL) and `converter.contract.ts` (B2a: the NaN string is one counted gap plus one reject; nothing vanishes) |
| `b0Carries[2]` (b) / should-fix 2: null stamps on a B2a collector before its first response | **C4**: null until the first authenticated response persisted (registration included where the route exists); the C1 exception is "stamp = null on a rebound install", whichever collector wrote it; `/status` shows `nullStampedRows` | ARCHITECTURE.md §5.2 (stamp bullet and the exception paragraph) | `b4_offline_rebind.py` cases H and the exception check | collector `actor-stamp.contract.ts` test 1 (B2a); cloud `actor-binding-stamp.contract.test.ts` test 4 (B6) |
| `b0Carries[3]` / should-fix 4: port `b22_false_complete.py` with repository-relative paths; behavioural half beside sf5 | **C5** | PROOF.md §8 item 1 (names both halves) | `fixtures/b22_false_complete.py`: `--docs DIR`, else the packet layout, else `docs/lean/` at the repository root; red against the round-5 stub prescriptions in `checks/b22-r5-docs/`, green against `out/contracts` | collector `b22-documents.contract.ts` (guard, green today, reads `docs/lean/`), `capture-gaps.contract.ts` (B22); cloud `capture-coverage.contract.test.ts` (B6/B22) |
| Should-fix 1 (fixture timeline) | in C1 | ARCHITECTURE.md §5.2 | `b4_offline_rebind.py` timeline checks (r6 red) | cloud test 5 (`firstEchoHeardAt`) |
| Should-fix 3 (should-fix list item 3: `conversion_rejects` DDL) | C3 (above) | | | |

## 2. Every fixture ported as a pending test (the round-5 and round-6 set)

| Fixture | Owning repository and test | Bead |
|---|---|---|
| `b1_membership_edges.py` | collector `membership.contract.ts` test 1, `schema.contract.ts` | B2a |
| `b2_epoch_day_keys.py` | collector `schema.contract.ts` (epoch_key first in the day tables' keys) | B2a |
| `b3_durable_target_refs.py` | collector `schema.contract.ts` (autoincrement, foreign keys), `membership.contract.ts` test 2 (retirement proof, no reuse) | B2a |
| `b4_actor_at_capture.py` | cloud `actor-binding-stamp.contract.test.ts` test 8 (parts, exports, digest sum) | B6 |
| `b5_lexical_boundary.py` | collector `day-key.contract.ts` (census classes, UTC-midnight window, schema version 3) | B5, B9a |
| `b6_unknown_volume_gates.py`, `sf_no_cache_columns_guard.py` | cloud `token-volume.contract.test.ts` | B15 |
| `sf1_ladder_rollback_parity.py` | collector `receipts-and-ladder.contract.ts` test 2 | B10a, B10b |
| `sf5_unresolved_file_open_gap.py`, `b22_false_complete.py` (behavioural) | collector `capture-gaps.contract.ts`; cloud `capture-coverage.contract.test.ts` | B22, B6 |
| `b1_day_target_receipt.py` | cloud `activity-summary.contract.test.ts` (item kinds, judge, receipts); collector `receipts-and-ladder.contract.ts` test 1 (acks only through receipts) | B6, B2a |
| `b4_offline_rebind.py` (round 7) | cloud `actor-binding-stamp.contract.test.ts`; collector `actor-stamp.contract.ts` | B6, B2a |
| `b5_non_iso_day_facts.py` | collector `converter.contract.ts`, `day-key.contract.ts` | B2a, B5 |
| `s1b_runway_geometry.py`, `s1b_runway_host_bound.py` (round 7) | collector `runway.contract.ts` | B1, B10a |
| `sf_abort_rebuild_bound.py` | collector `receipts-and-ladder.contract.ts` test 3 | B13 |
| Reviewer counterexample: actor (issued-but-unheard) | cloud `actor-binding-stamp.contract.test.ts` test 1 | B6 |
| Reviewer counterexample: runway (333 sessions / 1,000) | collector `runway.contract.ts` test 1 and 2 | B1, B10a |
| Hold red/green (PROOF.md §5 item 1) | collector `retention-hold.contract.ts` | B10a |
| Schema additions on the cloud (binding version, heard_at, parts, cloud gaps, lane closed) | cloud `schema-additions.contract.test.ts` | B6 |

## 3. How the tests are pending, and what was run

`node:test` `todo` (`pending("<bead>")` in each repository's `tests/contracts/lean/_pending.ts`): the tests run, print their
failure and count under `# todo`; the process exits 0. Cloud: `pnpm test:contracts:lean` (`tests/contracts/lean/*.contract.test.ts`,
outside the `tests/*.test.ts` glob of `pnpm test`; typechecked and linted, both green); a CI step after `Test` in `ci.yml`.
Collector: `pnpm contracts:lean` (`tests/contracts/lean/*.contract.ts`, outside `tsconfig` `include` and not matching the
proof-file rule of `scripts/ci-coverage-proof.ts`, which still passes); a CI step after `Typecheck` in `proof.yml`; run under
Node 22 as in CI. Counts: cloud 17 tests, all pending and red; collector 30 tests, 28 pending and red, 2 guards green (the b22 documents and the
foreign-keys pragma). Every pending test fails today for "contract surface missing: <path>" or an assertion on today's behaviour
(`checks/cloud-contracts.log`, `checks/collector-contracts.log`).

## 4. Kept unchanged

The round-6 decisions the review verified (day receipt, non-ISO fold, open gap, 0.7.42 target, usage tombstones, abort bound,
finite corpus) and the round-5 first wave and start order. `DECISION-METRICS.md` is copied unchanged. The one question for
James (30 or 90 days of local history) stands.

## 5. Assumptions B0 made on the lead's behalf (decide ambiguous points, report them)

- **C1 resolves the issued-but-unheard stamp to the historical actor, not to unallocated.** The reviewer allowed either;
  the stamp is the collector's attestation that it had persisted the version, the version exists in the cloud's own audit
  table, and the alternative (null on the raw row) is permanent and unrepairable. The echo became a diagnostic so the
  predicate has no timing input. If the lead prefers fail-closed, the change is one branch in `actorForStamp` and one
  expectation in each of the two actor tests.
- **Measured Studio4 on a copy.** The task allowed "measure the session/day/segment mix on a copy of each host"; only this
  host's ledger was reachable, so the census ran here (read-only `VACUUM INTO`, 2 s; the copy was deleted afterwards). The
  other hosts' counts stay estimates; the rule requires B1 to count them.
- **Pending = `todo`**, not a skipped or excluded suite, so the failures stay visible in every CI run without blocking.
- **Contract documents ride the collector branch** under `docs/lean/` (the b22 document test needs repository-relative
  paths); the cloud branch carries a README that points at them. No product path was touched in either repository.
- **Finding:** better-sqlite3 enforces foreign keys by default, so the collector's ledger connection already runs with
  `PRAGMA foreign_keys = 1` at `03445d3a`; the corresponding test is a green guard, not a pending test, and CONTRACTS.md C6
  says B2a must keep it on rather than set it.
- The task said "plimsoll (main 03445d3a)" without shipping a bundle; the commit was fetched read-only from
  `https://github.com/CryptoJym/plimsoll.git` after no local clone held it (`checks/source-pins.log`).
