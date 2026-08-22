# SINGLE-SHOT REWORK — no follow-up messages will arrive; finish the ENTIRE job before ending your turn.

You are back in your p163 sandbox (plimsoll #163, enrollment privacy quarantine). The audit found NO leak in your delivered code across four probe routes — the quarantine EFFECT is right. But the proof layer cannot see a leak, two proofs regressed, and the receipts lie. Fix EXACTLY the gaps below.

## Gap 1 (P0) — the adversarial proof does not bind the production row shape
Measured: a planted first-join relabel (and a planted outbox release that UPLOADED two pre-enrollment events over the wire) left proof:enrollment-privacy, proof:join-isolation, and proof:outbox ALL green. Cause: your seedUnmanagedHistory fixture opens the buffer WITHOUT workspaceId, producing NULL rows production never writes — cli.ts:283 openBuffer always passes workspaceId: config.tenantId.
FIX: seed the first-join, failed-handshake-rollback, and rejoin scenarios exactly the production way (workspaceId: config.tenantId → LOCAL tenant rows), keeping the unbound fixture as an ADDITIONAL case. Then add two red-team fixtures that reproduce the audit's plants and MUST FAIL if the defect exists: (a) first-join-from-LOCAL relabels prior LOCAL rows into the joined workspace → proof red; (b) outbox releases pre-enrollment rows to the new workspace (assert on upload bodies/marked counts, not row labels alone) → proof red. Prove both bite by temporarily planting each defect, capturing the red output into REWORK-NOTES.md, then reverting the plant.

## Gap 2 (P0) — two reproducible proof regressions, root-caused to your deleted backfill
proof (signal-fidelity) exit 0→1 (4/106 red incl. upload_watermark_drains before:23 firstMarked:0) and proof:privacy-mode 0→1 (5/15 red), 2-3/3 reproductions each. Cause: upload.ts:318 useWorkspace(config.tenantId) no longer backfills workspace_id IS NULL rows, so any ledger with pre-workspace rows (incl. every pre-migration install upgraded via migrateEventColumns) permanently loses normal upload of that history.
DESIGN RULING (directive — implement exactly this): restore adoption of NULL rows ONLY when the workspace being bound is the unmanaged default LOCAL tenant (LOCAL_TENANT_ID). Same local owner, no enrollment involved — legacy upload behavior returns. Binding ANY managed/joined workspace must NEVER adopt NULL rows — that is the exact leak class #163 quarantines. Add a fixture proving each direction: LOCAL bind adopts (upload drains again); managed bind does not (rows stay withheld).
ACCEPTANCE: proof and proof:privacy-mode BOTH exit 0 at HEAD again; proof:enrollment-privacy and proof:join-isolation stay green.

## Gap 3 (P1) — the receipt lies in the only shape production produces
doctor reported quarantinedHistoryRows: 0 against a joined ledger holding 3 withheld events + 3 withheld outbox rows. enrollmentStatus (buffer.ts:676) and readonlyQuarantinedHistoryRows count only workspace_id IS NULL.
FIX: count rows withheld from the CURRENT workspace (rows bound to a different workspace or NULL), payload-free as now. Fixture: the audit's exact shape (3+3 withheld) must report 6 (or 3 events + 3 outbox reported distinctly — pick one, document it in the receipt shape).

## Gap 4 (P1) — the in-join guard is weaker than its comment claims
join.ts claims "a regression that reattaches history fails loudly here" but compares NULL-counts only; the audit's Plant C/D passed through it. Make the guard compare the set of rows bound to the target workspace before vs after join (count by workspace, not by NULL), so a relabel OR an outbox release trips it.

## Gap 5 (P1) — no pre-existing proof still covers the unbound/legacy producer path
Your fixture reshaping made insertLegacyPoison's workspaceId mandatory and pre-bound ~14 fixtures. Restore at least one unbound-row case in outbox-proof and learning-facts-proof (the rollback-compatible raw producer the helper's own comment describes), asserting the current withholding semantics.

## Gap 6 (P2) — latent bypass: null workspaceId disables the quarantine predicate
listUnuploaded (buffer.ts:2435) and the outbox claim (outbox.ts:945) drop the workspace predicate entirely when this.workspaceId is null — an unbound reader saw all quarantined rows. No production caller today; make it fail closed anyway (unbound readers see ONLY NULL rows, never bound rows), with a small fixture.

## Rules
- Run at the end, and paste outputs + exit codes into REWORK-NOTES.md: proof, proof:privacy-mode, proof:enrollment-privacy, proof:join-isolation, proof:outbox, proof:learning-facts, npx tsc --noEmit.
- Do NOT weaken or delete any existing assertion. Do NOT touch the live collector home or any config outside this clone. No push, no gh.
- Commit in logical chunks. Create P163R.DONE when complete. State battery results ONLY from runs made in this session.
