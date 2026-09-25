/**
 * B2a / B10a / B10b / B13 (collector): acknowledgement only through receipts, the ladder's acknowledged-only release with
 * qualified rollback parity, and the abort rebuild bound (docs/lean/ARCHITECTURE.md §2.4, §5.2; MIGRATION.md §4, §8;
 * PROOF.md §5 items 2-2b, §6 item 7). Ports the collector halves of fixtures/b1_day_target_receipt.py,
 * sf1_ladder_rollback_parity.py and sf_abort_rebuild_bound.py. Pending until the named beads land lean/retention.ts and lean/rebuild.ts.
 */
import assert from "node:assert/strict";
import test from "node:test";

import { event, fn, loadSurface, openTempBuffer, pending, sumDigest } from "./_pending";

test("B2a: a day target's ack fields are set only by a held or duplicate receipt with a matching digest; conflict and stale change nothing", pending("B2a"), async () => {
  const apply = fn(await loadSurface("../../../packages/collector-cli/src/lean/retention.ts"), "applyUploadReceipts") as (db: unknown, receipts: unknown[]) => number[];
  const { buffer, close } = openTempBuffer({ workspaceId: "tenant-lean-contract", lean: { write: true } });
  try {
    const db = buffer.database;
    const digest = sumDigest(["u1", "u2", "a1", "a2"]);
    db.prepare("insert into summary_segments (target_kind, base_key, state, member_count, member_sum_digest, revision, sealed_revision, digest, upload_state) values ('day', 'epoch-1|codex|2026-09-25', 'sealed', 4, ?, 2, 2, 'd', 'pending')").run(digest);
    const ref = (db.prepare("select target_ref as ref from summary_segments").get() as { ref: number }).ref;
    const receipt = (status: string, memberSumDigest = digest, revision = 2) => ({ kind: "day_summary", key: "epoch-1|codex|2026-09-25", revision, digest: "d", status, coverage: "equal", segments: [{ seq: 1, sealed: true, memberSumDigest, status }] });
    const acked = () => db.prepare("select acked_revision as rev, acked_member_sum_digest as d from summary_segments where target_ref = ?").get(ref) as { rev: number | null; d: string | null };
    apply(db, [receipt("conflict", "f".repeat(32))]);
    assert.deepEqual(acked(), { rev: null, d: null }, "a conflict receipt acknowledges nothing");
    apply(db, [receipt("stale", digest, 1)]);
    assert.deepEqual(acked(), { rev: null, d: null }, "a stale receipt changes nothing");
    assert.deepEqual(apply(db, [receipt("held")]), [ref]);
    assert.deepEqual(acked(), { rev: 2, d: digest });
    apply(db, [receipt("duplicate")]);
    assert.deepEqual(acked(), { rev: 2, d: digest }, "a duplicate is idempotent");
  } finally { close(); }
});

test("B10a/B10b: the ladder's release deletes only acknowledged rows older than retentionDays, with receipts, and the old path after rollback equals the pre-release path minus the receipted set", pending("B10b"), async () => {
  const release = fn(await loadSurface("../../../packages/collector-cli/src/lean/retention.ts"), "releaseUnderLadder") as (buffer: unknown, options: Record<string, unknown>) => { released: string[] };
  const { buffer, close } = openTempBuffer({ workspaceId: "tenant-lean-contract", delivery: { enabled: true } });
  try {
    const db = buffer.database;
    const oldAcked1 = event({ observedAt: "2026-01-01T00:00:00.000Z" }), oldAcked2 = event({ observedAt: "2026-01-01T00:00:00.000Z" });
    const oldPending = event({ observedAt: "2026-01-01T00:00:00.000Z" }), recent = event();
    for (const e of [oldAcked1, oldAcked2, oldPending, recent]) buffer.append(e);
    db.prepare("update buffered_events set created_at = '2026-01-01T00:00:00.000Z' where id in (?, ?, ?)").run(oldAcked1.id, oldAcked2.id, oldPending.id);
    const lease = buffer.delivery.lease({ leaseId: "lean-contract-lease", now: new Date() });
    assert.equal(lease.items.length, 4);
    buffer.delivery.acknowledge(lease.leaseId, [oldAcked1.id, oldAcked2.id, recent.id], new Date());
    const before = (db.prepare("select count(*) as n from buffered_events").get() as { n: number }).n;
    const result = release(buffer, { retentionDays: 90, now: new Date("2026-09-25T00:00:00.000Z") });
    assert.deepEqual(result.released.sort(), [oldAcked1.id, oldAcked2.id].sort(), "only acknowledged rows older than retentionDays");
    const receipts = (db.prepare("select count(*) as n from raw_retention_receipts where reason = 'lean_hold_release_acked'").get() as { n: number }).n;
    const after = (db.prepare("select count(*) as n from buffered_events").get() as { n: number }).n;
    assert.equal(after, before - receipts, "old path after rollback == pre-release old path minus the receipted release set");
    assert.equal((db.prepare("select count(*) as n from buffered_events where id = ?").get(oldPending.id) as { n: number }).n, 1, "the pending row stays");
  } finally { close(); }
});

test("B13: the abort rebuild bound is pre-S2 live bytes plus the raw capture since S2, tight enough to catch new tables left in place", pending("B13"), async () => {
  const bound = fn(await loadSurface("../../../packages/collector-cli/src/lean/rebuild.ts"), "abortRebuildBound") as (input: Record<string, number>) => number;
  const L0 = 1_342_111_744, PER_RAW = 2_262, ROWS = 6 * 20_000, G = 365_900_000;
  const b = bound({ preS2LiveBytes: L0, rawRowsSinceS2: ROWS, bytesPerRawRow: PER_RAW });
  assert.equal(b, L0 + ROWS * PER_RAW);
  assert.ok(L0 + ROWS * PER_RAW <= b, "a correct abort passes");
  assert.ok(L0 + ROWS * PER_RAW + G > b, "an abort that left the new tables fails");
  assert.ok(b - (L0 + ROWS * PER_RAW) < 0.01 * L0, "not loose");
});
