/**
 * B2a / B10b (collector): conversion_rejects retention and the raw_rowid live pointer (docs/lean/CONTRACTS.md C3; ARCHITECTURE.md
 * §2.3, §3.5). Round 2 of B0 (review-r1 should-fix 3): the table and the raw rows with an open reject are in the never-delete set,
 * the raw-delete trigger nulls the reject's raw_rowid so a reused rowid cannot alias another row, and a resolved reject is retained.
 * Pending until B2a lands the table and trigger (test 1) and B10b the lean retention job (test 2). Round 3 of B0 (review-r2
 * blocker 3b): test 2's raw row is appended through the buffer, leased and acknowledged, so it has the outbox lineage and the
 * acknowledged receipt the ladder's acknowledged-only rule reads (a row inserted by SQL had none and could never be released);
 * the reject row, not the stored timestamp string, is what the retention rule keys on (helper.contract.ts guards the premise).
 */
import assert from "node:assert/strict";
import test from "node:test";

import { event, fn, loadSurface, openTempBuffer, pending, tableSql } from "./_pending";

const RAW = `insert into buffered_events (id, source, event_type, data_mode, observed_at, payload_json, suppressed_fields_json, created_at, session_id, input_tokens, output_tokens, workspace_id, installation_epoch_id)
  values (@id, 'codex', 'usage_rollout', 'metadata', @observedAt, '{}', '[]', @createdAt, 'sess-1', 1, 1, 'tenant-lean-contract', 'epoch-1')`;
const GAP = "insert into capture_gaps (gap_id, workspace_id, installation_epoch_id, source, reason, interval_basis, started_at_ms, ended_at_ms, count_basis, dropped_rows) values (@gapId, 'tenant-lean-contract', 'epoch-1', 'codex', 'contract_violation', 'fault_interval', 0, 1, 'counted', 1)";
const REJECT = "insert into conversion_rejects (event_id, raw_generation, raw_rowid, epoch_key, source, reason, observed_at_raw_digest, gap_id, first_seen_at_ms, last_seen_at_ms) values (@id, '', @rowid, 'epoch-1', 'codex', 'contract_violation', 'd', @gapId, 1, 1)";

test("B2a C3: a raw delete nulls the reject's raw_rowid and keeps the reject; a reused rowid never aliases it", pending("B2a"), () => {
  const { buffer, close } = openTempBuffer({ workspaceId: "tenant-lean-contract", lean: { write: false } });
  try {
    const db = buffer.database;
    assert.ok(tableSql(db, "conversion_rejects"), "conversion_rejects exists");
    db.prepare(RAW).run({ id: "x1", observedAt: "26/09/2026 00:00:00 +00:00", createdAt: "2026-01-01T00:00:00.000Z" });
    const rowid = (db.prepare("select rowid from buffered_events where id = 'x1'").get() as { rowid: number }).rowid;
    db.prepare(GAP).run({ gapId: "gap-x1" });
    db.prepare(REJECT).run({ id: "x1", rowid, gapId: "gap-x1" });
    db.prepare("delete from buffered_events where id = 'x1'").run();       // any raw delete path fires the raw-delete trigger
    const after = db.prepare("select event_id as id, raw_rowid as rowid, resolved_at_ms as resolved from conversion_rejects").all() as Array<{ id: string; rowid: number | null; resolved: number | null }>;
    assert.deepEqual(after, [{ id: "x1", rowid: null, resolved: null }], "the reject is retained with a null live pointer");
    db.prepare(RAW).run({ id: "x2", observedAt: "2026-09-26T00:00:00.000Z", createdAt: "2026-09-26T00:00:00.000Z" });
    const reused = (db.prepare("select rowid from buffered_events where id = 'x2'").get() as { rowid: number }).rowid;
    assert.equal(reused, rowid, "SQLite reuses the rowid (the aliasing hazard this test guards)");
    assert.equal((db.prepare("select count(*) as n from conversion_rejects where raw_rowid = ?").get(reused) as { n: number }).n, 0, "no reject points at the reused rowid");
  } finally { close(); }
});

test("B10b C3: a raw row with an OPEN reject is never deleted by the prune or the ladder, the reject row survives both, and only a resolved reject releases its raw row", pending("B10b"), async () => {
  const release = fn(await loadSurface("../../../packages/collector-cli/src/lean/retention.ts"), "releaseUnderLadder") as (buffer: unknown, options: Record<string, unknown>) => { released: string[] };
  const { buffer, close } = openTempBuffer({ workspaceId: "tenant-lean-contract", delivery: { enabled: true }, lean: { write: false } });
  try {
    const db = buffer.database;
    const x1 = event({ eventType: "usage_rollout", observedAt: "2026-09-26T00:00:00.000Z" });   // through the buffer: raw row, identity row and outbox lineage together
    assert.equal(buffer.append(x1), true);
    const rowid = (db.prepare("select rowid from buffered_events where id = ?").get(x1.id) as { rowid: number }).rowid;
    db.prepare(GAP).run({ gapId: "gap-x1" });
    db.prepare(REJECT).run({ id: x1.id, rowid, gapId: "gap-x1" });          // the converter's reject for a row it could not fold (its stored string is not what the rule reads)
    const lease = buffer.delivery.lease({ leaseId: "lean-contract-lease", now: new Date() });
    assert.equal(lease.items.length, 1);
    assert.equal(buffer.delivery.acknowledge(lease.leaseId, [x1.id], new Date()).acknowledged, 1);   // acknowledged (uploaded_at + receipt) and old at `far`: deletable by every rule except the open reject
    const far = new Date("2036-01-01T00:00:00.000Z");
    assert.equal(buffer.prune(0, { maxRows: 100, now: far }).events, 0, "the prune leaves the raw row with an open reject");
    assert.deepEqual(release(buffer, { retentionDays: 90, now: far }).released, [], "the ladder leaves it too");
    assert.equal((db.prepare("select count(*) as n from buffered_events where id = ?").get(x1.id) as { n: number }).n, 1);
    db.prepare("update conversion_rejects set resolved_at_ms = 2 where event_id = ?").run(x1.id);   // a later pass folded it
    assert.deepEqual(release(buffer, { retentionDays: 90, now: far }).released, [x1.id], "resolved: the raw row may go");
    const reject = db.prepare("select raw_rowid as rowid, resolved_at_ms as resolved from conversion_rejects where event_id = ?").get(x1.id) as { rowid: number | null; resolved: number };
    assert.deepEqual(reject, { rowid: null, resolved: 2 }, "the reject row is retained (never deleted by age, pressure or the ladder) with its resolution");
  } finally { close(); }
});
