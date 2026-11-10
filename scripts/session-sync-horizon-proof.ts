import assert from "node:assert/strict";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { buildSessionSyncRow, collectSessionSnapshots, commitDaemonSessionSyncResult,
  loadDaemonSessionSyncState, planDaemonSessionSync, runSessionSync,
  saveDaemonSessionSyncState } from "../packages/collector-cli/src/session-sync";
import type { SessionSummaryRead } from "../packages/collector-cli/src/session-summary";
import { aiInteractionEventSchema, type AiWorkSessionSyncRow } from "../packages/shared/src/index";
import { acceptedFixtureDelivery } from "./lib/delivery-fixture";
import { createProofCompletion } from "./lib/proof-completion";

const completion = createProofCompletion("session-sync-horizon", 4);
const tenantId = "00000000-0000-4000-8000-000000000105";
const config = collectorConfigSchema.parse({ tenantId, installKey: "horizon-proof",
  uploadUrl: "http://127.0.0.1:1/ingest", uploadSigningSecret: "horizon-fixture-secret" });
const buffer = new LocalEventBuffer(path.join(process.env.PLIMSOLL_PROOF_ROOT!, "horizon.sqlite"), { workspaceId: tenantId });
const uuid = (n: number) => `00000000-0000-4000-8000-${String(n).padStart(12, "0")}`;
const large = uuid(100_000), small = Array.from({ length: 20 }, (_, i) => uuid(100_001 + i));
const observedAt = new Date(Date.now() + 2_000).toISOString();
const append = (sessionId: string, n: number) => assert.equal(buffer.append(aiInteractionEventSchema.parse({
  id: uuid(n), sessionId, source: "codex", eventType: "assistant_response", observedAt,
  inputTokens: 3, outputTokens: 1,
})), true);
let clock = Date.now(), interrupt = false, interruptions = 0, unavailable = false, storageFailure = true;
const cloud = new Map<string, AiWorkSessionSyncRow>();
const read: SessionSummaryRead = async <T>(queries: Parameters<SessionSummaryRead>[0]) => {
  const rows: T[] = [];
  for (const q of queries) {
    // Inject a 32 ms storage budget at eight rows per ms: oversized reads fail
    // before returning anything, exactly as an interrupted SQLite .all() does.
    const chunk = buffer.database.prepare(q.sql).all(q.params) as T[], ms = chunk.length / 8;
    if (interrupt && ms > 32 && storageFailure) { storageFailure = false; throw new Error("sync_storage_busy"); }
    if (interrupt) {
      const budget = Math.min(32, q.maxMs ?? 32);
      await new Promise((resolve) => setTimeout(resolve, Math.min(ms, budget + 1)));
      if (ms > budget) { interruptions++; throw new Error("session_summary_read_interrupted"); }
    }
    rows.push(...chunk);
  }
  return rows;
};
const fetchImpl = (async (_input, init) => {
  if (unavailable) return new Response("unavailable", { status: 503 });
  const raw = String(init?.body), payload = JSON.parse(raw);
  for (const row of payload.sessions as AiWorkSessionSyncRow[]) {
    const prior = cloud.get(row.session.id);
    assert.ok(!prior || row.session.endedAt > prior.session.endedAt ||
      (row.session.endedAt === prior.session.endedAt && row.totals.events >= prior.totals.events));
    cloud.set(row.session.id, row);
  }
  return new Response(JSON.stringify(acceptedFixtureDelivery(raw, config.installKey!)), {
    status: 200, headers: { "content-type": "application/json" },
  });
}) as typeof fetch;
async function cycle() {
  const until = new Date(clock += 10_000).toISOString();
  const plan = planDaemonSessionSync({ db: buffer.database, state: loadDaemonSessionSyncState(buffer.database), uploadedBatches: [], until });
  const result = await runSessionSync(config, { ledgerDb: buffer.database, incremental: true,
    sessionIds: plan.sessionIds, until, fetchImpl, delayMs: 0, maxAttemptsPerBatch: 1,
    proofSummaryHooks: { read, now: () => new Date(clock) }, log: () => {} });
  saveDaemonSessionSyncState(buffer.database, commitDaemonSessionSyncResult(plan.state, plan.sessionIds, result));
  return { result, state: loadDaemonSessionSyncState(buffer.database), until };
}
async function main() {
  try {
    append(large, 1); clock = Date.now() - 10_000; await cycle();
    saveDaemonSessionSyncState(buffer.database, { ...loadDaemonSessionSyncState(buffer.database), caughtUp: false });
    buffer.database.transaction(() => { for (let n = 2; n <= 5_001; n++) append(large, n);
      small.forEach((id, i) => append(id, 6_000 + i)); })();
    interrupt = true;
    let current = await cycle();
    assert.ok(small.every((id) => cloud.has(id)), JSON.stringify(current.result));
    assert.equal(current.state.lastSuccessfulUntil, current.until);
    assert.deepEqual(current.state.pendingSessionIds, [large]);
    assert.deepEqual(current.state.pendingSessionReads, [{ sessionId: large, reason: "sync_storage_busy", cycles: 1 }]);
    completion.check("twenty_small_sessions_accepted_and_horizon_advances_in_one_cycle");
    current = await cycle();
    assert.equal(current.state.pendingSessionReads?.[0]?.cycles, 2);
    completion.check("read_failure_reason_and_cycle_count_survive_reload");
    let cycles = 2;
    while (current.state.pendingSessionIds.length && cycles < 20) { current = await cycle(); cycles++; }
    assert.ok(cycles < 20 && interruptions >= 2);
    assert.deepEqual(current.state.pendingSessionReads, []);
    const expected = buildSessionSyncRow(collectSessionSnapshots(buffer.database, { until: current.until, sessionIds: [large] })[0]!);
    assert.ok(expected.ok); if (expected.ok) assert.deepEqual(cloud.get(large), expected.row);
    assert.equal(cloud.get(large)!.totals.events, 5_001);
    completion.check("large_incremental_summary_completes_exactly_with_grow_only_wire_output");
    append(small[0]!, 7_000); unavailable = true;
    const failed = await cycle();
    assert.equal(failed.result.ok, false);
    assert.equal(failed.state.lastSuccessfulUntil, current.until);
    assert.ok(failed.state.pendingSessionIds.includes(small[0]!));
    completion.check("network_failure_keeps_horizon_and_unsent_session");
    console.log(JSON.stringify({ largeEvents: 5_001, smallSessions: 20, horizonAdvancedCycle: 1,
      largeCompletedCycle: cycles, readBudgetMs: 32, interruptions, pendingReads: current.state.pendingSessionReads }));
    completion.complete();
  } finally { buffer.close(); }
}
void main().catch((error) => { console.error(error); process.exitCode = 1; });
