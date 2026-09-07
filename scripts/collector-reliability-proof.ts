import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { once } from "node:events";
import type { AddressInfo } from "node:net";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { createCollectorServer } from "../packages/collector-cli/src/server";
import { AutomaticRetentionCadence } from "../packages/collector-cli/src/retention-cadence";
import { runRetentionDeletionStage } from "../packages/collector-cli/src/maintenance-stage-primitives";
import { AutomaticMaintenanceCadence, CoalescingMaintenanceScheduler, CollectorMaintenance,
  automaticRepairServiceStatus, type MaintenanceRunOutcome } from "../packages/collector-cli/src/maintenance";
import { captureBaselineStatus } from "../packages/collector-cli/src/capture-baseline";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { TranscriptTailer } from "../packages/collector-cli/src/transcript-tailer";
import { aiInteractionEventSchema } from "../packages/shared/src/index";

const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-reliability-"));
const realNow = Date.now;
const now = realNow();
const checks: Array<{ name: string; passed: boolean; detail?: unknown; error?: string }> = [];
const EXPECTED_CHECKS = 10;
let nextId = 0;
function event() {
  return aiInteractionEventSchema.parse({
    id: `00000000-0000-4000-8000-${String(++nextId).padStart(12, "0")}`,
    source: "codex", dataMode: "metadata", eventType: "assistant_response",
    observedAt: new Date(now - 1_000).toISOString(), inputTokens: 1,
    outputTokens: 1, sessionId: "reliability-session", actionClass: "other",
  });
}
function fixture(name: string) { return new LocalEventBuffer(path.join(root, `${name}.sqlite`)); }
function settle(buffer: LocalEventBuffer) {
  for (let i = 0; i < 100; i++) {
    buffer.projection.runMaintenance(new Date(now));
    const status = buffer.projection.status();
    if (status.ready && !status.dirty && status.parityReady &&
        Object.values(status.backlog).every(value => value === 0)) return;
  }
  assert.fail(`fixture projection did not settle: ${JSON.stringify(buffer.projection.status())}`);
}
async function check(name: string, run: () => unknown | Promise<unknown>) {
  Date.now = () => now;
  try { checks.push({ name, passed: true, detail: await run() }); }
  catch (error) { checks.push({ name, passed: false, error: error instanceof Error ? error.stack : String(error) }); }
  finally { Date.now = realNow; }
}
async function serve(buffer: LocalEventBuffer, options: Parameters<typeof createCollectorServer>[2] = {}) {
  const server = createCollectorServer(collectorConfigSchema.parse({}), buffer, options);
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  return { server, base, get: async (url: string) => {
    const response = await fetch(base + url, { signal: AbortSignal.timeout(5_000) });
    assert.equal(response.status, 200);
    return response.json() as Promise<any>;
  }, close: async () => { server.closeAllConnections(); server.close(); await once(server, "close"); } };
}

async function main() {
  await check("stale_status_cache_is_not_current", async () => {
    const buffer = fixture("status"); buffer.append(event()); settle(buffer);
    const http = await serve(buffer);
    try {
      Date.now = () => now + 12 * 3_600_000;
      // Availability must survive even a completely unavailable ledger.
      buffer.database.prepare = () => { throw new Error("status_must_be_cache_only"); };
      const result = await http.get("/status");
      assert.equal(result.statusFreshness.state, "expired");
      assert.equal(result.projection.parityReady, false);
      assert.equal(result.projection.ready, false);
      assert.ok(result.statusFreshness.lastGoodAt);
      return result.statusFreshness;
    } finally { await http.close(); buffer.close(); }
  });
  await check("dirty_get_preserves_generation_and_never_writes", async () => {
    const buffer = fixture("dirty"); const item = event(); buffer.append(item); settle(buffer);
    const http = await serve(buffer);
    try {
      const before = buffer.projection.status();
      buffer.database.prepare("update buffered_events set input_tokens=9 where id=?").run(item.id);
      const changes = () => (buffer.database.prepare("select total_changes() as n").get() as {n:number}).n;
      const start = changes();
      Date.now = () => now + 16 * 60_000;
      const result = await http.get("/api/snapshot?days=30");
      assert.equal(changes(), start, "GET must perform zero ledger writes");
      assert.equal(buffer.projection.status().generation, before.generation);
      assert.equal(result.projection.parityReady, false);
      assert.equal(result.status.projection.parityReady, false);
      assert.equal(result.projection.status, "stale");
      assert.ok(buffer.projection.status().backlog.repairs > 0);
      Date.now = realNow; settle(buffer);
      const read = buffer.projection.readSnapshot(30);
      assert.equal(read.kind, "ready");
      if (read.kind === "ready") assert.equal((read.snapshot.summary.totals as any).inputTokens, 9);
      const after = buffer.projection.status().counters.repairFacts;
      buffer.projection.runMaintenance(new Date(now));
      assert.equal(buffer.projection.status().counters.repairFacts, after, "correction applied once");
      return { generationBefore: before.generation, generationAfter: buffer.projection.status().generation };
    } finally { await http.close(); buffer.close(); }
  });
  await check("failed_refresh_and_clock_rollback_are_consistently_stale", async () => {
    const buffer = fixture("refresh-failure"); buffer.append(event()); settle(buffer);
    let refresh!: (failure?: "maintenance_failed") => boolean;
    const http = await serve(buffer, {registerStatusRefresher: callback => { refresh = callback; }});
    try {
      const good = await http.get("/status");
      assert.equal(refresh("maintenance_failed"), true);
      const failed = await http.get("/status");
      const snapshot = await http.get("/api/snapshot?days=30");
      assert.equal(failed.statusFreshness.lastGoodAt, good.statusFreshness.lastGoodAt);
      assert.equal(failed.statusFreshness.reason, "maintenance_failed");
      assert.equal(failed.projection.parityReady, false);
      assert.equal(snapshot.projection.parityReady, false);
      assert.equal(snapshot.status.projection.parityReady, false);
      assert.equal(refresh(), true);
      Date.now = () => now - 1_000;
      const backwards = await http.get("/status");
      assert.equal(backwards.statusFreshness.state, "expired");
      assert.equal(backwards.projection.parityReady, false);
      return { failedRefreshes: failed.statusRefreshCounters.failures, clockRollback: "stale" };
    } finally { await http.close(); buffer.close(); }
  });
  await check("partial_ingest_retry_and_deferred_projection_conserve_exact_totals", async () => {
    const buffer = fixture("partial-ingest"); settle(buffer);
    const http = await serve(buffer);
    const original = buffer.appendMany.bind(buffer);
    let calls = 0;
    buffer.appendMany = (...args) => {
      if (++calls === 2) throw Object.assign(new Error("synthetic_second_chunk_busy"), {code: "SQLITE_BUSY"});
      return original(args[0], args[1], args[2], {projectionDeadlineMs: 0});
    };
    const body = JSON.stringify({resourceLogs: [{scopeLogs: [{logRecords: Array.from({length: 40}, (_, index) => ({
      timeUnixNano: String(BigInt(now - 1_000) * 1_000_000n + BigInt(index)),
      attributes: [
        {key: "gen_ai.usage.input_tokens", value: {intValue: "1"}},
        {key: "gen_ai.usage.output_tokens", value: {intValue: "2"}},
      ],
    }))}]}]});
    const post = () => fetch(http.base + "/v1/logs", {method: "POST", body,
      headers: {"content-type": "application/json", "x-plimsoll-source": "codex"}});
    try {
      const interrupted = await post();
      assert.equal(interrupted.status, 503);
      assert.equal((await interrupted.json() as {reason:string}).reason, "storage_busy_retry");
      assert.equal((buffer.database.prepare("select count(*) as n from buffered_events").get() as {n:number}).n, 16);
      buffer.appendMany = (events, metrics, drops) => original(events, metrics, drops, {projectionDeadlineMs: 0});
      assert.equal((await post()).status, 202);
      assert.equal((await post()).status, 202);
      const raw = buffer.database.prepare("select count(*) as events,sum(input_tokens) as input,sum(output_tokens) as output from buffered_events").get();
      assert.deepEqual(raw, {events: 40, input: 40, output: 80});
      assert.ok(buffer.projection.status().backlog.repairs > 0);
      settle(buffer);
      const read = buffer.projection.readSnapshot(30);
      assert.equal(read.kind, "ready");
      if (read.kind === "ready") {
        assert.equal((read.snapshot.summary.totals as any).inputTokens, 40);
        assert.equal((read.snapshot.summary.totals as any).outputTokens, 80);
      }
      return {partialCommitted: 16, final: raw, projectionSettled: true};
    } finally {await http.close(); buffer.close();}
  });
  await check("repair_followup_respects_circuit_and_stalled_backoff", async () => {
    const buffer = fixture("repair-cadence");
    let tick = now, serial = 0, units = 0, calls = 0, notBefore: number | null = null;
    const pending = new Map<number, {callback: () => void; delay: number}>();
    const source = {filesRead: 0, parseErrors: 0, eventsAppended: 0, activity: {discoveryEntries: 0}};
    const result: MaintenanceRunOutcome = {recentOnly: true, rollout: source, transcript: source,
      reconciliation: {rowsChanged: 0, rowsVisited: 0}, repricing: {repriced: 0, rowsVisited: 0},
      enrichment: {backward: 0, forward: 0, rowsVisited: 0}, rawEventWrites: 0};
    const scheduler = new CoalescingMaintenanceScheduler(async () => {
      calls += 1;
      if (calls === 1) units += 1;
      if (calls === 2) {notBefore = tick + 30_000; throw new Error("synthetic_open_circuit");}
      return result;
    });
    const cadence = new AutomaticMaintenanceCadence(scheduler, () => captureBaselineStatus(buffer.database), {
      repairProgress: () => ({pending: true, units}), retryNotBefore: () => notBefore, onError: () => {},
      timer: {now: () => tick, setTimeout: (callback, delay) => {
        const id = ++serial; pending.set(id, {callback, delay}); return id;
      }, clearTimeout: handle => {pending.delete(handle as number);}},
    });
    const advance = async () => {
      const [id, task] = pending.entries().next().value!;
      pending.delete(id); tick += task.delay; task.callback();
      await new Promise<void>(resolve => setImmediate(resolve));
    };
    try {
      cadence.start(); await advance();
      assert.equal(cadence.status().retryClass, "repair");
      assert.equal(pending.values().next().value?.delay, 5_000);
      await advance();
      assert.equal(cadence.status().retryClass, "circuit");
      assert.equal(pending.values().next().value?.delay, 30_000);
      await advance();
      assert.equal(cadence.status().retryClass, "normal");
      assert.equal(pending.values().next().value?.delay, 60_000);
      cadence.stop(); assert.equal(pending.size, 0);
      return {calls, repairDelayMs: 5_000, circuitDelayMs: 30_000, stalledDelayMs: 60_000};
    } finally {cadence.stop(); buffer.close();}
  });
  await check("repair_progress_under_saturated_capture", async () => {
    const buffer = fixture("repair"); const item = event(); buffer.append(item); settle(buffer);
    buffer.database.prepare("update buffered_events set input_tokens=7 where id=?").run(item.id);
    const rollout = new RolloutTailer(buffer, path.join(root, "empty-codex"));
    const transcript = new TranscriptTailer(buffer, path.join(root, "empty-claude"));
    for (const tailer of [rollout, transcript]) {
      const scan = tailer.scan.bind(tailer);
      tailer.scan = (async (options: any) => {
        const result = await scan(options);
        options.automatic?.budget.recordSlice({ bytesRead: 512 * 1024, recordsParsed: 512, eventsAppended: 512 });
        return result;
      }) as typeof tailer.scan;
    }
    const maintenance = new CollectorMaintenance(buffer, rollout, transcript);
    try {
      const before = buffer.projection.status().counters.repairFacts;
      const runs = [];
      for (let i = 0; i < 5; i++) runs.push(await maintenance.runRecent());
      const after = buffer.projection.status().counters.repairFacts;
      assert.ok(after > before, "repair must receive service within five saturated cycles");
      assert.equal(buffer.projection.status().backlog.repairs, 0);
      return { cycles: runs.length, repaired: after - before, deferred: runs.map(r => r.postCaptureDeferred) };
    } finally { maintenance.close(); buffer.close(); }
  });
  await check("retention_metrics_receive_fair_service", () => {
    const buffer = fixture("retention");
    try {
      for (let i = 0; i < 12; i++) buffer.append(event());
      buffer.database.prepare("update buffered_events set created_at='2000-01-01T00:00:00.000Z'").run();
      for (let i = 0; i < 4; i++) buffer.database.prepare(`insert into metric_samples
        (id,source,metric_name,observed_at,value,created_at) values (?, 'codex','proof',?,1,?)`)
        .run(`metric-${i}`, new Date(now).toISOString(), "2000-01-01T00:00:00.000Z");
      const result = buffer.prune(90, { maxRows: 4 });
      assert.ok(result.events > 0 && result.metricSamples > 0, "both raw and metrics must progress");
      assert.ok(result.eventRowsVisited + result.metricRowsVisited <= 4);
      return result;
    } finally { buffer.close(); }
  });
  await check("retention_seek_preserves_migration_and_delivery", () => {
    const file = path.join(root, "migration.sqlite");
    let buffer = new LocalEventBuffer(file);
    const items = Array.from({length: 12}, () => event());
    for (const item of items) buffer.append(item);
    buffer.database.prepare("update buffered_events set created_at='2000-01-01T00:00:00.000Z'").run();
    buffer.markUploaded(items.slice(8).map(item => item.id));
    buffer.close();
    // Legacy migration preserves the old raw timestamp. Keep this synthetic
    // 2000-dated delivery alive so this proof isolates raw/outbox conservation.
    buffer = new LocalEventBuffer(file, {delivery: {enabled: true, limits: {maxOldestAgeDays: 20_000}}});
    try {
      const receipts = [];
      for (let i=0; i<10; i++) {
        const result=buffer.prune(90,{maxRows:3}); receipts.push(result);
        assert.ok(result.eventRowsVisited + result.metricRowsVisited <= 3);
        if (!result.hasMore) break;
      }
      assert.equal(receipts.reduce((n,r)=>n+r.events,0),4);
      assert.equal(receipts.reduce((n,r)=>n+r.migrationProtectedRows,0),8);
      assert.equal((buffer.database.prepare("select count(*) as n from buffered_events").get() as any).n,8);
      for (let i=0; i<10 && !buffer.delivery.status().migration.complete; i++) buffer.delivery.migrateLegacy({maxRows:2});
      assert.equal(buffer.delivery.status().remainingDelivery,8);
      // The actual worker stage uses the same bounded expiry/receipt path.
      runRetentionDeletionStage(buffer.database,{remainingMs:3000,batchSize:8,retentionDays:90,parityReady:true,
        prune:maxRows=>buffer.prune(90,{maxRows})});
      assert.equal((buffer.database.prepare("select count(*) as n from buffered_events").get() as any).n,0);
      assert.equal(buffer.delivery.status().remainingDelivery,8);
      assert.equal((buffer.database.prepare("select count(*) as n from raw_retention_receipts").get() as any).n,12);
      return {protected:8,expired:12,outboxPreserved:8,passes:receipts.length};
    } finally {buffer.close();}
  });
  await check("retention_followups_converge_and_stop", () => {
    const buffer=fixture("followups");
    let tick=now, serial=0, available=false, failOnce=true;
    const pending=new Map<number,{callback:()=>void;delay:number}>();
    const add=()=>{buffer.append(event());buffer.database.prepare("update buffered_events set created_at='2000-01-01T00:00:00.000Z'").run();};
    for(let i=0;i<12;i++)add();
    const cadence=new AutomaticRetentionCadence(()=>{
      if(failOnce){failOnce=false;throw new Error("synthetic_writer_busy");}
      return buffer.prune(90,{maxRows:4});
    },{canRun:()=>available,followupMs:5,intervalMs:1000,onError:()=>{},
      timer:{now:()=>tick,setTimeout:(callback,delay)=>{const id=++serial;pending.set(id,{callback,delay});return id;},
        clearTimeout:handle=>{pending.delete(handle as number);}}});
    try {
      cadence.start();cadence.start();assert.equal(pending.size,1);
      for(let cycle=0;cycle<20;cycle++){
        const [id,task]=pending.entries().next().value!;pending.delete(id);tick+=task.delay;
        if(cycle>=2&&cycle<6)add();
        task.callback();available=true; // arrival below the four-row service rate
        if(cadence.status().counters.passes>0&&!cadence.status().lastPass?.hasMore)break;
      }
      const status=cadence.status();
      assert.equal(status.counters.deferred,1);assert.equal(status.counters.failures,1);
      assert.equal(status.counters.eventsExpired,16);
      assert.equal(status.lastPass?.hasMore,false);
      assert.equal(pending.values().next().value?.delay,1000);
      cadence.stop();assert.equal(pending.size,0);
      return status;
    } finally {cadence.stop();buffer.close();}
  });
  await check("failed_repair_keeps_watermarks_and_rotates_service", async () => {
    const buffer=fixture("failure");buffer.append(event());settle(buffer);
    const rollout=new RolloutTailer(buffer,path.join(root,"failure-codex"),()=>[]);
    const transcript=new TranscriptTailer(buffer,path.join(root,"failure-claude"));
    const maintenance=new CollectorMaintenance(buffer,rollout,transcript);
    const before=buffer.projection.status();
    const original=buffer.projection.runMaintenance.bind(buffer.projection);
    buffer.projection.runMaintenance=()=>{throw new Error("synthetic_projection_failure");};
    try {
      await assert.rejects(maintenance.runRecent(),/synthetic_projection_failure/);
      const failure=automaticRepairServiceStatus(buffer.database);
      assert.equal(failure.stages.projection.failures,1);
      assert.equal(failure.stages.projection.completed,0);
      assert.equal(failure.next,1);
      assert.equal(buffer.projection.status().generation,before.generation);
      assert.deepEqual(buffer.projection.status().backfill,before.backfill);
      buffer.projection.runMaintenance=original;
      await maintenance.runRecent();
      const recovered=automaticRepairServiceStatus(buffer.database);
      assert.ok(recovered.stages.reconciliation.completed>0);
      assert.ok(recovered.stages.projection.completed>0);
      return recovered;
    } finally {maintenance.close();buffer.close();}
  });
  const receipt = { schema: "plimsoll-collector-reliability-proof/v1", complete: checks.length === EXPECTED_CHECKS,
    checks, expected: EXPECTED_CHECKS, passed: checks.filter(c => c.passed).length,
    failed: checks.filter(c => !c.passed).length };
  const receiptPath = process.argv[2];
  if (receiptPath) fs.writeFileSync(receiptPath, JSON.stringify(receipt, null, 2) + "\n");
  console.log(JSON.stringify(receipt));
  process.exitCode = receipt.failed || !receipt.complete ? 1 : 0;
}
main().finally(() => { Date.now = realNow; fs.rmSync(root, { recursive: true, force: true }); }).catch(error => {
  console.error(error); process.exitCode = 1;
});
