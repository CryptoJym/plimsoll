import assert from "node:assert/strict";
import Database from "better-sqlite3";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { once } from "node:events";
import type { AddressInfo } from "node:net";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { createCollectorServer } from "../packages/collector-cli/src/server";
import { AutomaticRetentionCadence, RETENTION_WAL_PRESSURE_BYTES, walPressureDefersRetention } from "../packages/collector-cli/src/retention-cadence";
import { countRetentionHoldsOffThread } from "../packages/collector-cli/src/retention-hold-count";
import { runRetentionDeletionStage } from "../packages/collector-cli/src/maintenance-stage-primitives";
import { AutomaticMaintenanceCadence, CoalescingMaintenanceScheduler, CollectorMaintenance,
  automaticRepairServiceStatus, type MaintenanceRunOutcome } from "../packages/collector-cli/src/maintenance";
import { captureBaselineStatus } from "../packages/collector-cli/src/capture-baseline";
import { ensureSessionSummarySchema } from "../packages/collector-cli/src/session-summary";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { TranscriptTailer } from "../packages/collector-cli/src/transcript-tailer";
import { MAINTENANCE_PROTOCOL_SCHEMA, parseMaintenanceWorkerReceipt } from "../packages/collector-cli/src/maintenance-protocol";
import { aiInteractionEventSchema } from "../packages/shared/src/index";

const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-reliability-"));
const realNow = Date.now;
const now = realNow();
const checks: Array<{ name: string; passed: boolean; detail?: unknown; error?: string }> = [];
const EXPECTED_CHECKS = 20;
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
    let markMaintenanceSuccess!: () => void;
    const http = await serve(buffer, {
      registerStatusRefresher: (callback, markSuccess) => {
        refresh = callback;
        markMaintenanceSuccess = markSuccess;
      },
    });
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
      markMaintenanceSuccess();
      Date.now = () => now - 1_000;
      const backwards = await http.get("/status");
      assert.equal(backwards.statusFreshness.state, "expired");
      assert.equal(backwards.projection.parityReady, false);
      return { failedRefreshes: failed.statusRefreshCounters.failures, clockRollback: "stale" };
    } finally { await http.close(); buffer.close(); }
  });
  await check("partial_ingest_retries_in_process_and_conserves_exact_totals", async () => {
    const ledgerPath = path.join(root, "partial-ingest.sqlite");
    const buffer = new LocalEventBuffer(ledgerPath, { databaseBusyTimeoutMs: 0 });
    settle(buffer);
    const maintenance = new Database(ledgerPath, { timeout: 0 });
    maintenance.pragma("journal_mode = WAL");
    const http = await serve(buffer);
    const original = buffer.appendMany.bind(buffer);
    let calls = 0, busyErrors = 0;
    let release: ReturnType<typeof setTimeout> | null = null;
    buffer.appendMany = (...args) => {
      calls += 1;
      if (calls === 2) {
        maintenance.exec("begin immediate");
        release = setTimeout(() => maintenance.exec("commit"), 120);
      }
      try {
        return original(args[0], args[1], args[2], {projectionDeadlineMs: 0});
      } catch (error) {
        const code = error && typeof error === "object" && "code" in error
          ? String(error.code)
          : "";
        if (code.startsWith("SQLITE_BUSY")) busyErrors += 1;
        throw error;
      }
    };
    const body = JSON.stringify({resourceLogs: [{scopeLogs: [{logRecords: Array.from({length: 40}, (_, index) => ({
      timeUnixNano: String(BigInt(now - 1_000) * 1_000_000n + BigInt(index)),
      attributes: [
        {key: "gen_ai.usage.input_tokens", value: {intValue: "1"}},
        {key: "gen_ai.usage.output_tokens", value: {intValue: "2"}},
        ...(index === 16 ? [{key: "cwd", value: {stringValue: root}}] : []),
      ],
    }))}]}]});
    const post = () => fetch(http.base + "/v1/logs", {method: "POST", body,
      headers: {"content-type": "application/json", "x-plimsoll-source": "codex"}});
    try {
      assert.equal((await post()).status, 202);
      assert.equal((buffer.database.prepare("select count(*) as n from buffered_events").get() as {n:number}).n, 40);
      assert.equal((await post()).status, 202);
      const raw = buffer.database.prepare("select count(*) as events,sum(input_tokens) as input,sum(output_tokens) as output from buffered_events").get();
      assert.deepEqual(raw, {events: 40, input: 40, output: 80});
      assert.ok(busyErrors >= 1);
      assert.deepEqual(
        buffer.database.prepare(
          `select
             (select count(*) from repo_context_event_links) as links,
             (select count(*) from repo_context_handoffs) as handoffs`,
        ).get(),
        { links: 1, handoffs: 1 },
      );
      assert.ok(buffer.projection.status().backlog.repairs > 0);
      settle(buffer);
      const read = buffer.projection.readSnapshot(30);
      assert.equal(read.kind, "ready");
      if (read.kind === "ready") {
        assert.equal((read.snapshot.summary.totals as any).inputTokens, 40);
        assert.equal((read.snapshot.summary.totals as any).outputTokens, 80);
      }
      return {
        partialCommittedBeforeRetry: 16,
        final: raw,
        projectionSettled: true,
        appendCalls: calls,
        busyErrors,
        contextLinks: 1,
        durableHandoffs: 1,
      };
    } finally {
      if (release) clearTimeout(release);
      if (maintenance.inTransaction) maintenance.exec("rollback");
      await http.close();
      maintenance.close();
      buffer.close();
    }
  });
  await check("hook_append_waits_for_maintenance_writer_without_blocking_event_loop", async () => {
    const ledgerPath = path.join(root, "hook-maintenance-writer.sqlite");
    const buffer = new LocalEventBuffer(ledgerPath, { databaseBusyTimeoutMs: 0 });
    const maintenance = new Database(ledgerPath, { timeout: 0 });
    maintenance.pragma("journal_mode = WAL");
    const http = await serve(buffer);
    const body = JSON.stringify({
      id: "00000000-0000-4000-8000-000000000099",
      event_type: "Stop",
      timestamp: new Date(now).toISOString(),
      session_id: "maintenance-contention-hook",
    });
    let release: ReturnType<typeof setTimeout> | null = null;
    let busyAttempts = 0;
    let busyObservedResolve: (() => void) | null = null;
    const busyObserved = new Promise<void>((resolve) => { busyObservedResolve = resolve; });
    let busyObservedAt = 0;
    let eventLoopTick: Promise<number> | null = null;
    const originalAppend = buffer.append.bind(buffer);
    (buffer as any).append = (...args: unknown[]) => {
      try {
        return (originalAppend as any)(...args);
      } catch (error) {
        const code = error && typeof error === "object" && "code" in error
          ? String(error.code)
          : "";
        if (code.startsWith("SQLITE_BUSY")) {
          busyAttempts += 1;
          if (busyObservedAt === 0) {
            busyObservedAt = performance.now();
            eventLoopTick = new Promise<number>((resolve) => {
              setTimeout(() => resolve(performance.now() - busyObservedAt), 10);
            });
            release = setTimeout(() => maintenance.exec("commit"), 120);
            busyObservedResolve?.();
          }
        }
        throw error;
      }
    };
    try {
      maintenance.exec("begin immediate");
      const responsePromise = fetch(http.base + "/hooks/codex", {
        method: "POST",
        body,
        headers: {"content-type": "application/json", "x-plimsoll-source": "codex"},
      });
      await busyObserved;
      assert.ok(eventLoopTick);
      const [response, eventLoopDelayMs] = await Promise.all([responsePromise, eventLoopTick]);
      const elapsedMs = performance.now() - busyObservedAt;
      const receipt = await response.json() as { accepted?: boolean; eventId?: string };
      assert.equal(response.status, 202);
      assert.equal(receipt.accepted, true);
      assert.ok(receipt.eventId);
      assert.ok(busyAttempts >= 1);
      assert.ok(elapsedMs >= 100 && elapsedMs < 1_000, `hook retry elapsed ${elapsedMs}ms`);
      assert.ok(eventLoopDelayMs < 100, `event loop timer delayed ${eventLoopDelayMs}ms`);
      assert.equal(
        (buffer.database.prepare("select count(*) as n from buffered_events where id = ?")
          .get(receipt.eventId) as { n: number }).n,
        1,
      );
      return { elapsedMs, eventLoopDelayMs, busyAttempts, status: response.status, durableRows: 1 };
    } finally {
      if (release) clearTimeout(release);
      if (maintenance.inTransaction) maintenance.exec("rollback");
      await http.close();
      maintenance.close();
      buffer.close();
    }
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
  await check("committed_capture_followups_are_bounded_and_fail_closed", async () => {
    const buffer = fixture("capture-cadence-controls");
    let tick = now, calls = 0, notBefore: number | null = null;
    let pending: { callback: () => void; delay: number } | null = null;
    const source = {filesRead: 0, parseErrors: 0, eventsAppended: 0, activity: {discoveryEntries: 0}};
    const result: MaintenanceRunOutcome = {recentOnly: true, rollout: source, transcript: source,
      reconciliation: {rowsChanged: 0, rowsVisited: 0}, repricing: {repriced: 0, rowsVisited: 0},
      enrichment: {backward: 0, forward: 0, rowsVisited: 0}, rawEventWrites: 0,
      stageTimings: {codexCaptureMs:0,claudeCaptureMs:0,reconciliationMs:0,repricingMs:0,enrichmentMs:0,projectionDrainMs:0,totalMs:0}};
    const wire = (captureAdvanced?: unknown) => parseMaintenanceWorkerReceipt({
      schema: MAINTENANCE_PROTOCOL_SCHEMA, type: "result", generation: 1,
      nonce: "00000000-0000-4000-8000-000000000001", sequence: 1, repoContexts: [],
      result: {...result, ...(captureAdvanced === undefined ? {} : {captureAdvanced})},
    });
    assert.equal(wire(true)?.type, "result");
    assert.equal(wire()?.type, "result");
    assert.equal(wire("true"), null, "IPC rejects an unvalidated progress hint");
    const baseline = captureBaselineStatus(buffer.database);
    const scheduler = new CoalescingMaintenanceScheduler(async () => {
      calls++;
      if (calls === 7) {notBefore = tick + 30_000; throw new Error("synthetic_capture_failure");}
      if (calls === 3) return {outcome: "PARTIAL_OK" as const,
        progress: {stage: "retention" as const, rows: 1, ms: 1, remaining: 1}};
      return {...result, captureAdvanced: [1, 6, 9].includes(calls),
        // Discovery and attempted reads alone must not renew the allowance.
        rollout: {...source, filesRead: 1, activity: {discoveryEntries: 1}}};
    });
    const cadence = new AutomaticMaintenanceCadence(scheduler, () => ({...baseline,
      progress: {...baseline.progress, state: "complete" as const}}), {
      retryNotBefore: () => notBefore, onError: () => {}, timer: {
        now: () => tick, setTimeout: (callback, delay) => {assert.equal(pending, null);pending={callback,delay};return 1;},
        clearTimeout: () => {pending=null;},
      },
    });
    const observed: Array<{retry: string | null; delay: number}> = [];
    const advance = async () => {
      assert(pending);const next=pending;pending=null;tick+=next.delay;next.callback();
      for (let i=0;i<20&&!pending;i++) await new Promise<void>(resolve=>setImmediate(resolve));
      assert(pending);observed.push({retry:cadence.status().retryClass,delay:(pending as {delay:number}).delay});
    };
    try {
      cadence.start();for(let i=0;i<9;i++)await advance();
      assert.deepEqual(observed.map(x=>x.delay),[5000,5000,5000,5000,60000,5000,30000,60000,5000]);
      assert.equal(observed[6]!.retry,"circuit");
      assert.equal(cadence.status().activeBudgetMs,200);
      assert.equal(cadence.status().maximumStartupDutyCycle,0.04);
      cadence.stop();assert.equal(pending,null);
      return {observed, noProgressFollowups:4, invalidHintRejected:true, stopCancelsFollowup:true};
    } finally {cadence.stop();buffer.close();}
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
    const acknowledge = buffer.database.prepare(`insert into upload_receipts
      (delivery_id,terminal_state,reason,status_class,attempt_count,created_at,terminal_at)
      values (?, 'acknowledged', 'remote_acknowledged', 'success', 1, ?, ?)`);
    for (const item of items.slice(8)) acknowledge.run(item.id, new Date(now).toISOString(), new Date(now).toISOString());
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
      assert.equal((buffer.database.prepare("select count(*) as n from buffered_events").get() as any).n,8);
      assert.equal(buffer.delivery.status().remainingDelivery,8);
      assert.equal((buffer.database.prepare("select count(*) as n from raw_retention_receipts").get() as any).n,4);
      return {protected:8,expired:4,outboxPreserved:8,passes:receipts.length};
    } finally {buffer.close();}
  });
  await check("idle_full_retention_page_has_no_ten_minute_followup", () => {
    const buffer = new LocalEventBuffer(path.join(root, "idle-full-page.sqlite"), {
      workspaceId: "idle-workspace", deviceId: "idle-device", delivery: { enabled: true },
    });
    let tick = now;
    let serial = 0;
    const pending = new Map<number, { callback: () => void; delay: number }>();
    const old = "2000-01-01T00:00:00.000Z";
    try {
      const insert = buffer.database.prepare(`insert into buffered_events
        (id,source,event_type,data_mode,observed_at,payload_json,created_at,workspace_id,device_id)
        values (?, 'codex', 'assistant_response', 'metadata', ?, ?, ?, ?, ?)`);
      for (let i = 0; i < 128; i++) {
        const item = event();
        insert.run(item.id, old, JSON.stringify(item), old, "idle-workspace", "idle-device");
      }
      assert.equal((buffer.database.prepare("select count(*) as n from buffered_events").get() as {n: number}).n, 128);
      const cadence = new AutomaticRetentionCadence(
        () => buffer.prune(90, { maxRows: 128, now: new Date(now) }),
        { followupMs: 5_000, intervalMs: 60 * 60_000,
          timer: { now: () => tick,
            setTimeout: (callback, delay) => { const id = ++serial; pending.set(id, { callback, delay }); return id; },
            clearTimeout: handle => { pending.delete(handle as number); } } },
      );
      try {
        cadence.start();
        const [id, first] = pending.entries().next().value!;
        pending.delete(id);
        first.callback();
        const status = cadence.status();
        assert.equal(status.counters.passes, 1);
        assert.equal(status.lastPass?.eventRowsVisited, 128);
        assert.equal(status.lastPass?.migrationProtectedRows, 128);
        assert.equal(status.lastPass?.events, 0);
        assert.equal(status.lastPass?.hasMore, false);
        tick += 10 * 60_000;
        assert.equal(pending.values().next().value?.delay, 60 * 60_000);
        assert.equal(cadence.status().counters.passes, 1);
        return { retentionPasses: 1, followupPassesOverTenMinutes: 0 };
      } finally { cadence.stop(); }
    } finally { buffer.close(); }
  });
  await check("no_progress_retention_continuation_backs_off", () => {
    let tick = now;
    let serial = 0;
    const pending = new Map<number, { callback: () => void; delay: number }>();
    const stalled = { cutoff: new Date(now).toISOString(), events: 0, metricSamples: 0,
      eventRowsVisited: 0, metricRowsVisited: 0, migrationProtectedRows: 0,
      hasMore: true, madeProgress: false } as ReturnType<LocalEventBuffer["prune"]>;
    const cadence = new AutomaticRetentionCadence(() => stalled, {
      followupMs: 5_000, intervalMs: 60 * 60_000,
      timer: { now: () => tick,
        setTimeout: (callback, delay) => { const id = ++serial; pending.set(id, { callback, delay }); return id; },
        clearTimeout: handle => { pending.delete(handle as number); } },
    });
    try {
      cadence.start();
      const [id, first] = pending.entries().next().value!;
      pending.delete(id);
      first.callback();
      assert.equal(cadence.status().counters.passes, 1);
      assert.equal(pending.values().next().value?.delay, 60 * 60_000);
      tick += 10 * 60_000;
      assert.equal(cadence.status().counters.passes, 1);
      return { retentionPasses: 1, followupPassesOverTenMinutes: 0 };
    } finally { cadence.stop(); }
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
  // 0.7.52: the pass yields to the ledger, not to another scheduler's flag.
  await check("retention_pass_defers_when_the_writer_is_held", () => {
    const file = path.join(root, "busy-writer.sqlite");
    const buffer = new LocalEventBuffer(file);
    const other = new Database(file, { timeout: 100 });
    try {
      for (let i = 0; i < 8; i++) buffer.append(event());
      buffer.database.prepare("update buffered_events set created_at='2000-01-01T00:00:00.000Z'").run();
      const before = Number(buffer.database.pragma("busy_timeout", { simple: true }));
      other.exec("begin immediate");
      const started = performance.now();
      const held = buffer.prune(90, { maxRows: 4, maxWriterMs: 250, busyTimeoutMs: 10 });
      const waitedMs = performance.now() - started;
      assert.deepEqual({ busy: held.busy, events: held.events, hasMore: held.hasMore, madeProgress: held.madeProgress },
        { busy: true, events: 0, hasMore: true, madeProgress: false });
      assert.ok(waitedMs < 1_000, `held pass waited ${waitedMs.toFixed(1)} ms`);
      assert.equal(Number(buffer.database.pragma("busy_timeout", { simple: true })), before, "busy timeout restored");
      other.exec("rollback");
      const free = buffer.prune(90, { maxRows: 4, maxWriterMs: 250, busyTimeoutMs: 10 });
      assert.deepEqual({ busy: free.busy, events: free.events }, { busy: false, events: 4 });
      assert.ok(free.elapsedMs >= 0);
      // The cadence counts a held pass as a busy deferral, keeps its pass size and retries at the follow-up.
      let serial = 0, hold = true;
      const pending = new Map<number, { callback: () => void; delay: number }>();
      const cadence = new AutomaticRetentionCadence((request) => {
        if (!hold) return buffer.prune(90, { maxRows: request.maxRows, busyTimeoutMs: 10 });
        other.exec("begin immediate");
        try { return buffer.prune(90, { maxRows: request.maxRows, busyTimeoutMs: 10 }); }
        finally { other.exec("rollback"); }
      }, { followupMs: 5_000, intervalMs: 60 * 60_000, passRows: { base: 4, max: 8, maxWriterMs: 250 },
        timer: { now: () => now,
          setTimeout: (callback, delay) => { const id = ++serial; pending.set(id, { callback, delay }); return id; },
          clearTimeout: handle => { pending.delete(handle as number); } } });
      try {
        cadence.start();
        let [id, task] = pending.entries().next().value!; pending.delete(id); task.callback();
        assert.deepEqual({ busy: cadence.status().counters.busyDeferrals, passes: cadence.status().counters.passes,
          rows: cadence.status().passRows }, { busy: 1, passes: 0, rows: 4 });
        assert.equal(pending.values().next().value?.delay, 5_000);
        hold = false;
        [id, task] = pending.entries().next().value!; pending.delete(id); task.callback();
        assert.deepEqual({ passes: cadence.status().counters.passes, expired: cadence.status().counters.eventsExpired },
          { passes: 1, expired: 4 });
        return { waitedMs: Number(waitedMs.toFixed(1)), busyDeferrals: 1 };
      } finally { cadence.stop(); }
    } finally { other.close(); buffer.close(); }
  });
  await check("retention_cadence_busy_retry_backs_off_under_a_large_backlog", () => {
    const file = path.join(root, "busy-backoff.sqlite");
    const buffer = new LocalEventBuffer(file);
    const other = new Database(file, { timeout: 100 });
    try {
      for (let i = 0; i < 4; i++) buffer.append(event());
      buffer.database.prepare("update buffered_events set created_at='2000-01-01T00:00:00.000Z'").run();
      let serial = 0, hold = true;
      const pending = new Map<number, { callback: () => void; delay: number }>();
      const cadence = new AutomaticRetentionCadence((request) => {
        if (!hold) return buffer.prune(90, { maxRows: request.maxRows, busyTimeoutMs: 10 });
        other.exec("begin immediate");
        try { return buffer.prune(90, { maxRows: request.maxRows, busyTimeoutMs: 10 }); }
        finally { other.exec("rollback"); }
      }, { followupMs: 5_000, fastFollowupMs: 200, intervalMs: 60 * 60_000, passRows: { base: 4, max: 8, maxWriterMs: 250 },
        largeBacklog: () => true,
        timer: { now: () => now,
          setTimeout: (callback, delay) => { const id = ++serial; pending.set(id, { callback, delay }); return id; },
          clearTimeout: handle => { pending.delete(handle as number); } } });
      const fire = () => {
        const [id, task] = pending.entries().next().value!;
        pending.delete(id); task.callback();
        return pending.values().next().value?.delay;
      };
      try {
        cadence.start();
        // Under a large backlog a held writer is retried soon; each
        // consecutive hold doubles the wait, up to the one-minute ceiling.
        const delays = Array.from({ length: 6 }, () => fire());
        assert.deepEqual(delays, [200, 400, 800, 1_600, 3_200, 6_400]);
        assert.deepEqual({ busy: cadence.status().counters.busyDeferrals, passes: cadence.status().counters.passes,
          rows: cadence.status().passRows, streak: cadence.status().busyStreak }, { busy: 6, passes: 0, rows: 4, streak: 6 });
        hold = false;
        fire();
        assert.deepEqual({ passes: cadence.status().counters.passes, expired: cadence.status().counters.eventsExpired,
          streak: cadence.status().busyStreak }, { passes: 1, expired: 4, streak: 0 });
        let rested: number | undefined;
        for (let i = 0; i < 3 && rested === undefined; i++) {
          if (cadence.status().lastPass?.hasMore === false) rested = pending.values().next().value?.delay;
          else fire();
        }
        assert.equal(rested, 60 * 60_000, "an empty backlog rests for the full interval");
        return { delays, rested };
      } finally { cadence.stop(); }
    } finally { other.close(); buffer.close(); }
  });
  // 0.7.53: the exact hold count reads in pages and stops at its deadline, so
  // no statement pins the WAL for long.
  await check("retention_hold_count_pages_its_read_and_stops_at_its_deadline", async () => {
    const buffer = fixture("hold-count");
    try {
      for (let i = 0; i < 2_400; i++) buffer.append(event());
      buffer.database.prepare("update buffered_events set created_at='2000-01-01T00:00:00.000Z'").run();
      const cutoff = "2001-01-01T00:00:00.000Z";
      const expected = (buffer.database.prepare("select count(*) as n from buffered_events where created_at < ?")
        .get(cutoff) as { n: number }).n;
      const paged = countRetentionHoldsOffThread(buffer.database, cutoff, "e.input_tokens >= 0", { pageRows: 500 });
      paged.worker.ref(); // the daemon keeps its loop alive; this script has nothing else running
      const count = await paged.result;
      await paged.exited;
      assert.equal(count, expected);
      assert.deepEqual({ pages: paged.progress()?.pages, rows: paged.progress()?.rows }, { pages: 5, rows: expected });
      // A predicate that scans the table for every row cannot finish 2,400 rows inside the deadline.
      const slow = countRetentionHoldsOffThread(buffer.database, cutoff,
        "(select count(*) from buffered_events x where x.created_at <= e.created_at) >= 0", { pageRows: 200, maxMs: 20 });
      slow.worker.ref();
      const started = performance.now();
      const outcome = await slow.result.then(() => "completed", (error: Error) => error.message);
      const waitedMs = performance.now() - started;
      await slow.exited;
      assert.equal(outcome, "retention_hold_count_interrupted");
      assert.ok(waitedMs < 10_000, `interrupted after ${waitedMs.toFixed(0)} ms`);
      const seen = slow.progress();
      assert.ok(seen && seen.pages >= 1 && seen.rows < expected, `stopped after ${JSON.stringify(seen)}`);
      return { expected, pages: paged.progress()?.pages, interruptedAfterMs: Number(waitedMs.toFixed(0)),
        pagesBeforeStop: seen?.pages, rowsBeforeStop: seen?.rows };
    } finally { buffer.close(); }
  });
  // 0.7.53: while a reader pins a large WAL, the cadence defers instead of growing the file.
  await check("retention_cadence_defers_while_a_reader_pins_a_large_wal", () => {
    const bound = RETENTION_WAL_PRESSURE_BYTES;
    assert.deepEqual([
      walPressureDefersRetention(null), walPressureDefersRetention({ heldByReader: false, walBytes: bound * 4 }),
      walPressureDefersRetention({ heldByReader: true, walBytes: bound - 1 }), walPressureDefersRetention({ heldByReader: true, walBytes: bound }),
      walPressureDefersRetention({ heldByReader: true, walBytes: null }),
    ], [false, false, false, true, false]);
    const buffer = fixture("wal-pressure");
    try {
      for (let i = 0; i < 8; i++) buffer.append(event());
      buffer.database.prepare("update buffered_events set created_at='2000-01-01T00:00:00.000Z'").run();
      let checkpoint = { heldByReader: true, walBytes: bound * 2 };
      let serial = 0;
      const pending = new Map<number, { callback: () => void; delay: number }>();
      const cadence = new AutomaticRetentionCadence(
        (request) => buffer.prune(90, { maxRows: request.maxRows, busyTimeoutMs: 10 }),
        { canRun: () => !walPressureDefersRetention(checkpoint), followupMs: 5_000, intervalMs: 60 * 60_000,
          passRows: { base: 4, max: 8, maxWriterMs: 250 },
          timer: { now: () => now,
            setTimeout: (callback, delay) => { const id = ++serial; pending.set(id, { callback, delay }); return id; },
            clearTimeout: handle => { pending.delete(handle as number); } } });
      try {
        cadence.start();
        const run = () => { const [id, task] = pending.entries().next().value!; pending.delete(id); task.callback(); };
        run();
        assert.deepEqual({ deferred: cadence.status().counters.deferred, passes: cadence.status().counters.passes,
          nextDelay: pending.values().next().value?.delay }, { deferred: 1, passes: 0, nextDelay: 5_000 });
        run();
        assert.equal(cadence.status().counters.deferred, 2);
        checkpoint = { heldByReader: false, walBytes: 4_096 };
        run();
        assert.deepEqual({ passes: cadence.status().counters.passes, expired: cadence.status().counters.eventsExpired },
          { passes: 1, expired: 4 });
        return { deferred: 2, passes: 1 };
      } finally { cadence.stop(); }
    } finally { buffer.close(); }
  });
  await check("retention_backlog_of_half_a_million_rows_clears_in_bounded_passes", () => {
    const buffer = new LocalEventBuffer(path.join(root, "backlog.sqlite"));
    try {
      const old = "2000-01-01T00:00:00.000Z";
      const total = 500_000;
      const insert = buffer.database.prepare(`insert into buffered_events
        (id,source,event_type,data_mode,observed_at,payload_json,created_at)
        values (?, 'codex', 'assistant_response', 'metadata', ?, '{}', ?)`);
      const seeded = performance.now();
      buffer.database.transaction(() => {
        for (let i = 0; i < total; i++) insert.run(`backlog-${String(i).padStart(7, "0")}`, old, old);
      })();
      const seedMs = performance.now() - seeded;
      const count = () => (buffer.database.prepare("select count(*) as n from buffered_events").get() as { n: number }).n;
      assert.equal(count(), total);
      assert.equal(buffer.rawRetentionBacklogAtLeast(90, 50_000, new Date(now)), true);
      // Real time first: a one-millisecond writer budget cuts a 4,096-row page
      // short after at least one row, and the cut page reports more work.
      const cut = buffer.prune(90, { maxRows: 4_096, maxWriterMs: 1, now: new Date(now) });
      assert.deepEqual({ hasMore: cut.hasMore, progress: cut.madeProgress, busy: cut.busy },
        { hasMore: true, progress: true, busy: false });
      assert.ok(cut.eventRowsVisited >= 1 && cut.eventRowsVisited < 4_096, `cut page visited ${cut.eventRowsVisited}`);
      // Then the cadence on a fake timer and a fake writer clock: 0.02 ms a
      // call, with one 1,000 ms stall injected into the first full-size pass.
      let fake = 0, stallArmed = -1, stalled = false;
      const clock = () => {
        fake += 0.02;
        if (stallArmed >= 0 && ++stallArmed === 2) { fake += 1_000; stallArmed = -1; }
        return fake;
      };
      let tick = now, serial = 0;
      const pending = new Map<number, { callback: () => void; delay: number }>();
      const cadence = new AutomaticRetentionCadence(
        (request) => buffer.prune(90, { maxRows: request.maxRows, maxWriterMs: request.maxWriterMs, now: new Date(now), clock }),
        { followupMs: 5_000, fastFollowupMs: 1_000, intervalMs: 60 * 60_000,
          passRows: { base: 128, max: 4_096, maxWriterMs: 250 },
          largeBacklog: () => buffer.rawRetentionBacklogAtLeast(90, 50_000, new Date(now)),
          timer: { now: () => tick,
            setTimeout: (callback, delay) => { const id = ++serial; pending.set(id, { callback, delay }); return id; },
            clearTimeout: handle => { pending.delete(handle as number); } } },
      );
      let passes = 0, maxPassMs = 0, fastFollowups = 0, overBudgetPasses = 0, maxRowsVisited = 0;
      let nextDelay: number | undefined, overBudgetNextDelay: number | undefined, slowFollowupsUnderBacklog = 0;
      const wall = performance.now();
      try {
        cadence.start();
        while (pending.size && passes < 5_000) {
          const [id, task] = pending.entries().next().value!;
          pending.delete(id); tick += task.delay;
          if (!stalled && cadence.status().passRows === 4_096) { stallArmed = 0; stalled = true; }
          task.callback(); passes += 1;
          const last = cadence.status().lastPass;
          if (last) {
            maxPassMs = Math.max(maxPassMs, last.elapsedMs ?? 0);
            maxRowsVisited = Math.max(maxRowsVisited, last.eventRowsVisited);
            if ((last.elapsedMs ?? 0) > 250) overBudgetPasses += 1;
          }
          const next = pending.values().next().value;
          if (next?.delay === 1_000) fastFollowups += 1;
          if (last && (last.elapsedMs ?? 0) > 250) overBudgetNextDelay = next?.delay;
          // A 5 s wait is right only once the backlog has shrunk below "large".
          if (next?.delay === 5_000 && last?.hasMore &&
            buffer.rawRetentionBacklogAtLeast(90, 50_000, new Date(now))) slowFollowupsUnderBacklog += 1;
          if (cadence.status().counters.passes > 0 && !last?.hasMore) break;
        }
        // Read the next delay before stop() clears the timer.
        nextDelay = pending.values().next().value?.delay;
      } finally { cadence.stop(); }
      const wallMs = performance.now() - wall;
      const status = cadence.status();
      assert.equal(count(), 0, "every backlog row expired");
      assert.equal(status.counters.eventsExpired, total - cut.events);
      assert.equal(status.counters.busyDeferrals, 0);
      assert.ok(passes <= 1_500, `${passes} passes`);
      assert.ok(stalled, "the pass size reached its maximum");
      assert.ok(maxRowsVisited <= 4_096, `largest page ${maxRowsVisited} rows`);
      // 128 to 4,096 is five doublings; the stalled pass halves once and the
      // next pass, inside its budget, doubles back.
      assert.deepEqual({ grown: status.counters.grownPasses, shrunk: status.counters.shrunkPasses, overBudget: overBudgetPasses },
        { grown: 6, shrunk: 1, overBudget: 1 });
      assert.ok(fastFollowups > 0, "the follow-up shortened under the large backlog");
      assert.equal(overBudgetNextDelay, Math.min(5_000, Math.max(1_000, maxPassMs)),
        "the overrun pass was followed after its own duration, its page already halved");
      assert.equal(slowFollowupsUnderBacklog, 0, "no 5 s wait while the backlog was large");
      assert.ok(maxPassMs >= 1_000 && maxPassMs < 1_100, `stalled pass ${maxPassMs} ms`);
      assert.equal(status.passRows, 128, "pass size resets once the backlog is gone");
      assert.equal(buffer.rawRetentionBacklogAtLeast(90, 1, new Date(now)), false);
      assert.equal((buffer.database.prepare("select count(*) as n from raw_retention_receipts").get() as { n: number }).n, total);
      assert.equal(nextDelay, 60 * 60_000, "an empty backlog rests for the full interval");
      return { passes, grownPasses: status.counters.grownPasses, shrunkPasses: status.counters.shrunkPasses,
        fastFollowups, overBudgetPasses, overBudgetNextDelay, cutPageRows: cut.eventRowsVisited, maxRowsVisited,
        maxPassMs: Number(maxPassMs.toFixed(1)), seedMs: Number(seedMs.toFixed(0)), wallMs: Number(wallMs.toFixed(0)) };
    } finally { buffer.close(); }
  });
  await check("secondary_retention_expires_only_settled_receipts_and_orphan_summary_rows", () => {
    const buffer = new LocalEventBuffer(path.join(root, "secondary.sqlite"));
    try {
      const db = buffer.database;
      // A ledger without the lazily created session-summary schema sweeps receipts only.
      const bare = buffer.pruneSecondary({ retentionDays: 90 }, { maxRows: 10, now: new Date(now), sweepIntervalMs: 0 });
      assert.deepEqual({ summaries: bare.summaryRowsVisited, hasMore: bare.hasMore, busy: bare.busy },
        { summaries: 0, hasMore: false, busy: false });
      ensureSessionSummarySchema(db);
      const items = Array.from({ length: 6 }, () => event());
      for (const item of items) buffer.append(item);
      const rowids = items.map(item => (db.prepare("select rowid as r from buffered_events where id=?").get(item.id) as { r: number }).r);
      const at = (daysAgo: number) => new Date(now - daysAgo * 86_400_000).toISOString();
      const receipt = db.prepare(`insert into upload_receipts
        (delivery_id,raw_rowid,raw_id,raw_created_at,raw_generation,terminal_state,reason,status_class,attempt_count,created_at,terminal_at)
        values (?,?,?,?,null,?,?,?,1,?,?)`);
      // d0: acknowledged 40 days ago, raw row gone -> expires.
      // d1: acknowledged 40 days ago, raw row present -> stays (its acknowledgement lets the raw row expire).
      // d2: dead 40 days ago, raw row gone -> stays (lineage the hold rule reads).
      // d3: acknowledged 10 days ago, raw row gone -> stays (inside the 30-day window).
      // d4: legacy acknowledged receipt without raw lineage, 40 days ago -> stays.
      receipt.run("d0", rowids[0], items[0]!.id, at(60), "acknowledged", "remote_acknowledged", "success", at(40), at(40));
      receipt.run("d1", rowids[1], items[1]!.id, at(60), "acknowledged", "remote_acknowledged", "success", at(40), at(40));
      receipt.run("d2", rowids[2], items[2]!.id, at(60), "dead", "local_item_oversize", "client_error", at(40), at(40));
      receipt.run("d3", rowids[3], items[3]!.id, at(60), "acknowledged", "remote_acknowledged", "success", at(10), at(10));
      db.prepare(`insert into upload_receipts (delivery_id,terminal_state,reason,status_class,attempt_count,created_at,terminal_at)
        values ('d4','acknowledged','remote_acknowledged','success',1,?,?)`).run(at(40), at(40));
      const summary = db.prepare("insert into session_sync_summary_rows (raw_rowid, session_id, created_at) values (?,?,?)");
      summary.run(rowids[1], "s-present", at(100));
      summary.run(9_000_001, "s-orphan-old", at(100));
      summary.run(9_000_002, "s-orphan-new", at(1));
      for (const index of [0, 2, 3]) db.prepare("delete from buffered_events where rowid=?").run(rowids[index]);
      const first = buffer.pruneSecondary({ retentionDays: 90 }, { maxRows: 1_000, now: new Date(now), sweepIntervalMs: 0 });
      assert.deepEqual(
        { receipts: first.uploadReceiptsExpired, summaries: first.summaryRowsExpired, hasMore: first.hasMore, busy: first.busy,
          visited: first.uploadReceiptsVisited },
        { receipts: 1, summaries: 1, hasMore: false, busy: false, visited: 5 });
      assert.deepEqual((db.prepare("select delivery_id as d from upload_receipts order by delivery_id").all() as Array<{ d: string }>).map(r => r.d),
        ["d1", "d2", "d3", "d4"]);
      assert.deepEqual((db.prepare("select session_id as s from session_sync_summary_rows order by session_id").all() as Array<{ s: string }>).map(r => r.s),
        ["s-orphan-new", "s-present"]);
      assert.deepEqual(db.prepare("select table_name as t, row_key as k, reason from secondary_retention_receipts order by t").all(), [
        { t: "session_sync_summary_rows", k: "9000001", reason: "summary_row_raw_expired" },
        { t: "upload_receipts", k: "d0", reason: "acknowledged_receipt_window_elapsed" },
      ]);
      const status = buffer.secondaryRetentionStatus();
      assert.deepEqual({ receipts: status.uploadReceipts.expiredTotal, summaries: status.summaryRows.expiredTotal,
        cursor: status.uploadReceipts.cursor, hasMore: status.lastHasMore }, { receipts: 1, summaries: 1, cursor: 0, hasMore: false });
      assert.ok(status.uploadReceipts.sweepCompletedAt && status.summaryRows.sweepCompletedAt);
      // A finished sweep rests: inside the interval the pass visits nothing.
      const resting = buffer.pruneSecondary({ retentionDays: 90 }, { maxRows: 1_000, now: new Date(now + 1_000) });
      assert.deepEqual({ visited: resting.uploadReceiptsVisited, progress: resting.madeProgress, hasMore: resting.hasMore },
        { visited: 0, progress: false, hasMore: false });
      // Paging: five more expired receipts behind the kept ones, two rows a pass, cursor carried between passes.
      for (let i = 0; i < 5; i++) {
        db.prepare(`insert into upload_receipts (delivery_id,raw_rowid,raw_id,raw_created_at,terminal_state,reason,status_class,attempt_count,created_at,terminal_at)
          values (?,?,?,?,'acknowledged','remote_acknowledged','success',1,?,?)`)
          .run(`page-${i}`, 8_000_000 + i, `raw-${i}`, at(60), at(40), at(40));
      }
      // Seven hours on, both sweeps are due again. Two rows a pass, shared by
      // both tables, cursor carried between passes: receipts page first; the
      // short fifth page completes their sweep and hands its spare row to the
      // summary table, whose sweep completes on the sixth pass while the
      // finished receipt sweep rests.
      const later = new Date(now + 7 * 3_600_000);
      const pages: Array<{ visited: number; expired: number; hasMore: boolean }> = [];
      for (let i = 0; i < 10; i++) {
        const pass = buffer.pruneSecondary({ retentionDays: 90 }, { maxRows: 2, now: later });
        pages.push({ visited: pass.uploadReceiptsVisited + pass.summaryRowsVisited, expired: pass.uploadReceiptsExpired,
          hasMore: pass.hasMore });
        if (!pass.hasMore) break;
      }
      assert.deepEqual(pages, [
        { visited: 2, expired: 0, hasMore: true }, { visited: 2, expired: 0, hasMore: true },
        { visited: 2, expired: 2, hasMore: true }, { visited: 2, expired: 2, hasMore: true },
        { visited: 2, expired: 1, hasMore: true }, { visited: 1, expired: 0, hasMore: false },
      ]);
      const paged = buffer.secondaryRetentionStatus();
      assert.deepEqual({ receipts: paged.uploadReceipts.expiredTotal, summaries: paged.summaryRows.expiredTotal,
        cursors: [paged.uploadReceipts.cursor, paged.summaryRows.cursor], hasMore: paged.lastHasMore },
        { receipts: 6, summaries: 1, cursors: [0, 0], hasMore: false });
      // A busy writer defers the secondary pass too, with nothing changed.
      const other = new Database(path.join(root, "secondary.sqlite"), { timeout: 100 });
      try {
        other.exec("begin immediate");
        const held = buffer.pruneSecondary({ retentionDays: 90 }, { maxRows: 2, now: new Date(now), sweepIntervalMs: 0, busyTimeoutMs: 10 });
        assert.deepEqual({ busy: held.busy, hasMore: held.hasMore, progress: held.madeProgress }, { busy: true, hasMore: true, progress: false });
        other.exec("rollback");
      } finally { other.close(); }
      return { expiredReceipts: 6, expiredSummaryRows: 1, pages };
    } finally { buffer.close(); }
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
