import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { MaintenanceProcessBoundary, type MaintenanceBoundaryChild } from "../packages/collector-cli/src/maintenance-boundary";
import { EnrichmentProcessBoundary } from "../packages/collector-cli/src/enrichment-job";
import { MAINTENANCE_PROTOCOL_SCHEMA } from "../packages/collector-cli/src/maintenance-protocol";

const checks: string[] = [];
const sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
const result = {
  recentOnly: true,
  rollout: { filesRead: 1, parseErrors: 0, eventsAppended: 1, activity: { discoveryEntries: 1 } },
  transcript: { filesRead: 0, parseErrors: 0, eventsAppended: 0, activity: { discoveryEntries: 0 } },
  reconciliation: { rowsChanged: 0, rowsVisited: 0 }, repricing: { repriced: 0, rowsVisited: 0 },
  enrichment: { backward: 0, forward: 0, rowsVisited: 0 }, rawEventWrites: 1,
  stageTimings: { codexCaptureMs: 1, claudeCaptureMs: 2, reconciliationMs: 3, repricingMs: 4, enrichmentMs: 5, projectionDrainMs: 6, totalMs: 21 },
};
class Child extends EventEmitter {
  pid = 24_200;
  connected = true;
  exitCode: number | null = null;
  signals: NodeJS.Signals[] = [];
  complete = true;
  enrichment = false;
  closeOnSignal = true;
  send(raw: unknown, callback?: (error: Error | null) => void) {
    const r = raw as { type: string; generation: number; nonce: string };
    callback?.(null);
    if (r.type === "shutdown") queueMicrotask(() => this.close());
    if (r.type === "run" && this.complete) queueMicrotask(() => {
      this.emit("message", { schema: this.enrichment ? 1 : MAINTENANCE_PROTOCOL_SCHEMA, type: "result", generation: r.generation, nonce: r.nonce, sequence: 1, ...(this.enrichment ? { rows: 1, ms: 1 } : { result, repoContexts: [] }) });
      if (this.enrichment) this.close();
    });
    return true;
  }
  kill(signal: NodeJS.Signals) {
    this.signals.push(signal);
    if (this.closeOnSignal) queueMicrotask(() => this.close());
    return true;
  }
  close() { if (!this.connected) return; this.connected = false; this.exitCode = 0; this.emit("close", 0, null); }
}
function ready(child: Child, nonce: string) { queueMicrotask(() => child.emit("message", { schema: child.enrichment ? 1 : MAINTENANCE_PROTOCOL_SCHEMA, type: "ready", spawnNonce: nonce })); }

async function main() {
  let probes = 0;
  let child!: Child;
  const startup = new MaintenanceProcessBoundary({
    entryPath: "injected", deadlineMs: 100, readyDeadlineMs: 100, termGraceMs: 5, killGraceMs: 5,
    fingerprint: async () => ++probes === 1 ? null : "owned-spawn",
    spawnChild: nonce => { child = new Child(); ready(child, nonce); return child as unknown as MaintenanceBoundaryChild; },
  });
  assert.equal((await startup.run()).rawEventWrites, 1);
  assert.equal(probes, 2); assert.equal(startup.status().reap.orphanRisk, false);
  assert.equal(await startup.shutdown(), true);
  checks.push("transient_initial_observation_recovers_without_restart");

  let identity: string | null = "original-spawn";
  let spawnCount = 0;
  const unknown = new MaintenanceProcessBoundary({
    entryPath: "injected", deadlineMs: 10, readyDeadlineMs: 100, termGraceMs: 5, killGraceMs: 5,
    initialCircuitMs: 1, orphanRetryBackoffMs: 1,
    fingerprint: async () => identity,
    spawnChild: nonce => { spawnCount++; child = new Child(); child.complete = false; child.closeOnSignal = false; ready(child, nonce); return child as unknown as MaintenanceBoundaryChild; },
  });
  await assert.rejects(unknown.run(), /maintenance_deadline_exceeded/);
  const signalsBefore = child.signals.length;
  identity = null;
  await assert.rejects(unknown.run(), /maintenance_child_not_reaped/);
  assert.equal(unknown.status().childPresent, true); assert.equal(unknown.status().reap.orphanRisk, true);
  assert.equal(spawnCount, 1); assert.equal(child.signals.length, signalsBefore);
  checks.push("missing_observation_is_not_exit_and_never_spawns_second_child");
  identity = "foreign-spawn";
  await assert.rejects(unknown.run(), /maintenance_child_not_reaped/);
  assert.equal(child.signals.length, signalsBefore); assert.equal(spawnCount, 1);
  checks.push("changed_fingerprint_never_signals_foreign_process");
  child.close(); assert.equal(unknown.status().childPresent, false); assert.equal(await unknown.shutdown(), true);
  checks.push("late_close_clears_unknown_fence");

  let verified = false;
  let enrichmentSpawns = 0;
  let old!: Child;
  const enrichment = new EnrichmentProcessBoundary({
    entryPath: "injected", deadlineMs: 100, readyDeadlineMs: 100, termGraceMs: 5, killGraceMs: 5,
    verifyChild: async () => verified,
    spawnChild: nonce => { const c = new Child(); c.enrichment = true; old = c; enrichmentSpawns++; ready(c, nonce); return c; },
  });
  await assert.rejects(enrichment.run(), /enrichment_child_not_reaped/);
  assert.equal(enrichment.status().orphanRisk, true); assert.equal(old.signals.length, 0);
  assert.equal(old.listenerCount("close"), 1);
  checks.push("failed_enrichment_keeps_exit_listener_and_sends_no_unverified_signal");
  verified = true;
  const recovered = await enrichment.run();
  assert.equal(recovered.outcome, "completed"); assert.equal(enrichmentSpawns, 2);
  assert.equal(enrichment.status().orphanRisk, false); assert.equal(enrichment.status().reapedChildren, 2);
  checks.push("next_enrichment_run_reaps_verified_old_child_before_spawn");
  assert.equal(await enrichment.shutdown(), true);
  await assert.rejects(enrichment.run(), /enrichment_boundary_stopping/);
  checks.push("enrichment_shutdown_is_terminal");

  verified = false;
  const late = new EnrichmentProcessBoundary({
    entryPath: "injected", deadlineMs: 100, readyDeadlineMs: 100,
    verifyChild: async () => verified,
    spawnChild: nonce => { old = new Child(); old.enrichment = true; ready(old, nonce); return old; },
  });
  await assert.rejects(late.run(), /enrichment_child_not_reaped/);
  old.emit("error", new Error("synthetic late error"));
  old.close(); await sleep(1);
  assert.equal(late.status().childPresent, false); assert.equal(late.status().orphanRisk, false);
  assert.equal(old.listenerCount("close"), 0); assert.equal(await late.shutdown(), true);
  checks.push("late_enrichment_error_and_exit_are_observed_after_failure");
  console.log(JSON.stringify({ proof: "child_identity_recovery", completed: true, checks: checks.length, passed: checks.length, failed: 0, cases: checks }, null, 2));
}
main().catch(error => { console.error(error); process.exitCode = 1; });
