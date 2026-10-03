import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import type { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { DashboardProjectionStore } from "../packages/collector-cli/src/dashboard-projection";
import { DeliveryOutbox } from "../packages/collector-cli/src/outbox";
import { ensureSessionSummarySchema } from "../packages/collector-cli/src/session-summary";
import { ensureCodexReconciliationSchema } from "../packages/collector-cli/src/codex-reconciliation";
import { ensureSessionContextIndexSchema } from "../packages/collector-cli/src/session-context-index";
import { ensureCodexLiveUsageSchema } from "../packages/collector-cli/src/codex-live-usage-ledger";
import { LearningFactStore } from "../packages/collector-cli/src/learning-facts";
import { ensureCaptureBaselineSchema } from "../packages/collector-cli/src/capture-baseline";
import { ensureRepoContextReplaySchema, writeRepoContextDrainReceipt,
  disabledRepoContextDrainReceipt } from "../packages/collector-cli/src/repo-context-replay-state";
import { ensureFinanceProvenanceSchema } from "../packages/collector-cli/src/history-coverage";
import { CollectorMaintenance } from "../packages/collector-cli/src/maintenance";
import { ensureMaintenanceStageSchema } from "../packages/collector-cli/src/maintenance-stage-primitives";
import { ensureRepoContextLinkDispositionSchema } from "../packages/collector-cli/src/repo-context-link-dispositions";
import { GrokUsageTailer } from "../packages/collector-cli/src/grok-usage-tailer";
import { ensureCaptureFrontierSchema } from "../packages/collector-cli/src/capture-frontier";
import { bindCaptureInventory } from "../packages/collector-cli/src/capture-root-inventory";
import { ensureJsonlContinuationStore } from "../packages/collector-cli/src/jsonl-continuation";
import { emptyDaemonSessionSyncState, saveDaemonSessionSyncState } from "../packages/collector-cli/src/session-sync";
import { rememberCaptureRotation } from "../packages/collector-cli/src/capture-fairness";
import { ensureJsonlScanState } from "../packages/collector-cli/src/jsonl-byte-tailer";
import { OtlpIntakeSpool } from "../packages/collector-cli/src/otlp-spool";
import { TranscriptTailer } from "../packages/collector-cli/src/transcript-tailer";
import { recordAccountAssertionSalt } from "../packages/collector-cli/src/account-assertion";
import { recordMaintenanceDeadlineKill } from "../packages/collector-cli/src/maintenance-starvation";
import { recordRuntimeFactDrop } from "../packages/collector-cli/src/runtime-fact-drops";
import { markRawPrivacyDisposition } from "../packages/collector-cli/src/privacy-disposition";
import { runRepoContextDrainStage } from "../packages/collector-cli/src/repo-context-drain";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { REQUIRED_REBUILD_WRITERS, observeRebuildConnectionOwnership } from
  "../packages/collector-cli/src/maintenance-rebuild";

export type WriterRouteObservation = {
  module: string;
  entryPoint: string;
  token: string;
  changeDelta: number;
};

/** Invoke each daemon writer's own entry point on the same leased connection.
 * The caller has started a maintenance pause listener and owns a copied ledger. */
export async function exerciseRebuildWriterRoutes(buffer: LocalEventBuffer, ledger: string,
  home: string): Promise<WriterRouteObservation[]> {
  const db = buffer.database;
  const tokenDir = `${ledger}.rebuild-open-leases`;
  const held = observeRebuildConnectionOwnership(ledger);
  assert.equal(held.openTokens.length, 1);
  assert.ok(held.openTokens[0]?.pid === process.pid);
  assert.ok(held.writerLeases.some((lease) => lease.pid === process.pid && lease.owner === "local_event_buffer"));
  const token = held.openTokens[0]!.token;
  const rows: WriterRouteObservation[] = [{ module: "buffer", entryPoint: "LocalEventBuffer.constructor",
    token, changeDelta: 0 }];
  const route = async (module: string, entryPoint: string, call: () => unknown | Promise<unknown>) => {
    const changes = () => (db.prepare("select total_changes() as n").get() as { n: number }).n;
    const before = changes();
    await call();
    const leases = fs.readdirSync(tokenDir).filter((entry) => entry.endsWith(".lease"));
    assert.deepEqual(leases, [token], `${module} must retain the observed physical lease`);
    rows.push({ module, entryPoint, token, changeDelta: changes() - before });
  };
  await route("dashboard-projection", "DashboardProjectionStore.constructor", () => { new DashboardProjectionStore(db); });
  await route("outbox", "DeliveryOutbox.constructor", () => { new DeliveryOutbox(db); });
  await route("session-summary", "ensureSessionSummarySchema", () => ensureSessionSummarySchema(db));
  await route("codex-reconciliation", "ensureCodexReconciliationSchema", () => ensureCodexReconciliationSchema(db));
  await route("session-context-index", "ensureSessionContextIndexSchema", () => ensureSessionContextIndexSchema(db));
  await route("codex-live-usage-ledger", "ensureCodexLiveUsageSchema", () => ensureCodexLiveUsageSchema(db));
  await route("learning-facts", "LearningFactStore.constructor", () => { new LearningFactStore(db); });
  await route("capture-baseline", "ensureCaptureBaselineSchema", () => ensureCaptureBaselineSchema(db));
  await route("repo-context-replay-state", "writeRepoContextDrainReceipt", () => {
    ensureRepoContextReplaySchema(db);
    writeRepoContextDrainReceipt(db, disabledRepoContextDrainReceipt());
  });
  await route("history-coverage", "ensureFinanceProvenanceSchema", () => ensureFinanceProvenanceSchema(db));
  await route("maintenance-stage-primitives", "ensureMaintenanceStageSchema", () => ensureMaintenanceStageSchema(db));
  await route("repo-context-link-dispositions", "ensureRepoContextLinkDispositionSchema",
    () => ensureRepoContextLinkDispositionSchema(db));
  await route("capture-frontier", "ensureCaptureFrontierSchema", () => ensureCaptureFrontierSchema(db));
  await route("capture-root-inventory", "bindCaptureInventory", () => {
    bindCaptureInventory(db, "codex", [{ rootId: "b13-proof", profileId: "b13-proof",
      installationEpochId: "b13-proof", source: "codex", directory: home }]);
  });
  await route("jsonl-continuation", "ensureJsonlContinuationStore", () => ensureJsonlContinuationStore(db));
  await route("session-sync", "saveDaemonSessionSyncState", () =>
    saveDaemonSessionSyncState(db, emptyDaemonSessionSyncState()));
  await route("capture-fairness", "rememberCaptureRotation", () =>
    rememberCaptureRotation(db, "codex", "b13-proof"));
  await route("jsonl-byte-tailer", "ensureJsonlScanState", () => ensureJsonlScanState(db));
  await route("account-assertion", "recordAccountAssertionSalt", () =>
    recordAccountAssertionSalt(db, "b13-proof", "v1"));
  await route("maintenance-starvation", "recordMaintenanceDeadlineKill", () =>
    recordMaintenanceDeadlineKill(db));
  await route("runtime-fact-drops", "recordRuntimeFactDrop", () =>
    recordRuntimeFactDrop(db, "invalid_signal"));
  await route("privacy-disposition", "markRawPrivacyDisposition", () =>
    markRawPrivacyDisposition(db, -1, "local_evidence_quarantined", new Date().toISOString()));
  let rollout!: RolloutTailer;
  let transcript!: TranscriptTailer;
  let grok!: GrokUsageTailer;
  await route("rollout-tailer", "RolloutTailer.constructor", () => {
    rollout = new RolloutTailer(buffer, path.join(home, "codex-sessions"), () => []);
  });
  await route("transcript-tailer", "TranscriptTailer.constructor", () => {
    transcript = new TranscriptTailer(buffer, path.join(home, "claude-projects"));
  });
  await route("grok-usage-tailer", "GrokUsageTailer.constructor", () => {
    grok = new GrokUsageTailer(buffer, null);
  });
  await route("maintenance", "CollectorMaintenance.constructor", () => {
    new CollectorMaintenance(buffer, rollout, transcript, undefined, grok);
  });
  await route("repo-context-drain", "runRepoContextDrainStage", () => {
    const config = collectorConfigSchema.parse({}).repoContextDrain;
    runRepoContextDrainStage(buffer, { config: { ...config, enabled: false }, captureRoots: [],
      captureElapsedMs: 0, freshContextsUsed: 0, freshDeferred: 0, remainingJobMs: 0,
      remainingLookupMs: 0 });
  });
  const spool = new OtlpIntakeSpool({ home });
  await route("otlp-spool", "OtlpIntakeSpool.drain", () => spool.drain(buffer));
  spool.stopDrain();
  rollout.close();
  grok.close();
  assert.equal(rows.length, 29);
  const independent = ["outcome-timeline-store", "learning-materializer"];
  assert.deepEqual(REQUIRED_REBUILD_WRITERS.filter((name) => !rows.some((row) => row.module === name)), independent);
  return rows;
}
