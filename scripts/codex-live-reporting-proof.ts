import assert from "node:assert/strict";
import fs from "node:fs";
import http from "node:http";
import path from "node:path";
import type { AddressInfo } from "node:net";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { createCollectorServer } from "../packages/collector-cli/src/server";
import { loadOrCreateLocalIngestAuth } from "../packages/collector-cli/src/local-auth";
import { provisionLiveProducer } from "../packages/collector-cli/src/codex-live-usage-auth";
import { canonicalJson, liveEventId } from "../packages/collector-cli/src/codex-live-usage-protocol";
import { collectSessionProjectAllocations } from "../packages/collector-cli/src/session-sync";
import { readFinanceProjectUsageProjection, type FinanceProjectionRequest } from "../packages/collector-cli/src/finance-project-usage-projection";
import { aiInteractionEventSchema } from "../packages/shared/src/schemas";
import { readLiveUsageObservation } from "../packages/shared/src/live-usage-metadata";
import { createProofCompletion } from "./lib/proof-completion";

// Actual HTTP, SQLite, projection and pruning in one disposable synthetic fixture.
// This establishes integration, not native conductor or real source completeness.
const completion = createProofCompletion("codex-live-reporting", 8);
const vectors = JSON.parse(fs.readFileSync(new URL("./fixtures/codex-live-usage-golden-r4.json", import.meta.url), "utf8")).vectors;
const baseline = vectors[0].packet, positive = vectors[1].packet;
const tenant = "33333333-3333-7333-8333-333333333333";
const device = "44444444-4444-7444-8444-444444444444";
const epochStart = "2026-09-08T08:00:00.000Z";
const period = { start: "2026-09-08T09:00:00.000Z", end: "2026-09-08T10:00:00.000Z" };
const now = new Date(Date.now() + 10_000).toISOString();
const home = path.join(process.env.PLIMSOLL_PROOF_ROOT!, "http-reporting");
fs.mkdirSync(home, { mode: 0o700 });
const ledger = path.join(home, "ledger.sqlite");
const options = { workspaceId: tenant, deviceId: device, enrollmentNow: () => new Date(epochStart), delivery: { enabled: true } };
let buffer = new LocalEventBuffer(ledger, options);
const config = collectorConfigSchema.parse({ tenantId: tenant, deviceId: device, captureRoots: [{
  rootId: "reporting-root", profileId: "reporting-profile", source: "codex",
  installationEpochId: buffer.workspaceBinding()!.currentInstallationEpochId!, directory: path.join(home, "empty-root"),
}] });
fs.mkdirSync(config.captureRoots![0].directory, { mode: 0o700 });
const ordinary = loadOrCreateLocalIngestAuth(home);
const credential = provisionLiveProducer({ home, buffer, config, producerId: baseline.producerId,
  credentialId: baseline.credentialId, captureRootId: "reporting-root", enrolledAt: epochStart });
const token = fs.readFileSync(credential.credentialFile, "utf8");
let server = createCollectorServer(config, buffer, { localAuth: ordinary, liveProducerHome: home });
let port = 0;
const start = async () => { await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve)); port = (server.address() as AddressInfo).port; };
const send = (packet: unknown) => new Promise<{ status: number; text: string; body: any }>((resolve, reject) => {
  const bytes = Buffer.from(canonicalJson(packet));
  const req = http.request({ host: "127.0.0.1", port, path: "/hooks/codex", method: "POST", headers: {
    "content-type": "application/json", "content-length": bytes.length, connection: "close",
    "x-plimsoll-producer-id": baseline.producerId, "x-plimsoll-token": token,
  } }, response => {
    const chunks: Buffer[] = [];
    response.on("data", chunk => chunks.push(chunk));
    response.on("end", () => { const text = Buffer.concat(chunks).toString(); resolve({ status: response.statusCode!, text, body: JSON.parse(text) }); });
  });
  req.setTimeout(5_000, () => req.destroy(new Error("fixture_http_timeout")));
  req.on("error", reject); req.end(bytes);
});
const count = (table: string) => (buffer.database.prepare(`select count(*) as n from ${table}`).get() as { n: number }).n;
const eventId = liveEventId(positive);
const economics = (requestedPeriod = period) => collectSessionProjectAllocations(buffer.database, { tenantId: tenant, period: requestedPeriod, now });
const request: FinanceProjectionRequest = {
  tenantRef: tenant, expectedWorkspaceId: tenant, period, now, maxSourceAgeMs: 86_400_000,
  retentionCutoff: epochStart, requiredSources: ["codex", "claude_code"], registryVersion: "registry.v1",
  sourceVersion: "finance_exact_period.v2", billingPoolBySource: {
    codex: "55555555-5555-4555-8555-555555555555", claude_code: "66666666-6666-4666-8666-666666666666",
  }, projectMappings: [],
};

// Synthetic coverage preconditions isolate the observer exclusion from unrelated
// missing-history holds. Actual source completeness is intentionally not asserted.
function publishFixtureCoverage() {
  const at = new Date().toISOString(), database = buffer.database;
  for (let i = 0; i < 4; i++) buffer.projection.runMaintenance(new Date(Date.now() + 1_000));
  database.prepare(`update dashboard_projection_control set schema_version=1,ready=1,parity_ready=1,
    dirty=0,degraded_reason=null,last_success_at=?,backfill_complete=1,parity_complete=1,metric_backfill_complete=1,
    repair_backlog=0,dirty_session_backlog=0,account_invalidation_backlog=0,compact_mutation_backlog=0,compact_gc_backlog=0`).run(at);
  database.prepare("update dashboard_window_control set target_cutoff_at=null").run();
  for (const source of ["codex", "claude_code"]) {
    const counters = { filesSeen: 1, filesRead: 1, bytesRead: 1, bytesDeferred: 0, eventsAppended: 1,
      parseErrors: 0, discoveryErrors: 0, statErrors: 0, readErrors: 0 };
    const marker = { version: 3, source, completion: { completedAt: at, ...counters },
      latestFullAttempt: { attemptedAt: at, status: "complete", reason: null, exhaustive: true, truncated: false, ...counters }, invalidation: null };
    database.prepare("insert into maintenance_state(key,value,updated_at) values(?,?,?) on conflict(key) do update set value=excluded.value,updated_at=excluded.updated_at")
      .run(`history_coverage_v2_${source}`, JSON.stringify(marker), at);
    database.prepare(`insert into capture_activity_state(source,last_activity_at,files_today,discovery_entries,last_scan_at,last_error_code,truncated)
      values(?,?,1,1,?,null,0) on conflict(source) do update set last_activity_at=excluded.last_activity_at,
      files_today=1,discovery_entries=1,last_scan_at=excluded.last_scan_at,last_error_code=null,truncated=0`).run(source, at, at);
    database.prepare(`update finance_source_coverage set retained_from=?,covered_through=?,latest_full_attempt_at=?,latest_full_complete=1,
      invalidated_at=null,last_scan_at=?,last_scan_ok=1,last_scan_truncated=0,state_revision=state_revision+1,published_revision=0
      where workspace_id=? and installation_epoch_id=? and source=?`).run(period.start, period.end, at, at, tenant,
      buffer.workspaceBinding()!.currentInstallationEpochId, source);
  }
  database.prepare("update finance_publication_control set dirty=1 where singleton=1").run();
  buffer.projection.runMaintenance(new Date(Date.now() + 2_000));
}

async function main() {
  await start();
  try {
    publishFixtureCoverage();
    const initialFinance = readFinanceProjectUsageProjection(buffer.database, request);
    assert.deepEqual(initialFinance.reasons, []);
    assert.equal(initialFinance.input.envelope.coverage.complete, true);
    assert.equal((await send(baseline)).text, vectors[0].receiptCanonicalUtf8);
    assert.equal(count("buffered_events"), 0); assert.equal(count("codex_live_pins"), 1);
    completion.check("healthy_fixture_and_authenticated_baseline_have_no_positive_usage");

    const accepted = await send(positive);
    assert.equal(accepted.status, 200); assert.equal(accepted.text, vectors[1].receiptCanonicalUtf8);
    const raw = buffer.database.prepare("select payload_json as payload,rowid from buffered_events where id=?").get(eventId) as { payload: string; rowid: number };
    const event = aiInteractionEventSchema.parse(JSON.parse(raw.payload));
    const observation = readLiveUsageObservation(event.metadata!, event.observedAt)!;
    assert.equal(observation.intervalStart, baseline.capturedAt); assert.equal(observation.intervalEnd, positive.capturedAt);
    assert.equal(observation.totalTokens, positive.total.totalTokens); assert.equal(observation.attributionState, "unresolved");
    assert.equal(count("upload_outbox"), 1);
    const deliveryId = (buffer.database.prepare("select delivery_id from upload_outbox where raw_id=?").get(eventId) as { delivery_id: string }).delivery_id;
    assert.notEqual(deliveryId, eventId);
    const outbound = JSON.parse((buffer.database.prepare("select base_envelope_json as envelope from upload_outbox where delivery_id=?").get(deliveryId) as { envelope: string }).envelope);
    assert.equal(outbound.event.metadata.sourcePayloadDigest, event.metadata.sourcePayloadDigest);
    assert.equal(outbound.event.metadata.sourceEventId, eventId);
    assert.equal(outbound.event.metadata.logicalSourceEventId, eventId);
    assert.equal(outbound.event.id, deliveryId);
    const fact = buffer.database.prepare("select live_usage_json,model,cost_nanos,project_key,account_hash from dashboard_event_facts where raw_rowid=?").get(raw.rowid) as any;
    assert.deepEqual(JSON.parse(fact.live_usage_json), observation);
    for (const key of ["model", "cost_nanos", "project_key", "account_hash"]) assert.equal(fact[key], null);
    completion.check("real_http_positive_marginal_reaches_shared_schema_outbox_and_unallocated_projection");

    const retry = await send(positive);
    assert.deepEqual(retry.body, { ...accepted.body, replayed: true });
    assert.equal(count("buffered_events"), 1); assert.equal(count("upload_outbox"), 1);
    completion.check("lost_ack_replay_preserves_one_event_and_one_delivery");

    publishFixtureCoverage();
    const before = economics();
    assert.equal(before.usage.inputTokens, positive.total.inputTokens); assert.equal(before.usage.outputTokens, positive.total.outputTokens);
    assert.equal(before.coverage.state, "partial"); assert.equal(before.costs.unpricedEvents, 1);
    const finance = readFinanceProjectUsageProjection(buffer.database, request);
    assert.deepEqual(finance.reasons, ["OBSERVED_INTERVAL_UNQUALIFIED"]);
    assert.equal(finance.input.source.records.length, 0); assert.equal(finance.input.envelope.coverage.coveredThrough, period.start);
    const crossing = economics({ start: "2026-09-08T09:00:02.000Z", end: period.end });
    assert.equal(crossing.usage.events, 0); assert.ok(crossing.evidenceGaps.includes("observed_interval_crosses_period"));
    completion.check("whole_interval_economics_and_finance_hold_follow_the_real_admitted_event");

    const receipts = buffer.database.prepare("select receipt_json from codex_live_receipts order by packet_key").all();
    const pin = buffer.database.prepare("select * from codex_live_pins").get();
    // Synthetic transport completion; do not contact a cloud endpoint.
    assert.equal(buffer.markUploaded([eventId], now), 1);
    assert.equal(buffer.prune(0, { maxRows: 64, now: new Date(Date.now() + 86_400_000) }).events, 1);
    assert.equal(count("buffered_events"), 0); assert.equal(count("dashboard_live_usage_retained"), 1);
    await new Promise<void>(resolve => server.close(() => resolve())); buffer.close();
    buffer = new LocalEventBuffer(ledger, options);
    server = createCollectorServer(config, buffer, { localAuth: ordinary, liveProducerHome: home }); await start();
    for (let i = 0; i < 4; i++) buffer.projection.runMaintenance(new Date(Date.now() + 3_000));
    assert.deepEqual(buffer.database.prepare("select receipt_json from codex_live_receipts order by packet_key").all(), receipts);
    assert.deepEqual(buffer.database.prepare("select * from codex_live_pins").get(), pin);
    assert.equal((await send(positive)).body.replayed, true); assert.equal(count("buffered_events"), 0);
    const retained = economics();
    assert.equal(retained.usage.inputTokens, before.usage.inputTokens); assert.equal(retained.usage.outputTokens, before.usage.outputTokens);
    completion.check("actual_raw_prune_restart_and_exact_retry_preserve_interval_and_pin");

    const retainedFinance = readFinanceProjectUsageProjection(buffer.database, request);
    assert.ok(retainedFinance.reasons.includes("OBSERVED_INTERVAL_UNQUALIFIED"));
    assert.equal(retainedFinance.input.source.records.length, 0); assert.equal(retainedFinance.input.envelope.coverage.coveredThrough, period.start);
    completion.check("retained_observer_keeps_finance_watermark_at_period_start");

    assert.equal(buffer.append({ id: "ordinary-takeover", source: "codex", dataMode: "metadata", eventType: "assistant_response",
      sessionId: baseline.threadId, observedAt: positive.capturedAt, tenantId: tenant, intent: "unknown", actionClass: "other", inputTokens: 999, metadata: {} }), false);
    assert.equal(count("buffered_events"), 0); assert.deepEqual(buffer.database.prepare("select * from codex_live_pins").get(), pin);
    completion.check("pruned_pinned_session_rejects_ordinary_token_takeover");

    buffer.database.prepare(`insert into upload_receipts(delivery_id,terminal_state,reason,status_class,attempt_count,created_at,terminal_at)
      values (?,'dead','local_privacy_violation','local',0,?,?)
      on conflict(delivery_id) do update set terminal_state='dead',reason='local_privacy_violation'`).run(deliveryId, now, now);
    assert.equal(count("dashboard_live_usage_retained"), 0); assert.equal(economics().usage.events, 0);
    assert.deepEqual(buffer.database.prepare("select * from codex_live_pins").get(), pin);
    completion.check("late_privacy_receipt_withdraws_usage_without_releasing_authority");
    completion.complete();
    console.log(JSON.stringify({ state: "PASS_HTTP_LIVE_REPORTING_INTEGRATION", checks: 8, nativeProof: false }));
  } finally { await new Promise<void>(resolve => server.close(() => resolve())); buffer.close(); fs.rmSync(home, { recursive: true, force: true }); }
}
main().catch(error => { console.error(error); process.exitCode = 1; });
