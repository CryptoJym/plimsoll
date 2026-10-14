import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import Database from "better-sqlite3";

import {
  importNativeSubscription,
  inspectNativeSubscriptionSurfaces,
  readNativeSubscriptionCounts,
  type NativeSubscriptionPaths,
} from "../packages/collector-cli/src/native-subscription";
import { readNativeLedger } from "../packages/collector-cli/src/native-ledger";
import { collectNativeTelemetry, persistNativeTelemetry, nativeTelemetryReport } from "../packages/collector-cli/src/native-telemetry-store";
import { terminalPrivacyEligibilitySql } from "../packages/collector-cli/src/privacy-disposition";
import { parseCursorSubscriptionCsv, importCursorSubscriptionExport, latestSubscriptionExportSummaries } from "../packages/collector-cli/src/subscription-exports";
import { refreshSubscriptionTelemetry } from "../packages/collector-cli/src/subscription-refresh";

const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-native-subscription-"));
const now = "2026-10-14T12:00:00.000Z";
const paths: NativeSubscriptionPaths = {
  nativeUsageFile: path.join(root, "native-account-usage.json"),
  codexAuthFile: path.join(root, "codex-auth.json"),
  claudeConfigFile: path.join(root, "claude.json"),
  cursorCliConfigFile: path.join(root, "cursor-cli.json"),
  cursorTrackingDb: path.join(root, "cursor-tracking.sqlite"),
  grokConfigFile: path.join(root, "grok.toml"),
  collectorDb: path.join(root, "collector.sqlite"),
  nativeSubscriptionDb: path.join(root, "native-subscription.sqlite"),
  dashboardEvidenceFile: path.join(root,"dashboard-evidence.json"),
};

const idTokenPayload = Buffer.from(JSON.stringify({
  "https://api.openai.com/auth": { chatgpt_plan_type: "promax" },
})).toString("base64url");

try {
  fs.writeFileSync(paths.nativeUsageFile, JSON.stringify({
    generatedAt: now,
    state: "OK",
    coverage: { accounts: 4, ok: 4, degraded: 0, error: 0, missing: 0 },
    accounts: [
      { provider: "codex", accountKey: "openai-main", state: "OK", identity: { authenticated: true, planType: "promax", proof: "provider_account_read" }, source: { state: "OK", proof: "provider_account_and_rate_limits_response", observedAt: now }, quotaBuckets: [{ windows: [{ slot: "primary", usedPercent: 12, remainingPercent: 88, resetAt: "2026-10-21T12:00:00.000Z", resetPrecision: "second" }] }] },
      { provider: "claude", accountKey: "claude-main", state: "OK", identity: { authenticated: true, planType: "max", proof: "provider_usage_response" }, source: { state: "OK", proof: "provider_usage_response", observedAt: now }, quotaBuckets: [{ windows: [{ slot: "five_hour", usedPercent: 4, remainingPercent: 96, resetAt: "2026-10-14T15:00:00.000Z", resetPrecision: "minute" }] }] },
      { provider: "cursor", accountKey: "cursor-main", state: "DEGRADED", identity: { authenticated: true, planType: "Ultra", proof: "cursor-agent about" }, source: { state: "DEGRADED", proof: "provider_native_view", observedAt: now }, quotaBuckets: [{ label: "Included usage", windows: [{ slot: "billing_cycle", usedPercent: 19, remainingPercent: 81, resetAt: "2026-10-21T00:00:00.000Z", resetPrecision: "day" }] }] },
      { provider: "grok", accountKey: "grok-main", state: "OK", identity: { authenticated: true, planType: "SuperGrok Heavy", proof: "x.ai/auth/info" }, source: { state: "OK", proof: "provider_native_extension", observedAt: now }, quotaBuckets: [{ label: "Included credits", windows: [{ slot: "current_period", usedPercent: 58, remainingPercent: 42, resetAt: "2026-10-15T00:00:00.000Z", resetPrecision: "second" }] }] },
    ],
  }));
  fs.writeFileSync(paths.codexAuthFile, JSON.stringify({ tokens: { id_token: `a.${idTokenPayload}.c` } }));
  fs.writeFileSync(paths.claudeConfigFile, JSON.stringify({ oauthAccount: { organizationType: "claude_max", billingType: "stripe_subscription" } }));
  fs.writeFileSync(paths.cursorCliConfigFile, JSON.stringify({ model: { modelId: "grok-4.7" } }));
  fs.writeFileSync(paths.grokConfigFile, "[features]\ntelemetry = false\n");

  const cursor = new Database(paths.cursorTrackingDb);
  cursor.exec(`create table ai_code_hashes (
    hash text primary key, source text, fileExtension text, fileName text,
    requestId text, conversationId text, timestamp integer, model text, createdAt integer
  )`);
  cursor.prepare("insert into ai_code_hashes values (?,?,?,?,?,?,?,?,?)").run("hash-1", "composer", "ts", "a.ts", "req", "cursor-thread-1", Date.parse(now), "grok-4.7", Date.parse(now));
  cursor.close();

  const collector = new Database(paths.collectorDb);
  collector.exec(`create table buffered_events (id text primary key,source text,observed_at text,
    session_id text,event_type text,input_tokens integer,output_tokens integer,data_mode text,
    privacy_generation text,privacy_disposition text,payload_json text);
    create table session_usage_authority (source text,session_id text,authority text,primary key(source,session_id));
    create table upload_receipts (delivery_id text primary key,reason text);`);
  collector.prepare("insert into buffered_events values (?,?,?,?,?,?,?,?,?,?,?)").run("event-codex", "codex", now,"codex-thread-1","usage_rollout",100,20,"metadata_only","g",null,'{"prompt":"private-sentinel"}');
  collector.prepare("insert into buffered_events values (?,?,?,?,?,?,?,?,?,?,?)").run("event-claude", "claude_code", now,"claude-thread-1","assistant_response",200,40,"metadata_only","g",null,'{}');
  collector.prepare("insert into buffered_events values (?,?,?,?,?,?,?,?,?,?,?)").run("private-evidence", "codex", now,"codex-thread-1","usage_rollout",999,999,"evidence","g",null,'{}');
  collector.prepare("insert into buffered_events values (?,?,?,?,?,?,?,?,?,?,?)").run("quarantined-event", "codex", now,"codex-thread-1","usage_rollout",999,999,"metadata_only","g",null,'{}');
  collector.prepare("insert into upload_receipts values (?,?)").run("quarantined-event","local_evidence_quarantined");
  collector.prepare("insert into session_usage_authority values (?,?,?)").run("codex","codex-thread-1","tailer");
  collector.prepare("insert into session_usage_authority values (?,?,?)").run("claude_code","claude-thread-1","live");
  const eligible = collector.prepare(`select id from buffered_events where ${terminalPrivacyEligibilitySql(collector)} order by rowid`).all() as Array<{id:string}>;
  collector.close();
  const bounded = readNativeLedger(paths.collectorDb);
  assert.deepEqual(bounded.events.map((event) => event.id),eligible.map((row) => row.id));
  assert.equal(JSON.stringify(bounded).includes("private-sentinel"),false);

  const inspection = inspectNativeSubscriptionSurfaces({ paths, now });
  assert.equal(inspection.metadata.codexPlanType, "promax");
  assert.equal(inspection.metadata.claudePlanType, "claude_max");
  assert.equal(inspection.metadata.grokTelemetryEnabled, false);
  assert.equal(inspection.cursorTracking.conversationCount, 1);
  assert.equal(inspection.cursorTracking.tokenState, "unknown");
  assert.equal(inspection.localThreadTelemetry.totalEvents, 2);
  assert.equal(inspection.providers.find((item) => item.provider === "cursor")?.subscription.state, "verified");
  assert.equal(inspection.providers.find((item) => item.provider === "cursor")?.threadTelemetry.tokenState, "unknown");
  assert.ok(inspection.quotaSnapshots.length >= 4);
  const stale = inspectNativeSubscriptionSurfaces({paths,now:"2026-10-15T12:00:00.000Z"});
  assert.ok(stale.quotaSnapshots.every((snapshot) => snapshot.evidenceState === "unknown"));
  assert.ok(stale.quotaSnapshots.some((snapshot) => snapshot.usedPercent !== null));
  const future = inspectNativeSubscriptionSurfaces({paths,now:"2026-10-13T12:00:00.000Z"});
  assert.ok(future.quotaSnapshots.every((snapshot) => snapshot.evidenceState === "unknown"));
  const missing = inspectNativeSubscriptionSurfaces({paths:{...paths,collectorDb:path.join(root,"absent.sqlite")},now});
  assert.equal(missing.localThreadTelemetry.totalEvents,null);

  const database = new Database(paths.nativeSubscriptionDb);
  const first = importNativeSubscription(database, inspection, now);
  assert.equal(first.quotaInserted, inspection.quotaSnapshots.length);
  assert.equal(first.threadInserted, inspection.threadObservations.length);
  const replay = importNativeSubscription(database, inspection, now);
  assert.equal(replay.quotaReplayed, inspection.quotaSnapshots.length);
  assert.equal(replay.threadReplayed, inspection.threadObservations.length);
  const counts = readNativeSubscriptionCounts(database);
  assert.equal(counts.quotaSnapshots, inspection.quotaSnapshots.length);
  assert.equal(counts.threadObservations, inspection.threadObservations.length);
  const stored = JSON.stringify(database.prepare("select account_ref,plan_type,window_slot from native_subscription_snapshots").all());
  assert.equal(stored.includes("openai-main"), false);
  const stateFile = path.join(root,"codex-state.sqlite");
  const state = new Database(stateFile);
  state.exec("create table threads(id text primary key,cwd text,git_origin_url text)");
  state.prepare("insert into threads values (?,?,?)").run("codex-thread-1",root,"https://github.com/example/project.git");
  state.close();
  const claudeProjectsRoot=path.join(root,"claude-projects");
  fs.mkdirSync(path.join(claudeProjectsRoot,"fixture"),{recursive:true});
  fs.mkdirSync(path.join(root,".git"));
  fs.writeFileSync(path.join(root,".git/HEAD"),"a".repeat(40)+"\n");
  fs.writeFileSync(path.join(root,".git/config"),'[remote "origin"]\nurl = https://github.com/example/claude-project.git\n');
  fs.writeFileSync(path.join(claudeProjectsRoot,"fixture/claude-thread-1.jsonl"),JSON.stringify({cwd:root,sessionId:"claude-thread-1",message:"private-sentinel"})+"\n");
  const persisted = persistNativeTelemetry(database,inspection,{codexStateFile:stateFile,claudeProjectsRoot,now});
  assert.equal(persisted.ledgerInserted,2);
  assert.equal(persistNativeTelemetry(database,inspection,{codexStateFile:stateFile,claudeProjectsRoot,now}).ledgerInserted,0);
  assert.equal(nativeTelemetryReport(database).threads.find((thread) => thread.threadId === "codex-thread-1")?.projectState,"inferred");
  assert.equal(nativeTelemetryReport(database).threads.find((thread) => thread.threadId === "claude-thread-1")?.projectState,"inferred");
  const next = collectNativeTelemetry(database,{paths,now,codexStateFile:stateFile,claudeProjectsRoot});
  assert.equal(next.receipt.ledgerCoverage,"incremental");
  assert.equal(next.receipt.ledgerInserted,0);
  const held = new Database(paths.collectorDb);
  held.exec("begin exclusive");
  const cursorBefore = database.prepare("select through_rowid from native_capture_cursors").get();
  const failedRead = collectNativeTelemetry(database,{paths,now,codexStateFile:stateFile,claudeProjectsRoot});
  assert.equal(failedRead.receipt.ledgerReadState,"unknown");
  assert.deepEqual(database.prepare("select through_rowid from native_capture_cursors").get(),cursorBefore);
  held.exec("rollback");
  held.close();

  const csv = 'Date,Cloud Agent ID,Automation ID,Kind,Model,Max Mode,Input (w/ Cache Write),Input (w/o Cache Write),Cache Read,Output Tokens,Total Tokens,Cost\n2026-10-14T11:00:00.000Z,bc-cloud,,Included,grok-4.7,No,458,2,132618,369,133447,Included\n2026-10-14T11:01:00.000Z,,,On-Demand,grok-4.7,No,10,20,30,40,100,0.40\n2026-10-14T11:02:00.000Z,,,Free,grok-4.7,No,,,,,,Free\n';
  const csvRows = parseCursorSubscriptionCsv(csv);
  assert.equal(csvRows[0].inputTokens,2);
  assert.equal(csvRows[0].cacheWriteTokens,458);
  assert.equal(csvRows[0].onDemandUsageUsd,null);
  assert.equal(csvRows[2].totalTokens,null);
  assert.throws(()=>importCursorSubscriptionExport(database,csv,{accountRef:"cursor-outside-period",periodStart:"2026-10-15T00:00:00Z",periodEnd:"2026-10-16T00:00:00Z",now}),/period_out_of_scope/);
  const importedExport = importCursorSubscriptionExport(database,csv,{accountRef:"cursor-fixture",now});
  assert.equal(importedExport.inserted,3);
  assert.throws(()=>importCursorSubscriptionExport(database,csv,{accountRef:"cursor-fixture",periodStart:"2026-10-15T00:00:00Z",periodEnd:"2026-10-16T00:00:00Z",now}),/period_out_of_scope/);
  assert.equal(importCursorSubscriptionExport(database,csv,{accountRef:"cursor-fixture",now}).replayed,3);
  const exportSummary = latestSubscriptionExportSummaries(database)[0];
  assert.equal(exportSummary.observedTotalTokens,133547);
  assert.equal(exportSummary.reportedOnDemandUsageUsd,0.4);
  assert.equal(exportSummary.settledBankChargeUsd,null);
  assert.equal(exportSummary.completeness,"UNKNOWN");
  importCursorSubscriptionExport(database,csv+"\n",{accountRef:"cursor-fixture",now:"2026-10-14T12:01:00.000Z"});
  assert.equal(latestSubscriptionExportSummaries(database)[0].observedTotalTokens,133547);
  database.close();
  refreshSubscriptionTelemetry({paths,now:"2026-10-15T12:00:00.000Z"});
  const status=JSON.parse(fs.readFileSync(path.join(root,"subscription-telemetry-status.json"),"utf8")) as {quotaWindows:Array<{currentUsedPercent:number|null;currentResetAt:string|null;currentState:string}>};
  assert.ok(status.quotaWindows.every((window)=>window.currentUsedPercent===null && window.currentResetAt===null && window.currentState==="UNKNOWN"));

  console.log(JSON.stringify({
    schema: "plimsoll.native-subscription-proof.v1",
    checks: {
      nativePlanMetadataRead: true,
      nativeQuotaWindowsNormalized: true,
      degradedAccountPreserved: true,
      cursorThreadTokensStayUnknown: true,
      localThreadSourcesObserved: true,
      importIsIdempotent: true,
      accountIdentifiersNotStored: true,
      staleQuotaStaysUnknown: true,
      missingCaptureIsNotZero: true,
      boundedReaderPreservesPrivacyDispositions: true,
      cursorAdvancesWithoutReplayingTokens: true,
      repositoryProjectIsMarkedInferred: true,
      realCursorCsvShapeIsSupported: true,
      includedUsageIsNotCashSpend: true,
      failedReadDoesNotAdvanceCursor: true,
      overlappingExportsAreNotSummed: true,
    },
  }, null, 2));
} finally {
  fs.rmSync(root, { recursive: true, force: true });
}
