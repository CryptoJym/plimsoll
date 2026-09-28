/** A skew refusal stops both later foreground chunks and a legacy rebuild in this cycle. */
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import fs from "node:fs";
import http from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";
import Database from "better-sqlite3";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import {
  beginLegacySessionSummaryRebuild, emptyDaemonSessionSyncState,
  listLedgerSessionIds, runSessionSync, saveDaemonSessionSyncState,
} from "../packages/collector-cli/src/session-sync";
import { aiInteractionEventSchema } from "../packages/shared/src/index";
import { createProofCompletion } from "./lib/proof-completion";

const root = process.env.PLIMSOLL_PROOF_ROOT!;
const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const cliPath = path.join(repoRoot, "packages/collector-cli/src/cli.ts");
const completion = createProofCompletion("session-sync-skew-cycle", 5);
const tenantId = "00000000-0000-4000-8000-000000000818";
const installKey = "skew-cycle-proof";

function config(port: number) {
  return collectorConfigSchema.parse({
    port: 48317, tenantId, installKey,
    uploadUrl: "http://127.0.0.1:" + port + "/ingest",
    uploadSigningSecret: "skew-cycle-proof-secret",
    syncIntervalSeconds: 30,
    delivery: { requestTimeoutSeconds: 1 },
    managedConfig: { reconcile: { enabled: false } },
  });
}

function addRow(buffer: LocalEventBuffer, sessionId: string, eventId: string, observedAt: string) {
  assert.equal(buffer.append(aiInteractionEventSchema.parse({
    id: eventId, sessionId, source: "codex", eventType: "assistant_response",
    observedAt, inputTokens: 1, outputTokens: 1,
  })), true);
}

async function waitFor(check: () => boolean, timeoutMs: number) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (check()) return true;
    await new Promise((resolve) => setTimeout(resolve, 40));
  }
  return check();
}

async function reservePort() {
  const probe = http.createServer();
  await new Promise<void>((resolve) => probe.listen(0, "127.0.0.1", resolve));
  const port = (probe.address() as { port: number }).port;
  await new Promise<void>((resolve) => probe.close(() => resolve()));
  assert.notEqual(port, 48271);
  return port;
}

async function main() {
  const multi = new LocalEventBuffer(path.join(root, "two-foreground-batches.sqlite"),
    { workspaceId: tenantId });
  const firstId = "11111111-1111-4111-8111-111111111818";
  const secondId = "22222222-2222-4222-8222-222222222818";
  const now = new Date().toISOString();
  addRow(multi, firstId, "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaa818", now);
  addRow(multi, secondId, "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbb818", now);
  let directRequests = 0;
  try {
    const result = await runSessionSync(config(48316), {
      ledgerDb: multi.database, sessionIds: [firstId, secondId],
      until: new Date(Date.now() + 60_000).toISOString(),
      incremental: true, batchSize: 1, concurrency: 1,
      maxAttemptsPerBatch: 1, delayMs: 0, log: () => undefined,
      fetchImpl: (async () => {
        directRequests += 1;
        return new Response(JSON.stringify({
          error: "session_sync_clock_skew", serverTime: new Date().toISOString(),
        }), { status: 409, headers: { "content-type": "application/json" } });
      }) as typeof fetch,
    });
    assert.equal(directRequests, 1, "a settled skew refusal stops the second foreground batch");
    assert.equal(result.settlements.filter((entry) => entry.status === "clock_skew").length, 1);
    completion.check("foreground_chunks_stop_after_first_skew_refusal");
  } finally {
    multi.close();
  }

  const home = path.join(root, "mixed-cycle-home");
  fs.mkdirSync(home, { mode: 0o700 });
  const ledgerPath = path.join(home, "work-ledger.sqlite");
  const oldId = "33333333-3333-4333-8333-333333333818";
  const currentId = "44444444-4444-4444-8444-444444444818";
  const oldAt = new Date(Date.now() - 60_000).toISOString();
  const horizon = new Date(Date.now() - 30_000).toISOString();
  const currentAt = new Date(Date.now() - 5_000).toISOString();
  const buffer = new LocalEventBuffer(ledgerPath, { workspaceId: tenantId });
  addRow(buffer, oldId, "cccccccc-cccc-4ccc-8ccc-ccccccccc818", new Date().toISOString());
  addRow(buffer, currentId, "dddddddd-dddd-4ddd-8ddd-ddddddddd818", new Date().toISOString());
  const update = buffer.database.prepare(
    "update buffered_events set created_at = ?, observed_at = ?, uploaded_at = ? where session_id = ?");
  update.run(oldAt, oldAt, oldAt, oldId);
  update.run(currentAt, currentAt, currentAt, currentId);
  saveDaemonSessionSyncState(buffer.database, {
    ...emptyDaemonSessionSyncState(), caughtUp: true, lastSuccessfulUntil: horizon,
  });
  assert.equal(beginLegacySessionSummaryRebuild(buffer.database)?.phase, "scan");
  assert.deepEqual(listLedgerSessionIds(buffer.database, {
    since: horizon, until: new Date().toISOString(),
  }), [currentId]);
  completion.check("current_foreground_id_coexists_with_historical_rebuild");
  buffer.close();

  const cloudRequests: string[][] = [];
  const cloud = http.createServer(async (request, response) => {
    const chunks: Buffer[] = [];
    for await (const chunk of request) chunks.push(Buffer.from(chunk));
    let batch: { kind?: string; sessions?: Array<{ session: { id: string } }> } = {};
    try { batch = JSON.parse(Buffer.concat(chunks).toString("utf8")); } catch { /* unrelated request */ }
    if (batch.kind === "session_sync") {
      cloudRequests.push((batch.sessions ?? []).map((row) => row.session.id));
      response.writeHead(409, { "content-type": "application/json" });
      response.end(JSON.stringify({
        error: "session_sync_clock_skew", serverTime: new Date().toISOString(),
      }));
      return;
    }
    response.writeHead(404, { "content-type": "application/json" });
    response.end("{}");
  });
  await new Promise<void>((resolve) => cloud.listen(0, "127.0.0.1", resolve));
  const cloudPort = (cloud.address() as { port: number }).port;
  assert.notEqual(cloudPort, 48271);
  const daemonPort = await reservePort();
  fs.writeFileSync(path.join(home, "collector.config.json"),
    JSON.stringify({ ...config(cloudPort), port: daemonPort }), { mode: 0o600 });
  const readDb = new Database(ledgerPath, { readonly: true, fileMustExist: true });
  const readStreak = () => {
    const row = readDb.prepare(
      "select value from maintenance_state where key = 'session_sync_daemon_v1'"
    ).get() as { value: string } | undefined;
    const state = row ? JSON.parse(row.value) as {
      clockSkewRefusalStreak?: number; clockSkewRetryAt?: string | null;
    } : {};
    return state;
  };
  const env = { ...process.env, PLIMSOLL_HOME: home, PLIMSOLL_BUDGET_SAMPLER: "off" };
  const daemon = spawn(process.execPath, ["--import",
    path.join(repoRoot, "node_modules/tsx/dist/loader.mjs"), cliPath, "start"], {
    cwd: repoRoot, env, stdio: ["ignore", "pipe", "pipe"],
  });
  let stdout = "";
  let stderr = "";
  daemon.stdout?.on("data", (chunk) => { stdout += String(chunk); });
  daemon.stderr?.on("data", (chunk) => { stderr += String(chunk); });
  try {
    assert.equal(await waitFor(() => stdout.includes('"active"') || daemon.exitCode !== null, 30_000),
      true, stderr.slice(-2_000));
    assert.match(stdout, /"status":"active"/, stderr.slice(-2_000));
    completion.check("mixed_cycle_daemon_started");
    assert.equal(await waitFor(() => cloudRequests.length >= 1 || daemon.exitCode !== null, 60_000),
      true, JSON.stringify({ requests: cloudRequests, state: readStreak(),
        stdout: stdout.slice(-4_000), stderr: stderr.slice(-2_000) }));
    assert.deepEqual(cloudRequests[0], [currentId], "the foreground sends the current id first");
    assert.equal(await waitFor(() => readStreak().clockSkewRefusalStreak === 1, 4_000),
      true, JSON.stringify({ requests: cloudRequests, state: readStreak(), stderr: stderr.slice(-2_000) }));
    const retryAt = readStreak().clockSkewRetryAt;
    assert.ok(retryAt && Date.parse(retryAt) > Date.now());
    completion.check("foreground_skew_settlement_is_durable_once");
    await new Promise((resolve) => setTimeout(resolve, 3_000));
    assert.deepEqual(cloudRequests, [[currentId]],
      "the new skew gate must stop the legacy rebuild in the same cycle");
    assert.equal(readStreak().clockSkewRefusalStreak, 1);
    completion.check("same_cycle_skew_gate_blocks_legacy_request");
    console.log(JSON.stringify({ directRequests, cloudRequests,
      streak: readStreak().clockSkewRefusalStreak, retryAt }));
    completion.complete();
  } finally {
    daemon.kill("SIGTERM");
    const stopped = await waitFor(() => daemon.exitCode !== null || daemon.signalCode !== null, 10_000);
    if (!stopped) daemon.kill("SIGKILL");
    readDb.close();
    cloud.closeAllConnections();
    await new Promise<void>((resolve) => cloud.close(() => resolve()));
  }
}

void main().catch((error) => { console.error(error); process.exitCode = 1; });
