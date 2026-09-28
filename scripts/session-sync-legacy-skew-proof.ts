/** Legacy summary rebuild refusals must reach the daemon's durable skew gate. */
import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import fs from "node:fs";
import http from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";
import Database from "better-sqlite3";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { loadOrCreateLocalIngestAuth } from "../packages/collector-cli/src/local-auth";
import {
  beginLegacySessionSummaryRebuild, emptyDaemonSessionSyncState,
  loadDaemonSessionSyncState, planDaemonSessionSync, saveDaemonSessionSyncState,
} from "../packages/collector-cli/src/session-sync";
import { STATUS_SUMMARY_FILE } from "../packages/collector-cli/src/status-summary";
import { aiInteractionEventSchema } from "../packages/shared/src/index";
import { createProofCompletion } from "./lib/proof-completion";

const root = process.env.PLIMSOLL_PROOF_ROOT!;
const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const cliPath = path.join(repoRoot, "packages/collector-cli/src/cli.ts");
const completion = createProofCompletion("session-sync-legacy-skew", 8);
const tenantId = "00000000-0000-4000-8000-000000000718";
const installKey = "legacy-skew-proof";
const sessionId = "77777777-7777-4777-8777-777777777718";

async function waitFor(check: () => boolean, timeoutMs: number) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (check()) return true;
    await new Promise((resolve) => setTimeout(resolve, 50));
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
  const home = path.join(root, "legacy-daemon-home");
  fs.mkdirSync(home, { mode: 0o700 });
  const ledgerPath = path.join(home, "work-ledger.sqlite");
  const skewRequests: number[] = [];
  const cloud = http.createServer(async (request, response) => {
    const chunks: Buffer[] = [];
    for await (const chunk of request) chunks.push(Buffer.from(chunk));
    const raw = Buffer.concat(chunks).toString("utf8");
    let kind = "";
    try { kind = (JSON.parse(raw) as { kind?: string }).kind ?? ""; } catch { /* unrelated request */ }
    if (kind === "session_sync") {
      skewRequests.push(Date.now());
      response.writeHead(409, { "content-type": "application/json" });
      response.end(JSON.stringify({ error: "session_sync_clock_skew",
        serverTime: new Date().toISOString() }));
      return;
    }
    response.writeHead(404, { "content-type": "application/json" });
    response.end("{}");
  });
  await new Promise<void>((resolve) => cloud.listen(0, "127.0.0.1", resolve));
  const cloudPort = (cloud.address() as { port: number }).port;
  assert.notEqual(cloudPort, 48271);
  const daemonPort = await reservePort();
  const config = collectorConfigSchema.parse({
    port: daemonPort, tenantId, installKey,
    uploadUrl: "http://127.0.0.1:" + cloudPort + "/ingest",
    uploadSigningSecret: "legacy-skew-proof-secret",
    syncIntervalSeconds: 30,
    delivery: { requestTimeoutSeconds: 1 },
    managedConfig: { reconcile: { enabled: false } },
  });
  fs.writeFileSync(path.join(home, "collector.config.json"), JSON.stringify(config), { mode: 0o600 });
  const buffer = new LocalEventBuffer(ledgerPath, { workspaceId: tenantId });
  assert.equal(buffer.append(aiInteractionEventSchema.parse({
    id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaa718", sessionId, source: "codex",
    eventType: "assistant_response", observedAt: new Date().toISOString(),
    inputTokens: 1, outputTokens: 1,
  })), true);
  buffer.database.prepare("update buffered_events set uploaded_at = ? where session_id = ?")
    .run(new Date().toISOString(), sessionId);
  saveDaemonSessionSyncState(buffer.database, {
    ...emptyDaemonSessionSyncState(), caughtUp: true,
    lastSuccessfulUntil: new Date().toISOString(),
  });
  assert.equal(beginLegacySessionSummaryRebuild(buffer.database)?.phase, "scan");
  const plan = planDaemonSessionSync({
    db: buffer.database, state: loadDaemonSessionSyncState(buffer.database),
    uploadedBatches: [], ledgerSessionIds: [], until: new Date().toISOString(),
  });
  assert.equal(plan.skip, true, "the foreground has no current session ids");
  completion.check("legacy_rebuild_starts_without_foreground_ids");
  buffer.close();
  const auth = loadOrCreateLocalIngestAuth(home);
  const readDb = new Database(ledgerPath, { readonly: true, fileMustExist: true });
  const readState = () => {
    const row = readDb.prepare("select value from maintenance_state where key = 'session_sync_daemon_v1'")
      .get() as { value: string } | undefined;
    return row ? JSON.parse(row.value) as {
      clockSkewRefusalStreak?: number; clockSkewRetryAt?: string | null;
    } : null;
  };
  const env = { ...process.env, PLIMSOLL_HOME: home, PLIMSOLL_BUDGET_SAMPLER: "off" };
  const daemon = spawn(process.execPath, ["--import", path.join(repoRoot, "node_modules/tsx/dist/loader.mjs"),
    cliPath, "start"], { cwd: repoRoot, env, stdio: ["ignore", "pipe", "pipe"] });
  let stdout = "";
  let stderr = "";
  daemon.stdout?.on("data", (chunk) => { stdout += String(chunk); });
  daemon.stderr?.on("data", (chunk) => { stderr += String(chunk); });
  try {
    assert.equal(await waitFor(() => stdout.includes('"active"') || daemon.exitCode !== null, 30_000), true);
    assert.match(stdout, /"status":"active"/, stderr.slice(-2_000));
    completion.check("disposable_daemon_started");
    assert.equal(await waitFor(() => skewRequests.length >= 3 || daemon.exitCode !== null, 80_000),
      true, JSON.stringify({ requests: skewRequests.length, stderr: stderr.slice(-2_000) }));
    assert.equal(skewRequests.length, 3, "three rebuild attempts reached the endpoint");
    completion.check("three_legacy_rebuild_batches_refused_for_skew");
    assert.equal(await waitFor(() => readState()?.clockSkewRefusalStreak === 3, 3_000),
      true, JSON.stringify({ requests: skewRequests.length, state: readState(), stderr: stderr.slice(-2_000) }));
    const durable = readState();
    assert.equal(durable?.clockSkewRefusalStreak, 3);
    assert.ok(durable.clockSkewRetryAt && Date.parse(durable.clockSkewRetryAt) > Date.now());
    completion.check("rebuild_settlements_persist_three_refusal_streak");

    const response = await fetch("http://127.0.0.1:" + daemonPort + "/status", {
      headers: { "x-plimsoll-token": auth.managementRead },
    });
    const status = await response.json() as { sync?: { sessionSync?: { reason?: string;
      refusalStreak?: number; retryAt?: string } } };
    assert.equal(response.status, 200);
    assert.equal(status.sync?.sessionSync?.reason, "clock_skew");
    assert.equal(status.sync?.sessionSync?.refusalStreak, 3);
    completion.check("http_status_names_rebuild_clock_skew");

    const oneShot = spawnSync(process.execPath, ["--import",
      path.join(repoRoot, "node_modules/tsx/dist/loader.mjs"), cliPath, "status"], {
      cwd: repoRoot, env, encoding: "utf8", timeout: 30_000, maxBuffer: 8 * 1024 * 1024,
    });
    assert.equal(oneShot.status, 0, oneShot.stderr);
    const commandStatus = JSON.parse(oneShot.stdout) as { sync?: { sessionSync?: { reason?: string } } };
    assert.equal(commandStatus.sync?.sessionSync?.reason, "clock_skew");
    completion.check("one_shot_status_names_rebuild_clock_skew");

    const thirdAt = skewRequests[2]!;
    await new Promise((resolve) => setTimeout(resolve, Math.max(0, thirdAt + 6_500 - Date.now())));
    assert.equal(skewRequests.length, 3, "the five-second catch-up turn must hit the skew retry gate");
    completion.check("rebuild_retry_gate_blocks_next_catchup_turn");
    const summary = JSON.parse(fs.readFileSync(path.join(home, STATUS_SUMMARY_FILE), "utf8")) as Record<string, unknown>;
    assert.equal(Object.hasOwn(summary, "sessionSync"), false);
    completion.check("legacy_skew_keeps_v1_summary_shape");
    console.log(JSON.stringify({ requests: skewRequests.length, streak: durable.clockSkewRefusalStreak,
      retryAt: durable.clockSkewRetryAt, httpReason: status.sync?.sessionSync?.reason,
      cliReason: commandStatus.sync?.sessionSync?.reason, summaryKeys: Object.keys(summary).sort() }));
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
