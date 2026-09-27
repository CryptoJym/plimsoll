/** The delayed receiver survives a local HTTP abort and a collector crash. */
import assert from "node:assert/strict";
import crypto from "node:crypto";
import { spawn } from "node:child_process";
import fs from "node:fs";
import http from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";
import Database from "better-sqlite3";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { loadOrCreateLocalIngestAuth } from "../packages/collector-cli/src/local-auth";
import { createCollectorServer } from "../packages/collector-cli/src/server";
import {
  emptyDaemonSessionSyncState, loadDaemonSessionSyncState, recordSessionSyncSettlement,
  runSessionSync, saveDaemonSessionSyncState, sessionSyncClockSkewStatus,
} from "../packages/collector-cli/src/session-sync";
import { startStatusSummaryWriter, STATUS_SUMMARY_FILE } from "../packages/collector-cli/src/status-summary";
import { aiInteractionEventSchema } from "../packages/shared/src/index";
import { acceptedFixtureDelivery } from "./lib/delivery-fixture";
import { createProofCompletion } from "./lib/proof-completion";

const root = process.env.PLIMSOLL_PROOF_ROOT!;
const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const ledgerPath = path.join(root, "remote-deadline.sqlite");
const resultPath = path.join(root, "child-result.json");
const tenantId = "00000000-0000-4000-8000-000000000118";
const installKey = "remote-deadline-fixture";
const firstSession = "11111111-1111-4111-8111-111111111118";
const secondSession = "22222222-2222-4222-8222-222222222218";
const thirdSession = "33333333-3333-4333-8333-333333333318";
const until = new Date(Date.now() + 120_000).toISOString();

function config(port: number, timeoutSeconds = 1) {
  return collectorConfigSchema.parse({
    port: 48319, uploadUrl: `http://127.0.0.1:${port}/ingest`, tenantId, installKey,
    uploadSigningSecret: "remote-deadline-fixture-secret",
    delivery: { requestTimeoutSeconds: timeoutSeconds },
  });
}

function addRow(buffer: LocalEventBuffer, sessionId: string, id: string) {
  assert.equal(buffer.append(aiInteractionEventSchema.parse({
    id, sessionId, source: "codex", eventType: "assistant_response",
    observedAt: new Date().toISOString(), inputTokens: 1, outputTokens: 1,
  })), true);
}

function lease(db: Database.Database, sessionId: string) {
  return db.prepare(`select lease_expires_at as expiresAt from session_sync_upload_leases
    where session_id = ?`).get(sessionId) as { expiresAt: string } | undefined;
}

async function waitFor(check: () => boolean, ms: number) {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    if (check()) return true;
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  return false;
}

async function child(port: number) {
  const buffer = new LocalEventBuffer(ledgerPath, { workspaceId: tenantId });
  try {
    const result = await runSessionSync(config(port), {
      ledgerDb: buffer.database, sessionIds: [firstSession], until,
      incremental: true, maxAttemptsPerBatch: 1, delayMs: 0, log: () => undefined,
    });
    fs.writeFileSync(resultPath, JSON.stringify({ ok: result.ok, reason: result.reason }));
    // Parent terminates this process after observing the local deadline.
    setInterval(() => undefined, 1_000);
  } catch (error) {
    fs.writeFileSync(resultPath, JSON.stringify({ error: String(error) }));
    process.exitCode = 1;
  } finally {
    buffer.close();
  }
}

async function main() {
  const completion = createProofCompletion("session-sync-remote-deadline", 21);
  const delayedLedgerPath = path.join(root, "lease-acquisition-delay.sqlite");
  const delayedBuffer = new LocalEventBuffer(delayedLedgerPath, { workspaceId: tenantId });
  const delayedSession = "00000900-1111-4111-8111-000000000118";
  addRow(delayedBuffer, delayedSession, "00000900-aaaa-4aaa-8aaa-aaaaaaaaa118");
  delayedBuffer.database.pragma("busy_timeout = 0");
  const blocker = new Database(delayedLedgerPath);
  let releaseTimer: ReturnType<typeof setTimeout> | undefined;
  let wireBody = "";
  let requestArrivedAtMs = 0;
  let decisionAtMs = 0;
  let guardAllowed = false;
  const delayedEndpoint = http.createServer(async (request, response) => {
    const chunks: Buffer[] = [];
    for await (const chunk of request) chunks.push(Buffer.from(chunk));
    wireBody = Buffer.concat(chunks).toString("utf8");
    requestArrivedAtMs = Date.now();
    const batch = JSON.parse(wireBody) as { sentAt: string; expiresAt: string };
    // The transport timer starts before the lease. The reserved acquisition
    // budget leaves time to answer after crossing the old sentAt + 1 s gate.
    const targetMs = Date.parse(batch.sentAt) + 1_150;
    await new Promise((resolve) => setTimeout(resolve, Math.max(0, targetMs - Date.now())));
    decisionAtMs = Date.now();
    guardAllowed = decisionAtMs <= Date.parse(batch.expiresAt) - 10_000;
    response.writeHead(guardAllowed ? 200 : 409, { "content-type": "application/json" });
    response.end(JSON.stringify(guardAllowed
      ? acceptedFixtureDelivery(wireBody, installKey)
      : { error: "session_sync_expired", serverTime: new Date(decisionAtMs).toISOString() }));
  });
  await new Promise<void>((resolve) => delayedEndpoint.listen(0, "127.0.0.1", resolve));
  try {
    const delayedPort = (delayedEndpoint.address() as { port: number }).port;
    assert.notEqual(delayedPort, 48271);
    const delayed = await runSessionSync(config(delayedPort, 1), {
      ledgerDb: delayedBuffer.database, sessionIds: [delayedSession], until,
      incremental: true, maxAttemptsPerBatch: 1, delayMs: 0, log: () => undefined,
      proofSummaryHooks: { onUpdate: () => {
        assert.equal(releaseTimer, undefined);
        blocker.exec("BEGIN IMMEDIATE");
        releaseTimer = setTimeout(() => blocker.exec("COMMIT"), 900);
      } },
    });
    assert.equal(await waitFor(() => decisionAtMs > 0, 1_000), true);
    assert.notEqual(wireBody, "", "a real signed HTTP request must reach the endpoint");
    const batch = JSON.parse(wireBody) as { sentAt: string; expiresAt: string };
    const requestAgeMs = requestArrivedAtMs - Date.parse(batch.sentAt);
    console.log(JSON.stringify({ case: "lease_acquisition_delay", sqliteHoldMs: 900, requestAgeMs,
      serverElapsedMs: decisionAtMs - Date.parse(batch.sentAt), guardAllowed,
      acceptedSessions: delayed.acceptedSessions, reason: delayed.reason }));
    assert.ok(requestAgeMs >= 800, `request arrived only ${requestAgeMs}ms after sentAt`);
    assert.ok(decisionAtMs - Date.parse(batch.sentAt) > 1_000);
    assert.ok(decisionAtMs - requestArrivedAtMs < 1_000);
    assert.equal(guardAllowed, true, "cloud guard admits the request while HTTP still waits");
    assert.equal(delayed.acceptedSessions, 1);
    assert.equal(lease(delayedBuffer.database, delayedSession), undefined);
    completion.check("one_second_timeout_survives_nine_hundred_ms_lease_acquisition");
  } finally {
    if (releaseTimer) clearTimeout(releaseTimer);
    if (blocker.inTransaction) blocker.exec("ROLLBACK");
    blocker.close();
    delayedBuffer.close();
    delayedEndpoint.closeAllConnections();
    await new Promise<void>((resolve) => delayedEndpoint.close(() => resolve()));
  }
  const timeoutBuffer = new LocalEventBuffer(path.join(root, "timeout-guard.sqlite"),
    { workspaceId: tenantId });
  try {
    for (const timeoutSeconds of [1, 5, 10, 30, 300]) {
      const effectiveTimeoutMs = Math.min(timeoutSeconds, 120) * 1_000;
      const segment = String(timeoutSeconds).padStart(8, "0");
      const sessionId = `${segment}-1111-4111-8111-000000000118`;
      addRow(timeoutBuffer, sessionId, `${segment}-aaaa-4aaa-8aaa-aaaaaaaaa118`);
      let wireBody = "";
      let guardAllowed = false;
      const result = await runSessionSync(config(48319, timeoutSeconds), {
        ledgerDb: timeoutBuffer.database, sessionIds: [sessionId], until,
        incremental: true, maxAttemptsPerBatch: 1, delayMs: 0, log: () => undefined,
        fetchImpl: (async (_url, init) => {
          const raw = String(init?.body ?? "");
          wireBody = raw;
          const wire = JSON.parse(raw) as { sentAt: string; expiresAt: string };
          // The cloud tests the DB clock against expiresAt minus its 10 s
          // transaction budget. This instant is just before the local HTTP
          // timeout, including the collector's 120 s timeout cap.
          const serverTimeMs = Date.parse(wire.sentAt) + effectiveTimeoutMs - 1;
          guardAllowed = serverTimeMs <= Date.parse(wire.expiresAt) - 10_000;
          return guardAllowed
            ? new Response(JSON.stringify(acceptedFixtureDelivery(raw, installKey)), { status: 200 })
            : new Response(JSON.stringify({ error: "session_sync_expired",
                serverTime: new Date(serverTimeMs).toISOString() }), { status: 409 });
        }) as typeof fetch,
      });
      assert.notEqual(wireBody, "");
      const wire = JSON.parse(wireBody) as { sentAt: string; expiresAt: string };
      assert.equal(Date.parse(wire.expiresAt) - Date.parse(wire.sentAt),
        Math.min(120_000, effectiveTimeoutMs + 1_000) + 10_000,
        `timeout ${timeoutSeconds}s deadline`);
      assert.equal(guardAllowed, true, `cloud guard must admit timeout ${timeoutSeconds}s`);
      assert.equal(result.acceptedSessions, 1);
      completion.check(`cloud_guard_admits_${timeoutSeconds}s_timeout_before_local_deadline`);
    }
  } finally {
    timeoutBuffer.close();
  }
  const seeded = new LocalEventBuffer(ledgerPath, { workspaceId: tenantId });
  addRow(seeded, firstSession, "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaa118");
  seeded.close();

  let received!: (body: string) => void;
  const receivedBody = new Promise<string>((resolve) => { received = resolve; });
  let proceed!: () => void;
  const gate = new Promise<void>((resolve) => { proceed = resolve; });
  let settled!: () => void;
  const settledPromise = new Promise<void>((resolve) => { settled = resolve; });
  const remoteRows = new Set<string>();
  let refusal = "";
  const endpoint = http.createServer(async (request, response) => {
    const chunks: Buffer[] = [];
    for await (const chunk of request) chunks.push(Buffer.from(chunk));
    const raw = Buffer.concat(chunks).toString("utf8");
    received(raw);
    await gate;
    const batch = JSON.parse(raw) as { sentAt?: string; expiresAt?: string;
      sessions: Array<{ session: { id: string } }> };
    const serverTime = new Date();
    if (!batch.sentAt || !batch.expiresAt) {
      for (const row of batch.sessions) remoteRows.add(row.session.id);
    } else if (Math.abs(serverTime.getTime() - Date.parse(batch.sentAt)) > 60_000) {
      refusal = "session_sync_clock_skew";
    } else if (serverTime.getTime() > Date.parse(batch.expiresAt) - 10_000) {
      refusal = "session_sync_expired";
    } else {
      for (const row of batch.sessions) remoteRows.add(row.session.id);
    }
    if (!response.destroyed) {
      response.writeHead(refusal ? 409 : 200, { "content-type": "application/json" });
      response.end(JSON.stringify(refusal
        ? { error: refusal, serverTime: serverTime.toISOString() }
        : acceptedFixtureDelivery(raw, installKey)));
    }
    settled();
  });
  await new Promise<void>((resolve) => endpoint.listen(0, "127.0.0.1", resolve));
  const port = (endpoint.address() as { port: number }).port;
  assert.notEqual(port, 48271);
  const proofEntry = fileURLToPath(import.meta.url);
  const runner = spawn(process.execPath, ["--import", path.join(repoRoot, "node_modules/tsx/dist/loader.mjs"),
    proofEntry, "--child-crash", String(port)], { cwd: repoRoot, env: process.env, stdio: "ignore" });
  try {
    const raw = await Promise.race([receivedBody,
      new Promise<never>((_, reject) => setTimeout(() => reject(new Error("request_not_received")), 8_000))]);
    const batch = JSON.parse(raw) as { sentAt?: string; expiresAt?: string;
      sessions: Array<{ session: { id: string } }> };
    assert.equal(batch.sessions[0]?.session.id, firstSession);
    assert.ok(batch.sentAt && batch.expiresAt);
    completion.check("new_batch_carries_sent_at_and_expires_at");
    assert.equal(Date.parse(batch.expiresAt!) - Date.parse(batch.sentAt!), 12_000);
    completion.check("wire_deadline_adds_cloud_commit_window");

    assert.equal(await waitFor(() => fs.existsSync(resultPath), 5_000), true);
    const childResult = JSON.parse(fs.readFileSync(resultPath, "utf8")) as { ok: boolean; reason: string };
    assert.equal(childResult.ok, false);
    completion.check("local_http_deadline_aborts_before_remote_decision");
    runner.kill("SIGKILL");
    assert.equal(await waitFor(() => runner.exitCode !== null || runner.signalCode !== null, 5_000), true);
    completion.check("collector_process_stopped_while_endpoint_waits");

    const restarted = new LocalEventBuffer(ledgerPath, { workspaceId: tenantId });
    try {
      const held = lease(restarted.database, firstSession);
      assert.ok(held);
      assert.equal(Date.parse(held.expiresAt) - Date.parse(batch.expiresAt!), 65_000);
      completion.check("crash_surviving_lease_has_skew_plus_slack_bound");
      assert.throws(() => restarted.database.prepare("delete from buffered_events where session_id = ?")
        .run(firstSession), /session_sync_upload_lease/);
      completion.check("erasure_waits_after_local_abort_and_crash");

      // The real wall clock reaches the stated bound; the receiver remains
      // paused throughout, so this is the old collector's late-commit window.
      const waitMs = Math.max(0, Date.parse(held.expiresAt) - Date.now() + 80);
      assert.ok(waitMs <= 78_000);
      await new Promise((resolve) => setTimeout(resolve, waitMs));
      assert.equal(restarted.database.prepare("delete from buffered_events where session_id = ?")
        .run(firstSession).changes, 1);
      completion.check("stuck_lease_expires_and_erasure_completes");
      proceed();
      await settledPromise;
      assert.ok(refusal === "session_sync_clock_skew" || refusal === "session_sync_expired");
      assert.equal(remoteRows.has(firstSession), false);
      completion.check("late_receiver_refuses_erased_row");

      addRow(restarted, secondSession, "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbb118");
      const accept = await runSessionSync(config(port), {
        ledgerDb: restarted.database, sessionIds: [secondSession], until,
        incremental: true, maxAttemptsPerBatch: 1, delayMs: 0, log: () => undefined,
        fetchImpl: (async (_url, init) => new Response(JSON.stringify(
          acceptedFixtureDelivery(String(init?.body ?? ""), installKey)),
          { status: 200, headers: { "content-type": "application/json" } })) as typeof fetch,
      });
      assert.equal(accept.acceptedSessions, 1);
      assert.equal(lease(restarted.database, secondSession), undefined);
      assert.equal(lease(restarted.database, firstSession), undefined);
      completion.check("accepted_send_releases_lease_and_reaps_expired_crash_lease");

      addRow(restarted, thirdSession, "cccccccc-cccc-4ccc-8ccc-ccccccccc118");
      const expired = await runSessionSync(config(port), {
        ledgerDb: restarted.database, sessionIds: [thirdSession], until,
        incremental: true, maxAttemptsPerBatch: 1, delayMs: 0, log: () => undefined,
        fetchImpl: (async () => new Response(JSON.stringify({
          error: "session_sync_expired", serverTime: new Date().toISOString(),
        }), { status: 409, headers: { "content-type": "application/json" } })) as typeof fetch,
      });
      assert.equal(expired.ok, false);
      assert.equal(expired.settlements[0]?.status, "expired");
      assert.equal(lease(restarted.database, thirdSession), undefined);
      completion.check("explicit_refusal_releases_lease_promptly");

      const retry = await runSessionSync(config(port), {
        ledgerDb: restarted.database, sessionIds: [thirdSession], until,
        incremental: true, maxAttemptsPerBatch: 1, delayMs: 0, log: () => undefined,
        fetchImpl: (async (_url, init) => new Response(JSON.stringify(
          acceptedFixtureDelivery(String(init?.body ?? ""), installKey)),
          { status: 200, headers: { "content-type": "application/json" } })) as typeof fetch,
      });
      assert.equal(retry.acceptedSessions, 1);
      completion.check("refused_rows_remain_eligible_for_later_send");

      let state = emptyDaemonSessionSyncState();
      for (let n = 0; n < 3; n++) {
        const skew = await runSessionSync(config(port), {
          ledgerDb: restarted.database, sessionIds: [thirdSession], until,
          incremental: true, maxAttemptsPerBatch: 1, delayMs: 0, log: () => undefined,
          fetchImpl: (async () => new Response(JSON.stringify({
            error: "session_sync_clock_skew", serverTime: "2026-09-27T17:00:00.000Z",
          }), { status: 409, headers: { "content-type": "application/json" } })) as typeof fetch,
        });
        assert.equal(skew.settlements[0]?.status, "clock_skew");
        for (const settlement of skew.settlements) {
          state = recordSessionSyncSettlement(state, settlement);
        }
        saveDaemonSessionSyncState(restarted.database, state);
        state = loadDaemonSessionSyncState(restarted.database);
      }
      const recovered = loadDaemonSessionSyncState(restarted.database);
      assert.equal(sessionSyncClockSkewStatus(recovered)?.reason, "clock_skew");
      assert.ok(Date.parse(recovered.clockSkewRetryAt!) - Date.now() <= 300_000);
      completion.check("three_skew_refusals_survive_restart_with_bounded_retry");
      const daemonSource = fs.readFileSync(path.join(repoRoot, "packages/collector-cli/src/cli.ts"), "utf8");
      assert.match(daemonSource,
        /for \(const settlement of sessionResult\.settlements\)[\s\S]{0,160}recordSessionSyncSettlement/);
      assert.match(daemonSource, /syncStatus: \(\) =>[\s\S]{0,150}sessionSyncClockSkewStatus/);
      assert.match(daemonSource, /sessionSyncStatus: \(\) => sessionSyncClockSkewStatus/);
      completion.check("daemon_wires_skew_streak_into_both_status_surfaces");

      const home = path.join(root, "status-home");
      fs.mkdirSync(home, { mode: 0o700 });
      const writer = startStatusSummaryWriter({ home, instanceId: crypto.randomUUID(),
        healthzKey: Buffer.alloc(32).toString("base64url"), collectorVersion: "fixture",
        port: 48319, stats: () => ({}),
        sessionSyncStatus: () => sessionSyncClockSkewStatus(recovered) });
      await writer.firstWrite;
      await writer.stop();
      const summary = JSON.parse(fs.readFileSync(path.join(home, STATUS_SUMMARY_FILE), "utf8")) as {
        sessionSync?: { reason?: string } };
      assert.equal(summary.sessionSync?.reason, "clock_skew");
      completion.check("status_summary_names_clock_skew");

      const auth = loadOrCreateLocalIngestAuth(home);
      const statusServer = createCollectorServer(config(port), restarted, {
        localAuth: auth, localAuthHome: home,
        syncStatus: () => ({ sessionSync: sessionSyncClockSkewStatus(recovered) }),
      });
      await new Promise<void>((resolve) => statusServer.listen(0, "127.0.0.1", resolve));
      try {
        const statusPort = (statusServer.address() as { port: number }).port;
        const response = await fetch(`http://127.0.0.1:${statusPort}/status`, {
          headers: { "x-plimsoll-token": auth.managementRead },
        });
        const body = await response.json() as { sync?: { sessionSync?: { reason?: string } } };
        assert.equal(response.status, 200);
        assert.equal(body.sync?.sessionSync?.reason, "clock_skew");
        completion.check("http_status_names_clock_skew");
      } finally {
        await new Promise<void>((resolve) => statusServer.close(() => resolve()));
      }
    } finally {
      restarted.close();
    }
    completion.complete();
  } finally {
    runner.kill("SIGKILL");
    proceed();
    endpoint.closeAllConnections();
    await new Promise<void>((resolve) => endpoint.close(() => resolve()));
  }
}

if (process.argv[2] === "--child-crash") {
  void child(Number(process.argv[3])).catch((error) => { console.error(error); process.exitCode = 1; });
} else {
  void main().catch((error) => { console.error(error); process.exitCode = 1; });
}
