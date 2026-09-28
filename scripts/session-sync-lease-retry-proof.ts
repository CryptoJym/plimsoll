/** A lease begins at the wire handoff and belongs to one batch across retries. */
import assert from "node:assert/strict";
import http from "node:http";
import path from "node:path";
import { Worker } from "node:worker_threads";
import { createRequire } from "node:module";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { runSessionSync } from "../packages/collector-cli/src/session-sync";
import { postHistoryBatch } from "../packages/collector-cli/src/upload-history";
import { aiInteractionEventSchema } from "../packages/shared/src/index";
import { acceptedFixtureDelivery } from "./lib/delivery-fixture";
import { createProofCompletion } from "./lib/proof-completion";

const root = process.env.PLIMSOLL_PROOF_ROOT!;
const tenantId = "00000000-0000-4000-8000-000000000618";
const installKey = "lease-retry-fixture";
const until = new Date(Date.now() + 120_000).toISOString();
const require = createRequire(import.meta.url);

function config(port: number, timeoutSeconds: number) {
  return collectorConfigSchema.parse({
    port: 48318, uploadUrl: `http://127.0.0.1:${port}/ingest`, tenantId, installKey,
    uploadSigningSecret: "lease-retry-fixture-secret",
    delivery: { requestTimeoutSeconds: timeoutSeconds },
  });
}

function addRow(buffer: LocalEventBuffer, sessionId: string, eventId: string) {
  assert.equal(buffer.append(aiInteractionEventSchema.parse({
    id: eventId, sessionId, source: "codex", eventType: "assistant_response",
    observedAt: new Date().toISOString(), inputTokens: 1, outputTokens: 1,
  })), true);
}

function leaseToken(buffer: LocalEventBuffer, sessionId: string): string | undefined {
  return (buffer.database.prepare(`select lease_token as token from session_sync_upload_leases
    where session_id = ?`).get(sessionId) as { token: string } | undefined)?.token;
}

async function server(handler: (raw: string, response: http.ServerResponse) => void | Promise<void>) {
  const endpoint = http.createServer(async (request, response) => {
    const chunks: Buffer[] = [];
    for await (const chunk of request) chunks.push(Buffer.from(chunk));
    await handler(Buffer.concat(chunks).toString("utf8"), response);
  });
  await new Promise<void>((resolve) => endpoint.listen(0, "127.0.0.1", resolve));
  const port = (endpoint.address() as { port: number }).port;
  assert.notEqual(port, 48271);
  return { endpoint, port };
}

async function closeServer(endpoint: http.Server) {
  endpoint.closeAllConnections();
  await new Promise<void>((resolve) => endpoint.close(() => resolve()));
}

async function main() {
  const completion = createProofCompletion("session-sync-lease-retry", 11);
  const lockedPath = path.join(root, "real-cli-busy-timeout.sqlite");
  const locked = new LocalEventBuffer(lockedPath, { workspaceId: tenantId });
  const lockedSession = "11111111-1111-4111-8111-111111111618";
  addRow(locked, lockedSession, "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaa618");
  assert.equal(locked.database.pragma("busy_timeout", { simple: true }), 5_000);
  completion.check("real_cli_buffer_keeps_five_second_busy_timeout");
  let worker: Worker | undefined;
  let heldFromMs = 0;
  let requestAtMs = 0;
  let wire = "";
  let admitted = false;
  const lockedServer = await server((raw, response) => {
    wire = raw;
    requestAtMs = Date.now();
    const batch = JSON.parse(raw) as { sentAt: string; expiresAt: string };
    admitted = requestAtMs <= Date.parse(batch.expiresAt) - 10_000;
    response.writeHead(admitted ? 200 : 409, { "content-type": "application/json" });
    response.end(JSON.stringify(admitted ? acceptedFixtureDelivery(raw, installKey) : {
      error: "session_sync_expired", serverTime: new Date(requestAtMs).toISOString(),
    }));
  });
  try {
    const result = await runSessionSync(config(lockedServer.port, 1), {
      ledgerDb: locked.database, sessionIds: [lockedSession], until,
      incremental: true, maxAttemptsPerBatch: 1, delayMs: 0, log: () => undefined,
      proofSummaryHooks: { onUpdate: () => {
        const signal = new Int32Array(new SharedArrayBuffer(4));
        worker = new Worker(`
          const { workerData } = require('node:worker_threads');
          const Database = require(workerData.modulePath);
          const db = new Database(workerData.path);
          db.exec('BEGIN IMMEDIATE');
          Atomics.store(workerData.signal, 0, 1);
          Atomics.notify(workerData.signal, 0);
          setTimeout(() => { db.exec('COMMIT'); db.close(); }, 3000);
        `, { eval: true, workerData: {
          modulePath: require.resolve("better-sqlite3"), path: lockedPath, signal,
        } });
        assert.equal(Atomics.wait(signal, 0, 0, 2_000), "ok", "writer obtained lock");
        heldFromMs = Date.now();
      } },
    });
    assert.notEqual(wire, "", "a real HTTP request reached the endpoint");
    const batch = JSON.parse(wire) as { sentAt: string; expiresAt: string };
    console.log(JSON.stringify({ case: "three_second_writer_lock", heldForMs: requestAtMs - heldFromMs,
      sentAfterLockMs: Date.parse(batch.sentAt) - heldFromMs,
      handoffAgeMs: requestAtMs - Date.parse(batch.sentAt), admitted,
      acceptedSessions: result.acceptedSessions }));
    assert.ok(requestAtMs - heldFromMs >= 2_750, "SQLite actually held the writer for about 3 s");
    assert.ok(Date.parse(batch.sentAt) - heldFromMs >= 2_750,
      "sentAt must be taken after the blocking lease acquisition");
    assert.equal(Date.parse(batch.expiresAt) - Date.parse(batch.sentAt), 11_000);
    assert.equal(admitted, true, "the cloud guard admits a 1 s transport after the writer clears");
    assert.equal(result.acceptedSessions, 1);
    assert.equal(leaseToken(locked, lockedSession), undefined);
    completion.check("three_second_writer_lock_does_not_spend_one_second_wire_deadline");
  } finally {
    await worker?.terminate();
    locked.close();
    await closeServer(lockedServer.endpoint);
  }

  const retryPath = path.join(root, "retry.sqlite");
  const retryBuffer = new LocalEventBuffer(retryPath, { workspaceId: tenantId });
  const retrySession = "22222222-2222-4222-8222-222222222618";
  addRow(retryBuffer, retrySession, "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbb618");
  const attempts: Array<{ raw: string; token: string | undefined; at: number }> = [];
  const committed = new Set<string>();
  let commitCount = 0;
  const retryServer = await server((raw, response) => {
    attempts.push({ raw, token: leaseToken(retryBuffer, retrySession), at: Date.now() });
    if (attempts.length === 1) {
      response.writeHead(503, { "content-type": "application/json" });
      response.end(JSON.stringify({ error: "try_again" }));
      return;
    }
    for (const row of (JSON.parse(raw) as { sessions: Array<{ session: { id: string } }> }).sessions) {
      committed.add(row.session.id);
    }
    commitCount += 1;
    response.writeHead(200, { "content-type": "application/json" });
    response.end(JSON.stringify(acceptedFixtureDelivery(raw, installKey)));
  });
  try {
    const result = await runSessionSync(config(retryServer.port, 5), {
      ledgerDb: retryBuffer.database, sessionIds: [retrySession], until,
      incremental: true, maxAttemptsPerBatch: 2, delayMs: 0,
      log: () => undefined,
    });
    console.log(JSON.stringify({ case: "503_then_success", requests: attempts.length,
      distinctTokens: new Set(attempts.map((attempt) => attempt.token)).size,
      acceptedSessions: result.acceptedSessions, commits: commitCount }));
    assert.equal(attempts.length, 2, "configured retry reaches transport twice");
    assert.ok(attempts[1]!.at - attempts[0]!.at >= 950,
      "the retry must observe the production one-second backoff");
    assert.equal(attempts[0]?.raw, attempts[1]?.raw, "retry sends the identical signed batch body");
    assert.ok(attempts[0]?.token);
    assert.equal(attempts[0]?.token, attempts[1]?.token, "one batch retains its lease token");
    assert.equal(result.acceptedSessions, 1);
    assert.equal(commitCount, 1);
    assert.deepEqual([...committed], [retrySession]);
    completion.check("503_retry_reuses_same_body_and_lease_token");
    completion.check("retry_commits_batch_once");
    assert.ok(leaseToken(retryBuffer, retrySession), "uncertain first response keeps fence until bound");
    completion.check("earlier_uncertain_attempt_keeps_lease_after_retry_success");
  } finally {
    retryBuffer.close();
    await closeServer(retryServer.endpoint);
  }

  for (const [index, status, retryAfter] of [[5, 503, null], [6, 429, "2"]] as const) {
    const short = new LocalEventBuffer(path.join(root, `short-retry-${status}.sqlite`), { workspaceId: tenantId });
    const sessionId = `${index}${index}${index}${index}${index}${index}${index}${index}-5555-4555-8555-555555555618`;
    const eventId = `${index}${index}${index}${index}${index}${index}${index}${index}-aaaa-4aaa-8aaa-aaaaaaaaa618`;
    addRow(short, sessionId, eventId);
    let requests = 0;
    const sleeps: number[] = [];
    const endpoint = await server((_raw, response) => {
      requests += 1;
      response.writeHead(status, { "content-type": "application/json",
        ...(retryAfter ? { "retry-after": retryAfter } : {}) });
      response.end(JSON.stringify({ error: "try_again" }));
    });
    try {
      const result = await runSessionSync(config(endpoint.port, 1), {
        ledgerDb: short.database, sessionIds: [sessionId], until,
        incremental: true, maxAttemptsPerBatch: 2, delayMs: 0,
        sleep: async (ms) => {
          sleeps.push(ms);
          await new Promise((resolve) => setTimeout(resolve, ms));
        },
        log: () => undefined,
      });
      assert.equal(result.ok, false);
      assert.equal(requests, 1, "the first transient response reached the endpoint");
      assert.deepEqual(sleeps, [], "a retry that cannot reach transport must not sleep");
      assert.match(result.reason ?? "", /retry_deadline_exceeded/);
      assert.ok(leaseToken(short, sessionId), "the uncertain first attempt keeps its finite lease");
      completion.check(`one_second_${status}_skips_unsendable_backoff`);
    } finally {
      short.close();
      await closeServer(endpoint.endpoint);
    }
  }

  const realNow = Date.now;
  try {
    // Run the uploader's production backoff against a clock that advances by
    // the actual requested sleep for every valid configured timeout.
    for (let timeoutSeconds = 1; timeoutSeconds <= 300; timeoutSeconds += 1) {
      let clockMs = realNow();
      Date.now = () => clockMs;
      const cutoffMs = clockMs + Math.min(timeoutSeconds, 120) * 1_000;
      const handoffs: number[] = [];
      const sleeps: number[] = [];
      let failure = "";
      try {
        const result = await postHistoryBatch({
          url: "http://127.0.0.1:48318/ingest", body: attempts[0]!.raw,
          installKey, signingSecret: "lease-retry-fixture-secret",
          fetchImpl: (async () => {
            handoffs.push(clockMs);
            return handoffs.length === 1
              ? new Response(JSON.stringify({ error: "try_again" }), { status: 503 })
              : new Response(JSON.stringify(acceptedFixtureDelivery(attempts[0]!.raw, installKey)), { status: 200 });
          }) as typeof fetch,
          sleep: async (ms) => { sleeps.push(ms); clockMs += ms; },
          maxAttempts: 2, timeoutMs: Math.min(timeoutSeconds, 120) * 1_000,
          retryDeadlineMs: cutoffMs, allowPartial: true, log: () => undefined,
        });
        assert.equal(result.accepted, 1);
      } catch (error) {
        failure = error instanceof Error ? error.message : String(error);
      }
      assert.equal(handoffs.length, timeoutSeconds === 1 ? 1 : 2, `timeout ${timeoutSeconds}s handoffs`);
      assert.deepEqual(sleeps, timeoutSeconds === 1 ? [] : [1_000], `timeout ${timeoutSeconds}s sleeps`);
      assert.ok(handoffs.every((at) => at < cutoffMs), `timeout ${timeoutSeconds}s handoff precedes cloud cutoff`);
      if (timeoutSeconds === 1) assert.match(failure, /retry_deadline_exceeded/);
      else assert.equal(failure, "");
    }
  } finally {
    Date.now = realNow;
  }
  completion.check("production_backoff_fits_all_valid_timeout_values_and_cap");

  const occupied = new LocalEventBuffer(path.join(root, "occupied.sqlite"), { workspaceId: tenantId });
  const occupiedSession = "33333333-3333-4333-8333-333333333618";
  addRow(occupied, occupiedSession, "cccccccc-cccc-4ccc-8ccc-ccccccccc618");
  let allowFirst!: () => void;
  const firstGate = new Promise<void>((resolve) => { allowFirst = resolve; });
  let firstAtServer!: () => void;
  const firstArrived = new Promise<void>((resolve) => { firstAtServer = resolve; });
  let sendCount = 0;
  const occupiedServer = await server(async (raw, response) => {
    sendCount += 1;
    firstAtServer();
    await firstGate;
    response.writeHead(200, { "content-type": "application/json" });
    response.end(JSON.stringify(acceptedFixtureDelivery(raw, installKey)));
  });
  try {
    const first = runSessionSync(config(occupiedServer.port, 5), {
      ledgerDb: occupied.database, sessionIds: [occupiedSession], until,
      incremental: true, maxAttemptsPerBatch: 1, delayMs: 0, log: () => undefined,
    });
    await firstArrived;
    const firstToken = leaseToken(occupied, occupiedSession);
    assert.ok(firstToken);
    const second = await runSessionSync(config(occupiedServer.port, 5), {
      ledgerDb: occupied.database, sessionIds: [occupiedSession], until,
      incremental: true, maxAttemptsPerBatch: 1, delayMs: 0, log: () => undefined,
    });
    assert.equal(second.ok, false);
    assert.equal(sendCount, 1, "a different batch must not reach the endpoint");
    assert.equal(leaseToken(occupied, occupiedSession), firstToken);
    completion.check("different_batch_cannot_take_occupied_lease");
    allowFirst();
    assert.equal((await first).acceptedSessions, 1);
    assert.equal(leaseToken(occupied, occupiedSession), undefined);
    completion.check("settled_single_attempt_releases_lease_promptly");
  } finally {
    allowFirst();
    occupied.close();
    await closeServer(occupiedServer.endpoint);
  }

  const unsent = new LocalEventBuffer(path.join(root, "pre-send.sqlite"), { workspaceId: tenantId });
  const unsentSession = "44444444-4444-4444-8444-444444444618";
  addRow(unsent, unsentSession, "dddddddd-dddd-4ddd-8ddd-ddddddddd618");
  try {
    const unsentConfig = config(48318, 5);
    let changed = false;
    let networkCalls = 0;
    // The signing-secret access occurs after the lease is installed and just
    // before postJson's freshness check. Change the source at that seam.
    Object.defineProperty(unsentConfig, "uploadSigningSecret", { get: () => {
      if (!changed) {
        changed = true;
        addRow(unsent, unsentSession, "eeeeeeee-eeee-4eee-8eee-eeeeeeeee618");
      }
      return "lease-retry-fixture-secret";
    } });
    const result = await runSessionSync(unsentConfig, {
      ledgerDb: unsent.database, sessionIds: [unsentSession], until,
      incremental: true, maxAttemptsPerBatch: 1, delayMs: 0, log: () => undefined,
      fetchImpl: (async () => { networkCalls += 1; throw new Error("unexpected_send"); }) as typeof fetch,
    });
    assert.equal(changed, true);
    assert.equal(networkCalls, 0);
    assert.equal(result.ok, false);
    assert.equal(leaseToken(unsent, unsentSession), undefined,
      "a confirmed pre-send abort must release the durable lease");
    completion.check("confirmed_pre_send_abort_releases_lease");
  } finally {
    unsent.close();
  }
  completion.complete();
}

void main().catch((error) => { console.error(error); process.exitCode = 1; });
