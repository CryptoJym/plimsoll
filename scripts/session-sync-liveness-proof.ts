/** A large touched session must not stop the collector HTTP loop during sync. */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import type { AddressInfo } from "node:net";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { createCollectorServer } from "../packages/collector-cli/src/server";
import { buildSessionSyncRow, collectSessionSnapshots, runSessionSync } from "../packages/collector-cli/src/session-sync";
import { aiWorkSessionSyncBatchSchema } from "../packages/shared/src/index";
import { acceptedFixtureDelivery } from "./lib/delivery-fixture";

const root = process.env.TMPDIR;
assert.ok(root && fs.realpathSync(root) === root && root.startsWith(process.env.HOME + path.sep));
const ledgerPath = path.join(root, `session-sync-liveness-${process.pid}.sqlite`);
const tenantId = "00000000-0000-4000-8000-000000000070";
const sessionId = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa70";
const rows = Number(process.env.SESSION_SYNC_LIVENESS_ROWS ?? 1_000_000);
const until = "2026-09-24T00:00:00.000Z";
const config = collectorConfigSchema.parse({
  uploadUrl: "http://127.0.0.1:1/ingest", tenantId, installKey: "fixture-install",
  uploadSigningSecret: "session-sync-fixture-secret",
});

async function main() {
  const buffer = new LocalEventBuffer(ledgerPath, { workspaceId: tenantId, databaseBusyTimeoutMs: 0 });
  const server = createCollectorServer(config, buffer);
  try {
    // Build a historical ledger without running live append side effects for
    // every seed row, then restore the real read indexes and triggers.
    const schema = buffer.database.prepare(`select type, name, sql from sqlite_master
      where tbl_name = 'buffered_events' and type in ('index', 'trigger') and sql is not null`)
      .all() as Array<{ type: string; name: string; sql: string }>;
    for (const item of schema) {
      buffer.database.exec(`drop ${item.type} "${item.name.replaceAll('"', '""')}"`);
    }
    const insert = buffer.database.prepare(`insert into buffered_events
      (id, source, event_type, data_mode, observed_at, payload_json, created_at,
       workspace_id, session_id, privacy_generation, input_tokens, output_tokens,
       cache_read_tokens, cache_creation_tokens, cost_usd, repo_hash, branch_hash, account_hash)
      values (?, 'codex', 'assistant_response', 'metadata', ?, '{}', ?, ?, ?, ?, 2, 1,
              1, 0, 0.000001, ?, ?, ?)`);
    const observedAt = "2026-09-23T00:00:00.000Z";
    const repo = `sha256:${"a".repeat(64)}`;
    const branch = `sha256:${"b".repeat(64)}`;
    const account = `sha256:${"c".repeat(64)}`;
    const seedStarted = performance.now();
    buffer.database.transaction(() => {
      for (let n = 0; n < rows; n++) {
        insert.run(`event-${n}`, observedAt, observedAt, tenantId, sessionId,
          `generation-${n}`, repo, branch, account);
      }
    })();
    for (const item of schema) buffer.database.exec(item.sql);
    buffer.database.pragma("wal_checkpoint(TRUNCATE)");
    console.log(JSON.stringify({ seededRows: rows, seedMs: Math.round(performance.now() - seedStarted),
      ledgerBytes: fs.statSync(ledgerPath).size, walBytes: fs.statSync(`${ledgerPath}-wal`).size }));

    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const port = (server.address() as AddressInfo).port;
    let body = "";
    const fetchImpl = (async (_input, init) => {
      body = String(init?.body ?? "");
      return new Response(JSON.stringify(acceptedFixtureDelivery(body, config.installKey)), {
        status: 200, headers: { "content-type": "application/json" },
      });
    }) as typeof fetch;
    const clock = performance.now();
    const health = new Promise<{ elapsedMs: number; status: number }>((resolve, reject) => {
      setTimeout(() => {
        fetch(`http://127.0.0.1:${port}/healthz`, { signal: AbortSignal.timeout(5_000) })
          .then(response => resolve({ elapsedMs: performance.now() - clock, status: response.status }), reject);
      }, 0);
    });
    const sync = runSessionSync(config, {
      ledgerDb: buffer.database, sessionIds: [sessionId], until, fetchImpl,
      delayMs: 0, maxAttemptsPerBatch: 1, log: () => undefined,
    });
    const ping = await health;
    const result = await sync;
    const totalMs = performance.now() - clock;
    console.log(JSON.stringify({ ping, totalMs: Math.round(totalMs), syncOk: result.ok,
      acceptedSessions: result.acceptedSessions }));
    assert.ok(totalMs > 1_500, `fixture query too small to test 1.5 s deadline: ${totalMs}ms`);
    assert.equal(ping.status, 200);
    assert.ok(ping.elapsedMs < 1_500, `HTTP loop blocked for ${ping.elapsedMs}ms`);
    assert.equal(result.acceptedSessions, 1);
    const direct = collectSessionSnapshots(buffer.database, { until, sessionIds: [sessionId] });
    const expected = buildSessionSyncRow(direct[0]);
    assert.equal(expected.ok, true);
    if (!expected.ok) throw new Error("fixture_session_invalid");
    const expectedBody = JSON.stringify(aiWorkSessionSyncBatchSchema.parse({
      kind: "session_sync", tenantId, installKey: config.installKey,
      appVersion: "0.1.0", sessions: [expected.row],
    }));
    assert.equal(body, expectedBody, "hosted payload bytes changed");
    console.log(JSON.stringify({ result: "PASS", snapshotBytes: JSON.stringify(direct).length,
      wireBytes: body.length, costUsd: direct[0].costUsd }));
  } finally {
    await new Promise<void>(resolve => server.close(() => resolve()));
    buffer.close();
    for (const suffix of ["", "-wal", "-shm"]) fs.rmSync(ledgerPath + suffix, { force: true });
  }
}

main().catch(error => { console.error(error); process.exitCode = 1; });
