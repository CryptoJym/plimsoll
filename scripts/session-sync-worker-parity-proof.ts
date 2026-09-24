/** Preserve the exact hosted session-sync bytes when reads move off-thread. */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import {
  buildSessionSyncRow, collectSessionSnapshots, listLedgerSessionIdsOffThread,
  planDaemonSessionSync, runSessionSync,
} from "../packages/collector-cli/src/session-sync";
import { aiWorkSessionSyncBatchSchema } from "../packages/shared/src/index";
import { acceptedFixtureDelivery } from "./lib/delivery-fixture";

const root = process.env.TMPDIR;
assert.ok(root && fs.realpathSync(root) === root && root.startsWith(process.env.HOME + path.sep));
const ledgerPath = path.join(root, `session-sync-parity-${process.pid}.sqlite`);
const tenantId = "00000000-0000-4000-8000-000000000070";
const sessionA = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa70";
const sessionB = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbb70";
const until = "2026-09-23T12:00:00.000Z";
const config = collectorConfigSchema.parse({
  uploadUrl: "http://127.0.0.1:1/ingest", tenantId, installKey: "fixture-install",
  uploadSigningSecret: "session-sync-fixture-secret",
});

async function main() {
  const buffer = new LocalEventBuffer(ledgerPath, { workspaceId: tenantId });
  try {
    const insert = buffer.database.prepare(`insert into buffered_events
      (id, source, event_type, data_mode, observed_at, payload_json, created_at,
       workspace_id, session_id, privacy_generation, input_tokens, output_tokens,
       cost_usd, repo_hash, branch_hash, account_hash)
      values (?, ?, 'assistant_response', ?, ?, '{}', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
    const hashA = `sha256:${"a".repeat(64)}`;
    const hashB = `sha256:${"b".repeat(64)}`;
    const before = "2026-09-23T08:00:00.000Z";
    const after = "2026-09-23T13:00:00.000Z";
    insert.run("a1", "codex", "metadata", before, before, tenantId, sessionA, "gen-a1", 5, 2, 0.1, hashA, hashB, hashA);
    insert.run("a2", "codex", "metadata", before, before, tenantId, sessionA, "gen-a2", 7, 3, null, hashB, hashB, hashA);
    insert.run("b1", "claude_code", "metadata", before, before, tenantId, sessionB, "gen-b1", 11, 4, 0.2, hashB, hashB, hashB);
    insert.run("b-evidence", "claude_code", "evidence", before, before, tenantId, sessionB, "gen-b2", 999, 999, 999, hashB, hashB, hashB);
    insert.run("b-late", "claude_code", "metadata", after, after, tenantId, sessionB, "gen-b3", 999, 999, 999, hashB, hashB, hashB);

    const direct = collectSessionSnapshots(buffer.database, { until });
    assert.equal(direct.length, 2);
    const rows = direct.map(snapshot => buildSessionSyncRow(snapshot));
    assert.ok(rows.every(row => row.ok));
    const expectedBody = JSON.stringify(aiWorkSessionSyncBatchSchema.parse({
      kind: "session_sync", tenantId, installKey: config.installKey,
      appVersion: "0.1.0", sessions: rows.flatMap(row => row.ok ? [row.row] : []),
    }));
    let wireBody = "";
    const fetchImpl = (async (_input, init) => {
      wireBody = String(init?.body ?? "");
      return new Response(JSON.stringify(acceptedFixtureDelivery(wireBody, config.installKey)), {
        status: 200, headers: { "content-type": "application/json" },
      });
    }) as typeof fetch;
    const result = await runSessionSync(config, {
      ledgerDb: buffer.database, until, fetchImpl, delayMs: 0,
      maxAttemptsPerBatch: 1, log: () => undefined,
    });
    assert.equal(result.ok, true);
    assert.equal(result.acceptedSessions, 2);
    assert.equal(wireBody, expectedBody, "hosted batch bytes changed");

    const input = {
      db: buffer.database,
      state: {
        schemaVersion: 1 as const, caughtUp: true,
        lastSuccessfulUntil: "2026-09-23T07:00:00.000Z",
        pendingSessionIds: ["pending-session"],
      },
      uploadedBatches: [],
      until,
    };
    const directPlan = planDaemonSessionSync(input);
    const workerPlan = planDaemonSessionSync({
      ...input,
      ledgerSessionIds: await listLedgerSessionIdsOffThread(buffer.database, {
        until, since: input.state.lastSuccessfulUntil,
      }),
    });
    assert.equal(JSON.stringify(workerPlan), JSON.stringify(directPlan), "daemon plan changed");
    console.log(JSON.stringify({ result: "PASS", cases: ["mixed_cost_and_hashes", "privacy_and_until", "hosted_bytes", "daemon_plan"],
      sessions: result.acceptedSessions, bodyBytes: wireBody.length }));
  } finally {
    buffer.close();
    for (const suffix of ["", "-wal", "-shm"]) fs.rmSync(ledgerPath + suffix, { force: true });
  }
}

main().catch(error => { console.error(error); process.exitCode = 1; });
