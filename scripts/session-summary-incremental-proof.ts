import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { markRawPrivacyDisposition } from "../packages/collector-cli/src/privacy-disposition";
import {
  buildSessionSyncRow,
  collectSessionSnapshots,
  readLedgerOffThread,
  runSessionSync,
} from "../packages/collector-cli/src/session-sync";
import {
  ensureSessionSummarySchema,
  sessionSummaryCounters,
  updateSessionSummary,
} from "../packages/collector-cli/src/session-summary";
import { acceptedFixtureDelivery } from "./lib/delivery-fixture";

const expect = process.env.EXPECT === "red" ? "red" : "green";
const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-session-summary-proof-"));
const ledgerPath = path.join(root, "ledger.sqlite");
const tenantId = "00000000-0000-4000-8000-000000000741";
const installKey = "session-summary-proof-install";
const until = "2026-09-30T23:59:59.000Z";
const sessionA = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa41";
const sessionB = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbb41";
const sessionC = "cccccccc-cccc-4ccc-8ccc-cccccccccc41";
const hashA = `sha256:${"a".repeat(64)}`;
const hashB = `sha256:${"b".repeat(64)}`;

const config = collectorConfigSchema.parse({
  uploadUrl: "http://127.0.0.1:1/ingest",
  tenantId,
  installKey,
  uploadSigningSecret: "session-summary-proof-secret",
});

function uuid(index: number) {
  return `00000000-0000-4000-8000-${String(index).padStart(12, "0")}`;
}

function insertRaw(
  buffer: LocalEventBuffer,
  input: {
    id: string;
    sessionId: string;
    observedAt: string;
    createdAt: string;
    inputTokens: number;
    outputTokens: number;
    costUsd?: number | null;
    repoHash?: string | null;
    branchHash?: string | null;
    accountHash?: string | null;
  },
) {
  buffer.database.prepare(`
    insert into buffered_events
      (id, source, event_type, data_mode, observed_at, payload_json,
       suppressed_fields_json, created_at, session_id, input_tokens, output_tokens,
       cost_usd, repo_hash, branch_hash, account_hash, workspace_id,
       privacy_generation)
    values (?, 'codex', 'assistant_response', 'metadata', ?, '{}', '[]', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    input.id,
    input.observedAt,
    input.createdAt,
    input.sessionId,
    input.inputTokens,
    input.outputTokens,
    input.costUsd ?? null,
    input.repoHash ?? null,
    input.branchHash ?? null,
    input.accountHash ?? null,
    tenantId,
    `generation-${input.id}`,
  );
}

function expectedSessions(buffer: LocalEventBuffer, sessionIds: string[]) {
  return collectSessionSnapshots(buffer.database, { until, sessionIds })
    .map(buildSessionSyncRow)
    .flatMap((row) => row.ok ? [row.row] : [])
    .sort((a, b) => a.session.id.localeCompare(b.session.id));
}

async function runIncremental(buffer: LocalEventBuffer, sessionIds: string[]) {
  let wire = "";
  const fetchImpl = (async (_input, init) => {
    wire = String(init?.body ?? "");
    return new Response(JSON.stringify({
      ...acceptedFixtureDelivery(wire, installKey),
      inserted: JSON.parse(wire).sessions.length,
      updated: 0,
      skippedStale: 0,
    }), { status: 200, headers: { "content-type": "application/json" } });
  }) as typeof fetch;
  const result = await runSessionSync(config, {
    ledgerDb: buffer.database,
    incremental: true,
    sessionIds,
    until,
    fetchImpl,
    sleep: async () => undefined,
    delayMs: 0,
    maxAttemptsPerBatch: 1,
    summaryMaxRows: 100_000,
    summaryMaxMs: 5_000,
    log: () => undefined,
  });
  const sent = wire ? JSON.parse(wire).sessions : [];
  return { result, sent };
}

function compareExact(buffer: LocalEventBuffer, sessionIds: string[], sent: unknown[]) {
  assert.deepEqual(
    sent.slice().sort((a, b) =>
      (a as { session: { id: string } }).session.id.localeCompare(
        (b as { session: { id: string } }).session.id,
      )),
    expectedSessions(buffer, sessionIds),
  );
}

async function main() {
  if (!fs.existsSync(path.join(process.cwd(), "packages/collector-cli/src/session-summary.ts"))) {
    console.error(JSON.stringify({ result: "RED", reason: "incremental_summary_module_missing" }));
    if (expect === "red") return;
    throw new Error("incremental_summary_module_missing");
  }

  const buffer = new LocalEventBuffer(ledgerPath, { workspaceId: tenantId });
  try {
    ensureSessionSummarySchema(buffer.database);
    for (let index = 0; index < 6; index += 1) {
      insertRaw(buffer, {
        id: uuid(index + 1), sessionId: sessionA,
        observedAt: `2026-09-20T00:0${index}:00.000Z`,
        createdAt: `2026-09-20T00:0${index}:00.000Z`,
        inputTokens: 10 + index, outputTokens: 2,
        costUsd: index % 2 === 0 ? 0.01 * (index + 1) : null,
        repoHash: hashA, branchHash: hashA, accountHash: hashA,
      });
    }
    for (let index = 0; index < 3; index += 1) {
      insertRaw(buffer, {
        id: uuid(index + 20), sessionId: sessionB,
        observedAt: `2026-09-20T01:0${index}:00.000Z`,
        createdAt: `2026-09-20T01:0${index}:00.000Z`,
        inputTokens: 4, outputTokens: 1, costUsd: 0.02,
        repoHash: hashB, branchHash: hashB, accountHash: hashB,
      });
    }

    const first = await runIncremental(buffer, [sessionA, sessionB]);
    assert.equal(first.result.ok, true);
    assert.equal(first.result.summaryComplete, true);
    compareExact(buffer, [sessionA, sessionB], first.sent);
    assert.equal(first.result.summaryStats.fullRecomputes, 0);

    // Late/out-of-order observation and a new row with an older created_at
    // both receive a higher rowid and are therefore visible past the HWM.
    insertRaw(buffer, {
      id: uuid(40), sessionId: sessionA,
      observedAt: "2026-09-19T23:59:00.000Z",
      createdAt: "2026-09-19T23:59:00.000Z",
      inputTokens: 99, outputTokens: 7, costUsd: 0.09,
      repoHash: hashA, branchHash: hashA, accountHash: hashA,
    });
    insertRaw(buffer, {
      id: uuid(41), sessionId: sessionB,
      observedAt: "2026-09-19T22:59:00.000Z",
      createdAt: "2026-09-19T22:59:00.000Z",
      inputTokens: 3, outputTokens: 8, costUsd: null,
      repoHash: hashB, branchHash: hashB, accountHash: hashB,
    });
    const appended = await runIncremental(buffer, [sessionA, sessionB]);
    assert.equal(appended.result.summaryComplete, true);
    assert.equal(appended.result.summaryStats.fullRecomputes, 0);
    assert.ok(
      appended.result.summaryStats.rowsRead >= 2,
      JSON.stringify(appended.result.summaryStats),
    );
    compareExact(buffer, [sessionA, sessionB], appended.sent);

    const oldA = uuid(1);
    buffer.database.prepare("update buffered_events set input_tokens = 777 where id = ?").run(oldA);
    const edited = await runIncremental(buffer, [sessionA, sessionB]);
    assert.equal(edited.result.summaryComplete, true);
    assert.ok(edited.result.summaryStats.fullRecomputes >= 1);
    compareExact(buffer, [sessionA, sessionB], edited.sent);

    buffer.database.prepare("delete from buffered_events where id = ?").run(uuid(2));
    const deleted = await runIncremental(buffer, [sessionA, sessionB]);
    assert.equal(deleted.result.summaryComplete, true);
    assert.ok(deleted.result.summaryStats.fullRecomputes >= 1);
    compareExact(buffer, [sessionA, sessionB], deleted.sent);

    const privacyRow = buffer.database.prepare(
      "select rowid from buffered_events where id = ?",
    ).get(uuid(20)) as { rowid: number };
    assert.equal(markRawPrivacyDisposition(
      buffer.database, privacyRow.rowid, "local_privacy_violation", new Date().toISOString(),
    ), 1);
    const privacy = await runIncremental(buffer, [sessionA, sessionB]);
    assert.equal(privacy.result.summaryComplete, true);
    assert.ok(privacy.result.summaryStats.fullRecomputes >= 1);
    compareExact(buffer, [sessionA, sessionB], privacy.sent);

    // A bounded pass persists its HWM before the process is restarted.
    for (let index = 0; index < 14; index += 1) {
      insertRaw(buffer, {
        id: uuid(100 + index), sessionId: sessionC,
        observedAt: `2026-09-21T00:${String(index).padStart(2, "0")}:00.000Z`,
        createdAt: `2026-09-21T00:${String(index).padStart(2, "0")}:00.000Z`,
        inputTokens: 1, outputTokens: 1, costUsd: 0.001,
      });
    }
    const partial = await updateSessionSummary(buffer.database, sessionC, until, {
      maxRows: 3,
      maxMs: 500,
      read: (queries) => readLedgerOffThread(buffer.database, queries),
    });
    assert.equal(partial.complete, false);
    const savedHwm = buffer.database.prepare(
      "select high_water as highWater, complete from session_sync_summary_state where session_id = ?",
    ).get(sessionC) as { highWater: number; complete: number };
    assert.ok(savedHwm.highWater > 0 && savedHwm.complete === 0);
    buffer.close();

    const restarted = new LocalEventBuffer(ledgerPath, { workspaceId: tenantId });
    try {
      const resumed = await runIncremental(restarted, [sessionC]);
      assert.equal(resumed.result.summaryComplete, true);
      compareExact(restarted, [sessionC], resumed.sent);

      // HWM corruption is detected from the durable checkpoint, not trusted.
      restarted.database.prepare(
        "update session_sync_summary_state set high_water = high_water + 100 where session_id = ?",
      ).run(sessionC);
      const hwmBroken = await runIncremental(restarted, [sessionC]);
      assert.equal(hwmBroken.result.summaryComplete, true);
      assert.ok(hwmBroken.result.summaryStats.fullRecomputes >= 1);
      compareExact(restarted, [sessionC], hwmBroken.sent);

      // Mutation-probe: delete the dirty marker after an edit. The global
      // mutation revision still forces a safe fallback instead of a wrong
      // incremental result.
      restarted.database.prepare("update buffered_events set output_tokens = 88 where id = ?").run(uuid(100));
      restarted.database.prepare("delete from session_sync_summary_dirty where session_id = ?").run(sessionC);
      const skippedFallback = await runIncremental(restarted, [sessionC]);
      assert.equal(skippedFallback.result.summaryComplete, true);
      assert.ok(skippedFallback.result.summaryStats.fullRecomputes >= 1);
      compareExact(restarted, [sessionC], skippedFallback.sent);

      const counters = sessionSummaryCounters(restarted.database);
      assert.ok(counters.fallbackRecomputes >= 5);

      // Optional real downgrade lane: a v0.7.37 LocalEventBuffer appends a
      // late row while the summary state is present, then the current code
      // resumes and must match a full recompute.
      const downgradeRoot = process.env.PLIMSOLL_0737_ROOT;
      if (downgradeRoot) {
        restarted.close();
        const legacy = await import(pathToFileURL(path.join(
          downgradeRoot, "packages/collector-cli/src/buffer.ts",
        )).href);
        const LegacyBuffer = legacy.LocalEventBuffer as typeof LocalEventBuffer;
        const oldBuffer = new LegacyBuffer(ledgerPath, { workspaceId: tenantId });
        try {
          insertRaw(oldBuffer, {
            id: uuid(200), sessionId: sessionC,
            observedAt: "2026-09-18T00:00:00.000Z",
            createdAt: "2026-09-18T00:00:00.000Z",
            inputTokens: 5, outputTokens: 6, costUsd: 0.005,
          });
        } finally {
          oldBuffer.close();
        }
        const afterDowngrade = new LocalEventBuffer(ledgerPath, { workspaceId: tenantId });
        try {
          const upgraded = await runIncremental(afterDowngrade, [sessionC]);
          assert.equal(upgraded.result.summaryComplete, true);
          compareExact(afterDowngrade, [sessionC], upgraded.sent);
        } finally {
          afterDowngrade.close();
        }
      }
    } finally {
      if (restarted.database.open) restarted.close();
    }
  } finally {
    try { buffer.close(); } catch { /* already closed by restart case */ }
    for (const suffix of ["", "-wal", "-shm"]) fs.rmSync(ledgerPath + suffix, { force: true });
    fs.rmSync(root, { recursive: true, force: true });
  }

  console.log(JSON.stringify({
    result: "PASS",
    proof: "session-summary-incremental",
    exactness: ["append", "late_row", "edit", "delete", "privacy_disposition", "restart", "hwm_mutation", "fallback_marker_mutation"],
    downgrade: Boolean(process.env.PLIMSOLL_0737_ROOT),
  }));
}

import { pathToFileURL } from "node:url";

main().catch((error) => {
  if (expect === "red") {
    console.log(JSON.stringify({ result: "RED", proof: "session-summary-incremental", reason: error instanceof Error ? error.message : String(error) }));
    process.exitCode = 0;
    return;
  }
  console.error(error);
  process.exitCode = 1;
});
