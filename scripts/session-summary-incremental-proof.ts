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
  emptyDaemonSessionSyncState,
  listLedgerSessionIdsOffThread,
  planDaemonSessionSync,
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

async function reviewRegressions() {
  const selected = process.env.PROBE_CASE;
  const cases = [
    "first_read_insert", "fallback_checkpoint", "missing_dirty_marker", "privacy_before_send",
    "unrelated_revision", "retry_erasure", "hard_bounds", "session_id_paging",
    "privacy_lineage_first_read", "checkpoint_timeout", "future_horizon",
  ];
  for (const name of cases) {
    if (selected && selected !== name) continue;
    const fixture = fs.mkdtempSync(path.join(os.tmpdir(), `p41-${name}-`));
    const buffer = new LocalEventBuffer(path.join(fixture, "ledger.sqlite"), { workspaceId: tenantId });
    const sessionId = `dddddddd-dddd-4ddd-8ddd-${String(cases.indexOf(name) + 1).padStart(12, "0")}`;
    const add = (index: number) => insertRaw(buffer, {
      id: uuid(400 + cases.indexOf(name) * 10 + index), sessionId,
      observedAt: `2026-09-20T02:${String(index).padStart(2, "0")}:00.000Z`,
      createdAt: `2026-09-20T02:${String(index).padStart(2, "0")}:00.000Z`,
      inputTokens: 1, outputTokens: 1, costUsd: 0.01,
    });
    try {
      ensureSessionSummarySchema(buffer.database);
      const directRead = async <T,>(queries: Array<{ sql: string; params: Record<string, unknown> }>): Promise<T[]> =>
        queries.flatMap((query) => buffer.database.prepare(query.sql).all(query.params) as T[]);
      if (name === "first_read_insert") {
        add(1);
        let injected = false;
        const read = async <T,>(queries: Array<{ sql: string; params: Record<string, unknown> }>): Promise<T[]> => {
          const rows = await directRead<T>(queries);
          if (!injected && queries.some((query) => query.sql.includes("order by e.observed_at, e.rowid asc"))) {
            injected = true;
            add(2);
          }
          return rows;
        };
        await updateSessionSummary(buffer.database, sessionId, until, { read });
        const resumed = await updateSessionSummary(buffer.database, sessionId, until, { read });
        assert.equal(injected, true);
        assert.equal(resumed.complete, true);
        assert.deepEqual(resumed.snapshot, collectSessionSnapshots(buffer.database, { until, sessionIds: [sessionId] })[0]);
      } else if (name === "fallback_checkpoint") {
        for (let index = 1; index <= 4; index += 1) add(index);
        await updateSessionSummary(buffer.database, sessionId, until, { read: directRead });
        buffer.database.prepare("update buffered_events set input_tokens = 99 where id = ?").run(uuid(411));
        const partial = await updateSessionSummary(buffer.database, sessionId, until, { read: directRead, maxRows: 1 });
        assert.equal(partial.complete, false);
        buffer.database.prepare("update session_sync_summary_state set high_water = high_water + 100 where session_id = ?").run(sessionId);
        const resumed = await updateSessionSummary(buffer.database, sessionId, until, { read: directRead });
        assert.equal(resumed.complete, true);
        assert.deepEqual(resumed.snapshot, collectSessionSnapshots(buffer.database, { until, sessionIds: [sessionId] })[0]);
      } else if (name === "missing_dirty_marker") {
        add(1);
        await updateSessionSummary(buffer.database, sessionId, until, { read: directRead });
        buffer.database.prepare("update buffered_events set input_tokens = 77 where id = ?").run(uuid(421));
        buffer.database.prepare("delete from session_sync_summary_dirty where session_id = ?").run(sessionId);
        const plan = planDaemonSessionSync({
          db: buffer.database,
          state: { ...emptyDaemonSessionSyncState(), caughtUp: true, lastSuccessfulUntil: "2026-09-21T00:00:00.000Z" },
          uploadedBatches: [], until, ledgerSessionIds: [],
        });
        assert.equal(plan.skip, false);
        assert.ok(plan.sessionIds?.includes(sessionId));
      } else if (name === "privacy_before_send") {
        add(1);
        let sent = "";
        let erased = false;
        const result = await runSessionSync(config, {
          ledgerDb: buffer.database, incremental: true, sessionIds: [sessionId], until,
          delayMs: 0, maxAttemptsPerBatch: 1,
          fetchImpl: (async (_input, init) => {
            sent = String(init?.body ?? "");
            return new Response(JSON.stringify(acceptedFixtureDelivery(sent, installKey)), {
              status: 200, headers: { "content-type": "application/json" },
            });
          }) as typeof fetch,
          log: (line) => {
            if (!erased && line.includes('"status":"session_sync_start"')) {
              buffer.database.prepare("delete from buffered_events where id = ?").run(uuid(431));
              erased = true;
            }
          },
        });
        assert.equal(erased, true);
        assert.equal(sent, "");
        assert.equal(result.summaryComplete, false);
        assert.ok(result.pendingSummarySessionIds.includes(sessionId));
      } else if (name === "unrelated_revision") {
        for (let index = 1; index <= 8; index += 1) add(index);
        await updateSessionSummary(buffer.database, sessionId, until, { read: directRead });
        buffer.database.prepare("update buffered_events set output_tokens = 42 where id = ?").run(uuid(441));
        const first = await updateSessionSummary(buffer.database, sessionId, until, { read: directRead, maxRows: 1 });
        assert.equal(first.complete, false);
        const unrelatedId = "eeeeeeee-eeee-4eee-8eee-000000000001";
        insertRaw(buffer, {
          id: uuid(470), sessionId: unrelatedId,
          observedAt: "2026-09-20T03:00:00.000Z", createdAt: "2026-09-20T03:00:00.000Z",
          inputTokens: 1, outputTokens: 1,
        });
        buffer.database.prepare("update buffered_events set output_tokens = 43 where id = ?").run(uuid(470));
        const second = await updateSessionSummary(buffer.database, sessionId, until, { read: directRead, maxRows: 1 });
        assert.ok(second.highWater > first.highWater, JSON.stringify({ first, second }));
      } else if (name === "retry_erasure") {
        add(1);
        let calls = 0;
        const result = await runSessionSync(config, {
          ledgerDb: buffer.database, incremental: true, sessionIds: [sessionId], until,
          delayMs: 0, maxAttemptsPerBatch: 2,
          fetchImpl: (async () => {
            calls += 1;
            return new Response("{}", { status: 503 });
          }) as typeof fetch,
          sleep: async () => {
            buffer.database.prepare("delete from buffered_events where id = ?").run(uuid(451));
          },
          log: () => undefined,
        });
        assert.equal(calls, 1);
        assert.equal(result.sentSessions, 0);
        assert.equal(result.summaryComplete, false);
        assert.ok(result.pendingSummarySessionIds.includes(sessionId));
      } else if (name === "hard_bounds") {
        for (let index = 1; index <= 5_200; index += 1) {
          insertRaw(buffer, {
            id: uuid(10_000 + index), sessionId,
            observedAt: "2026-09-20T00:00:00.000Z", createdAt: "2026-09-20T00:00:00.000Z",
            inputTokens: 1, outputTokens: 1,
          });
        }
        const result = await updateSessionSummary(buffer.database, sessionId, until, {
          read: directRead, maxRows: 100_000, maxMs: 5_000,
        });
        assert.ok(result.rowsRead <= 5_000, JSON.stringify(result));
        assert.equal(result.complete, false);
      } else if (name === "session_id_paging") {
        for (let index = 1; index <= 5_001; index += 1) {
          insertRaw(buffer, {
            id: uuid(20_000 + index),
            sessionId: `eeeeeeee-eeee-4eee-8eee-${String(index).padStart(12, "0")}`,
            observedAt: "2026-09-20T00:00:00.000Z", createdAt: "2026-09-20T00:00:00.000Z",
            inputTokens: 1, outputTokens: 1,
          });
        }
        const ids = await listLedgerSessionIdsOffThread(buffer.database, {
          until, maxIds: Number.POSITIVE_INFINITY,
        });
        assert.equal(ids.length, 5_001);
        const initialIds = await listLedgerSessionIdsOffThread(buffer.database, {
          until, maxIds: Number.POSITIVE_INFINITY, allSessions: true,
        });
        assert.deepEqual(initialIds, ids);
      } else if (name === "privacy_lineage_first_read") {
        add(1);
        let changed = false;
        const read = async <T,>(queries: Array<{ sql: string; params: Record<string, unknown> }>): Promise<T[]> => {
          const rows = await directRead<T>(queries);
          if (!changed && queries.some((query) => query.sql.includes("order by e.observed_at, e.rowid asc"))) {
            changed = true;
            buffer.database.prepare(`insert into upload_receipts
              (delivery_id, terminal_state, reason, status_class, attempt_count, created_at, terminal_at)
              values (?, 'dead', 'local_privacy_violation', 'local', 0, ?, ?)`).run(
              uuid(481), "2026-09-20T02:00:00.000Z", "2026-09-20T02:00:00.000Z",
            );
          }
          return rows;
        };
        const first = await updateSessionSummary(buffer.database, sessionId, until, { read });
        assert.equal(changed, true);
        assert.equal(first.complete, false);
        const next = await updateSessionSummary(buffer.database, sessionId, until, { read });
        assert.equal(next.complete, true);
        assert.equal(next.snapshot, null);
      } else if (name === "checkpoint_timeout") {
        for (let index = 1; index <= 4; index += 1) add(index);
        const first = await updateSessionSummary(buffer.database, sessionId, until, {
          read: directRead, maxRows: 1,
        });
        assert.equal(first.complete, false);
        const deferred = await updateSessionSummary(buffer.database, sessionId, until, {
          read: async <T,>(queries: Array<{ sql: string; params: Record<string, unknown> }>): Promise<T[]> => {
            if (queries.some((query) => query.sql.includes("from buffered_events where rowid = @rowid"))) {
              throw new Error("session_summary_read_interrupted");
            }
            return directRead<T>(queries);
          },
        });
        assert.equal(deferred.complete, false);
        assert.equal(deferred.fullRecompute, false);
        assert.equal(deferred.highWater, first.highWater);
        const resumed = await updateSessionSummary(buffer.database, sessionId, until, { read: directRead });
        assert.equal(resumed.complete, true);
        assert.deepEqual(resumed.snapshot, collectSessionSnapshots(buffer.database, { until, sessionIds: [sessionId] })[0]);
      } else if (name === "future_horizon") {
        insertRaw(buffer, {
          id: uuid(501), sessionId,
          observedAt: "2026-09-19T00:00:00.000Z", createdAt: "2026-10-02T00:00:00.000Z",
          inputTokens: 10, outputTokens: 1,
        });
        add(2);
        const first = await updateSessionSummary(buffer.database, sessionId, until, { read: directRead });
        assert.equal(first.snapshot?.events, 1);
        const later = "2026-10-10T00:00:00.000Z";
        const second = await updateSessionSummary(buffer.database, sessionId, later, { read: directRead });
        assert.equal(second.fullRecompute, true);
        assert.deepEqual(second.snapshot, collectSessionSnapshots(buffer.database, {
          until: later, sessionIds: [sessionId],
        })[0]);
      }
      console.log(JSON.stringify({ reviewCase: name, result: "PASS" }));
    } finally {
      buffer.close();
      fs.rmSync(fixture, { recursive: true, force: true });
    }
  }
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

  await reviewRegressions();
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
