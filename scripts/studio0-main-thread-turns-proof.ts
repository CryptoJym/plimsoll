/**
 * eco-6hoxj.163.24: the collector's event loop stays responsive while it takes
 * in live usage for a very large session.
 *
 * Studio0's busiest Codex session holds 1.88M ledger rows, 97% otel_span.
 * Before this bead every usage event the capture path projected counted all of
 * that session's live-usage rows through idx_events_session, one table point
 * lookup per session row, mostly on pages SQLite does not hold: a multi-second
 * synchronous turn per usage event (17.7 s worst in a replay of the real
 * snapshot; the Statement::JS_get -> BtreeTableMoveto -> pread stack in the
 * Studio0 sample). This fixture builds one Codex session of SESSION_ROWS rows
 * whose observed_at order is shuffled against insertion order, so walking the
 * session index is a cold point lookup per row, then serves Codex OTLP exports
 * for that session through the collector's own HTTP server and measures the
 * longest event-loop stall. It also holds an upload batch to the same budget
 * and checks the off-thread WAL checkpointer.
 *
 *   pnpm proof:studio0-main-thread-turns
 *   STUDIO0_TURNS_SESSION_ROWS=300000 pnpm proof:studio0-main-thread-turns
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import type { AddressInfo } from "node:net";

import Database from "better-sqlite3";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { createCollectorServer } from "../packages/collector-cli/src/server";
import { uploadBufferedEvents } from "../packages/collector-cli/src/upload";
import { acceptedFixtureDelivery } from "./lib/delivery-fixture";

/** The bead's budget for one synchronous main-thread turn on this data. */
const TURN_BUDGET_MS = 250;
const SESSION_ROWS = Number(process.env.STUDIO0_TURNS_SESSION_ROWS ?? 250_000);
/** Studio0's busiest session: 12,889 usage rows among 1.88M. */
const USAGE_EVERY = 146;
/** Studio0 ledger tail: ~600-850 bytes of payload_json per row. */
const PAYLOAD_PAD = "x".repeat(640);
const EXPORTS = 24;
const RECORDS_PER_EXPORT = 16;
const USAGE_RECORDS_PER_EXPORT = 2;

const root = process.env.TMPDIR;
assert.ok(root && fs.realpathSync(root) === root && root.startsWith(process.env.HOME + path.sep),
  "run under the CI layout: TMPDIR inside the synthetic HOME");
const ledgerPath = path.join(root, `studio0-main-thread-turns-${process.pid}.sqlite`);
const tenantId = "00000000-0000-4000-8000-000000000163";
const deviceId = "studio0-main-thread-turns-device";
const sessionId = "019b0000-0000-7000-8000-000000000163";
const config = collectorConfigSchema.parse({
  uploadUrl: "http://127.0.0.1:1/ingest", tenantId, installKey: "studio0-turns-install", deviceId, managed: true,
});
const LIVE_USAGE_ROW = `event_type not in ('usage_rollout','usage_transcript')
  and (input_tokens is not null or output_tokens is not null or cache_read_tokens is not null
    or cache_creation_tokens is not null or cost_usd is not null)`;

type Check = { name: string; passed: boolean; detail?: unknown };
const checks: Check[] = [];
const measurements: Record<string, unknown> = { sessionRows: SESSION_ROWS, turnBudgetMs: TURN_BUDGET_MS };

async function check(name: string, run: () => unknown | Promise<unknown>) {
  try {
    const detail = await run();
    checks.push({ name, passed: true, ...(detail === undefined ? {} : { detail }) });
  } catch (error) {
    checks.push({ name, passed: false, detail: error instanceof Error ? error.message : String(error) });
  }
  const last = checks.at(-1)!;
  console.log(JSON.stringify({ check: name, passed: last.passed, ...(last.passed ? {} : { detail: last.detail }) }));
}

/** Deterministic shuffle so every run builds the same ledger. */
function shuffled(count: number) {
  let seed = 0x163_24;
  const random = () => {
    seed = (seed + 0x6d2b79f5) | 0;
    let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4_294_967_296;
  };
  const order = Array.from({ length: count }, (_, index) => index);
  for (let index = count - 1; index > 0; index -= 1) {
    const swap = Math.floor(random() * (index + 1));
    [order[index], order[swap]] = [order[swap]!, order[index]!];
  }
  return order;
}

/** Studio0-shaped history for one Codex session, seeded without live side effects. */
function seedLedger() {
  const buffer = new LocalEventBuffer(ledgerPath, {
    workspaceId: tenantId, deviceId, databaseBusyTimeoutMs: 0,
    delivery: { enabled: true, limits: config.delivery },
  });
  const db = buffer.database;
  const schema = db.prepare(`select type, name, sql from sqlite_master
    where tbl_name = 'buffered_events' and type in ('index', 'trigger') and sql is not null`)
    .all() as Array<{ type: string; name: string; sql: string }>;
  for (const item of schema) db.exec(`drop ${item.type} "${item.name.replaceAll('"', '""')}"`);
  const insert = db.prepare(`insert into buffered_events
    (id, source, event_type, data_mode, observed_at, payload_json, created_at, uploaded_at, session_id,
     model, input_tokens, output_tokens, workspace_id, device_id, privacy_generation)
    values (@id, 'codex', @eventType, 'metadata', @observedAt, @payload, @observedAt, @observedAt, @sessionId,
     @model, @inputTokens, @outputTokens, @workspaceId, @deviceId, @generation)`);
  const historyStart = Date.now() - 30 * 24 * 60 * 60 * 1_000;
  const started = performance.now();
  const ranks = shuffled(SESSION_ROWS);
  db.transaction(() => {
    ranks.forEach((rank, index) => {
      // Usage rows are evenly spaced in observed order, as in the real session.
      const usage = rank % USAGE_EVERY === 7;
      const observedAt = new Date(historyStart + rank * 1_000).toISOString();
      const id = `studio0-history-${index}`;
      insert.run({
        id, observedAt, sessionId, workspaceId: tenantId, deviceId, generation: `generation-${index}`,
        eventType: usage ? "assistant_response" : "otel_span",
        model: usage ? "gpt-5.1-codex-max" : null,
        inputTokens: usage ? 40_000 : null,
        outputTokens: usage ? 900 : null,
        payload: JSON.stringify({ id, source: "codex", eventType: usage ? "assistant_response" : "otel_span",
          observedAt, sessionId, metadata: { otelEventName: "codex.sse_event", pad: PAYLOAD_PAD } }),
      });
    });
  })();
  for (const item of schema) db.exec(item.sql);
  // The session claimed live usage authority at its first usage event, long
  // ago; its history is delivered and the legacy outbox migration is done.
  db.prepare(`insert into session_usage_authority (source, session_id, authority, claimed_at)
    values ('codex', ?, 'live', ?)`).run(sessionId, new Date(historyStart).toISOString());
  db.prepare(`update upload_control set migration_complete = 1,
    migration_cursor_rowid = (select max(rowid) from buffered_events) where singleton = 1`).run();
  db.pragma("wal_checkpoint(TRUNCATE)");
  measurements.seedMs = Math.round(performance.now() - started);
  buffer.close();
  measurements.ledgerBytes = fs.statSync(ledgerPath).size;
}

function monitorEventLoop() {
  const intervalMs = 10;
  let last = performance.now();
  let maxMs = 0;
  let over = 0;
  const timer = setInterval(() => {
    const now = performance.now();
    const blocked = now - last - intervalMs;
    if (blocked > maxMs) maxMs = blocked;
    if (blocked > TURN_BUDGET_MS) over += 1;
    last = now;
  }, intervalMs);
  return () => {
    clearInterval(timer);
    return { maxBlockedMs: Math.round(maxMs), stallsOverBudget: over };
  };
}

const s = (key: string, value: string) => ({ key, value: { stringValue: value } });
const i = (key: string, value: number) => ({ key, value: { intValue: String(value) } });
let sequence = 0;

/**
 * One Codex OTLP log export: response.completed usage records first, then SSE
 * deltas. Usage leads so capture-time projection of every usage row fits the
 * route's 25 ms projection allowance on a slow runner, unless one of them
 * walks the session.
 */
function codexExport() {
  const now = BigInt(Date.now()) * 1_000_000n;
  return {
    resourceLogs: [{
      resource: { attributes: [s("service.name", "codex_cli_rs")] },
      scopeLogs: [{
        logRecords: Array.from({ length: RECORDS_PER_EXPORT }, (_, index) => {
          sequence += 1;
          const usage = index < USAGE_RECORDS_PER_EXPORT;
          return {
            timeUnixNano: String(now + BigInt(sequence)),
            attributes: [
              s("event.name", "codex.sse_event"), s("conversation.id", sessionId), s("model", "gpt-5.1-codex-max"),
              s("event.kind", usage ? "response.completed" : "response.output_text.delta"), i("event.sequence", sequence),
              ...(usage ? [i("input_token_count", 42_000), i("output_token_count", 800), i("cached_token_count", 30_000)] : []),
            ],
          };
        }),
      }],
    }],
  };
}

async function main() {
  seedLedger();
  console.log(JSON.stringify({ seeded: measurements }));

  await check("fixture_session_walk_is_cold_and_large_enough", () => {
    // The defect's own statement on a fresh connection: the fixture has to make
    // one whole-session walk cost well over the budget, or it proves nothing.
    const db = new Database(ledgerPath, { readonly: true, fileMustExist: true });
    try {
      const started = performance.now();
      const { n } = db.prepare(`select count(*) as n from buffered_events
        where source = 'codex' and session_id = ? and ${LIVE_USAGE_ROW}`).get(sessionId) as { n: number };
      const walkMs = Math.round(performance.now() - started);
      measurements.wholeSessionWalkMs = walkMs;
      assert.ok(n >= Math.floor(SESSION_ROWS / USAGE_EVERY), `fixture has ${n} usage rows`);
      assert.ok(walkMs > TURN_BUDGET_MS,
        `fixture too small to expose a whole-session walk on this host: ${walkMs} ms; raise STUDIO0_TURNS_SESSION_ROWS`);
      return { usageRows: n, walkMs };
    } finally {
      db.close();
    }
  });

  const buffer = new LocalEventBuffer(ledgerPath, {
    workspaceId: tenantId, deviceId, databaseBusyTimeoutMs: 0,
    delivery: { enabled: true, limits: config.delivery },
  });
  const server = createCollectorServer(config, buffer);
  try {
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const port = (server.address() as AddressInfo).port;
    const exportStartedAt = new Date().toISOString();

    await check("codex_usage_exports_for_the_big_session_never_hold_the_event_loop_over_budget", async () => {
      const stop = monitorEventLoop();
      const answers: Array<{ status: number; outcome: unknown; ms: number }> = [];
      for (let index = 0; index < EXPORTS; index += 1) {
        const started = performance.now();
        const response = await fetch(`http://127.0.0.1:${port}/v1/logs`, {
          method: "POST",
          headers: { "content-type": "application/json", "x-plimsoll-source": "codex" },
          body: JSON.stringify(codexExport()),
          signal: AbortSignal.timeout(60_000),
        });
        const body = await response.json() as { accepted?: boolean; status?: string };
        answers.push({ status: response.status, outcome: body.status ?? body.accepted, ms: Math.round(performance.now() - started) });
      }
      const loop = stop();
      measurements.exports = { ...loop, maxAnswerMs: Math.max(...answers.map((answer) => answer.ms)) };
      assert.ok(answers.every((answer) => answer.status === 202 && answer.outcome === true),
        `an export was not accepted in place: ${JSON.stringify(answers.filter((a) => a.outcome !== true).slice(0, 3))}`);
      assert.ok(loop.maxBlockedMs <= TURN_BUDGET_MS,
        `event loop held for ${loop.maxBlockedMs} ms (budget ${TURN_BUDGET_MS} ms, ${loop.stallsOverBudget} stalls over it)`);
      return measurements.exports;
    });

    await check("usage_rows_of_the_big_session_are_projected_at_capture", () => {
      const row = buffer.database.prepare(`select count(*) as rows, count(f.raw_rowid) as projected
        from (select rowid as raw_rowid from buffered_events
              where session_id = ? and created_at >= ? and ${LIVE_USAGE_ROW}) b
        left join dashboard_event_facts f on f.raw_rowid = b.raw_rowid`)
        .get(sessionId, exportStartedAt) as { rows: number; projected: number };
      assert.equal(row.rows, EXPORTS * USAGE_RECORDS_PER_EXPORT, "usage rows appended");
      assert.equal(row.projected, row.rows, "a usage row was left unprojected at capture");
      return row;
    });

    await check("an_upload_batch_never_holds_the_event_loop_over_budget", async () => {
      const fetchImpl = (async (_input: RequestInfo | URL, init?: RequestInit) => new Response(
        JSON.stringify(acceptedFixtureDelivery(String(init?.body ?? ""), config.installKey)),
        { status: 200, headers: { "content-type": "application/json" } },
      )) as typeof fetch;
      const stop = monitorEventLoop();
      let uploaded = 0;
      for (let batch = 0; batch < 4; batch += 1) {
        const result = await uploadBufferedEvents(config, buffer, { fetchImpl, includeLegacyRemainingUnuploaded: false });
        uploaded += result.uploadedEvents;
        if (result.uploadedEvents === 0) break;
      }
      const loop = stop();
      measurements.upload = { uploaded, ...loop };
      assert.equal(uploaded, EXPORTS * RECORDS_PER_EXPORT, "every captured event uploaded exactly once");
      assert.ok(loop.maxBlockedMs <= TURN_BUDGET_MS, `event loop held for ${loop.maxBlockedMs} ms during upload`);
      return measurements.upload;
    });

    await check("wal_checkpoints_run_off_the_event_loop_and_fall_back_on_worker_loss", async () => {
      const module = await import("../packages/collector-cli/src/wal-checkpoint-worker").catch(() => null);
      assert.ok(module, "no off-thread WAL checkpointer: the daemon connection checkpoints (and fsyncs) inside intake commits");
      const walCheckpoint = new module.WalCheckpointWorker(buffer.database, 50);
      assert.equal(walCheckpoint.start(), true);
      assert.equal(buffer.database.pragma("wal_autocheckpoint", { simple: true }), 0);
      const response = await fetch(`http://127.0.0.1:${port}/v1/logs`, {
        method: "POST",
        headers: { "content-type": "application/json", "x-plimsoll-source": "codex" },
        body: JSON.stringify(codexExport()),
      });
      assert.equal(response.status, 202);
      const deadline = Date.now() + 10_000;
      while (walCheckpoint.counters.framesCheckpointed === 0 && Date.now() < deadline) {
        await new Promise((resolve) => setTimeout(resolve, 25));
      }
      assert.ok(walCheckpoint.counters.framesCheckpointed > 0, "the worker checkpointed no frames");
      const counters = { ...walCheckpoint.counters };
      // Losing the worker thread restores SQLite's own automatic checkpoint.
      const worker = (walCheckpoint as unknown as { worker: { terminate(): Promise<number> } | null }).worker;
      assert.ok(worker, "worker thread running");
      await worker.terminate();
      await new Promise((resolve) => setTimeout(resolve, 50));
      assert.equal(buffer.database.pragma("wal_autocheckpoint", { simple: true }), 1_000);
      assert.equal(walCheckpoint.counters.failures, 1);
      await walCheckpoint.stop();
      return counters;
    });
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()));
    buffer.close();
    for (const suffix of ["", "-wal", "-shm"]) fs.rmSync(ledgerPath + suffix, { force: true });
  }

  const failed = checks.filter((entry) => !entry.passed);
  console.log(JSON.stringify({
    proof: "studio0-main-thread-turns",
    bead: "eco-6hoxj.163.24",
    passed: failed.length === 0,
    checks: checks.length,
    failures: failed.map((entry) => ({ name: entry.name, detail: entry.detail })),
    measurements,
  }, null, 2));
  if (failed.length > 0) process.exitCode = 1;
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
