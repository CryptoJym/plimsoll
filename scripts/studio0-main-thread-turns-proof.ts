/**
 * eco-6hoxj.163.24: the collector's event loop stays responsive while it takes
 * in live usage for a very large session, uploads, and checkpoints its WAL.
 *
 * Studio0's busiest Codex session holds 1.88M ledger rows, 97% otel_span.
 * Before this bead every usage event the capture path projected counted all of
 * that session's live-usage rows through idx_events_session, one table point
 * lookup per session row, mostly on pages SQLite does not hold: a multi-second
 * synchronous turn per usage event (17.7 s worst in a replay of the real
 * snapshot; the Statement::JS_get -> BtreeTableMoveto -> pread stack in the
 * Studio0 sample). This fixture builds one Codex session whose observed_at
 * order is shuffled against insertion order, so walking the session index is a
 * cold point lookup per row, sized on the running host until that walk costs
 * at least twice the turn budget. Then:
 *   - capture commits for that session (LocalEventBuffer.appendMany, the OTLP
 *     route's own commit) are timed directly, with the route's 25 ms projection
 *     allowance lifted so the result never depends on it;
 *   - Codex OTLP exports go through the collector's HTTP server under an
 *     event-loop monitor;
 *   - one upload batch must claim and acknowledge in slices of at most 125
 *     rows with the event loop turning between them;
 *   - the real daemon must report its WAL checkpoint worker (with the WAL's
 *     bound) and a labeled starvation census on /status.
 *
 * Budgets are checked against the main thread's CPU time per turn, the work a
 * turn does and the part the collector's code controls. Wall time is measured
 * and reported beside it: on a loaded or throttled runner it also counts time
 * the OS did not run the thread at all (at background priority on a busy
 * 32-core host, turns of at most ~120 ms of CPU took up to ~540 ms of wall
 * time).
 *
 *   pnpm proof:studio0-main-thread-turns
 *   STUDIO0_TURNS_SESSION_ROWS=400000 pnpm proof:studio0-main-thread-turns
 */
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import fs from "node:fs";
import http from "node:http";
import path from "node:path";
import type { AddressInfo } from "node:net";

import Database from "better-sqlite3";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { readLocalIngestAuth } from "../packages/collector-cli/src/local-auth";
import { createCollectorServer } from "../packages/collector-cli/src/server";
import { uploadBufferedEvents } from "../packages/collector-cli/src/upload";
import { aiInteractionEventSchema } from "../packages/shared/src/index";
import { acceptedFixtureDelivery } from "./lib/delivery-fixture";

/** The bead's budget for one synchronous main-thread turn on this data. */
const TURN_BUDGET_MS = 250;
/** The fixture must make the old whole-session walk cost this multiple of the budget. */
const WALK_FLOOR = 2;
const INITIAL_SESSION_ROWS = Number(process.env.STUDIO0_TURNS_SESSION_ROWS ?? 250_000);
const MAX_SESSION_ROWS = 1_500_000;
/** Studio0's busiest session: 12,889 usage rows among 1.88M. */
const USAGE_EVERY = 146;
/** Studio0 ledger tail: ~600-850 bytes of payload_json per row. */
const PAYLOAD_PAD = "x".repeat(640);
const CAPTURE_COMMITS = 16;
const EXPORTS = 24;
const RECORDS_PER_EXPORT = 16;
const USAGE_RECORDS_PER_EXPORT = 2;
/** The upload slice size (upload.ts LEASE_SLICE_ROWS). */
const SLICE_ROWS = 125;
/** The daemon connection's WAL bound (wal-checkpoint-worker.ts). */
const WAL_BOUND_FRAMES = 20_000;

const repoRoot = path.resolve(__dirname, "..");
const cliSource = path.join(repoRoot, "packages/collector-cli/src/cli.ts");
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
const measurements: Record<string, unknown> = { turnBudgetMs: TURN_BUDGET_MS };

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
function seedLedger(rows: number) {
  for (const suffix of ["", "-wal", "-shm"]) fs.rmSync(ledgerPath + suffix, { force: true });
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
  const ranks = shuffled(rows);
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
  buffer.close();
  return { rows, seedMs: Math.round(performance.now() - started), ledgerBytes: fs.statSync(ledgerPath).size };
}

/** CPU time the main thread has used, in ms (the whole process's before Node 22.19). */
function threadCpuMs() {
  const usage = (process as { threadCpuUsage?: () => NodeJS.CpuUsage }).threadCpuUsage?.() ?? process.cpuUsage();
  return (usage.user + usage.system) / 1_000;
}

/** The defect's own statement, timed on a fresh connection. */
function wholeSessionWalk() {
  const db = new Database(ledgerPath, { readonly: true, fileMustExist: true });
  try {
    const started = performance.now();
    const cpu = threadCpuMs();
    const { n } = db.prepare(`select count(*) as n from buffered_events
      where source = 'codex' and session_id = ? and ${LIVE_USAGE_ROW}`).get(sessionId) as { n: number };
    return { usageRows: n, walkCpuMs: Math.round(threadCpuMs() - cpu), walkMs: Math.round(performance.now() - started) };
  } finally {
    db.close();
  }
}

/**
 * Per turn, over a window: main-thread CPU time and timer lateness. The final
 * stretch is counted too: a timer cannot interrupt a synchronous run that ends
 * right before stop().
 */
function monitorEventLoop() {
  const intervalMs = 10;
  let last = performance.now();
  let lastCpu = threadCpuMs();
  let maxMs = 0;
  let maxCpuMs = 0;
  let over = 0;
  const note = (now: number) => {
    const cpu = threadCpuMs();
    const blocked = now - last - intervalMs;
    const worked = cpu - lastCpu;
    if (blocked > maxMs) maxMs = blocked;
    if (worked > maxCpuMs) maxCpuMs = worked;
    if (worked > TURN_BUDGET_MS) over += 1;
    last = now;
    lastCpu = cpu;
  };
  const timer = setInterval(() => note(performance.now()), intervalMs);
  return () => {
    clearInterval(timer);
    note(performance.now());
    return { maxTurnCpuMs: Math.round(maxCpuMs), turnsOverBudget: over, maxBlockedMs: Math.round(maxMs) };
  };
}

const s = (key: string, value: string) => ({ key, value: { stringValue: value } });
const i = (key: string, value: number) => ({ key, value: { intValue: String(value) } });
let sequence = 0;

/** One Codex OTLP log export: response.completed usage records, then SSE deltas. */
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

/** The same events as the route builds them, for a capture commit timed directly. */
function captureChunk() {
  return Array.from({ length: RECORDS_PER_EXPORT }, (_, index) => {
    sequence += 1;
    const usage = index < USAGE_RECORDS_PER_EXPORT;
    return {
      event: aiInteractionEventSchema.parse({
        id: `00000000-0000-4000-8163-${String(sequence).padStart(12, "0")}`,
        sessionId, source: "codex", dataMode: "metadata", actionClass: "other",
        eventType: usage ? "assistant_response" : "otel_span",
        observedAt: new Date().toISOString(),
        ...(usage ? { model: "gpt-5.1-codex-max", inputTokens: 42_000, outputTokens: 800, cacheReadTokens: 30_000 } : {}),
        metadata: { otelEventName: "codex.sse_event" },
      }),
      suppressedFields: [],
    };
  });
}

function reserveLoopbackPort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = http.createServer();
    server.once("listening", () => {
      const port = (server.address() as AddressInfo).port;
      server.close(() => resolve(port));
    });
    server.once("error", reject);
    server.listen(0, "127.0.0.1");
  });
}

function fetchStatus(port: number, token: string) {
  return new Promise<Record<string, any> | null>((resolve) => {
    const request = http.get(`http://127.0.0.1:${port}/status`,
      { timeout: 5_000, headers: { "x-plimsoll-token": token } }, (response) => {
        let body = "";
        response.on("data", (chunk) => { body += chunk; });
        response.on("end", () => {
          try { resolve(JSON.parse(body) as Record<string, any>); } catch { resolve(null); }
        });
      });
    request.on("error", () => resolve(null));
    request.on("timeout", () => { request.destroy(); resolve(null); });
  });
}

async function waitFor<T>(read: () => T | Promise<T>, done: (value: T) => boolean, timeoutMs: number) {
  const until = Date.now() + timeoutMs;
  let value = await read();
  while (!done(value) && Date.now() < until) {
    await new Promise((resolve) => setTimeout(resolve, 100));
    value = await read();
  }
  return value;
}

async function main() {
  // Size the fixture on this host: a whole-session walk must cost WALK_FLOOR x
  // the budget, so the defect this proof guards against would be visible here.
  let seeded = seedLedger(INITIAL_SESSION_ROWS);
  let walk = wholeSessionWalk();
  while (walk.walkCpuMs < WALK_FLOOR * TURN_BUDGET_MS && seeded.rows < MAX_SESSION_ROWS) {
    const rows = Math.min(MAX_SESSION_ROWS,
      Math.ceil((seeded.rows * WALK_FLOOR * TURN_BUDGET_MS * 1.25) / Math.max(1, walk.walkCpuMs)));
    seeded = seedLedger(rows);
    walk = wholeSessionWalk();
  }
  Object.assign(measurements, { sessionRows: seeded.rows, seedMs: seeded.seedMs, ledgerBytes: seeded.ledgerBytes,
    wholeSessionWalkCpuMs: walk.walkCpuMs, wholeSessionWalkMs: walk.walkMs });
  console.log(JSON.stringify({ seeded: measurements }));

  await check("fixture_session_walk_costs_twice_the_budget", () => {
    assert.ok(walk.usageRows >= Math.floor(seeded.rows / USAGE_EVERY), `fixture has ${walk.usageRows} usage rows`);
    assert.ok(walk.walkCpuMs >= WALK_FLOOR * TURN_BUDGET_MS,
      `a ${seeded.rows}-row session walk takes only ${walk.walkCpuMs} ms of CPU here; the proof cannot see the defect`);
    return walk;
  });

  const buffer = new LocalEventBuffer(ledgerPath, {
    workspaceId: tenantId, deviceId, databaseBusyTimeoutMs: 0,
    delivery: { enabled: true, limits: config.delivery },
  });
  const server = createCollectorServer(config, buffer);
  try {
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const port = (server.address() as AddressInfo).port;

    await check("capture_commits_for_the_big_session_stay_under_budget_and_project_every_usage_row", () => {
      const startedAt = new Date().toISOString();
      const commitMs: number[] = [];
      const commitCpuMs: number[] = [];
      for (let index = 0; index < CAPTURE_COMMITS; index += 1) {
        const chunk = captureChunk();
        const at = performance.now();
        const cpu = threadCpuMs();
        // No projection allowance: every row is projected unless capture defers it.
        buffer.appendMany(chunk, [], [], { projectionDeadlineMs: Number.POSITIVE_INFINITY });
        commitCpuMs.push(threadCpuMs() - cpu);
        commitMs.push(performance.now() - at);
      }
      const row = buffer.database.prepare(`select count(*) as rows, count(f.raw_rowid) as projected
        from (select rowid as raw_rowid from buffered_events
              where session_id = ? and created_at >= ? and ${LIVE_USAGE_ROW}) b
        left join dashboard_event_facts f on f.raw_rowid = b.raw_rowid`)
        .get(sessionId, startedAt) as { rows: number; projected: number };
      const maxCommitCpuMs = Math.round(Math.max(...commitCpuMs));
      measurements.capture = { commits: CAPTURE_COMMITS, maxCommitCpuMs, maxCommitMs: Math.round(Math.max(...commitMs)),
        usageRows: row.rows, projected: row.projected };
      assert.equal(row.rows, CAPTURE_COMMITS * USAGE_RECORDS_PER_EXPORT, "usage rows appended");
      assert.equal(row.projected, row.rows, "capture left a usage row of a live session unprojected");
      assert.ok(maxCommitCpuMs <= TURN_BUDGET_MS,
        `a capture commit held the thread for ${maxCommitCpuMs} ms of CPU (budget ${TURN_BUDGET_MS} ms)`);
      return measurements.capture;
    });

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
      assert.ok(loop.maxTurnCpuMs <= TURN_BUDGET_MS,
        `a turn took ${loop.maxTurnCpuMs} ms of CPU (budget ${TURN_BUDGET_MS} ms, ${loop.turnsOverBudget} turns over it)`);
      return measurements.exports;
    });

    await check("an_upload_batch_is_claimed_and_acknowledged_in_slices_with_the_loop_turning_between", async () => {
      // Counts event-loop turns: runs once per iteration's check phase.
      let turns = 0;
      let pumping = true;
      const pump = () => { turns += 1; if (pumping) setImmediate(pump); };
      setImmediate(pump);
      type Call = { rows: number; turnsAtStart: number; turnsAtEnd: number };
      const leases: Call[] = [];
      const acks: Call[] = [];
      const requests: number[] = [];
      const lease = buffer.delivery.lease.bind(buffer.delivery);
      const acknowledge = buffer.delivery.acknowledge.bind(buffer.delivery);
      buffer.delivery.lease = (options) => {
        const call = { rows: options?.maxRows ?? 500, turnsAtStart: turns, turnsAtEnd: turns };
        const result = lease(options);
        leases.push({ ...call, turnsAtEnd: turns });
        return result;
      };
      buffer.delivery.acknowledge = ((leaseId, ids, ...rest) => {
        const call = { rows: ids.length, turnsAtStart: turns, turnsAtEnd: turns };
        const result = acknowledge(leaseId, ids, ...rest);
        acks.push({ ...call, turnsAtEnd: turns });
        return result;
      }) as typeof acknowledge;
      // A cloud that answers on a later macrotask, as a real network does.
      const fetchImpl = (async (_input: RequestInfo | URL, init?: RequestInit) => {
        requests.push(turns);
        await new Promise((resolve) => setTimeout(resolve, 1));
        return new Response(JSON.stringify(acceptedFixtureDelivery(String(init?.body ?? ""), config.installKey)),
          { status: 200, headers: { "content-type": "application/json" } });
      }) as typeof fetch;
      const pending = buffer.delivery.status().active.pending;
      const stop = monitorEventLoop();
      let result: Awaited<ReturnType<typeof uploadBufferedEvents>>;
      try {
        result = await uploadBufferedEvents(config, buffer, { fetchImpl, includeLegacyRemainingUnuploaded: false });
      } finally {
        pumping = false;
        buffer.delivery.lease = lease;
        buffer.delivery.acknowledge = acknowledge;
      }
      const loop = stop();
      const turned = (calls: Call[]) => calls.every((call, index) => index === 0 || call.turnsAtStart > calls[index - 1]!.turnsAtEnd);
      measurements.upload = { pending, uploaded: result.uploadedEvents, leaseRows: leases.map((call) => call.rows),
        ackRows: acks.map((call) => call.rows), ...loop };
      assert.ok(pending >= 500, `fixture has only ${pending} pending rows`);
      assert.equal(result.uploadedEvents, 500, "one full batch");
      assert.ok(leases.length >= Math.ceil(500 / SLICE_ROWS) && leases.every((call) => call.rows <= SLICE_ROWS),
        `batch claimed in ${leases.length} lease call(s) of ${leases.map((call) => call.rows).join(",")} rows`);
      assert.ok(acks.length >= Math.ceil(500 / SLICE_ROWS) && acks.every((call) => call.rows <= SLICE_ROWS),
        `batch acknowledged in ${acks.length} call(s) of ${acks.map((call) => call.rows).join(",")} rows`);
      assert.ok(turned(leases), "no event-loop turn between lease slices");
      assert.ok(turned(acks), "no event-loop turn between acknowledge slices");
      assert.ok(requests.length === 1 && requests[0]! > leases.at(-1)!.turnsAtEnd,
        "revalidation and the request body ran in the same turn as the last lease slice");
      assert.ok(loop.maxTurnCpuMs <= TURN_BUDGET_MS, `a turn took ${loop.maxTurnCpuMs} ms of CPU during the upload`);
      return measurements.upload;
    });
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()));
    buffer.close();
    for (const suffix of ["", "-wal", "-shm"]) fs.rmSync(ledgerPath + suffix, { force: true });
  }

  await check("the_daemon_runs_its_wal_checkpoint_worker_and_labels_its_starvation_census", async () => {
    const home = fs.mkdtempSync(path.join(path.dirname(ledgerPath), "studio0-turns-daemon-"));
    fs.chmodSync(home, 0o700);
    const port = await reserveLoopbackPort();
    fs.writeFileSync(path.join(home, "collector.config.json"), `${JSON.stringify({ port }, null, 2)}\n`, { mode: 0o600 });
    const daemon = spawn(process.execPath, ["--import", "tsx", cliSource, "start"], {
      cwd: repoRoot, env: { ...process.env, PLIMSOLL_HOME: home }, stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    daemon.stdout.setEncoding("utf8");
    daemon.stderr.setEncoding("utf8");
    daemon.stdout.on("data", (text: string) => { stdout += text; });
    daemon.stderr.on("data", (text: string) => { stderr += text; });
    try {
      const active = await waitFor(() => stdout.includes('"status":"active"'), Boolean, 90_000);
      const auth = readLocalIngestAuth(home);
      if (!active || !auth) throw new Error(`daemon did not start: ${stderr.slice(-300)}`);
      const managementToken = auth.managementRead;
      // The first pass runs one worker interval (5 s) after start.
      const status = await waitFor(() => fetchStatus(port, managementToken),
        (body) => Boolean(body?.walCheckpoint?.lastSuccessAt), 30_000);
      const walCheckpoint = status?.walCheckpoint;
      const starvation = status?.maintenance?.starvation;
      measurements.daemon = { walCheckpoint, starvationBacklogObservedAt: starvation?.backlogObservedAt ?? null };
      assert.ok(walCheckpoint, "/status has no walCheckpoint: the daemon does not report its checkpoint worker");
      assert.equal(walCheckpoint.mode, "worker", "the daemon did not start its WAL checkpoint worker");
      assert.equal(walCheckpoint.autocheckpointFrames, WAL_BOUND_FRAMES, "the daemon connection has no WAL bound");
      assert.ok(walCheckpoint.lastSuccessAt, "the worker never completed a pass");
      assert.ok(typeof starvation?.backlogObservedAt === "string", "the starvation census is not labeled with its time");
      return measurements.daemon;
    } finally {
      daemon.kill("SIGTERM");
      await waitFor(() => daemon.exitCode !== null || daemon.signalCode !== null, Boolean, 20_000);
      if (daemon.exitCode === null && daemon.signalCode === null) daemon.kill("SIGKILL");
      fs.rmSync(home, { recursive: true, force: true });
    }
  });

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
