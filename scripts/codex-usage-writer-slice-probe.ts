/** Bounded large-ledger rehearsal. Run only on an isolated APFS clone. */
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import Database from "better-sqlite3";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { runCodexUsagePairingWriterSlice } from "../packages/collector-cli/src/codex-usage-pairing";
import { explodeOtlpPayload } from "../packages/collector-cli/src/otlp";

const file = path.resolve(process.argv[3] ?? "");
if (!file || !fs.existsSync(file) || !fs.lstatSync(file).isFile()) throw new Error("clone ledger required");

const pct = (values: number[], p: number) => {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.floor((sorted.length - 1) * p)] ?? 0;
};
const stats = (values: number[]) => ({ samples: values.length, p50: pct(values, 0.5),
  p95: pct(values, 0.95), max: Math.max(0, ...values) });

async function worker() {
  const db = new Database(file, { fileMustExist: true, timeout: 5_000 });
  try {
    process.stdout.write("READY\n");
    let visited = 0, paired = 0, transactions = 0, deferredPasses = 0, complete = false;
    const calls: number[] = [], holds: number[] = [], waits: number[] = [], slices: number[] = [];
    for (let pass = 0; pass < 5_000 && !complete; pass += 1) {
      const result = runCodexUsagePairingWriterSlice(db, { maxMs: 15, maxCandidates: 32 });
      visited += result.visited;
      paired += result.paired;
      transactions += result.transactions;
      if (result.deferredForWriter) deferredPasses += 1;
      calls.push(result.maxTransactionMs);
      holds.push(result.maxWriterHoldMs);
      waits.push(result.maxLockWaitMs);
      slices.push(result.elapsedMs);
      complete = result.complete;
      // Still far more frequent than the collector's repair cadence, while
      // allowing an intake writer to run between independent maintenance jobs.
      if (!complete) await new Promise((resolve) => setTimeout(resolve, 25));
    }
    process.stdout.write(JSON.stringify({ complete, visited, paired, transactions, deferredPasses,
      transactionCallMs: stats(calls), writerHoldMs: stats(holds), lockWaitMs: stats(waits),
      sliceMs: stats(slices) }) + "\n");
  } finally { db.close(); }
}

function attr(key: string, value: string | number) {
  return { key, value: typeof value === "number" ? { intValue: String(value) } : { stringValue: value } };
}
function intakeLog(at: number, input: number) {
  const result = explodeOtlpPayload({ resourceLogs: [{ resource: { attributes: [
    attr("service.name", "codex-app-server"),
  ] }, scopeLogs: [{ logRecords: [{ timeUnixNano: String(BigInt(at) * 1_000_000n), attributes: [
    attr("event.name", "codex.sse_event"), attr("event.kind", "response.completed"),
    attr("conversation.id", "019e9100-0000-7000-8000-00000000000a"),
    attr("user.account_id", "synthetic-account"), attr("input_token_count", input),
    attr("output_token_count", 148), attr("cached_token_count", 11_776),
  ] }] }] }] }, { source: "codex" });
  if (result.events.length !== 1 || result.parseFailures) throw new Error("intake fixture rejected");
  return result.events[0]!;
}

async function probe() {
  const buffer = new LocalEventBuffer(file, { delivery: { enabled: false }, databaseBusyTimeoutMs: 0,
    workspaceId: "00000000-0000-4000-8000-000000000084" });
  let child: ReturnType<typeof spawn> | null = null;
  try {
    const db = buffer.database;
    const count = 1_000;
    const nonce = randomUUID();
    const at0 = Date.parse("2026-09-26T02:00:00.000Z");
    const insert = db.prepare(`insert into buffered_events
      (id, source, event_type, data_mode, observed_at, payload_json, created_at,
       session_id, model, input_tokens, output_tokens, cache_read_tokens,
       account_hash, workspace_id, privacy_generation)
      values (@id, 'codex', 'assistant_response', 'metadata', @at, @payload, @at,
        @session, 'gpt-5.1-codex-max', @input, @output, @cache,
        @account, @workspace, @generation)`);
    db.transaction(() => {
      for (let i = 0; i < count; i += 1) {
        const at = new Date(at0 + i * 45_000).toISOString();
        const input = 30_000 + i, output = 100 + i, cache = 10_000 + i;
        for (const kind of ["log", "span"] as const) {
          const id = `${nonce}-${kind}-${i}`;
          const payload = { id, source: "codex", eventType: "assistant_response",
            observedAt: at, inputTokens: input, outputTokens: output, cacheReadTokens: cache,
            metadata: kind === "log"
              ? { otelEventName: "codex.sse_event", sessionId: "019e9100-0000-7000-8000-00000000000a" }
              : { otelEventName: "handle_responses", traceId: (i + 1).toString(16).padStart(32, "0"),
                  otelSpanEndAt: new Date(Date.parse(at) + 20).toISOString() } };
          insert.run({ id, at, payload: JSON.stringify(payload),
            session: kind === "log" ? "019e9100-0000-7000-8000-00000000000a" : null,
            input, output, cache, account: kind === "log" ? "sha256:account" : null,
            workspace: "b2b2b2b2-2222-4222-8222-222222222222", generation: id });
        }
      }
      db.exec(`update codex_usage_pairing_control set cursor_observed_at = '',
        cursor_rowid = 0, target_rowid = (select max(rowid) from buffered_events),
        complete = 0, visited = 0, paired = 0`);
    })();

    child = spawn(process.execPath, ["--import", "tsx", process.argv[1]!, "--worker", file], {
      cwd: process.cwd(), env: process.env, stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "", stderr = "", ready = false, childDone = false;
    child.stdout!.on("data", (chunk: Buffer) => { stdout += chunk.toString(); if (stdout.includes("READY\n")) ready = true; });
    child.stderr!.on("data", (chunk: Buffer) => { stderr += chunk.toString(); });
    const done = new Promise<number | null>((resolve) => child!.once("exit", (code) => {
      childDone = true; resolve(code);
    }));
    const readyUntil = Date.now() + 30_000;
    while (!ready && !childDone && Date.now() < readyUntil) {
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    if (!ready) throw new Error(`worker_not_ready ${stderr.slice(0, 300)}`);
    const loadBefore = os.loadavg()[0];
    const appendMs: number[] = [], concurrentMs: number[] = [], busyMs: number[] = [];
    const queued: ReturnType<typeof intakeLog>[] = [];
    const ids: string[] = [];
    for (let i = 0; i < 120; i += 1) {
      const entry = intakeLog(Date.now() - 60_000 + i * 1_000, 400_000 + i);
      ids.push(entry.event.id);
      const overlap = !childDone;
      const started = performance.now();
      try {
        if (!buffer.append(entry.event, entry.suppressedFields)) throw new Error("intake_append_refused");
        const elapsed = performance.now() - started;
        appendMs.push(elapsed);
        if (overlap) concurrentMs.push(elapsed);
      } catch (error) {
        const code = (error as { code?: unknown }).code;
        if (code !== "SQLITE_BUSY" && code !== "SQLITE_LOCKED") throw error;
        busyMs.push(performance.now() - started);
        queued.push(entry);
      }
      await new Promise((resolve) => setTimeout(resolve, 5));
    }
    const exit = await done;
    if (exit !== 0) throw new Error(`worker_failed ${stderr.slice(0, 500)}`);
    const workerResult = JSON.parse(stdout.slice(stdout.indexOf("READY\n") + 6)) as
      { complete: boolean; visited: number; paired: number; transactions: number; transactionCallMs: unknown;
        sliceMs: unknown };
    const control = db.prepare(`select complete, visited, paired from codex_usage_pairing_control`).get();
    let replayed = 0, duplicateReplays = 0;
    for (const entry of queued) {
      if (buffer.append(entry.event, entry.suppressedFields)) replayed += 1;
      if (!buffer.append(entry.event, entry.suppressedFields)) duplicateReplays += 1;
    }
    const present = db.prepare(`select 1 from buffered_events where id = ?`);
    const presentCount = ids.filter((id) => present.get(id)).length;
    if (!workerResult.complete || workerResult.visited !== count || workerResult.paired !== count ||
        concurrentMs.length < 20 || replayed !== queued.length || duplicateReplays !== queued.length ||
        presentCount !== ids.length) {
      throw new Error(`probe_incomplete ${JSON.stringify({ workerResult, concurrent: concurrentMs.length,
        replayed, duplicateReplays, presentCount })}`);
    }
    console.log(JSON.stringify({ fileBytes: fs.statSync(file).size, injectedPairs: count,
      worker: workerResult, control, loadAverage1m: { before: loadBefore, after: os.loadavg()[0] },
      intakeAppendMs: stats(appendMs), concurrentIntakeAppendMs: stats(concurrentMs),
      busyIntakeMs: stats(busyMs), queued: queued.length, replayed, duplicateReplays, presentCount }, null, 2));
  } finally {
    if (child && child.exitCode === null) {
      child.kill("SIGTERM");
      await new Promise<void>((resolve) => {
        const timer = setTimeout(() => child!.kill("SIGKILL"), 5_000);
        child!.once("exit", () => { clearTimeout(timer); resolve(); });
      });
    }
    buffer.close();
  }
}

const mode = process.argv[2];
if (mode === "--worker") worker().catch((error) => { console.error(error); process.exitCode = 1; });
else if (mode === "--probe") probe().catch((error) => { console.error(error); process.exitCode = 1; });
else throw new Error("usage: codex-usage-writer-slice-probe --probe|--worker CLONE.sqlite");
