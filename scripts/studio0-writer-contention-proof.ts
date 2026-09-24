/** Timed Studio0-shaped legacy outbox migration versus steady OTLP intake. */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import type { AddressInfo } from "node:net";
import Database from "better-sqlite3";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { createCollectorServer } from "../packages/collector-cli/src/server";
import { DeliveryUploadError, uploadBufferedEvents } from "../packages/collector-cli/src/upload";
import { SyncStorageRetryController } from "../packages/collector-cli/src/sqlite-contention";

const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-studio0-contention-"));
const ledger = path.join(root, "ledger.sqlite");
const workspace = "00000000-0000-4000-8000-000000000001";
const config = collectorConfigSchema.parse({
  tenantId: workspace, uploadUrl: "http://127.0.0.1:1/ingest", installKey: "fixture-install",
});
const rows = 5_000;
const padding = "x".repeat(5_000);
const uuid = (n: number) => `00000000-0000-4000-8000-${String(n).padStart(12, "0")}`;

function payload(n: number) {
  return JSON.stringify({
    id: uuid(n), source: "codex", dataMode: "metadata", eventType: "assistant_response",
    observedAt: "2026-09-23T00:00:00.000Z", actionClass: "other",
    inputTokens: 1, outputTokens: 1, metadata: { fixture: padding },
  });
}

function otlp(n: number) {
  return {
    resourceSpans: [{ scopeSpans: [{ spans: [{
      name: "handle_responses",
      traceId: n.toString(16).padStart(32, "0"),
      spanId: n.toString(16).padStart(16, "0"),
      startTimeUnixNano: String(BigInt(Date.now()) * 1_000_000n),
      attributes: [{ key: "gen_ai.usage.input_tokens", value: { intValue: "5" } }],
    }] }] }],
  };
}

async function main() {
  const intake = new LocalEventBuffer(ledger, { databaseBusyTimeoutMs: 0, workspaceId: workspace });
  const insert = intake.database.prepare(`insert into buffered_events
    (id, source, event_type, data_mode, observed_at, payload_json, created_at, workspace_id)
    values (?, 'codex', 'assistant_response', 'metadata', '2026-09-23T00:00:00.000Z', ?, '2026-09-23T00:00:00.000Z', ?)`);
  intake.database.transaction(() => {
    for (let n = 1; n <= rows; n++) insert.run(uuid(n), payload(n), workspace);
  })();
  const server = createCollectorServer(config, intake);
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const port = (server.address() as AddressInfo).port;
  const writer = spawn(process.execPath, ["--import", "tsx", __filename, "writer", ledger], {
    cwd: process.cwd(), env: process.env, stdio: ["ignore", "pipe", "pipe"],
  });
  const writerExit = new Promise<number | null>((resolve) => writer.once("exit", resolve));
  const answers: Array<{ status: number; reason: unknown; ms: number }> = [];
  let workerOutput = "";
  let workerErrors = "";
  writer.stdout.setEncoding("utf8");
  writer.stderr.setEncoding("utf8");
  writer.stdout.on("data", (text: string) => { workerOutput += text; });
  writer.stderr.on("data", (text: string) => { workerErrors += text; });
  const started = new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("writer_start_timeout")), 10_000);
    writer.stdout.on("data", () => {
      if (workerOutput.includes("WRITER_START")) { clearTimeout(timer); resolve(); }
    });
  });
  try {
    await started;
    const requests: Array<Promise<void>> = [];
    let n = 100_000;
    while (writer.exitCode === null && n < 100_100) {
      const current = n++;
      requests.push((async () => {
        const at = performance.now();
        const response = await fetch(`http://127.0.0.1:${port}/v1/traces`, {
          method: "POST", headers: { "content-type": "application/json", "x-plimsoll-source": "codex" },
          body: JSON.stringify(otlp(current)), signal: AbortSignal.timeout(5_000),
        });
        const body = await response.json() as Record<string, unknown>;
        answers.push({ status: response.status, reason: body.reason, ms: performance.now() - at });
      })());
      await new Promise((resolve) => setTimeout(resolve, 40));
    }
    await Promise.all(requests);
    const exit = await writerExit;
    assert.equal(exit, 0, workerErrors);
    const workerLine = workerOutput.split("\n").find((line) => line.startsWith("WRITER_RESULT "));
    assert.ok(workerLine, workerOutput);
    const migration = JSON.parse(workerLine.slice("WRITER_RESULT ".length));
    const refusals = answers.filter((answer) => answer.status !== 202);
    console.log(JSON.stringify({ migration, total: answers.length, accepted: answers.length - refusals.length, refusals, ledgerBytes: fs.statSync(ledger).size }));
    assert.ok(answers.length > 0);
    assert.equal(refusals.length, 0, "steady OTLP intake must not be refused during one legacy migration slice");

    // The bounded daemon slices must still finish the cursor, not merely
    // avoid the lock by abandoning pre-outbox rows.
    intake.delivery.configure({ enabled: true, limits: config.delivery });
    const sliceMs: number[] = [];
    let complete = false;
    for (let pass = 0; pass < 100 && !complete; pass++) {
      const at = performance.now();
      const result = intake.delivery.migrateLegacy({
        maxRows: 256, maxBytes: 1_048_576, maxWriterMs: 100,
      });
      sliceMs.push(performance.now() - at);
      complete = result.complete;
    }
    console.log(JSON.stringify({ cursorComplete: complete, slices: sliceMs.length, maxSliceMs: Math.max(...sliceMs) }));
    assert.ok(complete, "the legacy cursor must complete across bounded slices");
    assert.ok(Math.max(...sliceMs) < 750, "one bounded slice must fit the intake busy budget");

    // Another writer can still hold SQLite for longer than a request. The
    // collector must mark that 503 retryable for an OTLP exporter.
    const lock = new Database(ledger, { timeout: 0 });
    lock.exec("BEGIN IMMEDIATE");
    try {
      const response = await fetch(`http://127.0.0.1:${port}/v1/traces`, {
        method: "POST", headers: { "content-type": "application/json", "x-plimsoll-source": "codex" },
        body: JSON.stringify(otlp(900_000)), signal: AbortSignal.timeout(5_000),
      });
      const body = await response.json() as Record<string, unknown>;
      console.log(JSON.stringify({ forcedBusyStatus: response.status, reason: body.reason, retryAfter: response.headers.get("retry-after") }));
      assert.equal(response.status, 503);
      assert.equal(body.reason, "storage_busy_retry");
      assert.equal(response.headers.get("retry-after"), "1");
    } finally {
      lock.exec("ROLLBACK");
      lock.close();
    }
  } finally {
    server.close();
    intake.close();
    fs.rmSync(root, { recursive: true, force: true });
  }
}

if (process.argv[2] === "writer") {
  const writerBuffer = new LocalEventBuffer(process.argv[3]!, {
    databaseBusyTimeoutMs: 0, workspaceId: workspace,
    delivery: { enabled: true, limits: config.delivery },
  });
  process.stdout.write("WRITER_START\n");
  const started = performance.now();
  (async () => {
    try {
      await uploadBufferedEvents(config, writerBuffer, {
        fetchImpl: async () => new Response("", { status: 503 }),
        storageRetry: new SyncStorageRetryController(),
      });
    } catch (error) {
      if (!(error instanceof DeliveryUploadError) || error.failureClass !== "remote_transient") throw error;
    }
    process.stdout.write(`WRITER_RESULT ${JSON.stringify({ migration: writerBuffer.delivery.status().migration.lastSlice, ms: performance.now() - started })}\n`);
  })().catch((error) => {
    process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
    process.exitCode = 1;
  }).finally(() => writerBuffer.close());
} else {
  main().catch((error) => { console.error(error); process.exitCode = 1; });
}
