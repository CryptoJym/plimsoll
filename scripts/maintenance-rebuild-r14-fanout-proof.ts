/** An unrelated hook must have a constant-time refusal lookup at 3,000 pending receipts. */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash, randomUUID } from "node:crypto";
import { performance } from "node:perf_hooks";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { appendForwardedHook } from "../packages/collector-cli/src/forwarder";

const root = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "r14-receipt-fanout-")));
const emptyHome = path.join(root, "empty");
const occupiedHome = path.join(root, "occupied");
fs.mkdirSync(emptyHome); fs.mkdirSync(occupiedHome);
const buffers = [new LocalEventBuffer(path.join(emptyHome, "ledger.sqlite")),
  new LocalEventBuffer(path.join(occupiedHome, "ledger.sqlite"))];
try {
  const directory = path.join(occupiedHome, "maintenance-rebuild-refusals");
  fs.mkdirSync(path.join(emptyHome, "maintenance-rebuild-refusals"), { mode: 0o700 });
  fs.mkdirSync(directory, { mode: 0o700 });
  const config = collectorConfigSchema.parse({});
  const body = () => ({ id: randomUUID(), hook_event_name: "UserPromptSubmit",
    session_id: randomUUID(), timestamp: new Date().toISOString() });
  for (let index = 0; index < 3_000; index += 1) {
    const name = createHash("sha256").update(String(index)).digest("hex") + ".receipt";
    const receipt = { version: 5, route: "hook", receiptId: randomUUID(),
      at: new Date().toISOString(), source: "claude_code", eventId: randomUUID(),
      kind: "user_prompt_submit", sessionId: randomUUID(), originalTimestampDigest: null,
      eventDigest: null, receiveClockFallback: false, ledgerHighWater: 0 };
    fs.writeFileSync(path.join(directory, name), JSON.stringify(receipt));
  }
  for (let index = 0; index < 10; index += 1) {
    appendForwardedHook(body(), { config, source: "claude_code", buffer: buffers[0]! });
    appendForwardedHook(body(), { config, source: "claude_code", buffer: buffers[1]! });
  }
  let receiptReads = 0;
  const originalRead = fs.readFileSync;
  (fs as typeof fs & { readFileSync: typeof fs.readFileSync }).readFileSync = ((file: fs.PathOrFileDescriptor,
    ...rest: unknown[]) => {
    if (typeof file === "string" && file.endsWith(".receipt")) receiptReads += 1;
    return (originalRead as any)(file, ...rest);
  }) as typeof fs.readFileSync;
  const samples = 80;
  let emptyMs = 0;
  let occupiedMs = 0;
  try {
    for (let index = 0; index < samples; index += 1) {
      for (const current of (index % 2 === 0 ? [0, 1] : [1, 0])) {
        const started = performance.now();
        appendForwardedHook(body(), { config, source: "claude_code", buffer: buffers[current]! });
        const elapsed = performance.now() - started;
        if (current === 0) emptyMs += elapsed; else occupiedMs += elapsed;
      }
    }
  } finally {
    (fs as typeof fs & { readFileSync: typeof fs.readFileSync }).readFileSync = originalRead;
  }
  emptyMs /= samples; occupiedMs /= samples;
  console.log(JSON.stringify({ check: "unrelated_hook_receipt_fanout", receiptFiles: 3_000,
    receiptReads, samples, emptyMs, occupiedMs, ratio: occupiedMs / emptyMs,
    load1m: os.loadavg()[0], cores: os.cpus().length }));
  assert.ok(receiptReads < 10, "an unrelated hook must not synchronously read every pending receipt");
  assert.ok(occupiedMs <= emptyMs * 1.1,
    "3,000 unrelated receipts must add no more than 10% to hook append latency");
} finally {
  for (const buffer of buffers) buffer.close();
  fs.rmSync(root, { recursive: true, force: true });
}
