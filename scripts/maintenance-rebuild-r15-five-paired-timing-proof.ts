/** Five paired append trials with 3,000 pending receipts and the sequence trigger. */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash, randomUUID } from "node:crypto";
import { performance } from "node:perf_hooks";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { appendForwardedHook } from "../packages/collector-cli/src/forwarder";

const root = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "r15-five-pairs-")));
const homes = [path.join(root, "empty"), path.join(root, "occupied"),
  path.join(root, "no-sequence-trigger")];
for (const home of homes) fs.mkdirSync(home);
const buffers = homes.map((home) => new LocalEventBuffer(path.join(home, "ledger.sqlite")));
try {
  // A no-trigger control measures the ordering choice itself; the occupied
  // comparison measures the indexed receipt lookup at 3,000 pending files.
  buffers[2]!.database.exec("drop trigger trg_maintenance_rebuild_event_order_insert");
  buffers[2]!.database.exec("drop trigger trg_maintenance_rebuild_event_order_delete");
  buffers[2]!.database.exec("drop trigger trg_maintenance_rebuild_event_order_rekey");
  for (const home of homes) fs.mkdirSync(path.join(home, "maintenance-rebuild-refusals"), { mode: 0o700 });
  const directory = path.join(homes[1]!, "maintenance-rebuild-refusals");
  for (let index = 0; index < 3_000; index += 1) {
    const name = createHash("sha256").update(String(index)).digest("hex") + ".receipt";
    fs.writeFileSync(path.join(directory, name), JSON.stringify({ version: 7, route: "hook",
      receiptId: randomUUID(), at: new Date().toISOString(), source: "claude_code",
      eventId: randomUUID(), kind: "user_prompt_submit", sessionId: randomUUID(),
      tenantId: "other-tenant", bodyDigest: "f".repeat(64), ledgerAdmissionSequence: 0,
      ledgerAbsentAtRefusal: false }));
  }
  const config = collectorConfigSchema.parse({});
  const body = () => ({ id: randomUUID(), hook_event_name: "UserPromptSubmit",
    session_id: randomUUID(), timestamp: new Date().toISOString() });
  for (let index = 0; index < 20; index += 1) {
    appendForwardedHook(body(), { config, source: "claude_code", buffer: buffers[0]! });
    appendForwardedHook(body(), { config, source: "claude_code", buffer: buffers[1]! });
    appendForwardedHook(body(), { config, source: "claude_code", buffer: buffers[2]! });
  }
  let receiptReads = 0;
  const originalRead = fs.readFileSync;
  (fs as typeof fs & { readFileSync: typeof fs.readFileSync }).readFileSync = ((file: fs.PathOrFileDescriptor,
    ...rest: unknown[]) => {
    if (typeof file === "string" && file.endsWith(".receipt")) receiptReads += 1;
    return (originalRead as any)(file, ...rest);
  }) as typeof fs.readFileSync;
  const trials: Array<{ trial: number; emptyMs: number; occupiedMs: number;
    noTriggerMs: number; ratio: number; sequenceCostRatio: number;
    loadBefore: number; loadAfter: number }> = [];
  try {
    for (let trial = 0; trial < 5; trial += 1) {
      const loadBefore = os.loadavg()[0];
      let emptyMs = 0;
      let occupiedMs = 0;
      let noTriggerMs = 0;
      const samples = 100;
      for (let index = 0; index < samples; index += 1) {
        const rotation = (index + trial) % 3;
        for (const current of [rotation, (rotation + 1) % 3, (rotation + 2) % 3]) {
          const started = performance.now();
          appendForwardedHook(body(), { config, source: "claude_code", buffer: buffers[current]! });
          const elapsed = performance.now() - started;
          if (current === 0) emptyMs += elapsed;
          else if (current === 1) occupiedMs += elapsed;
          else noTriggerMs += elapsed;
        }
      }
      emptyMs /= samples; occupiedMs /= samples; noTriggerMs /= samples;
      const entry = { trial: trial + 1, emptyMs, occupiedMs, noTriggerMs,
        ratio: occupiedMs / emptyMs, sequenceCostRatio: emptyMs / noTriggerMs,
        loadBefore, loadAfter: os.loadavg()[0] };
      trials.push(entry);
      console.log(JSON.stringify({ check: "hook_append_paired_trial", receiptFiles: 3_000,
        cores: os.cpus().length, ...entry }));
    }
  } finally { (fs as typeof fs & { readFileSync: typeof fs.readFileSync }).readFileSync = originalRead; }
  const ratios = trials.map((trial) => trial.ratio).sort((left, right) => left - right);
  const median = ratios[2]!;
  const sequenceCosts = trials.map((trial) => trial.sequenceCostRatio).sort((left, right) => left - right);
  const sequenceCostMedian = sequenceCosts[2]!;
  console.log(JSON.stringify({ check: "hook_append_five_pair_median", receiptFiles: 3_000,
    trials: trials.length, medianRatio: median, sequenceCostMedian,
    receiptReads, cores: os.cpus().length }));
  assert.equal(receiptReads, 0, "unrelated appends must not read pending receipt files");
  assert.ok(median <= 1.10, "occupied/empty median must stay within +10%");
  assert.ok(sequenceCostMedian <= 1.10, "sequence-trigger/zero-trigger median must stay within +10%");
} finally {
  for (const buffer of buffers) buffer.close();
  fs.rmSync(root, { recursive: true, force: true });
}
