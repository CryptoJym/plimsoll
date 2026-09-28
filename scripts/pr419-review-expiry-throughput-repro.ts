import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { aiInteractionEventSchema } from "../packages/shared/src/index";

const root = fs.mkdtempSync(path.join(os.tmpdir(), "pr419-expiry-throughput-"));
const dayMs = 86_400_000;
const seedNow = new Date(Date.now() + 60_000);
const advanced = new Date(seedNow.getTime() + dayMs);
const sevenDayFacts = Number(process.env.PR419_EXPIRY_SEVEN_DAY_FACTS ?? 100);
assert(Number.isInteger(sevenDayFacts) && sevenDayFacts >= 100 && sevenDayFacts <= 20_000);

function fixture(file: string) {
  const buffer = new LocalEventBuffer(path.join(root, file));
  for (let n = 1; n <= sevenDayFacts + 1; n++) {
    const age = n === sevenDayFacts + 1 ? 29.5 : 6.5;
    const event = aiInteractionEventSchema.parse({
      id: `00000000-0000-4000-8000-${String(n).padStart(12, "0")}`,
      tenantId: "local", source: "codex", dataMode: "metadata",
      eventType: "assistant_response",
      observedAt: new Date(seedNow.getTime() - age * dayMs).toISOString(),
      sessionId: "pr419-expiry-session", actionClass: "other", model: "gpt-review",
      inputTokens: 1, outputTokens: 1, metadata: {},
    });
    assert.equal(buffer.append(event), true);
  }
  for (let i = 0; i < 200; i++) {
    const status = buffer.projection.status();
    if (status.parityReady && status.backlog.dirtySessions === 0) break;
    buffer.projection.runMaintenance(seedNow);
  }
  assert.equal(buffer.projection.status().parityReady, true);
  assert.equal(buffer.projection.status().backlog.dirtySessions, 0);
  return buffer;
}

function cutoff(buffer: LocalEventBuffer, days: number) {
  return (buffer.database.prepare(`select cutoff_at as cutoffAt from dashboard_window_control where days=?`)
    .get(days) as {cutoffAt:string}).cutoffAt;
}

let timed: LocalEventBuffer | undefined;
let unbounded: LocalEventBuffer | undefined;
let slow: LocalEventBuffer | undefined;
try {
  timed = fixture("timed.sqlite");
  unbounded = fixture("unbounded.sqlite");
  const old30 = cutoff(timed, 30);
  const receipts = [];
  for (let tick = 0; tick < 5; tick++)
    receipts.push(timed.projection.runMaintenance(advanced, { maxActiveMs: 25, clock: () => 0 }));
  const unboundedReceipt = unbounded.projection.runMaintenance(advanced);
  console.log(JSON.stringify({
    sevenDayFacts,
    timedExpiryFacts: receipts.map(receipt => receipt.expiryFacts),
    timed7DayCutoff: cutoff(timed, 7), timed30DayCutoff: cutoff(timed, 30),
    original30DayCutoff: old30,
    unboundedExpiryFacts: unboundedReceipt.expiryFacts,
    unbounded30DayCutoff: cutoff(unbounded, 30),
    timedStatus: timed.projection.status().backlog,
  }));
  assert.notEqual(cutoff(timed, 30), old30,
    "bounded automatic ticks should not let 7-day expiry block the 30-day dashboard for multiple cadences");
  if (sevenDayFacts > 100) {
    assert(receipts[0]!.expiryFacts >= 900,
      "a timed large-backlog pass should use most of its 1,000-row allowance");
  } else {
    slow = fixture("slow.sqlite");
    const oldSlow30 = cutoff(slow, 30);
    const slowReceipts = [];
    for (let tick = 0; tick < 2; tick++) {
      let elapsed = 0;
      slowReceipts.push(slow.projection.runMaintenance(advanced, {
        maxActiveMs: 25, clock: () => elapsed,
        onWorkRowForProof: phase => { if (phase === "expiry") elapsed += 1; },
      }).expiryFacts);
    }
    assert.equal(cutoff(slow, 30) === oldSlow30, false,
      "a slow first expiry batch must rotate the next tick to the later window");
    assert(slowReceipts.every(rows => rows <= 33),
      "each timed expiry pass may overrun by only one 32-row batch");
    console.log(JSON.stringify({slowExpiryFacts:slowReceipts,
      slow30DayCutoff:cutoff(slow,30),originalSlow30DayCutoff:oldSlow30}));
  }
} finally {
  timed?.close();
  unbounded?.close();
  slow?.close();
  fs.rmSync(root, {recursive: true, force: true});
}
