/** Paired OTLP/tailer appendMany timing on the production schema. */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { performance } from "node:perf_hooks";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { aiInteractionEventSchema } from "../packages/shared/src/index";

const root = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "r8-tailer-bulk-")));
const withTrigger = new LocalEventBuffer(path.join(root, "with.sqlite"));
const noTrigger = new LocalEventBuffer(path.join(root, "without.sqlite"));
noTrigger.database.exec(`drop trigger trg_maintenance_rebuild_event_order_insert;
  drop trigger trg_maintenance_rebuild_event_order_delete;
  drop trigger trg_maintenance_rebuild_event_order_rekey;`);

function chunk() {
  const observedAt = new Date().toISOString();
  return Array.from({ length: 16 }, (_, index) => ({
    event: aiInteractionEventSchema.parse({
      id: randomUUID(), sessionId: "019b0000-0000-7000-8000-000000000163",
      source: "codex", dataMode: "metadata", actionClass: "other",
      eventType: index < 2 ? "assistant_response" : "otel_span", observedAt,
      ...(index < 2 ? { model: "gpt-5.1-codex-max", inputTokens: 42000, outputTokens: 800 } : {}),
      metadata: { otelEventName: "codex.sse_event" },
    }),
    suppressedFields: [],
  }));
}

try {
  for (let index = 0; index < 20; index++) {
    const sample = chunk();
    withTrigger.appendMany(sample, [], [], { projectionDeadlineMs: Infinity });
    noTrigger.appendMany(sample, [], [], { projectionDeadlineMs: Infinity });
  }
  const ratios: number[] = [];
  for (let trial = 0; trial < 5; trial++) {
    const loadBefore = os.loadavg()[0];
    let withMs = 0;
    let withoutMs = 0;
    for (let index = 0; index < 60; index++) {
      const sample = chunk();
      const order = (index + trial) % 2 === 0
        ? [[withTrigger, "with"], [noTrigger, "without"]] as const
        : [[noTrigger, "without"], [withTrigger, "with"]] as const;
      for (const [buffer, label] of order) {
        const started = performance.now();
        buffer.appendMany(sample, [], [], { projectionDeadlineMs: Infinity });
        if (label === "with") withMs += performance.now() - started;
        else withoutMs += performance.now() - started;
      }
    }
    const ratio = withMs / withoutMs;
    ratios.push(ratio);
    console.log(JSON.stringify({ check: "tailer_appendMany", trial: trial + 1, batches: 60,
      eventsPerBatch: 16, withMs, withoutMs, ratio, loadBefore, loadAfter: os.loadavg()[0] }));
  }
  const median = ratios.sort((a, b) => a - b)[2]!;
  const expectedRows = 20 * 16 + 5 * 60 * 16;
  const actualRows = (withTrigger.database.prepare("select count(*) as n from buffered_events").get() as { n: number }).n;
  const orderedRows = (withTrigger.database.prepare("select count(*) as n from maintenance_rebuild_event_order").get() as { n: number }).n;
  assert.equal(actualRows, expectedRows);
  assert.equal(orderedRows, actualRows);
  console.log(JSON.stringify({ check: "tailer_appendMany_median", medianRatio: median,
    rows: actualRows, orderedRows, cores: os.cpus().length }));
} finally {
  withTrigger.close();
  noTrigger.close();
  fs.rmSync(root, { recursive: true, force: true });
}
