import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { AutomaticRetentionCadence } from "../packages/collector-cli/src/retention-cadence";

const root = fs.mkdtempSync(path.join(os.tmpdir(), "pr417-cadence-boundaries-"));
const oldAt = "2000-01-01T00:00:00.000Z";
const now = new Date("2026-09-28T00:00:00.000Z");

function runCadence(buffer: LocalEventBuffer, expectedPasses: number) {
  let tick = now.getTime();
  let nextId = 0;
  const scheduled = new Map<number, { callback: () => void; delay: number }>();
  const cadence = new AutomaticRetentionCadence(
    () => buffer.prune(90, { maxRows: 128, now }),
    { followupMs: 5_000, intervalMs: 3_600_000,
      timer: { now: () => tick,
        setTimeout: (callback, delay) => {
          const id = ++nextId;
          scheduled.set(id, { callback, delay });
          return id;
        },
        clearTimeout: (handle) => { scheduled.delete(handle as number); } } },
  );
  const delays: number[] = [];
  const receipts: Array<ReturnType<LocalEventBuffer["prune"]>> = [];
  try {
    cadence.start();
    for (let pass = 0; pass < expectedPasses; pass++) {
      const [id, task] = scheduled.entries().next().value!;
      scheduled.delete(id);
      delays.push(task.delay);
      tick += task.delay;
      task.callback();
      receipts.push(cadence.status().lastPass!);
    }
    assert.equal(cadence.status().counters.passes, expectedPasses);
    assert.equal(receipts.at(-1)?.hasMore, false);
    assert.equal(scheduled.values().next().value?.delay, 3_600_000);
    assert.deepEqual(delays, [0, ...Array(expectedPasses - 1).fill(5_000)]);
    return { delays, receipts: receipts.map((r) => ({
      visited: r.eventRowsVisited + r.metricRowsVisited,
      held: r.migrationProtectedRows,
      events: r.events,
      metrics: r.metricSamples,
      hasMore: r.hasMore,
      madeProgress: r.madeProgress,
    })) };
  } finally {
    cadence.stop();
    assert.equal(scheduled.size, 0);
  }
}

try {
  const held = new LocalEventBuffer(path.join(root, "held.sqlite"), {
    workspaceId: "cadence-held", deviceId: "cadence-device",
    delivery: { enabled: true },
  });
  try {
    const insert = held.database.prepare(`insert into buffered_events
      (id,source,event_type,data_mode,observed_at,payload_json,created_at,workspace_id,device_id)
      values (?,'codex','assistant_response','metadata',?,'{}',?,?,?)`);
    held.database.transaction(() => {
      for (let n = 0; n < 257; n++) {
        insert.run(`00000000-0000-4000-8000-${String(n).padStart(12, "0")}`,
          oldAt, oldAt, "cadence-held", "cadence-device");
      }
    })();
    const result = runCadence(held, 3);
    assert.deepEqual(result.receipts.map((r) => r.visited), [128, 128, 1]);
    assert.deepEqual(result.receipts.map((r) => r.held), [128, 128, 1]);
    assert.equal(result.receipts.reduce((n, r) => n + r.events, 0), 0);
    assert.equal((held.database.prepare("select count(*) as n from buffered_events")
      .get() as { n: number }).n, 257);
    console.log(JSON.stringify({ case: "three_pages_all_held", ...result }));
  } finally { held.close(); }

  const metrics = new LocalEventBuffer(path.join(root, "metrics.sqlite"));
  try {
    const insert = metrics.database.prepare(`insert into metric_samples
      (id,source,metric_name,observed_at,value,created_at)
      values (?,'codex','proof',?,1,?)`);
    metrics.database.transaction(() => {
      for (let n = 0; n < 257; n++) insert.run(`metric-${n}`, oldAt, oldAt);
    })();
    const result = runCadence(metrics, 3);
    assert.deepEqual(result.receipts.map((r) => r.metrics), [128, 128, 1]);
    assert.equal((metrics.database.prepare("select count(*) as n from metric_samples")
      .get() as { n: number }).n, 0);
    console.log(JSON.stringify({ case: "three_pages_metrics", ...result }));
  } finally { metrics.close(); }

  const idle = new LocalEventBuffer(path.join(root, "idle.sqlite"));
  try {
    const result = runCadence(idle, 1);
    assert.deepEqual(result.receipts.map((r) => r.visited), [0]);
    assert.deepEqual(result.receipts.map((r) => r.madeProgress), [false]);
    console.log(JSON.stringify({ case: "empty_idle", ...result }));
  } finally { idle.close(); }
} finally {
  fs.rmSync(root, { recursive: true, force: true });
}
