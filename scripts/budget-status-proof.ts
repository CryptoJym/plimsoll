import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { createCollectorServer } from "../packages/collector-cli/src/server";

function p95(values: number[]) {
  return [...values].sort((a, b) => a - b)[Math.ceil(values.length * 0.95) - 1]!;
}

const root = fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "plimsoll-budget-status-"));
const buffer = new LocalEventBuffer(path.join(root, "ledger.sqlite"));
const config = collectorConfigSchema.parse({ port: 49123 });
const budget = { mode: "advisory", latest: { dbBytes: 123 }, p50: { dbBytes: 100 },
  p95: { dbBytes: 120 }, hostClass: "light", targets: { ledgerBytes: 750_000_000 },
  targetStatus: "hypothesis", unavailable: ["exact_wal_write_bytes_unavailable_stat_only"] };

async function listen(server: ReturnType<typeof createCollectorServer>) {
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  assert.ok(address && typeof address !== "string");
  return `http://127.0.0.1:${address.port}/status`;
}

async function timed(url: string) {
  const start = performance.now();
  const response = await fetch(url);
  const body = await response.json() as Record<string, unknown>;
  assert.equal(response.status, 200);
  return { ms: performance.now() - start, body };
}

async function main() {
  const baseline = createCollectorServer(config, buffer);
  const instrumented = createCollectorServer(config, buffer, { budgetStatus: () => budget });
  try {
    const baseUrl = await listen(baseline);
    const budgetUrl = await listen(instrumented);
    const first = await timed(budgetUrl);
    assert.deepEqual(first.body.budget, budget);
    for (let i = 0; i < 20; i++) { await timed(baseUrl); await timed(budgetUrl); }
    const baseTimes: number[] = [], budgetTimes: number[] = [];
    for (let i = 0; i < 300; i++) {
      baseTimes.push((await timed(baseUrl)).ms);
      budgetTimes.push((await timed(budgetUrl)).ms);
    }
    const baseP95 = p95(baseTimes), budgetP95 = p95(budgetTimes);
    console.log(JSON.stringify({ baselineP95Ms: baseP95, budgetP95Ms: budgetP95,
      deltaMs: budgetP95 - baseP95, samples: baseTimes.length }));
    assert.ok(budgetP95 <= baseP95 * 1.2 + 1, "cached budget block must stay within HTTP timing noise");
  } finally {
    await Promise.all([baseline, instrumented].map((server) => new Promise<void>((resolve) => server.close(() => resolve()))));
    buffer.close();
    fs.rmSync(root, { recursive: true, force: true });
  }
}

main().catch((error) => { console.error(error); process.exitCode = 1; });
