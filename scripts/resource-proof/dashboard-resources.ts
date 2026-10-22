import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import fs from "node:fs";
import net from "node:net";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { SYSTEM_E2E_BUDGETS } from "../system-e2e/contract";
import {
  buildAllowlistedChildEnvironment,
  runDashboardProjectionBudgetContract,
  type ResourceSandbox,
} from "./scenarios";
import type { ScenarioReceipt } from "./types";

export type DashboardRssMeasurement = {
  schema: "plimsoll.dashboard-rss.v1";
  scope: "owned-dashboard-process-tree";
  maxRssBytes: number;
  rootMaxRssBytes: number;
  maxDescendantRssBytes: number;
  sampleWindowMs: number;
  sampleIntervalMs: number;
  samples: number;
  maxProcessCount: number;
};

type ProcessRow = { pid: number; parent: number; rssBytes: number };

/** Select descendants of this live ChildProcess, never its parent or siblings. */
export function ownedDashboardProcesses(rows: ProcessRow[], rootPid: number) {
  const owned = new Set([rootPid]);
  let previousSize = 0;
  while (previousSize !== owned.size) {
    previousSize = owned.size;
    for (const row of rows) if (owned.has(row.parent)) owned.add(row.pid);
  }
  return rows.filter((row) => owned.has(row.pid));
}

export function assertDashboardRss(measurement: DashboardRssMeasurement) {
  assert.equal(measurement.schema, "plimsoll.dashboard-rss.v1");
  assert.equal(measurement.scope, "owned-dashboard-process-tree");
  for (const key of ["maxRssBytes", "rootMaxRssBytes", "maxDescendantRssBytes", "sampleWindowMs",
    "sampleIntervalMs", "samples", "maxProcessCount"] as const) {
    assert.ok(Number.isFinite(measurement[key]) && measurement[key] >= 0, `invalid dashboard RSS ${key}`);
  }
  assert.ok(measurement.samples > 0 && measurement.maxProcessCount > 0 && measurement.sampleWindowMs > 0,
    "dashboard RSS sample window is empty");
  assert.ok(measurement.rootMaxRssBytes > 0 &&
    measurement.maxRssBytes >= measurement.rootMaxRssBytes + measurement.maxDescendantRssBytes,
    "dashboard RSS does not include the owned tree high-water marks");
  assert.ok(measurement.maxRssBytes <= SYSTEM_E2E_BUDGETS.maxRssBytes,
    `idle_dashboard_resources exceeded RSS budget: maxRssBytes=${measurement.maxRssBytes} ` +
    `budgetBytes=${SYSTEM_E2E_BUDGETS.maxRssBytes} sampleWindowMs=${measurement.sampleWindowMs} ` +
    `samples=${measurement.samples} scope=${measurement.scope}`);
}

export async function runOwnedDashboardScenario(sandbox: ResourceSandbox, negativeControl = false) {
  const started = performance.now();
  const repoRoot = path.resolve(import.meta.dirname, "../..");
  const worker = spawn(process.execPath, ["--import", path.join(repoRoot, "node_modules/tsx/dist/loader.mjs"),
    fileURLToPath(import.meta.url), "--dashboard-child"], {
    cwd: repoRoot, env: buildAllowlistedChildEnvironment(sandbox), stdio: ["pipe", "pipe", "pipe"],
  });
  assert.ok(worker.pid, "dashboard resource child did not start");
  let output = "";
  let errors = "";
  let samples = 0;
  let maxTreeRssBytes = 0;
  let maxDescendantRssBytes = 0;
  let maxProcessCount = 0;
  let samplingError: Error | undefined;
  const sample = () => {
    if (worker.exitCode !== null || worker.signalCode !== null) return;
    const result = spawnSync("/bin/ps", ["-axo", "pid=,ppid=,rss="], { encoding: "utf8", timeout: 5_000 });
    if (result.status !== 0 || result.error) {
      samplingError = new Error(`dashboard process-tree sample failed: ${result.error?.message ?? result.stderr}`);
      return;
    }
    const rows = result.stdout.trim().split("\n").map((line) => {
      const [pid, parent, rss] = line.trim().split(/\s+/).map(Number);
      return { pid: pid!, parent: parent!, rssBytes: rss! * 1024 };
    });
    const owned = ownedDashboardProcesses(rows, worker.pid!);
    // A missing root is not a zero-RSS observation. The exit event may race ps.
    if (!owned.some((row) => row.pid === worker.pid)) return;
    samples += 1;
    maxProcessCount = Math.max(maxProcessCount, owned.length);
    maxTreeRssBytes = Math.max(maxTreeRssBytes, owned.reduce((sum, row) => sum + row.rssBytes, 0));
    maxDescendantRssBytes = Math.max(maxDescendantRssBytes,
      owned.filter((row) => row.pid !== worker.pid).reduce((sum, row) => sum + row.rssBytes, 0));
  };
  worker.stdout.on("data", (data: Buffer) => { output += data.toString(); });
  worker.stderr.on("data", (data: Buffer) => { errors += data.toString(); });
  const exited = new Promise<void>((resolve, reject) => {
    worker.once("error", reject);
    worker.once("close", (code, signal) => code === 0 && signal === null ? resolve()
      : reject(new Error(`dashboard resource child failed code=${code} signal=${signal}: ${errors.slice(-2_000)}`)));
  });
  const interval = setInterval(sample, 20);
  const timeout = setTimeout(() => worker.kill("SIGKILL"), 60_000);
  try {
    sample();
    const { portReservation: _reservation, ...fixture } = sandbox;
    worker.stdin.end(JSON.stringify({ fixture, negativeControl }));
    await exited;
    if (samplingError) throw samplingError;
    const result = JSON.parse(output) as { scenario: ScenarioReceipt; rootMaxRssBytes: number; pid: number };
    assert.equal(result.pid, worker.pid, "dashboard RSS report came from a different process");
    assert.equal(result.scenario.id, "dashboard_projection_budget", "dashboard child ran a different scenario");
    const resources: DashboardRssMeasurement = {
      schema: "plimsoll.dashboard-rss.v1",
      scope: "owned-dashboard-process-tree",
      // Conservative sum of owned high-water marks also covers peaks between
      // ps samples. No resource-suite controller or foreign process is counted.
      maxRssBytes: Math.max(maxTreeRssBytes, result.rootMaxRssBytes + maxDescendantRssBytes),
      rootMaxRssBytes: result.rootMaxRssBytes,
      maxDescendantRssBytes,
      sampleWindowMs: Math.ceil(performance.now() - started),
      sampleIntervalMs: 20,
      samples,
      maxProcessCount,
    };
    return { scenario: result.scenario, resources };
  } finally {
    clearInterval(interval);
    clearTimeout(timeout);
    if (worker.exitCode === null && worker.signalCode === null) {
      worker.kill("SIGKILL");
      await exited.catch(() => {});
    }
  }
}

async function dashboardChild() {
  const input = JSON.parse(fs.readFileSync(0, "utf8")) as {
    fixture: Omit<ResourceSandbox, "portReservation">; negativeControl: boolean;
  };
  // This control allocates and touches real owned pages, above the unchanged
  // production budget. Keep them reachable until the high-water report.
  const allocation = input.negativeControl ? Buffer.alloc(SYSTEM_E2E_BUDGETS.maxRssBytes + 64 * 1024 * 1024, 0x5a) : null;
  const reservation = net.createServer();
  const scenario = await runDashboardProjectionBudgetContract({ ...input.fixture, portReservation: reservation });
  process.stdout.write(JSON.stringify({ scenario, pid: process.pid, rootMaxRssBytes: process.resourceUsage().maxRSS * 1024,
    negativeControlBytes: allocation?.length ?? 0 }));
  // Keep the process live through at least one final tree observation.
  await new Promise((resolve) => setTimeout(resolve, 100));
}

if (process.argv[2] === "--dashboard-child" && path.resolve(process.argv[1] ?? "") === fileURLToPath(import.meta.url)) {
  dashboardChild().catch((error: unknown) => { console.error(error); process.exitCode = 1; });
}
