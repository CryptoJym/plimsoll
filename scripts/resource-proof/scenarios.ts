import { spawnSync } from "node:child_process";
import fs from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { LocalEventBuffer } from "../../packages/collector-cli/src/buffer";
import { collectorBufferPath, collectorHome } from "../../packages/collector-cli/src/config";
import {
  WORK_COUNTER_NAMES,
  emptyWorkCounters,
  type ScenarioReceipt,
  type WorkCounterName,
} from "./types";

type IntegrationFixture = {
  schemaVersion: number;
  scenarios: Array<{
    id: string;
    issue: number;
    blockedBy: number[];
    detail: string;
    requiredCounters: string[];
  }>;
};

export type ResourceSandbox = {
  root: string;
  home: string;
  plimsollHome: string;
  ledger: string;
  claudeProjects: string;
  codexSessions: string;
  port: number;
};

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");

function within(parent: string, candidate: string) {
  const relative = path.relative(path.resolve(parent), path.resolve(candidate));
  return relative === "" || (!relative.startsWith("..") && !path.isAbsolute(relative));
}

async function reserveLoopbackPort() {
  return new Promise<number>((resolve, reject) => {
    const server = net.createServer();
    server.unref();
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (!address || typeof address === "string") {
        server.close();
        reject(new Error("loopback port reservation returned no numeric address"));
        return;
      }
      const port = address.port;
      server.close((error) => (error ? reject(error) : resolve(port)));
    });
  });
}

export async function createResourceSandbox(): Promise<ResourceSandbox> {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-resource-proof-"));
  const home = path.join(root, "home");
  const plimsollHome = path.join(root, "plimsoll");
  const claudeProjects = path.join(root, "sessions", "claude-projects");
  const codexSessions = path.join(root, "sessions", "codex-sessions");
  for (const directory of [home, plimsollHome, claudeProjects, codexSessions]) {
    fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  }
  return {
    root,
    home,
    plimsollHome,
    ledger: path.join(plimsollHome, "work-ledger.sqlite"),
    claudeProjects,
    codexSessions,
    port: await reserveLoopbackPort(),
  };
}

export function removeResourceSandbox(sandbox: ResourceSandbox) {
  fs.rmSync(sandbox.root, { recursive: true, force: true });
}

export function runIsolationContract(
  sandbox: ResourceSandbox,
  operatorHome: string,
): ScenarioReceipt {
  const started = performance.now();
  const liveCollectorHome = path.join(
    operatorHome,
    "Library",
    "Application Support",
    "Plimsoll",
  );
  const checks = {
    rootIsTemporary: within(os.tmpdir(), sandbox.root),
    homeIsSandboxed: within(sandbox.root, sandbox.home),
    collectorHomeIsSandboxed: within(sandbox.root, sandbox.plimsollHome),
    ledgerIsSandboxed: within(sandbox.root, sandbox.ledger),
    sessionRootsAreSandboxed:
      within(sandbox.root, sandbox.claudeProjects) && within(sandbox.root, sandbox.codexSessions),
    liveCollectorNotOverlapped:
      !within(liveCollectorHome, sandbox.root) && !within(sandbox.root, liveCollectorHome),
    loopbackPortIsUnprivileged: sandbox.port >= 1024 && sandbox.port <= 65535,
  };
  const passed = Object.values(checks).every(Boolean);
  return {
    id: "temporary_resource_isolation",
    required: true,
    status: passed ? "pass" : "fail",
    detail: passed
      ? "Temporary HOME, collector home, ledger, session roots, and loopback port are isolated from operator state."
      : `Isolation contract failed: ${Object.entries(checks)
          .filter(([, ok]) => !ok)
          .map(([name]) => name)
          .join(", ")}`,
    durationMs: Math.round((performance.now() - started) * 100) / 100,
    counters: emptyWorkCounters(),
    measurements: { ...checks, loopbackPortReserved: true },
  };
}

export function runArchitectureContract(): ScenarioReceipt {
  const started = performance.now();
  const adrPath = path.join(
    repoRoot,
    "docs",
    "architecture",
    "0001-resource-bounded-local-collector.md",
  );
  const budgetPath = path.join(repoRoot, "docs", "architecture", "resource-budget-gates.md");
  const adr = fs.readFileSync(adrPath, "utf8");
  const budget = fs.readFileSync(budgetPath, "utf8");
  const requiredAdrSections = [
    "## Status",
    "\nAccepted\n",
    "Accepted for incremental delivery",
    "## Requirements",
    "## Decision",
    "## Consequences",
    "## Alternatives considered",
    "## Security and privacy analysis",
    "## Failure modes and recovery",
    "## 80/20 migration order",
  ];
  const requiredBudgetSections = [
    "## Budget matrix",
    "## Required work counters",
    "## Adversarial scenarios",
    "## Gate sequence",
    "`not_wired` is never a release pass",
  ];
  const missing = [
    ...requiredAdrSections.filter((item) => !adr.includes(item)),
    ...requiredBudgetSections.filter((item) => !budget.includes(item)),
  ];
  return {
    id: "architecture_contract",
    required: true,
    status: missing.length === 0 ? "pass" : "fail",
    detail:
      missing.length === 0
        ? "Accepted ADR, NFR budgets, failure modes, alternatives, privacy analysis, migration order, and adversarial gates are present."
        : `Architecture contract is missing: ${missing.join(", ")}`,
    durationMs: Math.round((performance.now() - started) * 100) / 100,
    counters: emptyWorkCounters(),
    measurements: {
      adrBytes: Buffer.byteLength(adr),
      budgetBytes: Buffer.byteLength(budget),
      requiredSections: requiredAdrSections.length + requiredBudgetSections.length,
      missingSections: missing.length,
    },
  };
}

export function runEmptyLedgerContract(sandbox: ResourceSandbox): ScenarioReceipt {
  const started = performance.now();
  const previousHome = process.env.HOME;
  const previousPlimsollHome = process.env.PLIMSOLL_HOME;
  process.env.HOME = sandbox.home;
  process.env.PLIMSOLL_HOME = sandbox.plimsollHome;
  try {
    const configuredHome = collectorHome();
    const configuredLedger = collectorBufferPath();
    const buffer = new LocalEventBuffer(sandbox.ledger);
    const stats = buffer.stats();
    const integrity = buffer.database.pragma("integrity_check") as Array<{ integrity_check: string }>;
    const tables = buffer.database
      .prepare("select name from sqlite_master where type = 'table' order by name")
      .all() as Array<{ name: string }>;
    buffer.close();
    const checks = {
      configuredHomeIsSandboxed: path.resolve(configuredHome) === path.resolve(sandbox.plimsollHome),
      configuredLedgerIsSandboxed: path.resolve(configuredLedger) === path.resolve(sandbox.ledger),
      ledgerExists: fs.existsSync(sandbox.ledger),
      noEvents: stats.count === 0 && stats.unuploadedCount === 0,
      noMetrics: stats.metricSampleCount === 0,
      integrityOk: integrity.length === 1 && integrity[0]?.integrity_check === "ok",
      expectedSchemaPresent:
        tables.some((row) => row.name === "buffered_events") &&
        tables.some((row) => row.name === "metric_samples"),
    };
    const passed = Object.values(checks).every(Boolean);
    return {
      id: "temporary_empty_ledger",
      required: true,
      status: passed ? "pass" : "fail",
      detail: passed
        ? "Current collector storage initializes and passes integrity checks inside the temporary collector home with zero captured events."
        : `Temporary ledger contract failed: ${Object.entries(checks)
            .filter(([, ok]) => !ok)
            .map(([name]) => name)
            .join(", ")}`,
      durationMs: Math.round((performance.now() - started) * 100) / 100,
      counters: emptyWorkCounters(),
      measurements: { ...checks, schemaTables: tables.length },
    };
  } finally {
    if (previousHome === undefined) delete process.env.HOME;
    else process.env.HOME = previousHome;
    if (previousPlimsollHome === undefined) delete process.env.PLIMSOLL_HOME;
    else process.env.PLIMSOLL_HOME = previousPlimsollHome;
  }
}

function scrubbedEnvironment(sandbox: ResourceSandbox) {
  const env = { ...process.env };
  for (const name of [
    "GITHUB_TOKEN",
    "GH_TOKEN",
    "PLIMSOLL_CLOUD_URL",
    "PLIMSOLL_INGEST_KEY",
    "PLIMSOLL_UPLOAD_SIGNING_SECRET",
  ]) {
    delete env[name];
  }
  return {
    ...env,
    HOME: sandbox.home,
    PLIMSOLL_HOME: sandbox.plimsollHome,
    TMPDIR: sandbox.root,
  };
}

export function runExistingSignalFidelityProof(
  sandbox: ResourceSandbox,
  requested: boolean,
): ScenarioReceipt {
  if (!requested) {
    return {
      id: "existing_signal_fidelity_proof",
      required: false,
      status: "skipped",
      detail: "Optional current proof was not requested; pass --run-existing-proof to include it in this receipt.",
      durationMs: null,
      counters: emptyWorkCounters(),
    };
  }
  const started = performance.now();
  const tsxCli = path.join(repoRoot, "node_modules", "tsx", "dist", "cli.mjs");
  const proof = path.join(repoRoot, "scripts", "signal-fidelity-proof.ts");
  const result = spawnSync(process.execPath, [tsxCli, proof], {
    cwd: repoRoot,
    env: scrubbedEnvironment(sandbox),
    encoding: "utf8",
    timeout: 300_000,
    maxBuffer: 16 * 1024 * 1024,
  });
  const passed = result.status === 0 && !result.error;
  const failureDetail =
    result.error
      ? "child_process_spawn_error (child output omitted from receipt)"
      : result.signal
        ? `terminated by ${result.signal} (child output omitted from receipt)`
        : `exit ${result.status}; child output omitted from receipt`;
  return {
    id: "existing_signal_fidelity_proof",
    required: false,
    status: passed ? "pass" : "fail",
    detail: passed
      ? "Existing signal-fidelity proof exited 0 under scrubbed credentials and a temporary HOME."
      : `Existing signal-fidelity proof failed: ${failureDetail}`,
    durationMs: Math.round((performance.now() - started) * 100) / 100,
    counters: emptyWorkCounters(),
    measurements: {
      exitCode: result.status,
      timedOut: result.signal === "SIGTERM" && Boolean(result.error),
      stdoutBytes: Buffer.byteLength(result.stdout ?? ""),
      stderrBytes: Buffer.byteLength(result.stderr ?? ""),
    },
  };
}

export function loadUnwiredIntegrationScenarios(): ScenarioReceipt[] {
  const fixturePath = path.join(
    repoRoot,
    "scripts",
    "resource-proof",
    "fixtures",
    "integration-scenarios.json",
  );
  const fixture = JSON.parse(fs.readFileSync(fixturePath, "utf8")) as IntegrationFixture;
  if (fixture.schemaVersion !== 1 || !Array.isArray(fixture.scenarios)) {
    throw new Error("resource-proof integration fixture must use schemaVersion 1");
  }
  const knownCounters = new Set<string>(WORK_COUNTER_NAMES);
  return fixture.scenarios.map((scenario) => {
    const unknownCounters = scenario.requiredCounters.filter((name) => !knownCounters.has(name));
    if (unknownCounters.length > 0) {
      throw new Error(`${scenario.id} names unknown counters: ${unknownCounters.join(", ")}`);
    }
    const counters = emptyWorkCounters();
    for (const name of scenario.requiredCounters as WorkCounterName[]) counters[name] = 0;
    return {
      id: scenario.id,
      required: true,
      status: "not_wired",
      detail: scenario.detail,
      durationMs: null,
      counters,
      measurements: { requiredCounterCount: scenario.requiredCounters.length },
      blockedBy: scenario.blockedBy.map((issue) => `#${issue}`),
    };
  });
}
