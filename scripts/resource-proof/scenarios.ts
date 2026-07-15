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

type EnvironmentSentinelFixture = {
  schemaVersion: number;
  credentialNamePattern: string;
  parentSentinels: Record<string, string>;
  requiredChildNames: string[];
  optionalPassThroughNames: string[];
};

export type ResourceSandbox = {
  root: string;
  home: string;
  plimsollHome: string;
  ledger: string;
  claudeProjects: string;
  codexSessions: string;
  port: number;
  portReservation: net.Server;
};

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");

function within(parent: string, candidate: string) {
  const relative = path.relative(path.resolve(parent), path.resolve(candidate));
  return relative === "" || (!relative.startsWith("..") && !path.isAbsolute(relative));
}

async function holdLoopbackPort() {
  return new Promise<{ port: number; server: net.Server }>((resolve, reject) => {
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
      server.removeListener("error", reject);
      resolve({ port: address.port, server });
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
  try {
    const reservation = await holdLoopbackPort();
    return {
      root,
      home,
      plimsollHome,
      ledger: path.join(plimsollHome, "work-ledger.sqlite"),
      claudeProjects,
      codexSessions,
      port: reservation.port,
      portReservation: reservation.server,
    };
  } catch (error) {
    fs.rmSync(root, { recursive: true, force: true });
    throw error;
  }
}

export async function removeResourceSandbox(sandbox: ResourceSandbox) {
  if (sandbox.portReservation.listening) {
    await new Promise<void>((resolve) => sandbox.portReservation.close(() => resolve()));
  }
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
    measurements: checks,
  };
}

export async function runPortReservationContract(
  sandbox: ResourceSandbox,
): Promise<ScenarioReceipt> {
  const started = performance.now();
  const address = sandbox.portReservation.address();
  const reservationHeld = Boolean(
    sandbox.portReservation.listening &&
      address &&
      typeof address !== "string" &&
      address.address === "127.0.0.1" &&
      address.port === sandbox.port,
  );

  const challengerResult = await new Promise<string>((resolve) => {
    const challenger = net.createServer();
    challenger.unref();
    challenger.once("error", (error: NodeJS.ErrnoException) => resolve(error.code ?? "ERROR"));
    challenger.listen(sandbox.port, "127.0.0.1", () => {
      challenger.close(() => resolve("BOUND"));
    });
  });
  const competingBindRejected = challengerResult === "EADDRINUSE";
  const passed = reservationHeld && competingBindRejected;
  return {
    id: "loopback_port_reservation_truth",
    required: true,
    status: passed ? "pass" : "fail",
    detail: passed
      ? "A live port-0 listener remains held and a challenger bind to the assigned loopback port is rejected with EADDRINUSE."
      : `Port reservation contract failed (held=${reservationHeld}, challenger=${challengerResult}).`,
    durationMs: Math.round((performance.now() - started) * 100) / 100,
    counters: emptyWorkCounters(),
    measurements: {
      reservationListenerHeld: reservationHeld,
      assignedPortMatchesListener: reservationHeld,
      competingBindRejected,
      challengerResult,
    },
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
    "Proposed — pending owner acceptance",
    "The outbox stores a copy, not a foreign-key-only reference",
    "raw evidence expires under the configured raw age/byte policy",
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
        ? "Proposed owner-pending ADR, explicit envelope-copy retention semantics, NFR budgets, failure modes, alternatives, privacy analysis, migration order, and adversarial gates are present."
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

export function buildAllowlistedChildEnvironment(
  sandbox: ResourceSandbox,
  parentEnvironment: NodeJS.ProcessEnv = process.env,
) {
  const env: NodeJS.ProcessEnv = {
    HOME: sandbox.home,
    USERPROFILE: sandbox.home,
    PLIMSOLL_HOME: sandbox.plimsollHome,
    TMPDIR: sandbox.root,
    TMP: sandbox.root,
    TEMP: sandbox.root,
    TZ: "UTC",
    LANG: "C",
    LC_ALL: "C",
  };
  for (const name of ["PATH", "SystemRoot", "WINDIR", "ComSpec", "PATHEXT"]) {
    const value = parentEnvironment[name];
    if (value !== undefined) env[name] = value;
  }
  return env;
}

export function runChildEnvironmentContract(sandbox: ResourceSandbox): ScenarioReceipt {
  const started = performance.now();
  const fixturePath = path.join(
    repoRoot,
    "scripts",
    "resource-proof",
    "fixtures",
    "environment-sentinels.json",
  );
  const fixture = JSON.parse(fs.readFileSync(fixturePath, "utf8")) as EnvironmentSentinelFixture;
  if (fixture.schemaVersion !== 1) {
    throw new Error("resource-proof environment fixture must use schemaVersion 1");
  }
  const credentialName = new RegExp(fixture.credentialNamePattern, "i");
  // Construct a deliberately hostile parent from only the system variables the
  // allowlist may pass plus credential sentinels. Actual credential values are
  // never copied into this fixture object or compared in the receipt.
  const adversarialParent: NodeJS.ProcessEnv = {};
  for (const name of fixture.optionalPassThroughNames) {
    const value = process.env[name];
    if (value !== undefined) adversarialParent[name] = value;
  }
  Object.assign(adversarialParent, fixture.parentSentinels);
  const adversarialChildEnvironment = buildAllowlistedChildEnvironment(
    sandbox,
    adversarialParent,
  );
  const actualChildEnvironment = buildAllowlistedChildEnvironment(sandbox);
  const childNames = Object.keys(actualChildEnvironment);
  const allowedNames = new Set([
    ...fixture.requiredChildNames,
    ...fixture.optionalPassThroughNames,
  ]);
  const adversarialChildValues = Object.values(adversarialChildEnvironment).filter(
    (value): value is string => typeof value === "string",
  );
  const sentinelNames = Object.keys(fixture.parentSentinels);
  const sentinelValues = Object.values(fixture.parentSentinels);
  const checks = {
    requiredNamesPresent: fixture.requiredChildNames.every(
      (name) => name in actualChildEnvironment,
    ),
    unexpectedNamesAbsent: childNames.every((name) => allowedNames.has(name)),
    credentialLikeNamesAbsent: childNames.every((name) => !credentialName.test(name)),
    sentinelNamesAbsent: sentinelNames.every(
      (name) => !(name in adversarialChildEnvironment),
    ),
    sentinelValuesAbsent: sentinelValues.every(
      (sentinel) => !adversarialChildValues.some((value) => value.includes(sentinel)),
    ),
    isolatedHomeValues:
      actualChildEnvironment.HOME === sandbox.home &&
      actualChildEnvironment.USERPROFILE === sandbox.home &&
      actualChildEnvironment.PLIMSOLL_HOME === sandbox.plimsollHome,
    isolatedTempValues: ["TMPDIR", "TMP", "TEMP"].every(
      (name) => actualChildEnvironment[name] === sandbox.root,
    ),
  };
  const passed = Object.values(checks).every(Boolean);
  return {
    id: "child_environment_allowlist",
    required: true,
    status: passed ? "pass" : "fail",
    detail: passed
      ? "Child processes receive only the fixed system/path and isolated-home allowlist; credential-name and value sentinels are absent."
      : `Child environment contract failed: ${Object.entries(checks)
          .filter(([, ok]) => !ok)
          .map(([name]) => name)
          .join(", ")}`,
    durationMs: Math.round((performance.now() - started) * 100) / 100,
    counters: emptyWorkCounters(),
    measurements: {
      ...checks,
      childEnvironmentKeyCount: childNames.length,
      allowedEnvironmentNameCount: allowedNames.size,
      parentCredentialLikeNameCount: Object.keys(process.env).filter((name) =>
        credentialName.test(name),
      ).length,
      fixtureCredentialSentinelCount: sentinelNames.length,
    },
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
    env: buildAllowlistedChildEnvironment(sandbox),
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
      ? "Existing signal-fidelity proof exited 0 under the verified minimal child-environment allowlist and a temporary HOME."
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
