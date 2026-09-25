import { acceptedFixtureDelivery } from "../lib/delivery-fixture";
import assert from "node:assert/strict";
import { spawn, spawnSync, type ChildProcessByStdio } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import { createRequire } from "node:module";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import type { Readable } from "node:stream";
import { fileURLToPath } from "node:url";

import { LocalEventBuffer } from "../../packages/collector-cli/src/buffer";
import {
  AUTOMATIC_DISCOVERY_PENDING_METADATA_CAP,
  captureBaselineStatus,
  type CaptureBaselineStatus,
} from "../../packages/collector-cli/src/capture-baseline";
import {
  codexReconciliationStatus,
  runCodexReconciliationMaintenance,
} from "../../packages/collector-cli/src/codex-reconciliation";
import {
  collectorBufferPath,
  collectorConfigSchema,
  collectorHome,
} from "../../packages/collector-cli/src/config";
import { LAUNCH_AGENT_LABEL } from "../../packages/collector-cli/src/launch-agent";
import { readLocalIngestAuth } from "../../packages/collector-cli/src/local-auth";
import {
  CoalescingMaintenanceScheduler,
  CollectorMaintenance,
  requestAutomaticRecentMaintenance,
  type CollectorMaintenanceRunResult,
} from "../../packages/collector-cli/src/maintenance";
import {
  EXPLICIT_FULL_BACKFILL_NOT_COMPLETED,
  historyCoverageStatus,
  recordExplicitFullHistoryCoverage,
} from "../../packages/collector-cli/src/history-coverage";
import {
  DEFAULT_JSONL_TAILER_IO,
  jsonlScanStateKey,
} from "../../packages/collector-cli/src/jsonl-byte-tailer";
import { RolloutTailer } from "../../packages/collector-cli/src/rollout-tailer";
import {
  readCollectorPidFile,
  runtimeIdentityMatches,
  type CollectorRuntimeIdentity,
} from "../../packages/collector-cli/src/runtime-ownership";
import { TranscriptTailer } from "../../packages/collector-cli/src/transcript-tailer";
import { uploadBufferedEvents } from "../../packages/collector-cli/src/upload";
import { createCollectorServer } from "../../packages/collector-cli/src/server";
import {
  classifyOwnerShutdown,
  classifyStopCommand,
  observeChildExit,
  reapFixtureChild,
  withSymbolicDeadline,
  type ReapOutcome,
} from "./owner-shutdown";
import { aiInteractionEventSchema } from "../../packages/shared/src/index";
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

type MetadataPrivacyFixture = {
  schemaVersion: number;
  prefixLength: number;
  sentinels: Record<string, string>;
};

type IntegratedWorkerResult = {
  schema: "plimsoll.resource-proof.integrated-worker.v1";
  scenario: "integrated" | "privacy";
  passed: boolean;
  checks: Record<string, boolean>;
  counters: {
    eventsObserved: number;
    eventsAdmitted: number;
    eventsDropped: number;
    rawEventWrites: number;
    projectionRowsWritten: number;
    outboxRowsEnqueued: number;
  };
  measurements: Record<string, number | boolean>;
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
const HISTORICAL_FILES_PER_SOURCE = 1_200;
const RECENT_BASELINE_CODEX_FILES = 200;
const BASELINE_FIXTURE_CODEX_GENERATIONS = RECENT_BASELINE_CODEX_FILES + 1;
const BASELINE_FIXTURE_CLAUDE_GENERATIONS = HISTORICAL_FILES_PER_SOURCE + 1;
const MAX_FIXTURE_METADATA_FAILURES = 16;
// Both sources require two stable sweeps. Alternating source priority means a
// cadence may spend its budget on only one source, so the safe deterministic
// ceiling is the sum of both chunk counts plus mutation/replay quiet sweeps.
const MAX_DISCOVERY_CADENCES = 2 * (
  Math.ceil(BASELINE_FIXTURE_CODEX_GENERATIONS / AUTOMATIC_DISCOVERY_PENDING_METADATA_CAP) +
  Math.ceil(BASELINE_FIXTURE_CLAUDE_GENERATIONS / AUTOMATIC_DISCOVERY_PENDING_METADATA_CAP)
) + 12;
const PACKAGED_COLLECTOR_ESBUILD_SHA256 =
  "2b06d83aadb4a03505a738d3c38f174b01c08d1ed4210fd2263320b6eebfbe3a";
const PACKAGED_COLLECTOR_NATIVE_ESBUILD_SHA256: Readonly<Record<string, string>> = {
  "darwin-arm64": "6f0e1237f63fa3bc03963e58f0b0be1b9bfacd8f2dc9a3f28483e8f97e4ef2d6",
  "darwin-x64": "23dadf855f4ef8cf7c159ed5e4018b8f95d66f7e230a3a04f0add7fb809640ad",
};

export type PackagedCollectorBuildOptions = {
  builderPath?: string;
  expectedBuilderPath?: string;
  expectedBuilderSha256?: string;
  nativeBinaryPath?: string;
  expectedNativeBinaryPath?: string;
  expectedNativeBinarySha256?: string;
  outputDirectory?: string;
  injectPublicationFailureAfterBackup?: boolean;
};

class PackagedCollectorBuildError extends Error {
  constructor(readonly code: string) {
    super(code);
    this.name = code;
  }
}

function metadataPrivacyFixture() {
  const fixture = JSON.parse(
    fs.readFileSync(
      path.join(
        repoRoot,
        "scripts",
        "resource-proof",
        "fixtures",
        "metadata-privacy-sentinels.json",
      ),
      "utf8",
    ),
  ) as MetadataPrivacyFixture;
  if (fixture.schemaVersion !== 1 || fixture.prefixLength < 8) {
    throw new Error("resource-proof metadata privacy fixture must use schemaVersion 1");
  }
  return fixture;
}

function metadataPrivacyTerms(operatorHome: string) {
  const fixture = metadataPrivacyFixture();
  const values = Object.values(fixture.sentinels);
  return [...values, ...values.map((value) => value.slice(0, fixture.prefixLength)), operatorHome];
}

export function resourceReceiptPrivacyLeakCount(serialized: string, operatorHome: string) {
  return metadataPrivacyTerms(operatorHome).filter(
    (term) => term && serialized.includes(term),
  ).length;
}

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
  fs.rmSync(sandbox.root, {
    recursive: true,
    force: true,
    maxRetries: 50,
    retryDelay: 200,
  });
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
    "Accepted by James on 2026-07-17",
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
        ? "Owner-accepted ADR, explicit envelope-copy retention semantics, NFR budgets, failure modes, alternatives, privacy analysis, migration order, and adversarial gates are present."
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
  const tsxLoader = path.join(repoRoot, "node_modules", "tsx", "dist", "loader.mjs");
  const proof = path.join(repoRoot, "scripts", "signal-fidelity-proof.ts");
  const result = spawnSync(process.execPath, ["--import", tsxLoader, proof], {
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

export function runMaintenanceRegressionContract(
  sandbox: ResourceSandbox,
): ScenarioReceipt {
  const started = performance.now();
  const tsxLoader = path.join(repoRoot, "node_modules", "tsx", "dist", "loader.mjs");
  const proof = path.join(repoRoot, "scripts", "maintenance-proof.ts");
  const result = spawnSync(process.execPath, ["--import", tsxLoader, proof], {
    cwd: repoRoot,
    env: buildAllowlistedChildEnvironment(sandbox),
    encoding: "utf8",
    timeout: 300_000,
    maxBuffer: 16 * 1024 * 1024,
  });
  let receipt: {
    status?: unknown;
    checks?: unknown;
    names?: unknown;
  } = {};
  try {
    receipt = JSON.parse(result.stdout || "{}") as typeof receipt;
  } catch {
    // The structured receipt check below fails closed. Child output remains
    // omitted so a failure cannot copy fixture or operator paths upstream.
  }
  const names = Array.isArray(receipt.names)
    ? receipt.names.filter((name): name is string => typeof name === "string")
    : [];
  const exactPendingIdentityProved = names.includes(
    "crash_resume_binds_exact_pending_generations_for_both_sources",
  );
  const stalledCadenceBackoffProved = names.includes(
    "successful_stalled_baseline_returns_to_normal_cadence",
  );
  const passed =
    result.status === 0 &&
    !result.error &&
    receipt.status === "pass" &&
    typeof receipt.checks === "number" &&
    receipt.checks === names.length &&
    exactPendingIdentityProved &&
    stalledCadenceBackoffProved;
  const failureDetail = result.error
    ? "child_process_spawn_error"
    : result.signal
      ? `terminated by ${result.signal}`
      : `exit ${result.status}; structured receipt or required checks missing`;
  return {
    id: "maintenance_regression_proof",
    required: true,
    status: passed ? "pass" : "fail",
    detail: passed
      ? "The isolated maintenance proof binds crash recovery to exact Codex and Claude generation identities and backs stalled work off to normal cadence."
      : `Maintenance regression proof failed: ${failureDetail}; child output omitted.`,
    durationMs: Math.round((performance.now() - started) * 100) / 100,
    counters: emptyWorkCounters(),
    measurements: {
      exitCode: result.status,
      checks: typeof receipt.checks === "number" ? receipt.checks : 0,
      exactPendingIdentityProved,
      stalledCadenceBackoffProved,
      stdoutBytes: Buffer.byteLength(result.stdout ?? ""),
      stderrBytes: Buffer.byteLength(result.stderr ?? ""),
    },
  };
}

type EventMutationCounts = {
  inserted: number;
  updated: number;
  deleted: number;
};

function eventMutationCounts(buffer: LocalEventBuffer): EventMutationCounts {
  const rows = buffer.database
    .prepare(
      `select operation, count(*) as count
       from resource_proof_event_mutations
       group by operation`,
    )
    .all() as Array<{ operation: keyof EventMutationCounts; count: number }>;
  const counts: EventMutationCounts = { inserted: 0, updated: 0, deleted: 0 };
  for (const row of rows) counts[row.operation] = row.count;
  return counts;
}

function eventMutationDelta(before: EventMutationCounts, after: EventMutationCounts) {
  return {
    inserted: after.inserted - before.inserted,
    updated: after.updated - before.updated,
    deleted: after.deleted - before.deleted,
  };
}

function installTemporaryEventMutationAudit(buffer: LocalEventBuffer) {
  buffer.database.exec(`
    create temp table resource_proof_event_mutations (
      operation text not null check(operation in ('inserted','updated','deleted'))
    );
    create temp trigger resource_proof_event_insert
      after insert on buffered_events begin
        insert into resource_proof_event_mutations values ('inserted');
      end;
    create temp trigger resource_proof_event_update
      after update on buffered_events begin
        insert into resource_proof_event_mutations values ('updated');
      end;
    create temp trigger resource_proof_event_delete
      after delete on buffered_events begin
        insert into resource_proof_event_mutations values ('deleted');
      end;
  `);
}

function syntheticUuid(prefix: "codex" | "claude", index: number) {
  const head = prefix === "codex" ? "019d0000" : "019c0000";
  return `${head}-0000-7000-8000-${String(index).padStart(12, "0")}`;
}

function fixtureDirectoryTopology(root: string, recursive: boolean) {
  let entries = 0;
  let calls = 0;
  const walk = (directory: string) => {
    const listed = fs.readdirSync(directory, { withFileTypes: true });
    calls += 1;
    entries += listed.length;
    if (recursive) {
      for (const entry of listed) {
        if (entry.isDirectory()) walk(path.join(directory, entry.name));
      }
    }
  };
  walk(root);
  return { entries, calls };
}

function writeFirstBootFixtures(sandbox: ResourceSandbox) {
  const now = new Date();
  const observedAt = now.toISOString();
  const [year, month, day] = observedAt.slice(0, 10).split("-");
  const rolloutDay = path.join(sandbox.codexSessions, year!, month!, day!);
  fs.mkdirSync(rolloutDay, { recursive: true, mode: 0o700 });
  const rolloutSession = "019e6000-0000-7000-8000-000000000001";
  const rollout = [
    {
      timestamp: observedAt,
      type: "session_meta",
      payload: { id: rolloutSession, originator: "resource-proof" },
    },
    {
      timestamp: observedAt,
      type: "turn_context",
      payload: { model: "gpt-5.5" },
    },
    {
      timestamp: observedAt,
      type: "event_msg",
      payload: {
        type: "token_count",
        info: {
          total_token_usage: {
            input_tokens: 20,
            cached_input_tokens: 5,
            output_tokens: 3,
            reasoning_output_tokens: 0,
            total_tokens: 23,
          },
        },
        rate_limits: { plan_type: "proof" },
      },
    },
  ];
  const recentRollout = path.join(
    rolloutDay,
    `rollout-resource-proof-${rolloutSession}.jsonl`,
  );
  fs.writeFileSync(recentRollout, `${rollout.map((entry) => JSON.stringify(entry)).join("\n")}\n`, {
    mode: 0o600,
  });
  for (let index = 0; index < RECENT_BASELINE_CODEX_FILES; index += 1) {
    const sessionId = `019e6100-0000-7000-8000-${String(index).padStart(12, "0")}`;
    fs.writeFileSync(
      path.join(rolloutDay, `rollout-baseline-${sessionId}.jsonl`),
      `${JSON.stringify({ type: "response_item", payload: { baselineFixture: index } })}\n`,
      { mode: 0o600 },
    );
  }

  const oldRolloutDay = path.join(sandbox.codexSessions, "2020", "01", "01");
  fs.mkdirSync(oldRolloutDay, { recursive: true, mode: 0o700 });
  const oldMtime = new Date("2020-01-01T00:00:00.000Z");
  for (let index = 0; index < HISTORICAL_FILES_PER_SOURCE; index += 1) {
    const sessionId = syntheticUuid("codex", index);
    const file = path.join(oldRolloutDay, `rollout-old-${sessionId}.jsonl`);
    fs.writeFileSync(
      file,
      `${JSON.stringify({
        type: "response_item",
        payload: { syntheticOldContent: `codex-old-${index}` },
      })}\n`,
      { mode: 0o600 },
    );
    fs.utimesSync(file, oldMtime, oldMtime);
  }

  const transcriptSession = "019e6000-0000-7000-8000-000000000002";
  const transcriptDirectory = path.join(sandbox.claudeProjects, "resource-proof");
  fs.mkdirSync(transcriptDirectory, { recursive: true, mode: 0o700 });
  const recentTranscript = path.join(transcriptDirectory, `${transcriptSession}.jsonl`);
  fs.writeFileSync(
    recentTranscript,
    `${JSON.stringify({
      type: "assistant",
      sessionId: transcriptSession,
      timestamp: observedAt,
      message: {
        id: "resource-proof-message",
        model: "claude-sonnet-4-20250514",
        usage: {
          input_tokens: 10,
          cache_read_input_tokens: 2,
          cache_creation_input_tokens: 0,
          output_tokens: 1,
        },
      },
    })}\n`,
    { mode: 0o600 },
  );

  const oldTranscriptDirectory = path.join(sandbox.claudeProjects, "old-history");
  fs.mkdirSync(oldTranscriptDirectory, { recursive: true, mode: 0o700 });
  for (let index = 0; index < HISTORICAL_FILES_PER_SOURCE; index += 1) {
    const sessionId = syntheticUuid("claude", index);
    const file = path.join(oldTranscriptDirectory, `${sessionId}.jsonl`);
    fs.writeFileSync(
      file,
      `${JSON.stringify({
        type: "user",
        sessionId,
        syntheticOldContent: `claude-old-${index}`,
      })}\n`,
      { mode: 0o600 },
    );
    fs.utimesSync(file, oldMtime, oldMtime);
  }
  const nestedNoncandidateDirectory = path.join(
    sandbox.claudeProjects,
    "nested-noncandidates",
    "deeper",
  );
  fs.mkdirSync(nestedNoncandidateDirectory, { recursive: true, mode: 0o700 });
  for (let index = 0; index < 300; index += 1) {
    fs.writeFileSync(
      path.join(nestedNoncandidateDirectory, `ignored-${String(index).padStart(4, "0")}.txt`),
      "ignored\n",
      { mode: 0o600 },
    );
  }

  const codexStableTopology = fixtureDirectoryTopology(path.dirname(recentRollout), false);
  const claudeStableTopology = fixtureDirectoryTopology(sandbox.claudeProjects, true);

  return {
    observedAt,
    recentRollout,
    recentTranscript,
    oldRolloutDay,
    oldRolloutTarget: path.join(
      oldRolloutDay,
      `rollout-old-${syntheticUuid("codex", 0)}.jsonl`,
    ),
    oldTranscriptTarget: path.join(
      oldTranscriptDirectory,
      `${syntheticUuid("claude", HISTORICAL_FILES_PER_SOURCE - 1)}.jsonl`,
    ),
    recentBytes: fs.statSync(recentRollout).size + fs.statSync(recentTranscript).size,
    oldFiles: HISTORICAL_FILES_PER_SOURCE * 2,
    baselineCodexGenerations: BASELINE_FIXTURE_CODEX_GENERATIONS,
    baselineClaudeGenerations: BASELINE_FIXTURE_CLAUDE_GENERATIONS,
    nestedNoncandidateEntries: 300,
    expectedStableDirectoryEntries:
      codexStableTopology.entries + claudeStableTopology.entries,
    expectedStableEnumerationCalls: codexStableTopology.calls + claudeStableTopology.calls,
    // Startup and baseline each walk the same fixed topology. Derive their
    // exact contract from the files just written so a fixture edit cannot
    // leave a stale hand-counted setup limit in the proof.
    expectedSetupFilesystemEntriesScanned:
      (codexStableTopology.entries + claudeStableTopology.entries) * 2,
    expectedSetupFilesystemEnumerationCalls:
      (codexStableTopology.calls + claudeStableTopology.calls) * 2,
  };
}

function unchangedMaintenanceResult(result: CollectorMaintenanceRunResult) {
  return (
    result.rollout.filesRead === 0 &&
    result.rollout.bytesRead === 0 &&
    result.transcript.filesRead === 0 &&
    result.transcript.bytesRead === 0 &&
    result.rawEventWrites === 0 &&
    result.reconciliation.rowsVisited === 0 &&
    result.repricing.rowsVisited === 0 &&
    result.enrichment.rowsVisited === 0
  );
}

type DirectoryEnumerationRecord = {
  root: string;
  calls: number;
  entries: number;
  failedCalls: number;
  readFailures: number;
  deduplicatedEntries: number;
  metadataCalls: number;
  metadataFailures: number;
  unregistered: boolean;
  restorationVerified: boolean;
};

const directoryEnumerationObservers = new Map<string, DirectoryEnumerationRecord>();
const directoryObserverDuplicateProbes = new WeakMap<fs.Dir, (entry: fs.Dirent) => void>();
type DirectoryObserverSlot = {
  owner: Record<string, unknown>;
  name: string;
  original: unknown;
  wrapper: unknown;
};
let directoryObserverSlots: DirectoryObserverSlot[] | undefined;

type DirectoryFsApis = {
  readdir: (...args: any[]) => any;
  readdirSync: (...args: any[]) => any;
  opendir: (...args: any[]) => any;
  opendirSync: (...args: any[]) => any;
  glob?: (...args: any[]) => any;
  globSync?: (...args: any[]) => any;
  promises: {
    readdir: (...args: any[]) => any;
    opendir: (...args: any[]) => any;
    glob?: (...args: any[]) => any;
  };
  statSync: (...args: any[]) => any;
  lstatSync: (...args: any[]) => any;
  realpathSync: (...args: any[]) => any;
};

const directoryFs = fs as unknown as DirectoryFsApis;

const OBSERVED_CAPTURE_DIRECTORY_APIS = new Set([
  "fs.readdir",
  "fs.readdirSync",
  "fs.promises.readdir",
  "fs.opendir",
  "fs.opendirSync",
  "fs.promises.opendir",
  "fs.glob",
  "fs.globSync",
  "fs.promises.glob",
]);

const CAPTURE_DIRECTORY_API_PATTERN = /\bfs\.(?:(promises)\.)?(readdir|opendir|glob)(Sync)?\b/g;

function captureSourceFiles(directory: string): string[] {
  const files: string[] = [];
  const pending = [directory];
  while (pending.length > 0) {
    const current = pending.pop()!;
    for (const entry of fs.readdirSync(current, { withFileTypes: true })) {
      const candidate = path.join(current, entry.name);
      if (entry.isDirectory()) pending.push(candidate);
      else if (entry.isFile() && /\.(?:ts|tsx|js|mjs|cjs)$/.test(entry.name)) files.push(candidate);
    }
  }
  return files.sort();
}

/**
 * Capture owns one allowlisted directory API surface. A new direct fs call
 * must be added to the observer before it can enter the collector; otherwise
 * this source check fails closed instead of silently reporting zero work.
 */
export function assertCaptureDirectoryApisObserved() {
  const sourceRoot = path.join(repoRoot, "packages", "collector-cli", "src");
  const violations: string[] = [];
  for (const file of captureSourceFiles(sourceRoot)) {
    const source = fs.readFileSync(file, "utf8");
    for (const match of source.matchAll(CAPTURE_DIRECTORY_API_PATTERN)) {
      const api = `fs.${match[1] ? "promises." : ""}${match[2]}${match[3] ? "Sync" : ""}`;
      if (!OBSERVED_CAPTURE_DIRECTORY_APIS.has(api)) {
        violations.push(`${path.relative(repoRoot, file)}:${source.slice(0, match.index ?? 0).split("\n").length}:${api}`);
      }
    }
  }
  assert.deepEqual(violations, [], "capture directory APIs must be covered by the resource observer");
}

function readdirPath(value: Parameters<typeof fs.readdirSync>[0]) {
  if (typeof value === "string") return value;
  if (Buffer.isBuffer(value)) return value.toString();
  if (value instanceof URL && value.protocol === "file:") return fileURLToPath(value);
  return null;
}

function directoryScope(value: unknown, options?: { cwd?: unknown }) {
  if (typeof value !== "string" && !Buffer.isBuffer(value) && !(value instanceof URL)) return null;
  const raw = readdirPath(value as Parameters<typeof fs.readdirSync>[0]);
  if (!raw) return null;
  const cwd = typeof options?.cwd === "string" ? options.cwd : process.cwd();
  const resolved = path.resolve(cwd, raw);
  const magic = resolved.search(/[\\*?\[\]{}()!]/);
  if (magic === -1) return resolved;
  const prefix = resolved.slice(0, magic);
  return path.resolve(prefix.endsWith(path.sep) ? prefix : path.dirname(prefix));
}

function recordDirectoryEnumeration(directory: string | null, calls: number, entries: number) {
  if (!directory) return;
  for (const observer of directoryEnumerationObservers.values()) {
    if (!observer.unregistered && within(observer.root, directory)) {
      observer.calls += calls;
      observer.entries += entries;
    }
  }
}

function recordDirectoryEnumerationFailure(
  directory: string | null,
  kind: "open" | "read",
) {
  if (!directory) return;
  for (const observer of directoryEnumerationObservers.values()) {
    if (!observer.unregistered && within(observer.root, directory)) {
      if (kind === "open") observer.failedCalls += 1;
      else observer.readFailures += 1;
    }
  }
}

function recordDirectoryMetadata(directory: string | null, failed = false) {
  if (!directory) return;
  for (const observer of directoryEnumerationObservers.values()) {
    if (!observer.unregistered && within(observer.root, directory)) {
      observer.metadataCalls += 1;
      if (failed) observer.metadataFailures += 1;
    }
  }
}

function recordDirectoryEnumerationDuplicate(directory: string | null) {
  if (!directory) return;
  for (const observer of directoryEnumerationObservers.values()) {
    if (!observer.unregistered && within(observer.root, directory)) {
      observer.deduplicatedEntries += 1;
    }
  }
}

function instrumentDirectoryHandle(handle: fs.Dir, directory: string | null) {
  // Node's async iterator can delegate to read(), so de-duplicate Dirent
  // objects by identity while covering every supported Dir API.
  const observedEntries = new WeakSet<object>();
  const recordEntry = <T>(entry: T): T => {
    if (entry && typeof entry === "object") {
      if (observedEntries.has(entry)) {
        recordDirectoryEnumerationDuplicate(directory);
        return entry;
      }
      observedEntries.add(entry);
      recordDirectoryEnumeration(directory, 0, 1);
    }
    return entry;
  };

  const originalReadSync = handle.readSync.bind(handle);
  handle.readSync = (() => {
    try {
      return recordEntry(originalReadSync());
    } catch (error) {
      recordDirectoryEnumerationFailure(directory, "read");
      throw error;
    }
  }) as typeof handle.readSync;

  const originalRead = handle.read.bind(handle) as unknown as (
    callback?: (error: NodeJS.ErrnoException | null, entry: fs.Dirent | null) => void,
  ) => unknown;
  handle.read = ((callback?:
    (error: NodeJS.ErrnoException | null, entry: fs.Dirent | null) => void) => {
    if (callback) {
      try {
        return originalRead((error, entry) => {
          if (error) recordDirectoryEnumerationFailure(directory, "read");
          callback(error, recordEntry(entry));
        });
      } catch (error) {
        recordDirectoryEnumerationFailure(directory, "read");
        throw error;
      }
    }
    try {
      const promise = originalRead() as Promise<fs.Dirent | null>;
      return promise.then(
        (entry) => recordEntry(entry),
        (error: unknown) => {
          recordDirectoryEnumerationFailure(directory, "read");
          throw error;
        },
      );
    } catch (error) {
      recordDirectoryEnumerationFailure(directory, "read");
      throw error;
    }
  }) as unknown as typeof handle.read;

  const originalAsyncIterator = handle[Symbol.asyncIterator].bind(handle);
  handle[Symbol.asyncIterator] = (() => {
    const iterator = originalAsyncIterator();
    const originalNext = iterator.next.bind(iterator);
    const wrapped: AsyncIterableIterator<fs.Dirent> = {
      async next(...args: [] | [undefined]) {
        try {
          const result = await originalNext(...args);
          if (!result.done) recordEntry(result.value);
          return result;
        } catch (error) {
          recordDirectoryEnumerationFailure(directory, "read");
          throw error;
        }
      },
      [Symbol.asyncIterator]() {
        return this;
      },
    };
    if (iterator.return) wrapped.return = iterator.return.bind(iterator);
    if (iterator.throw) wrapped.throw = iterator.throw.bind(iterator);
    return wrapped;
  }) as typeof handle[typeof Symbol.asyncIterator];

  // The production wrappers only expose the public Dir APIs. This private
  // probe lets the concurrency proof feed the same Dirent through the
  // recorder twice and prove that identity de-duplication is active without
  // changing any production collector surface.
  directoryObserverDuplicateProbes.set(handle, (entry) => {
    recordEntry(entry);
    recordEntry(entry);
  });

  return handle;
}

function installDirectoryEnumerationWrapper() {
  if (directoryObserverSlots) {
    if (directoryObserverSlots.some((slot) => slot.owner[slot.name] !== slot.wrapper)) {
      throw new Error("DirectoryObserverIntegrityLost");
    }
    return;
  }
  const slots: DirectoryObserverSlot[] = [];
  const replace = (owner: Record<string, unknown>, name: string, make: (original: any) => any) => {
    const original = owner[name];
    if (typeof original !== "function") return;
    const wrapper = make(original);
    owner[name] = wrapper;
    slots.push({ owner, name, original, wrapper });
  };

  replace(directoryFs, "readdirSync", (original) => (...args: any[]) => {
    const directory = directoryScope(args[0]);
    try {
      const result = original(...args);
      recordDirectoryEnumeration(directory, 1, Array.isArray(result) ? result.length : 0);
      return result;
    } catch (error) {
      recordDirectoryEnumerationFailure(directory, "open");
      throw error;
    }
  });
  replace(directoryFs, "readdir", (original) => (...args: any[]) => {
    const directory = directoryScope(args[0], args[1]);
    const callback = typeof args.at(-1) === "function" ? args.at(-1) : undefined;
    if (!callback) {
      try {
        const result = original(...args);
        return Promise.resolve(result).then(
          (entries) => {
            recordDirectoryEnumeration(directory, 1, Array.isArray(entries) ? entries.length : 0);
            return entries;
          },
          (error) => {
            recordDirectoryEnumerationFailure(directory, "open");
            throw error;
          },
        );
      } catch (error) {
        recordDirectoryEnumerationFailure(directory, "open");
        throw error;
      }
    }
    try {
      const callbackIndex = args.length - 1;
      const wrapped = (...callbackArgs: any[]) => {
        const error = callbackArgs[0];
        if (error) recordDirectoryEnumerationFailure(directory, "open");
        else recordDirectoryEnumeration(directory, 1, Array.isArray(callbackArgs[1]) ? callbackArgs[1].length : 0);
        callback(...callbackArgs);
      };
      const callArgs = [...args];
      callArgs[callbackIndex] = wrapped;
      return original(...callArgs);
    } catch (error) {
      recordDirectoryEnumerationFailure(directory, "open");
      throw error;
    }
  });
  replace(directoryFs.promises, "readdir", (original) => async (...args: any[]) => {
    const directory = directoryScope(args[0], args[1]);
    try {
      const result = await original(...args);
      recordDirectoryEnumeration(directory, 1, Array.isArray(result) ? result.length : 0);
      return result;
    } catch (error) {
      recordDirectoryEnumerationFailure(directory, "open");
      throw error;
    }
  });

  const wrapOpen = (original: any, directory: string | null, args: any[]) => {
    try {
      const handle = original(...args);
      recordDirectoryEnumeration(directory, 1, 0);
      return instrumentDirectoryHandle(handle, directory);
    } catch (error) {
      recordDirectoryEnumerationFailure(directory, "open");
      throw error;
    }
  };
  replace(directoryFs, "opendirSync", (original) => (...args: any[]) =>
    wrapOpen(original, directoryScope(args[0], args[1]), args));
  replace(directoryFs, "opendir", (original) => (...args: any[]) => {
    const directory = directoryScope(args[0], args[1]);
    const callback = typeof args.at(-1) === "function" ? args.at(-1) : undefined;
    if (!callback) {
      try {
        const handle = original(...args);
        recordDirectoryEnumeration(directory, 1, 0);
        return instrumentDirectoryHandle(handle, directory);
      } catch (error) {
        recordDirectoryEnumerationFailure(directory, "open");
        throw error;
      }
    }
    try {
      const callArgs = [...args];
      callArgs[callArgs.length - 1] = (error: unknown, handle: fs.Dir | undefined) => {
        if (error || !handle) recordDirectoryEnumerationFailure(directory, "open");
        else {
          recordDirectoryEnumeration(directory, 1, 0);
          handle = instrumentDirectoryHandle(handle, directory);
        }
        callback(error, handle);
      };
      return original(...callArgs);
    } catch (error) {
      recordDirectoryEnumerationFailure(directory, "open");
      throw error;
    }
  });
  replace(directoryFs.promises, "opendir", (original) => async (...args: any[]) => {
    const directory = directoryScope(args[0], args[1]);
    try {
      const handle = await original(...args);
      recordDirectoryEnumeration(directory, 1, 0);
      return instrumentDirectoryHandle(handle, directory);
    } catch (error) {
      recordDirectoryEnumerationFailure(directory, "open");
      throw error;
    }
  });

  const wrapGlobResult = (directory: string | null, result: unknown) => {
    recordDirectoryEnumeration(directory, 1, Array.isArray(result) ? result.length : 0);
    return result;
  };
  replace(directoryFs, "globSync", (original) => (...args: any[]) => {
    const directory = directoryScope(args[0], args[1]);
    try { return wrapGlobResult(directory, original(...args)); }
    catch (error) { recordDirectoryEnumerationFailure(directory, "open"); throw error; }
  });
  replace(directoryFs, "glob", (original) => (...args: any[]) => {
    const directory = directoryScope(args[0], args[1]);
    const callback = typeof args.at(-1) === "function" ? args.at(-1) : undefined;
    if (!callback) {
      try { return original(...args); }
      catch (error) { recordDirectoryEnumerationFailure(directory, "open"); throw error; }
    }
    try {
      const callArgs = [...args];
      callArgs[callArgs.length - 1] = (error: unknown, matches: unknown) => {
        if (error) recordDirectoryEnumerationFailure(directory, "open");
        else recordDirectoryEnumeration(directory, 1, Array.isArray(matches) ? matches.length : 0);
        callback(error, matches);
      };
      return original(...callArgs);
    } catch (error) {
      recordDirectoryEnumerationFailure(directory, "open");
      throw error;
    }
  });
  replace(directoryFs.promises, "glob", (original) => (...args: any[]) => {
    const directory = directoryScope(args[0], args[1]);
    try {
      const iterator = original(...args) as AsyncIterable<unknown>;
      const originalIterator = iterator[Symbol.asyncIterator]();
      recordDirectoryEnumeration(directory, 1, 0);
      const wrapped: AsyncIterableIterator<unknown> = {
        async next(...nextArgs: [] | [undefined]) {
          try {
            const result = await originalIterator.next(...nextArgs);
            if (!result.done) recordDirectoryEnumeration(directory, 0, 1);
            return result;
        } catch (error) {
          recordDirectoryEnumerationFailure(directory, "read");
          throw error;
        }
        },
        [Symbol.asyncIterator]() { return this; },
      };
      if (originalIterator.return) wrapped.return = originalIterator.return.bind(originalIterator);
      if (originalIterator.throw) wrapped.throw = originalIterator.throw.bind(originalIterator);
      return wrapped;
    } catch (error) {
      recordDirectoryEnumerationFailure(directory, "open");
      throw error;
    }
  });

  for (const name of ["statSync", "lstatSync", "realpathSync"]) {
    replace(directoryFs, name, (original) => (...args: any[]) => {
      const directory = directoryScope(args[0]);
      try { const result = original(...args); recordDirectoryMetadata(directory); return result; }
      catch (error) { recordDirectoryMetadata(directory, true); throw error; }
    });
  }
  directoryObserverSlots = slots;
}

function observeDirectoryEnumeration(root: string) {
  const resolvedRoot = path.resolve(root);
  if (directoryEnumerationObservers.has(resolvedRoot)) {
    throw new Error("DirectoryObserverAlreadyRegistered");
  }
  installDirectoryEnumerationWrapper();
  const record: DirectoryEnumerationRecord = {
    root: resolvedRoot,
    calls: 0,
    entries: 0,
    failedCalls: 0,
    readFailures: 0,
    deduplicatedEntries: 0,
    metadataCalls: 0,
    metadataFailures: 0,
    unregistered: false,
    restorationVerified: false,
  };
  directoryEnumerationObservers.set(resolvedRoot, record);
  return {
    snapshot: () => ({
      calls: record.calls,
      entries: record.entries,
      failedCalls: record.failedCalls,
      deduplicatedEntries: record.deduplicatedEntries,
      readFailures: record.readFailures,
      metadataCalls: record.metadataCalls,
      metadataFailures: record.metadataFailures,
    }),
    unregister: () => {
      if (record.unregistered) return;
      if (directoryEnumerationObservers.get(resolvedRoot) !== record) {
        record.unregistered = true;
        record.restorationVerified = false;
        return;
      }
      directoryEnumerationObservers.delete(resolvedRoot);
      record.unregistered = true;
      if (directoryEnumerationObservers.size === 0) {
        const slots = directoryObserverSlots ?? [];
        for (const slot of slots) {
          if (slot.owner[slot.name] === slot.wrapper) slot.owner[slot.name] = slot.original;
        }
        record.restorationVerified = slots.length > 0 && slots.every(
          (slot) => slot.owner[slot.name] === slot.original,
        );
        directoryObserverSlots = undefined;
      } else {
        record.restorationVerified = (directoryObserverSlots ?? []).every(
          (slot) => slot.owner[slot.name] === slot.wrapper,
        );
      }
    },
    status: () => ({
      calls: record.calls,
      entries: record.entries,
      failedCalls: record.failedCalls,
      deduplicatedEntries: record.deduplicatedEntries,
      readFailures: record.readFailures,
      metadataCalls: record.metadataCalls,
      metadataFailures: record.metadataFailures,
      restored: record.unregistered && record.restorationVerified,
    }),
  };
}

type DirectoryObserverExercise = {
  root: string;
  expectedEntries: number;
  expectedCalls: number;
  entries: number;
  calls: number;
  failedCalls: number;
  readFailures: number;
  deduplicatedEntries: number;
  metadataCalls: number;
  metadataFailures: number;
  restored: boolean;
};

async function readDirectoryWithCallback(handle: fs.Dir) {
  let entries = 0;
  await new Promise<void>((resolve, reject) => {
    const readNext = () => {
      handle.read((error, entry) => {
        if (error) {
          reject(error);
          return;
        }
        if (!entry) {
          resolve();
          return;
        }
        entries += 1;
        readNext();
      });
    };
    readNext();
  });
  return entries;
}

async function readDirectoryCallback(root: string) {
  return new Promise<number>((resolve, reject) => {
    directoryFs.readdir(root, { withFileTypes: true }, (error: unknown, entries: unknown[]) => {
      if (error) { reject(error); return; }
      resolve(Array.isArray(entries) ? entries.length : 0);
    });
  });
}

async function openDirectoryCallback(root: string) {
  return new Promise<void>((resolve, reject) => {
    directoryFs.opendir(root, (error: unknown, handle: fs.Dir | undefined) => {
      if (error || !handle) { reject(error ?? new Error("DirectoryHandleMissing")); return; }
      closeDirectoryHandle(handle);
      resolve();
    });
  });
}

async function readGlobCallback(pattern: string) {
  return new Promise<number>((resolve, reject) => {
    directoryFs.glob!(pattern, (error: unknown, entries: unknown[]) => {
      if (error) { reject(error); return; }
      resolve(Array.isArray(entries) ? entries.length : 0);
    });
  });
}

function closeDirectoryHandle(handle: fs.Dir) {
  try {
    handle.closeSync();
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ERR_DIR_CLOSED") throw error;
  }
}

async function exerciseDirectoryObserver(root: string, fileCount: number) {
  const probeRoot = path.join(root, "resource-observer-concurrency");
  fs.rmSync(probeRoot, { recursive: true, force: true });
  fs.mkdirSync(probeRoot, { recursive: true, mode: 0o700 });
  for (let index = 0; index < fileCount; index += 1) {
    fs.writeFileSync(path.join(probeRoot, `entry-${index}.txt`), `${index}\n`, {
      mode: 0o600,
    });
  }
  fs.mkdirSync(path.join(probeRoot, "nested"), { mode: 0o700 });

  const observer = observeDirectoryEnumeration(probeRoot);
  try {
    await new Promise<void>((resolve) => setImmediate(resolve));
    const listed = fs.readdirSync(probeRoot, { withFileTypes: true });
    assert.equal(listed.length, fileCount + 1);
    fs.statSync(probeRoot);
    fs.lstatSync(probeRoot);
    fs.realpathSync(probeRoot);
    assert.equal(await readDirectoryCallback(probeRoot), listed.length);
    assert.equal((await directoryFs.promises.readdir(probeRoot, { withFileTypes: true })).length, listed.length);

    const syncHandle = fs.opendirSync(probeRoot);
    let firstSyncEntry: fs.Dirent | null = null;
    while (true) {
      const entry = syncHandle.readSync();
      if (!entry) break;
      firstSyncEntry ??= entry;
      // The sync path is counted by the observer wrapper.
    }
    assert.ok(firstSyncEntry);
    directoryObserverDuplicateProbes.get(syncHandle)?.(firstSyncEntry);
    closeDirectoryHandle(syncHandle);

    const callbackHandle = fs.opendirSync(probeRoot);
    await readDirectoryWithCallback(callbackHandle);
    closeDirectoryHandle(callbackHandle);

    const iteratorHandle = fs.opendirSync(probeRoot);
    for await (const _entry of iteratorHandle) {
      // Async iteration is counted through the wrapped iterator.next() path.
    }
    closeDirectoryHandle(iteratorHandle);

    const promiseHandle = fs.opendirSync(probeRoot);
    while (await promiseHandle.read()) {
      // Direct promise-form Dir.read() is a supported Node API.
    }
    closeDirectoryHandle(promiseHandle);

    await openDirectoryCallback(probeRoot);
    const promiseOpenHandle = await directoryFs.promises.opendir(probeRoot);
    for await (const _entry of promiseOpenHandle) {
      // Promise-form opendir handles expose the same async iterator API.
    }
    closeDirectoryHandle(promiseOpenHandle);

    const globPattern = path.join(probeRoot, "*");
    assert.equal(directoryFs.globSync!(globPattern).length, listed.length);
    assert.equal(await readGlobCallback(globPattern), listed.length);
    let promiseGlobEntries = 0;
    for await (const _entry of directoryFs.promises.glob!(globPattern)) promiseGlobEntries += 1;
    assert.equal(promiseGlobEntries, listed.length);

    for (const missing of ["missing-readdir", "missing-opendir"]) {
      const missingPath = path.join(probeRoot, missing);
      try {
        if (missing.endsWith("readdir")) fs.readdirSync(missingPath);
        else fs.opendirSync(missingPath);
      } catch {
        // Failed directory attempts are part of the observer evidence.
      }
    }

    // Node 22's glob implementations perform one additional opendir walk for
    // each async glob form. Those internal calls are real directory work and
    // stay in the exact fixture contract.
    const expectedEntries = listed.length * 14;
    const expectedCalls = 15;
    const beforeUnregister = observer.status();
    observer.unregister();
    const afterUnregister = observer.status();
    assert.equal(beforeUnregister.entries, expectedEntries);
    assert.equal(beforeUnregister.calls, expectedCalls);
    assert.equal(beforeUnregister.failedCalls, 2);
    assert.equal(beforeUnregister.readFailures, 0);
    assert.equal(beforeUnregister.deduplicatedEntries, 2);
    assert.equal(beforeUnregister.metadataCalls, 4);
    assert.equal(beforeUnregister.metadataFailures, 0);
    assert.equal(afterUnregister.restored, true);
    return {
      root: probeRoot,
      expectedEntries,
      expectedCalls,
      entries: afterUnregister.entries,
      calls: afterUnregister.calls,
      failedCalls: afterUnregister.failedCalls,
      readFailures: afterUnregister.readFailures,
      deduplicatedEntries: afterUnregister.deduplicatedEntries,
      metadataCalls: afterUnregister.metadataCalls,
      metadataFailures: afterUnregister.metadataFailures,
      restored: afterUnregister.restored,
    } satisfies DirectoryObserverExercise;
  } finally {
    observer.unregister();
    fs.rmSync(probeRoot, { recursive: true, force: true });
  }
}

export async function runDirectoryObserverConcurrencyContract(
  roots: readonly [string, string],
) {
  const originalDirectoryApis = [
    [directoryFs, "readdir"],
    [directoryFs, "readdirSync"],
    [directoryFs, "opendir"],
    [directoryFs, "opendirSync"],
    [directoryFs, "glob"],
    [directoryFs, "globSync"],
    [directoryFs.promises, "readdir"],
    [directoryFs.promises, "opendir"],
    [directoryFs.promises, "glob"],
    [directoryFs, "statSync"],
    [directoryFs, "lstatSync"],
    [directoryFs, "realpathSync"],
  ].map(([owner, name]) => [owner, name, owner[name as keyof typeof owner]] as const);
  const exercises = await Promise.all([
    exerciseDirectoryObserver(roots[0], 2),
    exerciseDirectoryObserver(roots[1], 4),
  ]);
  const isolationAssertions = exercises.map(
    (exercise) =>
      exercise.entries === exercise.expectedEntries &&
      exercise.calls === exercise.expectedCalls &&
      exercise.failedCalls === 2 &&
      exercise.readFailures === 0 &&
      exercise.deduplicatedEntries === 2 &&
      exercise.metadataCalls === 4 &&
      exercise.metadataFailures === 0 &&
      exercise.restored,
  );
  const isolationProved = isolationAssertions.every(Boolean);
  const crossCountedEntries = !isolationProved;
  assert.equal(isolationProved, true, "directory observers must remain isolated");

  const failureRoot = path.join(roots[0], "resource-observer-injected-failure");
  fs.mkdirSync(failureRoot, { recursive: true, mode: 0o700 });
  const failureObserver = observeDirectoryEnumeration(failureRoot);
  let injectedFailure = false;
  try {
    throw new Error("InjectedDirectoryObserverFailure");
  } catch {
    injectedFailure = true;
  } finally {
    failureObserver.unregister();
  }
  const failureStatus = failureObserver.status();
  fs.rmSync(failureRoot, { recursive: true, force: true });
  assert.equal(injectedFailure, true);
  assert.equal(failureStatus.entries, 0);
  assert.equal(failureStatus.calls, 0);
  assert.equal(failureStatus.restored, true);

  const exactGlobalIdentityRestored = originalDirectoryApis.every(
    ([owner, name, original]) => owner[name as keyof typeof owner] === original,
  );
  return {
    exercises,
    isolationAssertions,
    isolationProved,
    crossCountedEntries,
    injectedFailure: injectedFailure ? "fail" : "pass",
    injectedFailureEntriesScanned: failureStatus.entries,
    injectedFailureObserverRestored: failureStatus.restored,
    exactGlobalIdentityRestored,
  };
}

/**
 * Production #79 path: one deterministic remote-validation item cannot block
 * later valid rows or cause raw evidence rewrites.
 */
export async function runPoisonContinuationContract(
  sandbox: ResourceSandbox,
): Promise<ScenarioReceipt> {
  const started = performance.now();
  const config = collectorConfigSchema.parse({
    uploadUrl: "http://127.0.0.1:1/fake-ingest",
    installKey: "resource-proof-install",
    delivery: { maxOldestAgeDays: 3650, requestTimeoutSeconds: 1 },
  });
  const buffer = new LocalEventBuffer(path.join(sandbox.plimsollHome, "poison-continuation.sqlite"), {
    // Issue 0089: capture pre-bound to config.tenantId (future-only enrollment).
    workspaceId: config.tenantId,
    delivery: { enabled: true, limits: config.delivery },
  });
  const eventId = (n: number) =>
    `00000000-0000-4000-8000-${String(n).padStart(12, "0")}`;
  const poisonId = eventId(1);
  const events = [1, 2, 3].map((n) =>
    aiInteractionEventSchema.parse({
      id: eventId(n),
      sessionId: eventId(100 + n),
      source: "codex",
      dataMode: "metadata",
      eventType: "assistant_response",
      observedAt: new Date(Date.now() + n * 1_000).toISOString(),
      actionClass: "other",
      inputTokens: n,
      outputTokens: 1,
      metadata: { resourceProof: true },
    }),
  );
  try {
    for (const event of events) buffer.append(event);
    const before = buffer.delivery.status();
    const payloadsBefore = buffer.database
      .prepare(`select id, payload_json as payload from buffered_events order by id`)
      .all() as Array<{ id: string; payload: string }>;
    let probes = 0;
    const result = await uploadBufferedEvents(config, buffer, {
      fetchImpl: async (_input, init) => {
        probes += 1;
        const body = JSON.parse(String(init?.body ?? "{}")) as {
          events: Array<{ event: { id: string } }>;
        };
        const ids = body.events.map((entry) => entry.event.id);
        return new Response(JSON.stringify(ids.includes(poisonId) ? { accepted: 0 } : acceptedFixtureDelivery(String(init?.body ?? ""), config.installKey)), {
          status: ids.includes(poisonId) ? 422 : 200,
          headers: { "content-type": "application/json" },
        });
      },
      maxProbes: 15,
    });
    const after = buffer.delivery.status();
    const payloadsAfter = buffer.database
      .prepare(`select id, payload_json as payload from buffered_events order by id`)
      .all() as Array<{ id: string; payload: string }>;
    const acknowledgedIds = result.batch?.events.map((entry) => entry.event.id) ?? [];
    const counters = emptyWorkCounters();
    counters.outboxAttempts = after.counters.outboxAttempts - before.counters.outboxAttempts;
    counters.deadLettersWritten =
      after.counters.deadLettersWritten - before.counters.deadLettersWritten;
    counters.rawEventRewrites =
      JSON.stringify(payloadsBefore) === JSON.stringify(payloadsAfter) ? 0 : 1;
    const passed =
      result.uploadedEvents === 2 &&
      result.delivery.deadLetters === 1 &&
      after.remainingDelivery === 0 &&
      counters.outboxAttempts === 3 &&
      counters.deadLettersWritten === 1 &&
      counters.rawEventRewrites === 0 &&
      acknowledgedIds.length === 2 &&
      !acknowledgedIds.includes(poisonId) &&
      probes <= 7;
    return {
      id: "poison_continuation",
      required: true,
      status: passed ? "pass" : "fail",
      detail: passed
        ? "One remote-validation poison item was quarantined once; both later valid items were acknowledged with zero raw payload rewrites."
        : "Poison continuation counters or acknowledged-only batch semantics did not match the production delivery seam.",
      durationMs: Math.round((performance.now() - started) * 100) / 100,
      counters,
      measurements: {
        probes,
        acknowledgedEvents: result.uploadedEvents,
        remainingDelivery: after.remainingDelivery,
        acknowledgedBatchEvents: acknowledgedIds.length,
      },
    };
  } finally {
    buffer.close();
  }
}

/**
 * Production #124 path: thousands of synthetic historical files plus a small
 * recent tail exercise the real tailers, recent-only daemon worker, coalescing
 * scheduler, persisted history coverage, restart, and explicit full backfill.
 */
export async function runNoChangeConstantWorkContract(
  sandbox: ResourceSandbox,
  options: { injectFailureAfterObserverRegistration?: boolean } = {},
): Promise<ScenarioReceipt> {
  const started = performance.now();
  const counters = emptyWorkCounters();
  let directoryApiCoverageChecked = false;
  let buffer: LocalEventBuffer | undefined;
  let maintenance: CollectorMaintenance | undefined;
  let directoryObserver: ReturnType<typeof observeDirectoryEnumeration> | undefined;
  let stableEmptyRoot: string | undefined;
  try {
    assertCaptureDirectoryApisObserved();
    directoryApiCoverageChecked = true;
    const fixture = writeFirstBootFixtures(sandbox);
    await new Promise<void>((resolve) => setTimeout(resolve, 2));
    buffer = new LocalEventBuffer(sandbox.ledger);
    installTemporaryEventMutationAudit(buffer);
    const makeMaintenance = () =>
      new CollectorMaintenance(
        buffer!,
        new RolloutTailer(buffer!, sandbox.codexSessions, () => []),
        new TranscriptTailer(buffer!, sandbox.claudeProjects),
      );
    maintenance = makeMaintenance();
    const initialCoverage = historyCoverageStatus(buffer.database);

    let firstStarted!: () => void;
    let releaseFirst!: () => void;
    const firstStartedSignal = new Promise<void>((resolve) => {
      firstStarted = resolve;
    });
    const firstReleaseSignal = new Promise<void>((resolve) => {
      releaseFirst = resolve;
    });
    let automaticInvocations = 0;
    const mutationDeltas: EventMutationCounts[] = [];
    const directoryEntryDeltas: number[] = [];
    const activeDirectoryObserver = observeDirectoryEnumeration(sandbox.root);
    directoryObserver = activeDirectoryObserver;
    const scheduler = new CoalescingMaintenanceScheduler(async () => {
      automaticInvocations += 1;
      if (automaticInvocations === 1) {
        firstStarted();
        await firstReleaseSignal;
      }
      const before = eventMutationCounts(buffer!);
      const directoryBefore = activeDirectoryObserver.snapshot();
      const result = await maintenance!.runRecent();
      mutationDeltas.push(eventMutationDelta(before, eventMutationCounts(buffer!)));
      directoryEntryDeltas.push(
        activeDirectoryObserver.snapshot().entries - directoryBefore.entries,
      );
      return result;
    });

    let initialDrain: CollectorMaintenanceRunResult[] = [];
    let queuedStatus = scheduler.status();
    let finalStatus = scheduler.status();
    if (options.injectFailureAfterObserverRegistration) {
      throw new Error("InjectedDirectoryObserverFailure");
    }
    const initial = requestAutomaticRecentMaintenance(scheduler);
    await firstStartedSignal;
    const concurrentRecent = requestAutomaticRecentMaintenance(scheduler);
    const concurrentInterval = requestAutomaticRecentMaintenance(scheduler);
    queuedStatus = scheduler.status();
    releaseFirst();
    [initialDrain] = await Promise.all([initial, concurrentRecent, concurrentInterval]);
    finalStatus = scheduler.status();
    const startupDirectoryObservation = activeDirectoryObserver.snapshot();

    const bootRuns = [...initialDrain];
    const baselineSnapshots: CaptureBaselineStatus[] = [
      captureBaselineStatus(buffer.database),
    ];
    const baselineDirectoryBefore = activeDirectoryObserver.snapshot();
    for (
      let cadence = 0;
      cadence < MAX_DISCOVERY_CADENCES && captureBaselineStatus(buffer.database).status !== "complete";
      cadence += 1
    ) {
      bootRuns.push(await maintenance.runRecent());
      baselineSnapshots.push(captureBaselineStatus(buffer.database));
    }
    const baselineDirectoryAfter = activeDirectoryObserver.snapshot();
    const baselineFilesystemEntriesScanned =
      baselineDirectoryAfter.entries - baselineDirectoryBefore.entries;
    const baselineFilesystemEnumerationCalls =
      baselineDirectoryAfter.calls - baselineDirectoryBefore.calls;
    // Recreate the tailers after baseline completion so the proof's stable
    // unchanged run starts a fresh recent discovery sweep. The ledger and
    // baseline state remain the same; only the two bounded sweep cursors are
    // reset below to make the measured metadata work explicit.
    maintenance?.close();
    maintenance = undefined;
    buffer.close();
    buffer = new LocalEventBuffer(sandbox.ledger);
    installTemporaryEventMutationAudit(buffer);
    // A completed baseline normally leaves a recent-sweep cursor at its last
    // root. Clear only those two bounded cursor rows so this proof's stable
    // unchanged run measures a fresh metadata-only sweep without changing any
    // fixture files or durable event content.
    for (const source of ["codex", "claude_code"] as const) {
      buffer.database
        .prepare("delete from maintenance_state where key = ?")
        .run(`capture_sweep_resume:${source}`);
    }
    const stableBefore = eventMutationCounts(buffer);
    const stableDirectoryBefore = activeDirectoryObserver.snapshot();
    const stableRuns: CollectorMaintenanceRunResult[] = [];
    stableEmptyRoot = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-resource-proof-empty-"));
    const runStableSource = async (source: "codex" | "claude_code") => {
      const stableMaintenance = source === "codex"
        ? new CollectorMaintenance(
            buffer!,
            new RolloutTailer(buffer!, sandbox.codexSessions, () => []),
            new TranscriptTailer(buffer!, stableEmptyRoot!),
          )
        : new CollectorMaintenance(
            buffer!,
            new RolloutTailer(buffer!, stableEmptyRoot!, () => []),
            new TranscriptTailer(buffer!, sandbox.claudeProjects),
          );
      maintenance = stableMaintenance;
      let finalRun: CollectorMaintenanceRunResult | undefined;
      try {
        for (let cadence = 0; cadence < MAX_DISCOVERY_CADENCES; cadence += 1) {
          finalRun = await stableMaintenance.runRecent();
          stableRuns.push(finalRun);
          const sweep = source === "codex"
            ? finalRun.rollout.activity.scan
            : finalRun.transcript.activity.scan;
          if (sweep?.sweepComplete === true && sweep.converging === false) break;
        }
      } finally {
        stableMaintenance.close();
      }
      if (!finalRun) throw new Error("StableMaintenanceResultMissing");
      return finalRun;
    };
    const stableRolloutRun = await runStableSource("codex");
    const stableTranscriptRun = await runStableSource("claude_code");
    const stableMutations = eventMutationDelta(stableBefore, eventMutationCounts(buffer));
    const stableDirectoryAfter = activeDirectoryObserver.snapshot();
    const unchangedFilesystemEntriesScanned =
      stableDirectoryAfter.entries - stableDirectoryBefore.entries;
    const unchangedFilesystemEnumerationCalls =
      stableDirectoryAfter.calls - stableDirectoryBefore.calls;
    const stableRolloutSweep = stableRolloutRun.rollout.activity.scan;
    const stableTranscriptSweep = stableTranscriptRun.transcript.activity.scan;
    const stableSweepCompleted =
      stableRolloutSweep?.sweepComplete === true &&
      stableRolloutSweep.converging === false &&
      stableTranscriptSweep?.sweepComplete === true &&
      stableTranscriptSweep.converging === false &&
      unchangedFilesystemEntriesScanned === fixture.expectedStableDirectoryEntries &&
      unchangedFilesystemEnumerationCalls === fixture.expectedStableEnumerationCalls;
    const stableRunsUnchanged = stableRuns.every(unchangedMaintenanceResult);
    fs.rmSync(stableEmptyRoot, { recursive: true, force: true, maxRetries: 20, retryDelay: 100 });
    stableEmptyRoot = undefined;
    const observedDirectoryBeforeUnregister = activeDirectoryObserver.status();
    activeDirectoryObserver.unregister();
    const directoryObservation = activeDirectoryObserver.status();

    const firstRun = initialDrain[0];
    const secondRun = initialDrain[1];
    const firstMutations = mutationDeltas[0];
    const secondMutations = mutationDeltas[1];
    if (!firstRun || !secondRun || !firstMutations || !secondMutations) {
      throw new Error("MaintenanceResultMissing");
    }

    const finalBootRun = bootRuns.at(-1)!;
    const sourcePending = (snapshot: CaptureBaselineStatus, source: "codex" | "claude_code") => {
      const run = snapshot.sources.find((entry) => entry.source === source)?.latestRun;
      return run ? Math.max(0, run.filesDiscovered - run.filesValidated) : 0;
    };
    const maxCodexPendingMetadata = Math.max(
      ...baselineSnapshots.map((snapshot) => sourcePending(snapshot, "codex")),
      ...bootRuns.map((run) => run.rollout.baselinePendingMetadataPeak ?? 0),
    );
    const maxClaudePendingMetadata = Math.max(
      ...baselineSnapshots.map((snapshot) => sourcePending(snapshot, "claude_code")),
      ...bootRuns.map((run) => run.transcript.baselinePendingMetadataPeak ?? 0),
    );
    const maxAggregatePendingMetadata = Math.max(
      maxCodexPendingMetadata + maxClaudePendingMetadata,
      ...baselineSnapshots.map((snapshot) => snapshot.progress.pendingMetadata),
    );
    const pendingMetadataWithinCap =
      maxCodexPendingMetadata <= AUTOMATIC_DISCOVERY_PENDING_METADATA_CAP &&
      maxClaudePendingMetadata <= AUTOMATIC_DISCOVERY_PENDING_METADATA_CAP &&
      maxAggregatePendingMetadata <= AUTOMATIC_DISCOVERY_PENDING_METADATA_CAP * 2;
    const codexValidatedBeforeComplete = baselineSnapshots.some((snapshot) => {
      const source = snapshot.sources.find((entry) => entry.source === "codex");
      return source?.status === "in_progress" && Number(source.latestRun?.filesValidated ?? 0) > 0;
    });
    const claudeValidatedBeforeComplete = baselineSnapshots.some((snapshot) => {
      const source = snapshot.sources.find((entry) => entry.source === "claude_code");
      return source?.status === "in_progress" && Number(source.latestRun?.filesValidated ?? 0) > 0;
    });
    const baselineProgressFair = codexValidatedBeforeComplete && claudeValidatedBeforeComplete;
    const baselineCadenceBounded = bootRuns.length <= MAX_DISCOVERY_CADENCES + initialDrain.length;
    const startupReadinessUpperBoundSeconds = MAX_DISCOVERY_CADENCES * 5;
    const firstBootRecentOnly =
      captureBaselineStatus(buffer.database).status === "complete" &&
      bootRuns.every((run) =>
        run.recentOnly &&
        run.rollout.scope === "recent" &&
        run.transcript.scope === "recent" &&
        run.rollout.filesRead === 0 &&
        run.transcript.filesRead === 0 &&
        run.rawEventWrites === 0 &&
        run.rollout.bytesRead + run.transcript.bytesRead === 0
      ) &&
      finalBootRun.rollout.excludedGenerations === fixture.baselineCodexGenerations &&
      finalBootRun.transcript.excludedGenerations === fixture.baselineClaudeGenerations;
    const oldContentReadsAtBoot = bootRuns.reduce(
      (total, run) => total + run.rollout.filesRead + run.transcript.filesRead,
      0,
    );
    const unchangedCoalescedCycle =
      stableRunsUnchanged &&
      stableMutations.inserted === 0 &&
      stableMutations.updated === 0 &&
      stableMutations.deleted === 0;
    const coalescingProved =
      queuedStatus.inFlight &&
      queuedStatus.pending &&
      queuedStatus.triggerCount === 3 &&
      queuedStatus.coalescedTriggerCount === 2 &&
      automaticInvocations === 2 &&
      finalStatus.triggerCount === 3 &&
      finalStatus.runCount === 2 &&
      finalStatus.maxConcurrentJobs === 1 &&
      finalStatus.overlappingJobs === 0 &&
      finalStatus.failedRuns === 0;
    const startupFilesystemEntriesScanned = startupDirectoryObservation.entries;
    const startupFilesystemEnumerationCalls = startupDirectoryObservation.calls;
    const setupFilesystemEntriesScanned =
      startupFilesystemEntriesScanned + baselineFilesystemEntriesScanned;
    const setupFilesystemEnumerationCalls =
      startupFilesystemEnumerationCalls + baselineFilesystemEnumerationCalls;
    const counterProvenanceProved =
      directoryObservation.restored &&
      directoryEntryDeltas.length === finalStatus.runCount &&
      setupFilesystemEntriesScanned + unchangedFilesystemEntriesScanned ===
        directoryObservation.entries &&
      setupFilesystemEnumerationCalls + unchangedFilesystemEnumerationCalls ===
        directoryObservation.calls;
    const filesystemEnumerationObserved =
      unchangedFilesystemEnumerationCalls > 0 && unchangedFilesystemEntriesScanned > 0;
    const unchangedFilesystemEntriesBounded =
      unchangedFilesystemEntriesScanned === fixture.expectedStableDirectoryEntries;
    const unchangedFilesystemEnumerationCallsBounded =
      unchangedFilesystemEnumerationCalls === fixture.expectedStableEnumerationCalls;
    const filesystemMetadataObserved =
      activeDirectoryObserver.snapshot().metadataCalls > 0;
    const filesystemMetadataFailuresBounded =
      activeDirectoryObserver.snapshot().metadataFailures <= MAX_FIXTURE_METADATA_FAILURES;
    const recentDidNotPromote =
      initialCoverage.status === "incomplete" &&
      initialCoverage.reason === EXPLICIT_FULL_BACKFILL_NOT_COMPLETED &&
      historyCoverageStatus(buffer.database).status === "incomplete";

    // Reopen the real ledger and construct new tailers/scheduler. With no file
    // changes the restart must perform no content reads or durable writes.
    maintenance = undefined;
    buffer.close();
    buffer = new LocalEventBuffer(sandbox.ledger);
    installTemporaryEventMutationAudit(buffer);
    maintenance = makeMaintenance();
    const restartScheduler = new CoalescingMaintenanceScheduler(() => maintenance!.runRecent());
    const restartBefore = eventMutationCounts(buffer);
    const restartRun = (await requestAutomaticRecentMaintenance(restartScheduler))[0];
    const restartMutations = eventMutationDelta(restartBefore, eventMutationCounts(buffer));
    if (!restartRun) throw new Error("RestartMaintenanceResultMissing");
    const restartZeroWork =
      unchangedMaintenanceResult(restartRun) &&
      restartMutations.inserted === 0 &&
      restartMutations.updated === 0 &&
      restartMutations.deleted === 0;

    // The baseline excludes old bytes, but a later append must be captured
    // from that exact byte boundary. Its first Codex cumulative total has no
    // prior total to subtract, so it stays unvalidated.
    const rolloutBeforeGrowth = fs.statSync(fixture.recentRollout).size;
    const transcriptBeforeGrowth = fs.statSync(fixture.recentTranscript).size;
    fs.appendFileSync(
      fixture.recentRollout,
      `${JSON.stringify({
        timestamp: new Date().toISOString(),
        type: "event_msg",
        payload: {
          type: "token_count",
          info: {
            total_token_usage: {
              input_tokens: 30,
              cached_input_tokens: 7,
              output_tokens: 5,
              reasoning_output_tokens: 0,
              total_tokens: 35,
            },
          },
        },
      })}\n`,
    );
    fs.appendFileSync(
      fixture.recentTranscript,
      `${JSON.stringify({
        type: "assistant",
        sessionId: "019e6000-0000-7000-8000-000000000002",
        timestamp: new Date().toISOString(),
        message: {
          id: "resource-proof-message-2",
          model: "claude-sonnet-4-20250514",
          usage: {
            input_tokens: 4,
            cache_read_input_tokens: 1,
            cache_creation_input_tokens: 0,
            output_tokens: 2,
          },
        },
      })}\n`,
    );
    const excludedAppendBefore = eventMutationCounts(buffer);
    const excludedAppendRuns: CollectorMaintenanceRunResult[] = [];
    for (let cadence = 0; cadence < MAX_DISCOVERY_CADENCES; cadence += 1) {
      const run = (await requestAutomaticRecentMaintenance(restartScheduler))[0];
      if (!run) throw new Error("ExcludedAppendMaintenanceResultMissing");
      excludedAppendRuns.push(run);
      if (excludedAppendRuns.some((item) => item.rollout.filesRead > 0) &&
        excludedAppendRuns.some((item) => item.transcript.filesRead > 0)) {
        break;
      }
    }
    const excludedAppendMutations = eventMutationDelta(
      excludedAppendBefore,
      eventMutationCounts(buffer),
    );
    const preinstallGrowthRecovered =
      excludedAppendRuns.length > 0 &&
      excludedAppendRuns.reduce((sum, run) => sum + run.rollout.filesRead, 0) === 1 &&
      excludedAppendRuns.reduce((sum, run) => sum + run.transcript.filesRead, 0) === 1 &&
      excludedAppendRuns.reduce((sum, run) => sum + run.rollout.bytesRead, 0) <=
        fs.statSync(fixture.recentRollout).size - rolloutBeforeGrowth + 1024 &&
      excludedAppendRuns.reduce((sum, run) => sum + run.transcript.bytesRead, 0) <=
        fs.statSync(fixture.recentTranscript).size - transcriptBeforeGrowth + 1024 &&
      excludedAppendRuns.reduce((sum, run) => sum + (run.rollout.unvalidatedFirstRows ?? 0), 0) === 1 &&
      excludedAppendRuns.reduce((sum, run) => sum + run.rollout.eventsAppended, 0) === 1 &&
      excludedAppendRuns.reduce((sum, run) => sum + run.transcript.eventsAppended, 0) === 1 &&
      excludedAppendRuns.reduce((sum, run) => sum + run.rawEventWrites, 0) === 2 &&
      excludedAppendMutations.inserted === 2;

    // New path/generation fixtures begin at byte zero and capture exactly
    // once. They are deliberately created only after both baseline receipts.
    const newRolloutSession = "019e6000-0000-7000-8000-000000000011";
    const newTranscriptSession = "019e6000-0000-7000-8000-000000000012";
    const newRollout = path.join(
      path.dirname(fixture.recentRollout),
      `rollout-resource-proof-${newRolloutSession}.jsonl`,
    );
    const newTranscript = path.join(
      path.dirname(fixture.recentTranscript),
      `${newTranscriptSession}.jsonl`,
    );
    fs.writeFileSync(
      newRollout,
      [
        JSON.stringify({
          timestamp: new Date().toISOString(),
          type: "session_meta",
          payload: { id: newRolloutSession, originator: "resource-proof" },
        }),
        JSON.stringify({
          timestamp: new Date().toISOString(),
          type: "turn_context",
          payload: { model: "gpt-5.5" },
        }),
        JSON.stringify({
          timestamp: new Date().toISOString(),
          type: "event_msg",
          payload: {
            type: "token_count",
            info: {
              total_token_usage: {
                input_tokens: 30,
                cached_input_tokens: 7,
                output_tokens: 5,
                reasoning_output_tokens: 0,
                total_tokens: 35,
              },
            },
          },
        }),
      ].join("\n") + "\n",
      { mode: 0o600 },
    );
    fs.writeFileSync(
      newTranscript,
      `${JSON.stringify({
        type: "assistant",
        sessionId: newTranscriptSession,
        timestamp: new Date().toISOString(),
        message: {
          id: "resource-proof-new-message",
          model: "claude-sonnet-4-20250514",
          usage: {
            input_tokens: 4,
            cache_read_input_tokens: 1,
            cache_creation_input_tokens: 0,
            output_tokens: 2,
          },
        },
      })}\n`,
      { mode: 0o600 },
    );
    const appendBefore = eventMutationCounts(buffer);
    const appendedRuns: CollectorMaintenanceRunResult[] = [];
    for (let cadence = 0; cadence < MAX_DISCOVERY_CADENCES; cadence += 1) {
      const run = (await requestAutomaticRecentMaintenance(restartScheduler))[0];
      if (!run) throw new Error("AppendMaintenanceResultMissing");
      appendedRuns.push(run);
      const captured = buffer.database.prepare(
        `select count(*) as count from buffered_events where session_id in (?, ?)`,
      ).get(newRolloutSession, newTranscriptSession) as { count: number };
      if (captured.count === 2) break;
    }
    const appendedRun = appendedRuns[0]!;
    const appendMutations = eventMutationDelta(appendBefore, eventMutationCounts(buffer));
    // Capture returns before all four fair repair stages finish. Require the
    // same zero-work receipt after at most one complete repair rotation.
    let afterAppendRun: CollectorMaintenanceRunResult | undefined;
    for (let stage = 0; stage < 4; stage += 1) {
      afterAppendRun = (await requestAutomaticRecentMaintenance(restartScheduler))[0];
      if (afterAppendRun && unchangedMaintenanceResult(afterAppendRun)) break;
    }
    if (!appendedRun || !afterAppendRun) throw new Error("AppendMaintenanceResultMissing");
    const appendedExactlyOnce =
      appendedRuns.reduce((total, run) => total + run.rawEventWrites, 0) === 2 &&
      appendedRuns.reduce((total, run) => total + run.rollout.eventsAppended, 0) === 1 &&
      appendedRuns.reduce((total, run) => total + run.transcript.eventsAppended, 0) === 1 &&
      appendMutations.inserted === 2 &&
      unchangedMaintenanceResult(afterAppendRun);

    // Force deterministic event replay by removing only the two recent byte
    // cursors. Receipt counters must remain zero because append returned false.
    buffer.database
      .prepare(`delete from rollout_scan_state where file in (?, ?)`)
      .run(
        jsonlScanStateKey(newRollout),
        jsonlScanStateKey(newTranscript),
    );
    const replayBefore = eventMutationCounts(buffer);
    const replayRuns: CollectorMaintenanceRunResult[] = [];
    for (let cadence = 0; cadence < MAX_DISCOVERY_CADENCES; cadence += 1) {
      const run = (await requestAutomaticRecentMaintenance(restartScheduler))[0];
      if (!run) throw new Error("ReplayMaintenanceResultMissing");
      replayRuns.push(run);
      const cursors = buffer.database.prepare(
        `select count(*) as count from rollout_scan_state where file in (?, ?)`,
      ).get(jsonlScanStateKey(newRollout), jsonlScanStateKey(newTranscript)) as { count: number };
      if (cursors.count === 2) break;
    }
    const replayRun = replayRuns[0]!;
    const replayMutations = eventMutationDelta(replayBefore, eventMutationCounts(buffer));
    if (!replayRun) throw new Error("ReplayMaintenanceResultMissing");
    const durableReceiptCounters =
      replayRuns.reduce((total, run) => total + run.rollout.filesRead, 0) === 1 &&
      replayRuns.reduce((total, run) => total + run.transcript.filesRead, 0) === 1 &&
      replayRuns.every((run) => run.rollout.eventsAppended === 0 && run.transcript.eventsAppended === 0) &&
      replayRuns.every((run) => run.rawEventWrites === 0) &&
      replayRuns.every((run) => Object.values(run.rollout.tokensAppended).every((value) => value === 0)) &&
      replayRuns.every((run) => Object.values(run.transcript.tokensAppended).every((value) => value === 0)) &&
      replayMutations.inserted === 0;

    let recentPromotionRejected = false;
    try {
      recordExplicitFullHistoryCoverage(buffer.database, "codex", appendedRun.rollout);
    } catch {
      recentPromotionRejected = true;
    }

    // HTTP status exposes current capture health separately from the still
    // incomplete historical-coverage marker.
    const statusServer = createCollectorServer(collectorConfigSchema.parse({}), buffer);
    statusServer.unref();
    await new Promise<void>((resolve) => statusServer.listen(0, "127.0.0.1", resolve));
    const statusAddress = statusServer.address();
    if (!statusAddress || typeof statusAddress === "string") {
      throw new Error("HistoryStatusListenerMissing");
    }
    const statusResponse = await fetch(`http://127.0.0.1:${statusAddress.port}/status`);
    const httpStatus = (await statusResponse.json()) as Record<string, unknown>;
    await new Promise<void>((resolve) => statusServer.close(() => resolve()));
    const httpHistory = httpStatus.historyCoverage as
      | { status?: unknown; reason?: unknown }
      | undefined;
    const statusSeparatesCurrentFromHistory =
      statusResponse.ok &&
      Object.hasOwn(httpStatus, "captureHealth") &&
      httpHistory?.status === "incomplete" &&
      httpHistory.reason === EXPLICIT_FULL_BACKFILL_NOT_COMPLETED;

    // Full history is literal and explicit. Truncation and forced discovery,
    // stat, and open failures all persist honest failed-attempt receipts but
    // cannot promote coverage. Later exhaustive passes resume from cursors.
    const transcriptPartial = await new TranscriptTailer(
      buffer,
      sandbox.claudeProjects,
    ).scan({ scope: "full", discoveryLimit: 500 });
    const partialCoverage = recordExplicitFullHistoryCoverage(
      buffer.database,
      "claude_code",
      transcriptPartial,
    );

    const syntheticFsError = (code: string) =>
      Object.assign(new Error("SyntheticHistoryFilesystemFailure"), { code });
    const rolloutDiscoveryFailure = await new RolloutTailer(
      buffer,
      sandbox.codexSessions,
      () => [],
      {
        ...DEFAULT_JSONL_TAILER_IO,
        readNames: (directory) => {
          if (path.resolve(directory) === path.resolve(fixture.oldRolloutDay)) {
            throw syntheticFsError("EACCES");
          }
          return DEFAULT_JSONL_TAILER_IO.readNames(directory);
        },
      },
    ).scan({ scope: "full" });
    const rolloutDiscoveryCoverage = recordExplicitFullHistoryCoverage(
      buffer.database,
      "codex",
      rolloutDiscoveryFailure,
    );

    const transcriptStatFailure = await new TranscriptTailer(
      buffer,
      sandbox.claudeProjects,
      {
        ...DEFAULT_JSONL_TAILER_IO,
        stat: (file) => {
          if (path.resolve(file) === path.resolve(fixture.oldTranscriptTarget)) {
            throw syntheticFsError("ENOENT");
          }
          return DEFAULT_JSONL_TAILER_IO.stat(file);
        },
      },
    ).scan({ scope: "full" });
    const transcriptStatCoverage = recordExplicitFullHistoryCoverage(
      buffer.database,
      "claude_code",
      transcriptStatFailure,
    );

    // A readTail failure is converted by the continuation boundary into a
    // path-free source_changed refusal. Keep the target unchanged so this
    // exercises the existing cursor/restart path without creating writes.
    const rolloutOpenFailure = await new RolloutTailer(
      buffer,
      sandbox.codexSessions,
      () => [],
      {
        ...DEFAULT_JSONL_TAILER_IO,
        readTail: (file, stat, cursor) => {
          if (path.resolve(file) === path.resolve(fixture.oldRolloutTarget)) {
            throw syntheticFsError("ENOENT");
          }
          return DEFAULT_JSONL_TAILER_IO.readTail(file, stat, cursor);
        },
      },
    ).scan({ scope: "full" });
    const rolloutOpenCoverage = recordExplicitFullHistoryCoverage(
      buffer.database,
      "codex",
      rolloutOpenFailure,
    );

    const rolloutFull = await new RolloutTailer(
      buffer,
      sandbox.codexSessions,
      () => [],
    ).scan({ scope: "full" });
    const rolloutCoverage = recordExplicitFullHistoryCoverage(
      buffer.database,
      "codex",
      rolloutFull,
    );
    const transcriptFull = await new TranscriptTailer(
      buffer,
      sandbox.claudeProjects,
    ).scan({ scope: "full" });
    const completedCoverage = recordExplicitFullHistoryCoverage(
      buffer.database,
      "claude_code",
      transcriptFull,
    );
    const rolloutIdempotent = await new RolloutTailer(
      buffer,
      sandbox.codexSessions,
      () => [],
    ).scan({ scope: "full" });
    const transcriptIdempotent = await new TranscriptTailer(
      buffer,
      sandbox.claudeProjects,
    ).scan({ scope: "full" });

    // Complete malformed lines must remain unresolved across unchanged scans
    // and process restarts. Repairing the file then causes a real re-read and
    // permits a new exhaustive full attempt to promote both sources.
    const malformedRollout = path.join(
      path.dirname(fixture.recentRollout),
      "rollout-malformed-019e6000-0000-7000-8000-000000000003.jsonl",
    );
    const malformedTranscript = path.join(
      path.dirname(fixture.recentTranscript),
      "019e6000-0000-7000-8000-000000000004.jsonl",
    );
    fs.writeFileSync(
      malformedRollout,
      '{"type":"event_msg","payload":{"type":"token_count"\n',
      { mode: 0o600 },
    );
    fs.writeFileSync(
      malformedTranscript,
      '{"type":"assistant","message":{"usage":\n',
      { mode: 0o600 },
    );
    const rolloutParseFailure = await new RolloutTailer(
      buffer,
      sandbox.codexSessions,
      () => [],
    ).scan({ scope: "full" });
    const rolloutParseFailureCoverage = recordExplicitFullHistoryCoverage(
      buffer.database,
      "codex",
      rolloutParseFailure,
    );
    const transcriptParseFailure = await new TranscriptTailer(
      buffer,
      sandbox.claudeProjects,
    ).scan({ scope: "full" });
    const transcriptParseFailureCoverage = recordExplicitFullHistoryCoverage(
      buffer.database,
      "claude_code",
      transcriptParseFailure,
    );
    const parseFailureCursorsAbsent =
      (buffer.database
        .prepare(`select count(*) as count from rollout_scan_state where file in (?, ?)`)
        .get(
          jsonlScanStateKey(malformedRollout),
          jsonlScanStateKey(malformedTranscript),
        ) as { count: number }).count === 0;
    const rolloutParseRetry = await new RolloutTailer(
      buffer,
      sandbox.codexSessions,
      () => [],
    ).scan({ scope: "full" });
    const rolloutParseRetryCoverage = recordExplicitFullHistoryCoverage(
      buffer.database,
      "codex",
      rolloutParseRetry,
    );
    const transcriptParseRetry = await new TranscriptTailer(
      buffer,
      sandbox.claudeProjects,
    ).scan({ scope: "full" });
    const transcriptParseRetryCoverage = recordExplicitFullHistoryCoverage(
      buffer.database,
      "claude_code",
      transcriptParseRetry,
    );
    const unchangedParseFailuresRetained =
      rolloutParseFailure.parseErrors === 1 &&
      transcriptParseFailure.parseErrors === 1 &&
      !rolloutParseFailureCoverage.promoted &&
      !transcriptParseFailureCoverage.promoted &&
      rolloutParseFailureCoverage.coverage.status === "complete" &&
      transcriptParseFailureCoverage.coverage.status === "complete" &&
      rolloutParseRetry.filesRead === 1 &&
      transcriptParseRetry.filesRead === 1 &&
      rolloutParseRetry.parseErrors === 1 &&
      transcriptParseRetry.parseErrors === 1 &&
      !rolloutParseRetryCoverage.promoted &&
      !transcriptParseRetryCoverage.promoted &&
      parseFailureCursorsAbsent;

    maintenance.close();
    maintenance = undefined;
    buffer.close();
    buffer = new LocalEventBuffer(sandbox.ledger);
    const rolloutParseRestart = await new RolloutTailer(
      buffer,
      sandbox.codexSessions,
      () => [],
    ).scan({ scope: "full" });
    const rolloutParseRestartCoverage = recordExplicitFullHistoryCoverage(
      buffer.database,
      "codex",
      rolloutParseRestart,
    );
    const transcriptParseRestart = await new TranscriptTailer(
      buffer,
      sandbox.claudeProjects,
    ).scan({ scope: "full" });
    const transcriptParseRestartCoverage = recordExplicitFullHistoryCoverage(
      buffer.database,
      "claude_code",
      transcriptParseRestart,
    );
    const parseFailuresPersistAcrossRestart =
      rolloutParseRestart.filesRead === 1 &&
      transcriptParseRestart.filesRead === 1 &&
      rolloutParseRestart.parseErrors === 1 &&
      transcriptParseRestart.parseErrors === 1 &&
      !rolloutParseRestartCoverage.promoted &&
      !transcriptParseRestartCoverage.promoted &&
      rolloutParseRestartCoverage.coverage.status === "complete" &&
      transcriptParseRestartCoverage.coverage.status === "complete" &&
      rolloutParseRestartCoverage.coverage.sources.find(
        (source) => source.source === "codex",
      )?.latestFullAttempt?.parseErrors === 1 &&
      transcriptParseRestartCoverage.coverage.sources.find(
        (source) => source.source === "claude_code",
      )?.latestFullAttempt?.parseErrors === 1;

    fs.writeFileSync(
      malformedRollout,
      `${JSON.stringify({
        timestamp: fixture.observedAt,
        type: "event_msg",
        payload: { type: "token_count" },
      })}\n`,
      { mode: 0o600 },
    );
    fs.writeFileSync(
      malformedTranscript,
      `${JSON.stringify({
        type: "assistant",
        sessionId: "019e6000-0000-7000-8000-000000000004",
        timestamp: fixture.observedAt,
        message: { id: "repaired-empty-usage", usage: {} },
      })}\n`,
      { mode: 0o600 },
    );
    const rolloutParseRepair = await new RolloutTailer(
      buffer,
      sandbox.codexSessions,
      () => [],
    ).scan({ scope: "full" });
    const rolloutParseRepairCoverage = recordExplicitFullHistoryCoverage(
      buffer.database,
      "codex",
      rolloutParseRepair,
    );
    const transcriptParseRepair = await new TranscriptTailer(
      buffer,
      sandbox.claudeProjects,
    ).scan({ scope: "full" });
    const transcriptParseRepairCoverage = recordExplicitFullHistoryCoverage(
      buffer.database,
      "claude_code",
      transcriptParseRepair,
    );
    const repairedParseFailuresPromote =
      rolloutParseRepair.filesRead === 1 &&
      transcriptParseRepair.filesRead === 1 &&
      rolloutParseRepair.parseErrors === 0 &&
      transcriptParseRepair.parseErrors === 0 &&
      rolloutParseRepairCoverage.promoted &&
      transcriptParseRepairCoverage.promoted &&
      transcriptParseRepairCoverage.coverage.status === "complete" &&
      (buffer.database
        .prepare(`select count(*) as count from rollout_scan_state where file in (?, ?)`)
        .get(
          jsonlScanStateKey(malformedRollout),
          jsonlScanStateKey(malformedTranscript),
        ) as { count: number }).count === 2;

    const transcriptFailureAfterComplete = await new TranscriptTailer(
      buffer,
      sandbox.claudeProjects,
      {
        ...DEFAULT_JSONL_TAILER_IO,
        stat: (file) => {
          if (path.resolve(file) === path.resolve(fixture.recentTranscript)) {
            throw syntheticFsError("ENOENT");
          }
          return DEFAULT_JSONL_TAILER_IO.stat(file);
        },
      },
    ).scan({ scope: "full" });
    const preservedCoverage = recordExplicitFullHistoryCoverage(
      buffer.database,
      "claude_code",
      transcriptFailureAfterComplete,
    );
    const preservedClaudeStatus = preservedCoverage.coverage.sources.find(
      (source) => source.source === "claude_code",
    );
    const inaccessibleItemsBlockPromotion =
      transcriptPartial.activity.truncated &&
      !transcriptPartial.exhaustive &&
      !partialCoverage.promoted &&
      rolloutDiscoveryFailure.discoveryErrors === 1 &&
      !rolloutDiscoveryFailure.exhaustive &&
      !rolloutDiscoveryCoverage.promoted &&
      transcriptStatFailure.statErrors === 1 &&
      !transcriptStatFailure.exhaustive &&
      !transcriptStatCoverage.promoted &&
      rolloutOpenFailure.continuationReasons?.source_changed === 1 &&
      !rolloutOpenFailure.exhaustive &&
      !rolloutOpenCoverage.promoted;
    const failedAttemptDisclosedAfterComplete =
      !preservedCoverage.promoted &&
      preservedCoverage.coverage.status === "complete" &&
      preservedClaudeStatus?.status === "complete" &&
      preservedClaudeStatus.latestFullAttempt?.status === "incomplete" &&
      preservedClaudeStatus.latestFullAttempt.statErrors === 1;
    const fullBackfillResumableIdempotent =
      rolloutFull.scope === "full" &&
      rolloutFull.exhaustive &&
      rolloutCoverage.promoted &&
      rolloutCoverage.coverage.status === "incomplete" &&
      transcriptFull.exhaustive &&
      transcriptFull.filesRead > 0 &&
      completedCoverage.promoted &&
      completedCoverage.coverage.status === "complete" &&
      rolloutIdempotent.filesRead === 0 &&
      transcriptIdempotent.filesRead === 0 &&
      rolloutIdempotent.eventsAppended === 0 &&
      transcriptIdempotent.eventsAppended === 0 &&
      inaccessibleItemsBlockPromotion &&
      failedAttemptDisclosedAfterComplete &&
      unchangedParseFailuresRetained &&
      parseFailuresPersistAcrossRestart &&
      repairedParseFailuresPromote;

    buffer.close();
    buffer = new LocalEventBuffer(sandbox.ledger);
    const persistedCoverage = historyCoverageStatus(buffer.database);
    const persistedClaudeStatus = persistedCoverage.sources.find(
      (source) => source.source === "claude_code",
    );
    const coveragePersistsAcrossRestart =
      persistedCoverage.status === "complete" &&
      persistedClaudeStatus?.status === "complete" &&
      persistedClaudeStatus.latestFullAttempt?.status === "incomplete" &&
      persistedClaudeStatus.latestFullAttempt.statErrors === 1;

    const explicitFullReads =
      transcriptPartial.filesRead +
      rolloutDiscoveryFailure.filesRead +
      transcriptStatFailure.filesRead +
      rolloutOpenFailure.filesRead +
      rolloutFull.filesRead +
      transcriptFull.filesRead +
      rolloutIdempotent.filesRead +
      transcriptIdempotent.filesRead +
      rolloutParseFailure.filesRead +
      transcriptParseFailure.filesRead +
      rolloutParseRetry.filesRead +
      transcriptParseRetry.filesRead +
      rolloutParseRestart.filesRead +
      transcriptParseRestart.filesRead +
      rolloutParseRepair.filesRead +
      transcriptParseRepair.filesRead +
      transcriptFailureAfterComplete.filesRead;
    counters.filesOpened =
      firstRun.rollout.filesRead +
      firstRun.transcript.filesRead +
      restartRun.rollout.filesRead +
      restartRun.transcript.filesRead +
      excludedAppendRuns.reduce((total, run) => total + run.rollout.filesRead + run.transcript.filesRead, 0) +
      appendedRuns.reduce((total, run) => total + run.rollout.filesRead + run.transcript.filesRead, 0) +
      afterAppendRun.rollout.filesRead +
      afterAppendRun.transcript.filesRead +
      replayRuns.reduce((total, run) => total + run.rollout.filesRead + run.transcript.filesRead, 0) +
      explicitFullReads;
    counters.fileBytesRead =
      firstRun.rollout.bytesRead +
      firstRun.transcript.bytesRead +
      excludedAppendRuns.reduce((total, run) => total + run.rollout.bytesRead + run.transcript.bytesRead, 0) +
      appendedRuns.reduce((total, run) => total + run.rollout.bytesRead + run.transcript.bytesRead, 0) +
      replayRuns.reduce((total, run) => total + run.rollout.bytesRead + run.transcript.bytesRead, 0) +
      transcriptPartial.bytesRead +
      rolloutDiscoveryFailure.bytesRead +
      transcriptStatFailure.bytesRead +
      rolloutOpenFailure.bytesRead +
      rolloutFull.bytesRead +
      transcriptFull.bytesRead +
      rolloutIdempotent.bytesRead +
      transcriptIdempotent.bytesRead +
      rolloutParseFailure.bytesRead +
      transcriptParseFailure.bytesRead +
      rolloutParseRetry.bytesRead +
      transcriptParseRetry.bytesRead +
      rolloutParseRestart.bytesRead +
      transcriptParseRestart.bytesRead +
      rolloutParseRepair.bytesRead +
      transcriptParseRepair.bytesRead +
      transcriptFailureAfterComplete.bytesRead;
    counters.fullHistoryFileReads = explicitFullReads;
    counters.rawEventWrites = firstMutations.inserted + appendMutations.inserted;
    counters.rawEventRewrites =
      firstMutations.updated +
      firstMutations.deleted +
      secondMutations.updated +
      secondMutations.deleted +
      restartMutations.updated +
      restartMutations.deleted +
      appendMutations.updated +
      appendMutations.deleted +
      replayMutations.updated +
      replayMutations.deleted;
    counters.maintenanceRuns = finalStatus.runCount + restartScheduler.status().runCount;
    counters.overlappingJobs =
      finalStatus.overlappingJobs + restartScheduler.status().overlappingJobs;
    counters.listenersCreated = 1;
    counters.filesystemEntriesScanned = directoryObservation.entries;

    const passed =
      fixture.oldFiles >= 2_000 &&
      fixture.baselineCodexGenerations >= 200 &&
      fixture.baselineClaudeGenerations >= 1_200 &&
      fixture.expectedStableDirectoryEntries > 0 &&
      fixture.expectedStableEnumerationCalls > 0 &&
      firstBootRecentOnly &&
      pendingMetadataWithinCap &&
      baselineProgressFair &&
      baselineCadenceBounded &&
      oldContentReadsAtBoot === 0 &&
      unchangedCoalescedCycle &&
      stableSweepCompleted &&
      coalescingProved &&
      counterProvenanceProved &&
      setupFilesystemEntriesScanned === fixture.expectedSetupFilesystemEntriesScanned &&
      setupFilesystemEnumerationCalls === fixture.expectedSetupFilesystemEnumerationCalls &&
      recentDidNotPromote &&
      restartZeroWork &&
      preinstallGrowthRecovered &&
      appendedExactlyOnce &&
      durableReceiptCounters &&
      recentPromotionRejected &&
      statusSeparatesCurrentFromHistory &&
      fullBackfillResumableIdempotent &&
      coveragePersistsAcrossRestart &&
      explicitFullReads >= fixture.oldFiles &&
      counters.rawEventWrites === 2 &&
      counters.rawEventRewrites === 0 &&
      counters.overlappingJobs === 0 &&
      filesystemEnumerationObserved &&
      filesystemMetadataObserved &&
      filesystemMetadataFailuresBounded &&
      unchangedFilesystemEntriesBounded &&
      unchangedFilesystemEnumerationCallsBounded;

    return {
      id: "no_change_constant_work",
      required: true,
      status: passed ? "pass" : "fail",
      detail: passed
        ? "Metadata-only first boot excluded pre-install bytes, recovered later appends from their boundary, captured new generations exactly once, and left history import explicit and resumable."
        : `Recent-first boot, durable receipt, restart, status, or explicit history coverage assertions failed: ${JSON.stringify({ growthRuns: excludedAppendRuns.map((run) => ({ raw: run.rawEventWrites, rolloutRead: run.rollout.filesRead, rolloutBytes: run.rollout.bytesRead, rolloutUnvalidated: run.rollout.unvalidatedFirstRows, claudeRead: run.transcript.filesRead, claudeBytes: run.transcript.bytesRead, claudeEvents: run.transcript.eventsAppended })), growthMutations: excludedAppendMutations, appendedWrites: appendedRuns.map((run) => ({ raw: run.rawEventWrites, rollout: run.rollout.eventsAppended, transcript: run.transcript.eventsAppended })), appendMutations, afterAppendRun })}`,
      durationMs: Math.round((performance.now() - started) * 100) / 100,
      counters,
      measurements: {
        syntheticHistoricalFiles: fixture.oldFiles,
        baselineCodexGenerations: fixture.baselineCodexGenerations,
        baselineClaudeGenerations: fixture.baselineClaudeGenerations,
        nestedNoncandidateEntries: fixture.nestedNoncandidateEntries,
        expectedStableDirectoryEntries: fixture.expectedStableDirectoryEntries,
        expectedStableEnumerationCalls: fixture.expectedStableEnumerationCalls,
        expectedSetupFilesystemEntriesScanned: fixture.expectedSetupFilesystemEntriesScanned,
        expectedSetupFilesystemEnumerationCalls: fixture.expectedSetupFilesystemEnumerationCalls,
        baselineCadences: bootRuns.length,
        baselineCadenceLimit: MAX_DISCOVERY_CADENCES + initialDrain.length,
        startupReadinessUpperBoundSeconds,
        maximumStartupDutyCycle: 0.04,
        maxCodexPendingMetadata,
        maxClaudePendingMetadata,
        maxAggregatePendingMetadata,
        pendingMetadataPerSourceCap: AUTOMATIC_DISCOVERY_PENDING_METADATA_CAP,
        pendingMetadataAggregateCap: AUTOMATIC_DISCOVERY_PENDING_METADATA_CAP * 2,
        pendingMetadataWithinCap,
        codexValidatedBeforeComplete,
        claudeValidatedBeforeComplete,
        baselineProgressFair,
        firstBootRecentOnly,
        oldContentReadsAtBoot,
        initialEventsAppended: firstRun.rawEventWrites,
        bootBytesRead: firstRun.rollout.bytesRead + firstRun.transcript.bytesRead,
        recentFixtureBytes: fixture.recentBytes,
        claudeOldMetadataEntriesSkipped:
          firstRun.transcript.filesSkippedOutsideRecentWindow +
          secondRun.transcript.filesSkippedOutsideRecentWindow,
        claudeMetadataEnumerationCaveat:
          "baseline stats every discovered Claude generation but opens no old content",
        unchangedCoalescedCycle,
        stableRuns: stableRuns.length,
        stableRunsUnchanged,
        stableSweepCompleted,
        stableRolloutSweepComplete: stableRolloutSweep?.sweepComplete ?? false,
        stableTranscriptSweepComplete: stableTranscriptSweep?.sweepComplete ?? false,
        restartZeroWork,
        preinstallGrowthRecovered,
        appendedExactlyOnce,
        durableReceiptCounters,
        replayRolloutFilesRead: replayRuns.reduce(
          (total, run) => total + run.rollout.filesRead,
          0,
        ),
        replayTranscriptFilesRead: replayRuns.reduce(
          (total, run) => total + run.transcript.filesRead,
          0,
        ),
        replayEventsAppended: replayRuns.reduce(
          (total, run) => total + run.rollout.eventsAppended + run.transcript.eventsAppended,
          0,
        ),
        replayRawEventWrites: replayRuns.reduce((total, run) => total + run.rawEventWrites, 0),
        replayEventMutationsInserted: replayMutations.inserted,
        recentPromotionRejected,
        statusSeparatesCurrentFromHistory,
        fullBackfillResumableIdempotent,
        inaccessibleItemsBlockPromotion,
        failedAttemptDisclosedAfterComplete,
        unchangedParseFailuresRetained,
        parseFailuresPersistAcrossRestart,
        repairedParseFailuresPromote,
        parseFailureRetryReads:
          rolloutParseRetry.filesRead + transcriptParseRetry.filesRead,
        parseFailureRestartReads:
          rolloutParseRestart.filesRead + transcriptParseRestart.filesRead,
        parseRepairReads:
          rolloutParseRepair.filesRead + transcriptParseRepair.filesRead,
        forcedDiscoveryErrors: rolloutDiscoveryFailure.discoveryErrors,
        forcedStatErrors: transcriptStatFailure.statErrors,
        forcedReadErrors: rolloutOpenFailure.readErrors,
        persistedLatestFullAttemptStatus:
          persistedClaudeStatus?.latestFullAttempt?.status ?? "missing",
        coveragePersistsAcrossRestart,
        finalHistoricalCoverage: persistedCoverage.status,
        truncatedTranscriptFilesRead: transcriptPartial.filesRead,
        resumedTranscriptFilesRead: transcriptFull.filesRead,
        explicitFullReads,
        concurrentTriggersQueued: 2,
        triggerCount: finalStatus.triggerCount,
        coalescedTriggerCount: finalStatus.coalescedTriggerCount,
        schedulerRunCount: finalStatus.runCount,
        maxConcurrentJobs: finalStatus.maxConcurrentJobs,
        filesystemEnumerationCalls: directoryObservation.calls,
        filesystemEnumerationFailedCalls: directoryObservation.failedCalls,
        filesystemEnumerationReadFailures: directoryObservation.readFailures,
        startupFilesystemEntriesScanned,
        startupFilesystemEnumerationCalls,
        baselineFilesystemEntriesScanned,
        baselineFilesystemEnumerationCalls,
        setupFilesystemEntriesScanned,
        setupFilesystemEnumerationCalls,
        unchangedFilesystemEntriesScanned,
        unchangedFilesystemEnumerationCalls,
        unchangedFilesystemEntriesBounded,
        unchangedFilesystemEnumerationCallsBounded,
        stableSweepCursorReset: true,
        filesystemObserverRestored: directoryObservation.restored,
        counterProvenanceProved,
        filesystemEnumerationObserved,
        filesystemMetadataObserved,
        filesystemMetadataFailuresBounded,
        filesystemMetadataFailureBound: MAX_FIXTURE_METADATA_FAILURES,
        directoryApiCoverageChecked,
        filesystemCounterSource:
          "successful returned directory entries from all observed fs readdir/opendir/glob forms; failed calls and metadata operations are reported separately",
        filesystemMetadataOperations: directoryObservation.metadataCalls,
        filesystemMetadataFailures: directoryObservation.metadataFailures,
        maintenanceRunCounterSource: "scheduler runCount",
      },
    };
  } catch (error) {
    if (stableEmptyRoot) {
      fs.rmSync(stableEmptyRoot, { recursive: true, force: true, maxRetries: 20, retryDelay: 100 });
      stableEmptyRoot = undefined;
    }
    directoryObserver?.unregister();
    const directoryObservation = directoryObserver?.status();
    if (directoryObservation) {
      counters.filesystemEntriesScanned = directoryObservation.entries;
    }
    return {
      id: "no_change_constant_work",
      required: true,
      status: "fail",
      detail: `Recent-first boot production contract raised ${
        error instanceof Error ? error.name : "UnknownError"
      }; error text is omitted from the receipt.`,
      durationMs: Math.round((performance.now() - started) * 100) / 100,
      counters,
      measurements: {
        deterministicIdleCounters: false,
        directoryApiCoverageChecked,
        filesystemObserverRestored: directoryObservation?.restored ?? false,
        counterProvenanceProved: directoryObservation?.restored ?? false,
        filesystemEnumerationCalls: directoryObservation?.calls ?? 0,
        filesystemEnumerationFailedCalls: directoryObservation?.failedCalls ?? 0,
        filesystemEnumerationReadFailures: directoryObservation?.readFailures ?? 0,
        filesystemMetadataOperations: directoryObservation?.metadataCalls ?? 0,
        filesystemMetadataFailures: directoryObservation?.metadataFailures ?? 0,
      },
    };
  } finally {
    directoryObserver?.unregister();
    maintenance?.close();
    buffer?.close();
  }
}

/** Production #91 compact queues, later-context invalidation, and idle bound. */
export function runBoundedCodexReconciliationContract(
  sandbox: ResourceSandbox,
): ScenarioReceipt {
  const started = performance.now();
  const counters = emptyWorkCounters();
  const ledger = path.join(sandbox.plimsollHome, "codex-reconciliation-proof.sqlite");
  const buffer = new LocalEventBuffer(ledger);
  try {
    const sessionId = "019e9100-0000-7000-8000-000000000091";
    for (let index = 0; index < 4; index += 1) {
      buffer.append(
        aiInteractionEventSchema.parse({
          id: `resource-reconciliation-candidate-${index}`,
          tenantId: "local",
          source: "codex",
          dataMode: "metadata",
          eventType: "assistant_response",
          observedAt: `2026-07-15T12:0${index}:00.000Z`,
          actionClass: "other",
          inputTokens: 100 + index,
          outputTokens: 10,
          metadata: {},
        }),
        [],
      );
    }
    const waiting = runCodexReconciliationMaintenance(buffer.database, {
      legacyRowLimit: 100,
      contextRowLimit: 2,
      candidateLimit: 2,
      timeLimitMs: 1_000,
    });
    counters.reconciliationRowsVisited += waiting.rowsVisited;
    counters.rawEventRewrites += waiting.rowsChanged;
    buffer.append(
      aiInteractionEventSchema.parse({
        id: "resource-reconciliation-context",
        tenantId: "local",
        source: "codex",
        dataMode: "metadata",
        eventType: "tool_use",
        observedAt: "2026-07-15T12:05:00.000Z",
        sessionId,
        model: "gpt-5.5",
        actionClass: "shell",
        metadata: {},
      }),
      [],
    );
    const slices = [];
    for (let iteration = 0; iteration < 20; iteration += 1) {
      const result = runCodexReconciliationMaintenance(buffer.database, {
        legacyRowLimit: 2,
        contextWindowLimit: 1,
        contextRowLimit: 2,
        candidateLimit: 1,
        timeLimitMs: 1_000,
      });
      slices.push(result);
      counters.reconciliationRowsVisited += result.rowsVisited;
      counters.rawEventRewrites += result.rowsChanged;
      if (
        codexReconciliationStatus(buffer.database).candidateBacklog === 0 &&
        codexReconciliationStatus(buffer.database).contextWindowBacklog === 0
      ) {
        break;
      }
    }
    const rows = buffer.database
      .prepare(
        `select session_id as sessionId, model, cost_usd as costUsd
         from buffered_events where id like 'resource-reconciliation-candidate-%'`,
      )
      .all() as Array<{
      sessionId: string | null;
      model: string | null;
      costUsd: number | null;
    }>;
    const idle = runCodexReconciliationMaintenance(buffer.database, { timeLimitMs: 1_000 });
    counters.reconciliationRowsVisited += idle.rowsVisited;
    counters.rawEventRewrites += idle.rowsChanged;
    const status = codexReconciliationStatus(buffer.database);
    const bounded = slices.every(
      (result) => result.contextRowsVisited <= 2 && result.candidateRowsVisited <= 1,
    );
    const passed =
      rows.length === 4 &&
      rows.every(
        (row) => row.sessionId === sessionId && row.model === "gpt-5.5" && row.costUsd !== null,
      ) &&
      bounded &&
      idle.rowsVisited === 0 &&
      idle.rowsChanged === 0 &&
      status.candidateBacklog === 0 &&
      status.contextWindowBacklog === 0 &&
      counters.rawEventRewrites === 4;
    return {
      id: "bounded_codex_reconciliation",
      required: true,
      status: passed ? "pass" : "fail",
      detail: passed
        ? "Four prior unresolved Codex usage rows were repaired exactly once by later context through fixed durable slices; the next run visited zero rows."
        : "Bounded Codex reconciliation failed one or more queue, slice, parity, or idle assertions.",
      durationMs: Math.round((performance.now() - started) * 100) / 100,
      counters,
      measurements: {
        candidateRows: rows.length,
        maintenanceSlices: slices.length,
        maxContextRowsVisited: Math.max(0, ...slices.map((result) => result.contextRowsVisited)),
        maxCandidateRowsVisited: Math.max(
          0,
          ...slices.map((result) => result.candidateRowsVisited),
        ),
        changedRows: counters.rawEventRewrites,
        idleRowsVisited: idle.rowsVisited,
        candidateBacklog: status.candidateBacklog,
        contextWindowBacklog: status.contextWindowBacklog,
      },
    };
  } catch (error) {
    return {
      id: "bounded_codex_reconciliation",
      required: true,
      status: "fail",
      detail: `Bounded Codex reconciliation raised ${
        error instanceof Error ? error.name : "UnknownError"
      }; error text is omitted from the receipt.`,
      durationMs: Math.round((performance.now() - started) * 100) / 100,
      counters,
    };
  } finally {
    buffer.close();
  }
}

export async function runDashboardProjectionBudgetContract(
  sandbox: ResourceSandbox,
): Promise<ScenarioReceipt> {
  const started = performance.now();
  const buffer = new LocalEventBuffer(sandbox.ledger);
  const config = collectorConfigSchema.parse({});
  const eventId = `00000000-0000-4000-8000-000000008080`;
  const event = aiInteractionEventSchema.parse({
    id: eventId,
    tenantId: "local",
    source: "codex",
    dataMode: "metadata",
    eventType: "assistant_response",
    observedAt: new Date(Date.now() - 60_000).toISOString(),
    sessionId: "resource-dashboard-session",
    actionClass: "other",
    model: "resource-proof-model",
    inputTokens: 80,
    outputTokens: 8,
    costUsd: 0.08,
    metadata: { resourceProof: true },
  });
  const server = createCollectorServer(config, buffer);
  try {
    buffer.append(event);
    for (let slice = 0; slice < 10; slice += 1) {
      const status = buffer.projection.status();
      if (status.ready && !status.dirty && Object.values(status.backlog).every((n) => n === 0)) break;
      buffer.projection.runMaintenance(new Date(Date.now()));
    }
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(0, "127.0.0.1", () => resolve());
    });
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("dashboard proof server has no port");
    const base = `http://127.0.0.1:${address.port}`;
    const before = buffer.projection.workCounters();
    const buildsBefore = before.snapshotBuilds;
    const totalChanges = () => (buffer.database.prepare("select total_changes() as n").get() as { n: number }).n;
    const writesBefore = totalChanges();
    const durations: number[] = [];
    let generation: number | null = null;
    let coherent = true;
    for (let index = 0; index < 25; index += 1) {
      const requestStarted = performance.now();
      const response = await fetch(`${base}/api/snapshot?days=30`);
      const body = await response.json() as {
        generation?: number;
        summary?: { totals?: { events?: number } };
        sessions?: unknown[];
        repos?: unknown[];
        accounts?: { accounts?: unknown[] };
        status?: unknown;
      };
      if (!response.ok || !body.summary || !body.sessions || !body.repos || !body.accounts || !body.status) {
        coherent = false;
      }
      if (generation === null) generation = body.generation ?? null;
      else if (generation !== body.generation) coherent = false;
      if (index >= 5) durations.push(performance.now() - requestStarted);
    }
    const sqliteWritesDuringRefresh = totalChanges() - writesBefore;
    const after = buffer.projection.workCounters();
    const ordered = [...durations].sort((a, b) => a - b);
    const warmP95 = ordered[Math.ceil(ordered.length * 0.95) - 1] ?? Number.POSITIVE_INFINITY;
    const counters = emptyWorkCounters();
    counters.rawRowsScanned =
      after.rawRowsScannedByDashboard - before.rawRowsScannedByDashboard;
    counters.projectionRowsVisited =
      after.snapshotRowsVisited - before.snapshotRowsVisited;
    counters.filesystemEntriesScanned =
      after.filesystemEntriesScannedByDashboard - before.filesystemEntriesScannedByDashboard;
    const passed =
      coherent &&
      generation !== null &&
      counters.rawRowsScanned === 0 &&
      counters.filesystemEntriesScanned === 0 &&
      after.snapshotBuilds === buildsBefore &&
      sqliteWritesDuringRefresh === 0 &&
      durations.length > 0 &&
      warmP95 <= 500;
    return {
      id: "dashboard_projection_budget",
      required: true,
      status: passed ? "pass" : "fail",
      detail: passed
        ? "One coherent production snapshot served all five dashboard surfaces; twenty warm refreshes performed zero raw/filesystem scans and no snapshot rebuild."
        : "Dashboard snapshot coherence, deterministic no-scan counters, unchanged-refresh build count, or warm refresh diagnostics failed the production gate.",
      durationMs: Math.round((performance.now() - started) * 100) / 100,
      counters,
      measurements: {
        generation: generation ?? -1,
        coherent,
        warmRequests: durations.length,
        warmP95Ms: Math.round(warmP95 * 100) / 100,
        snapshotBuildsDuringRefresh: after.snapshotBuilds - buildsBefore,
        sqliteWritesDuringRefresh,
        snapshotCacheHits: after.snapshotCacheHits - before.snapshotCacheHits,
      },
    };
  } finally {
    if (server.listening) {
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
    buffer.close();
  }
}

function runIntegratedWorker(
  sandbox: ResourceSandbox,
  mode: "integrated" | "privacy",
  operatorHome: string,
) {
  const started = performance.now();
  const tsxLoader = path.join(repoRoot, "node_modules", "tsx", "dist", "loader.mjs");
  const worker = path.join(repoRoot, "scripts", "resource-proof", "integrated-worker.ts");
  const workerRoot = path.join(sandbox.root, `worker-${mode}`);
  fs.mkdirSync(workerRoot, { recursive: true, mode: 0o700 });
  const result = spawnSync(
    process.execPath,
    [
      "--import",
      tsxLoader,
      worker,
      "--scenario",
      mode,
      "--root",
      workerRoot,
      "--operator-home",
      operatorHome,
    ],
    {
      cwd: repoRoot,
      env: buildAllowlistedChildEnvironment(sandbox),
      encoding: "utf8",
      timeout: 120_000,
      maxBuffer: 4 * 1024 * 1024,
    },
  );
  const stdout = result.stdout ?? "";
  const stderr = result.stderr ?? "";
  const childLeakCount = metadataPrivacyTerms(operatorHome).filter(
    (term) => term && (stdout.includes(term) || stderr.includes(term)),
  ).length;
  let parsed: IntegratedWorkerResult | undefined;
  try {
    parsed = JSON.parse(stdout.trim()) as IntegratedWorkerResult;
  } catch {
    parsed = undefined;
  }
  const workerShapeValid = Boolean(
    parsed &&
      parsed.schema === "plimsoll.resource-proof.integrated-worker.v1" &&
      parsed.scenario === mode &&
      typeof parsed.checks === "object" &&
      typeof parsed.counters === "object" &&
      typeof parsed.measurements === "object",
  );
  const childNode22 = parsed?.measurements?.nodeMajor === 22;
  const passed = Boolean(
    result.status === 0 &&
      !result.error &&
      childLeakCount === 0 &&
      workerShapeValid &&
      childNode22 &&
      parsed?.passed,
  );
  return { started, result, parsed, childLeakCount, workerShapeValid, childNode22, passed };
}

export function runIntegratedCaptureProjectionOutboxContract(
  sandbox: ResourceSandbox,
  operatorHome: string,
): ScenarioReceipt {
  const execution = runIntegratedWorker(sandbox, "integrated", operatorHome);
  const counters = emptyWorkCounters();
  const observed = execution.parsed?.counters;
  if (observed) {
    counters.eventsObserved = observed.eventsObserved;
    counters.eventsAdmitted = observed.eventsAdmitted;
    counters.eventsDropped = observed.eventsDropped;
    counters.rawEventWrites = observed.rawEventWrites;
    counters.projectionRowsWritten = observed.projectionRowsWritten;
    counters.outboxRowsEnqueued = observed.outboxRowsEnqueued;
  }
  return {
    id: "integrated_capture_projection_outbox",
    required: true,
    status: execution.passed ? "pass" : "fail",
    detail: execution.passed
      ? "A minimal Node 22 child proved loopback OTLP admission, atomic raw/projection/outbox capture, coherent snapshot, reopen, duplicate and rollback safety, and one acknowledgement through an injected upload transport with an exact loopback target."
      : "The isolated capture/projection/outbox worker failed one or more required production-boundary assertions; child content is omitted.",
    durationMs: Math.round((performance.now() - execution.started) * 100) / 100,
    counters,
    measurements: {
      childExitCode: execution.result.status,
      childTimedOut: execution.result.signal === "SIGTERM" && Boolean(execution.result.error),
      childOutputPrivacyLeaks: execution.childLeakCount,
      workerShapeValid: execution.workerShapeValid,
      childNode22: execution.childNode22,
      ...(execution.parsed?.measurements ?? {}),
    },
  };
}

export function runMetadataPrivacySentinelsContract(
  sandbox: ResourceSandbox,
  operatorHome: string,
): ScenarioReceipt {
  const execution = runIntegratedWorker(sandbox, "privacy", operatorHome);
  const counters = emptyWorkCounters();
  const observed = execution.parsed?.counters;
  if (observed) {
    counters.eventsObserved = observed.eventsObserved;
    counters.eventsAdmitted = observed.eventsAdmitted;
    counters.eventsDropped = observed.eventsDropped;
  }
  return {
    id: "metadata_privacy_sentinels",
    required: true,
    status: execution.passed ? "pass" : "fail",
    detail: execution.passed
      ? "Full and prefix sentinels plus the operator-home path were absent from live SQLite text, open database/WAL/SHM byte copies scanned after close, surviving closed artifacts, upload bytes, request logs/responses, child output, and the worker receipt."
      : "The isolated metadata privacy worker failed a required boundary or detected a private-term leak; child content is omitted.",
    durationMs: Math.round((performance.now() - execution.started) * 100) / 100,
    counters,
    measurements: {
      childExitCode: execution.result.status,
      childTimedOut: execution.result.signal === "SIGTERM" && Boolean(execution.result.error),
      childOutputPrivacyLeaks: execution.childLeakCount,
      workerShapeValid: execution.workerShapeValid,
      childNode22: execution.childNode22,
      ...(execution.parsed?.measurements ?? {}),
    },
  };
}

type LearningFactsProofResult = {
  schema: "plimsoll.learning-facts-proof.v1";
  passed: boolean;
  checks: number;
  measurements: {
    learningFactRowsWritten: number;
    privacyLeaks: number;
    uploadedFactRows: number;
    nodeMajor: number;
  };
  liveStateTouched: false;
  providerNetworkCalled: false;
  backgroundScansStarted: false;
  llmCalled: false;
};

export function runLearningFactPrivacyAndResourceContract(
  sandbox: ResourceSandbox,
  operatorHome: string,
): ScenarioReceipt {
  const started = performance.now();
  const tsxLoader = path.join(repoRoot, "node_modules", "tsx", "dist", "loader.mjs");
  const proof = path.join(repoRoot, "scripts", "learning-facts-proof.ts");
  const result = spawnSync(process.execPath, ["--import", tsxLoader, proof], {
    cwd: repoRoot,
    env: buildAllowlistedChildEnvironment(sandbox),
    encoding: "utf8",
    timeout: 120_000,
    maxBuffer: 2 * 1024 * 1024,
  });
  const stdout = result.stdout ?? "";
  const stderr = result.stderr ?? "";
  const outputPrivacyLeaks = metadataPrivacyTerms(operatorHome).filter(
    (term) => term && (stdout.includes(term) || stderr.includes(term)),
  ).length;
  let parsed: LearningFactsProofResult | undefined;
  try {
    parsed = JSON.parse(stdout.trim()) as LearningFactsProofResult;
  } catch {
    parsed = undefined;
  }
  const shapeValid = Boolean(
    parsed &&
      parsed.schema === "plimsoll.learning-facts-proof.v1" &&
      typeof parsed.checks === "number" &&
      typeof parsed.measurements?.learningFactRowsWritten === "number",
  );
  const passed = Boolean(
    result.status === 0 &&
      !result.error &&
      outputPrivacyLeaks === 0 &&
      shapeValid &&
      parsed?.passed &&
      parsed.measurements.nodeMajor === 22 &&
      parsed.measurements.learningFactRowsWritten > 0 &&
      parsed.measurements.privacyLeaks === 0 &&
      parsed.measurements.uploadedFactRows === 0 &&
      parsed.liveStateTouched === false &&
      parsed.providerNetworkCalled === false &&
      parsed.backgroundScansStarted === false &&
      parsed.llmCalled === false,
  );
  const counters = emptyWorkCounters();
  counters.learningFactRowsWritten = parsed?.measurements?.learningFactRowsWritten ?? 0;
  return {
    id: "learning_fact_privacy_and_resource_bounds",
    required: true,
    status: passed ? "pass" : "fail",
    detail: passed
      ? "A Node 22 child proved bounded indexed attempt/episode/exposure facts, paired retry semantics, explicit-only technique exposure, zero private sentinel persistence/upload, and no background scan or LLM path."
      : "The isolated learning-fact contract failed; child content is omitted.",
    durationMs: Math.round((performance.now() - started) * 100) / 100,
    counters,
    measurements: {
      childExitCode: result.status,
      childTimedOut: result.signal === "SIGTERM" && Boolean(result.error),
      childOutputPrivacyLeaks: outputPrivacyLeaks,
      proofShapeValid: shapeValid,
      proofChecks: parsed?.checks ?? 0,
      privacyLeaks: parsed?.measurements?.privacyLeaks ?? -1,
      uploadedFactRows: parsed?.measurements?.uploadedFactRows ?? -1,
      nodeMajor: parsed?.measurements?.nodeMajor ?? -1,
      backgroundScansStarted: parsed?.backgroundScansStarted ?? true,
      llmCalled: parsed?.llmCalled ?? true,
    },
  };
}

type CapturedChild = {
  child: ChildProcessByStdio<null, Readable, Readable>;
  output: { stdout: string; stderr: string };
  active: Promise<Record<string, unknown> | null>;
  exit: Promise<{ code: number | null; signal: NodeJS.Signals | null }>;
};

async function assignLoopbackPort() {
  const reservation = await holdLoopbackPort();
  const port = reservation.port;
  await new Promise<void>((resolve) => reservation.server.close(() => resolve()));
  return port;
}

function buildFailure(code: string): never {
  throw new PackagedCollectorBuildError(code);
}

function requireRegularFile(candidate: string, unavailableCode: string) {
  let stat: fs.Stats;
  try {
    stat = fs.lstatSync(candidate);
  } catch {
    buildFailure(unavailableCode);
  }
  if (!stat.isFile() || stat.isSymbolicLink()) buildFailure(unavailableCode);
}

/**
 * Validate the exact esbuild program before any packaged collector process is
 * eligible to start. The optional expectations are a proof-only seam: the
 * production resource scenario always uses the pinned repository path/hash.
 */
export function validatePackagedCollectorBuilder(
  builderPath: string,
  expectedBuilderPath: string,
  expectedBuilderSha256: string,
) {
  if (path.resolve(builderPath) !== path.resolve(expectedBuilderPath)) {
    buildFailure("PackagedCollectorBuilderPathInvalid");
  }
  requireRegularFile(builderPath, "PackagedCollectorBuilderUnavailable");
  let digest: string;
  try {
    digest = createHash("sha256").update(fs.readFileSync(builderPath)).digest("hex");
  } catch {
    buildFailure("PackagedCollectorBuilderUnavailable");
  }
  if (digest !== expectedBuilderSha256) {
    buildFailure("PackagedCollectorBuilderIntegrityInvalid");
  }
  return { digest, exactPath: true };
}

function resolvePackagedCollectorNativeBinary(builderPath: string) {
  const platformKey = `${process.platform}-${process.arch}`;
  const expectedDigest = PACKAGED_COLLECTOR_NATIVE_ESBUILD_SHA256[platformKey];
  if (!expectedDigest) buildFailure("PackagedCollectorNativePlatformUnsupported");
  let resolved: string;
  try {
    const builderRequire = createRequire(fs.realpathSync(builderPath));
    resolved = builderRequire.resolve(`@esbuild/${platformKey}/bin/esbuild`);
  } catch {
    buildFailure("PackagedCollectorNativeBinaryUnavailable");
  }
  return { path: resolved, expectedDigest, platformKey };
}

export function validatePackagedCollectorNativeBinary(
  nativeBinaryPath: string,
  expectedNativeBinaryPath: string,
  expectedNativeBinarySha256: string,
) {
  if (path.resolve(nativeBinaryPath) !== path.resolve(expectedNativeBinaryPath)) {
    buildFailure("PackagedCollectorNativeBinaryPathInvalid");
  }
  requireRegularFile(nativeBinaryPath, "PackagedCollectorNativeBinaryUnavailable");
  let digest: string;
  try {
    digest = createHash("sha256").update(fs.readFileSync(nativeBinaryPath)).digest("hex");
  } catch {
    buildFailure("PackagedCollectorNativeBinaryUnavailable");
  }
  if (digest !== expectedNativeBinarySha256) {
    buildFailure("PackagedCollectorNativeBinaryIntegrityInvalid");
  }
  return { digest, exactPath: true };
}

function requireRegularDirectory(candidate: string, errorCode: string) {
  let stat: fs.Stats;
  try {
    stat = fs.lstatSync(candidate);
  } catch {
    buildFailure(errorCode);
  }
  if (!stat.isDirectory() || stat.isSymbolicLink()) buildFailure(errorCode);
}

function resourceFixtureManifest(root: string) {
  const entries = new Map<string, string>();
  const visit = (current: string) => {
    for (const entry of fs
      .readdirSync(current, { withFileTypes: true })
      .sort((left, right) => left.name.localeCompare(right.name))) {
      const full = path.join(current, entry.name);
      const relative = path.relative(root, full);
      const stat = fs.lstatSync(full);
      if (entry.isDirectory()) {
        entries.set(relative, `directory:${stat.mode & 0o777}`);
        visit(full);
      } else if (entry.isFile()) {
        const digest = createHash("sha256").update(fs.readFileSync(full)).digest("hex");
        entries.set(relative, `file:${stat.mode & 0o777}:${stat.size}:${digest}`);
      } else if (entry.isSymbolicLink()) {
        entries.set(relative, `symlink:${fs.readlinkSync(full)}`);
      } else {
        entries.set(relative, `other:${stat.mode & 0o777}:${stat.size}`);
      }
    }
  };
  visit(root);
  return entries;
}

function manifestHasExactAdditions(
  before: ReadonlyMap<string, string>,
  after: ReadonlyMap<string, string>,
  expectedAdditions: readonly string[],
) {
  const expected = new Set(expectedAdditions);
  if (after.size !== before.size + expected.size) return false;
  for (const [name, fingerprint] of before) {
    if (after.get(name) !== fingerprint) return false;
  }
  for (const name of expected) {
    if (!after.has(name) || before.has(name)) return false;
  }
  return true;
}

const PACKAGED_RUNTIME_DEPENDENCY_VERSIONS = {
  "better-sqlite3": "12.10.0",
  bindings: "1.5.0",
  "file-uri-to-path": "1.0.0",
} as const;

function canonicalFrozenRuntimeDependencyGraphRoot() {
  try {
    const canonicalRoot = fs.realpathSync.native(
      path.join(repoRoot, "node_modules"),
    );
    const rootStat = fs.lstatSync(canonicalRoot);
    if (!rootStat.isDirectory() || rootStat.isSymbolicLink()) {
      buildFailure("PackagedCollectorRuntimeDependencyGraphInvalid");
    }
    return canonicalRoot;
  } catch (error) {
    if (error instanceof PackagedCollectorBuildError) throw error;
    buildFailure("PackagedCollectorRuntimeDependencyGraphInvalid");
  }
}

function resolveFrozenPackageRoot(
  resolver: NodeRequire,
  packageName: keyof typeof PACKAGED_RUNTIME_DEPENDENCY_VERSIONS,
  dependencyGraphRoot: string,
) {
  let entry: string;
  try {
    entry = fs.realpathSync.native(resolver.resolve(packageName));
  } catch {
    buildFailure("PackagedCollectorRuntimeDependencyUnavailable");
  }
  const entryStat = fs.lstatSync(entry);
  if (
    !within(dependencyGraphRoot, entry) ||
    !entryStat.isFile() ||
    entryStat.isSymbolicLink()
  ) {
    buildFailure("PackagedCollectorRuntimeDependencyInvalid");
  }
  let current = path.dirname(entry);
  for (;;) {
    if (!within(dependencyGraphRoot, current)) {
      buildFailure("PackagedCollectorRuntimeDependencyInvalid");
    }
    const packageJsonPath = path.join(current, "package.json");
    try {
      const stat = fs.lstatSync(packageJsonPath);
      if (!stat.isFile() || stat.isSymbolicLink()) {
        buildFailure("PackagedCollectorRuntimeDependencyInvalid");
      }
      const parsed = JSON.parse(fs.readFileSync(packageJsonPath, "utf8")) as {
        name?: unknown;
        version?: unknown;
      };
      if (parsed.name === packageName) {
        if (
          typeof parsed.version !== "string" ||
          parsed.version !== PACKAGED_RUNTIME_DEPENDENCY_VERSIONS[packageName]
        ) {
          buildFailure("PackagedCollectorRuntimeDependencyVersionInvalid");
        }
        const root = fs.realpathSync.native(current);
        const rootStat = fs.lstatSync(root);
        if (
          !within(dependencyGraphRoot, root) ||
          !within(root, entry) ||
          !rootStat.isDirectory() ||
          rootStat.isSymbolicLink()
        ) {
          buildFailure("PackagedCollectorRuntimeDependencyInvalid");
        }
        return { entry, root, packageJsonPath, version: parsed.version };
      }
    } catch (error) {
      if (error instanceof PackagedCollectorBuildError) throw error;
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") {
        buildFailure("PackagedCollectorRuntimeDependencyInvalid");
      }
    }
    const parent = path.dirname(current);
    if (parent === current) {
      buildFailure("PackagedCollectorRuntimeDependencyUnavailable");
    }
    current = parent;
  }
}

function collectAllowlistedRuntimeTree(root: string) {
  const stat = fs.lstatSync(root);
  if (!stat.isDirectory() || stat.isSymbolicLink()) {
    buildFailure("PackagedCollectorRuntimeDependencyInvalid");
  }
  const files: string[] = [];
  const visit = (current: string) => {
    for (const entry of fs
      .readdirSync(current, { withFileTypes: true })
      .sort((left, right) => left.name.localeCompare(right.name))) {
      const full = path.join(current, entry.name);
      const observed = fs.lstatSync(full);
      if (entry.isDirectory()) {
        if (!observed.isDirectory() || observed.isSymbolicLink()) {
          buildFailure("PackagedCollectorRuntimeDependencyInvalid");
        }
        visit(full);
      } else if (entry.isFile()) {
        if (!observed.isFile() || observed.isSymbolicLink()) {
          buildFailure("PackagedCollectorRuntimeDependencyInvalid");
        }
        files.push(path.relative(root, full));
      } else {
        buildFailure("PackagedCollectorRuntimeDependencyInvalid");
      }
    }
  };
  visit(root);
  return files;
}

function manifestDigest(manifest: ReadonlyMap<string, string>) {
  const hash = createHash("sha256");
  for (const [name, fingerprint] of [...manifest].sort(([left], [right]) =>
    left.localeCompare(right),
  )) {
    hash.update(`${name}\0${fingerprint}\0`);
  }
  return hash.digest("hex");
}

function runtimeDependencyManifest(root: string) {
  return new Map(
    [...resourceFixtureManifest(root)].filter(
      ([name]) => name === "node_modules" || name.startsWith(`node_modules${path.sep}`),
    ),
  );
}

function sameManifest(
  expected: ReadonlyMap<string, string>,
  observed: ReadonlyMap<string, string>,
) {
  return (
    expected.size === observed.size &&
    [...expected].every(
      ([name, fingerprint]) => observed.get(name) === fingerprint,
    )
  );
}

function stagePackagedRuntimeDependencies(stagedOutputDirectory: string) {
  const dependencyGraphRoot = canonicalFrozenRuntimeDependencyGraphRoot();
  const repositoryRequire = createRequire(path.join(repoRoot, "package.json"));
  const betterSqlite = resolveFrozenPackageRoot(
    repositoryRequire,
    "better-sqlite3",
    dependencyGraphRoot,
  );
  const betterSqliteRequire = createRequire(betterSqlite.entry);
  const bindings = resolveFrozenPackageRoot(
    betterSqliteRequire,
    "bindings",
    dependencyGraphRoot,
  );
  const bindingsRequire = createRequire(bindings.entry);
  const fileUriToPath = resolveFrozenPackageRoot(
    bindingsRequire,
    "file-uri-to-path",
    dependencyGraphRoot,
  );
  const copies: Array<{ source: string; relative: string; mode: number }> = [];
  const addFile = (
    packageName: keyof typeof PACKAGED_RUNTIME_DEPENDENCY_VERSIONS,
    packageRoot: string,
    packageRelative: string,
    mode = 0o644,
  ) => {
    copies.push({
      source: path.join(packageRoot, packageRelative),
      relative: path.join("node_modules", packageName, packageRelative),
      mode,
    });
  };

  for (const packageRelative of ["package.json", "LICENSE"]) {
    addFile("better-sqlite3", betterSqlite.root, packageRelative);
  }
  for (const libraryRelative of collectAllowlistedRuntimeTree(
    path.join(betterSqlite.root, "lib"),
  )) {
    addFile(
      "better-sqlite3",
      betterSqlite.root,
      path.join("lib", libraryRelative),
    );
  }
  addFile(
    "better-sqlite3",
    betterSqlite.root,
    path.join("build", "Release", "better_sqlite3.node"),
    0o755,
  );
  for (const packageRelative of ["package.json", "LICENSE.md", "bindings.js"]) {
    addFile("bindings", bindings.root, packageRelative);
  }
  for (const packageRelative of ["package.json", "LICENSE", "index.js"]) {
    addFile("file-uri-to-path", fileUriToPath.root, packageRelative);
  }

  for (const copy of copies.sort((left, right) =>
    left.relative.localeCompare(right.relative),
  )) {
    let sourceStat: fs.Stats;
    try {
      sourceStat = fs.lstatSync(copy.source);
    } catch {
      buildFailure("PackagedCollectorRuntimeDependencyUnavailable");
    }
    if (!sourceStat.isFile() || sourceStat.isSymbolicLink()) {
      buildFailure("PackagedCollectorRuntimeDependencyInvalid");
    }
    const destination = path.join(stagedOutputDirectory, copy.relative);
    fs.mkdirSync(path.dirname(destination), { recursive: true, mode: 0o755 });
    try {
      fs.copyFileSync(copy.source, destination, fs.constants.COPYFILE_EXCL);
      fs.chmodSync(destination, copy.mode);
    } catch {
      buildFailure("PackagedCollectorRuntimeDependencyCopyFailed");
    }
    const destinationStat = fs.lstatSync(destination);
    if (
      !destinationStat.isFile() ||
      destinationStat.isSymbolicLink() ||
      (destinationStat.mode & 0o777) !== copy.mode
    ) {
      buildFailure("PackagedCollectorRuntimeDependencyCopyInvalid");
    }
  }

  const expectedManifest = runtimeDependencyManifest(stagedOutputDirectory);
  return {
    expectedManifest,
    digest: manifestDigest(expectedManifest),
    fileCount: copies.length,
    versions: {
      betterSqlite: betterSqlite.version,
      bindings: bindings.version,
      fileUriToPath: fileUriToPath.version,
    },
  };
}

export function buildPackagedCollectorCli(
  sandbox: ResourceSandbox,
  options: PackagedCollectorBuildOptions = {},
) {
  const packageDirectory = path.join(repoRoot, "packages", "collector-cli");
  const sourcePath = path.join(packageDirectory, "src", "cli.ts");
  const dashboardSourcePath = path.join(packageDirectory, "src", "dashboard.html");
  const outputDirectory =
    options.outputDirectory ??
    path.join(sandbox.root, `packaged-collector-${randomUUID()}`);
  const usedDefaultSandboxOutput =
    options.outputDirectory === undefined && within(sandbox.root, outputDirectory);
  const repositoryBuilderPath = path.join(
    repoRoot,
    "node_modules",
    "esbuild",
    "bin",
    "esbuild",
  );
  const builderPath = options.builderPath ?? repositoryBuilderPath;
  const expectedBuilderPath = options.expectedBuilderPath ?? repositoryBuilderPath;
  const expectedBuilderSha256 =
    options.expectedBuilderSha256 ?? PACKAGED_COLLECTOR_ESBUILD_SHA256;
  const validatedBuilder = validatePackagedCollectorBuilder(
    builderPath,
    expectedBuilderPath,
    expectedBuilderSha256,
  );
  const resolvedNative = resolvePackagedCollectorNativeBinary(builderPath);
  const nativeBinaryPath = options.nativeBinaryPath ?? resolvedNative.path;
  const expectedNativeBinaryPath =
    options.expectedNativeBinaryPath ?? resolvedNative.path;
  const expectedNativeBinarySha256 =
    options.expectedNativeBinarySha256 ?? resolvedNative.expectedDigest;
  const validatedNativeBinary = validatePackagedCollectorNativeBinary(
    nativeBinaryPath,
    expectedNativeBinaryPath,
    expectedNativeBinarySha256,
  );
  requireRegularFile(sourcePath, "PackagedCollectorSourceUnavailable");
  requireRegularFile(dashboardSourcePath, "PackagedCollectorDashboardUnavailable");

  const suffix = `${process.pid}-${randomUUID()}`;
  const outputParent = path.dirname(outputDirectory);
  const outputName = path.basename(outputDirectory);
  const stagedOutputDirectory = path.join(outputParent, `.${outputName}.stage-${suffix}`);
  const backupOutputDirectory = path.join(outputParent, `.${outputName}.backup-${suffix}`);
  const failedOutputDirectory = path.join(outputParent, `.${outputName}.failed-${suffix}`);
  const stagedCliPath = path.join(stagedOutputDirectory, "cli.mjs");
  const stagedDashboardPath = path.join(stagedOutputDirectory, "dashboard.html");
  const fixtureManifestBeforeBuild = resourceFixtureManifest(sandbox.root);
  fs.mkdirSync(outputParent, { recursive: true, mode: 0o755 });
  requireRegularDirectory(outputParent, "PackagedCollectorOutputParentInvalid");
  if (fs.existsSync(outputDirectory)) {
    requireRegularDirectory(outputDirectory, "PackagedCollectorOutputDirectoryInvalid");
  }
  fs.mkdirSync(stagedOutputDirectory, { mode: 0o755 });
  const buildEnvironment = buildAllowlistedChildEnvironment(sandbox, {});
  const homeEntryCountBeforeBuild = fs.readdirSync(sandbox.home).length;
  const buildArguments = [
    builderPath,
    sourcePath,
    "--bundle",
    "--platform=node",
    "--target=node20",
    "--format=esm",
    `--outfile=${stagedCliPath}`,
    "--external:better-sqlite3",
  ];
  const build = spawnSync(
    process.execPath,
    buildArguments,
    {
      cwd: repoRoot,
      env: buildEnvironment,
      encoding: "utf8",
      timeout: 120_000,
      maxBuffer: 16 * 1024 * 1024,
    },
  );
  let backupMoved = false;
  let publicationCompleted = false;
  let stagedRuntimeDependencies:
    | ReturnType<typeof stagePackagedRuntimeDependencies>
    | undefined;
  try {
    if (build.status !== 0 || build.error) {
      buildFailure("PackagedCollectorBuildFailed");
    }
    requireRegularFile(stagedCliPath, "PackagedCollectorBuildOutputInvalid");
    fs.chmodSync(stagedCliPath, 0o755);
    fs.copyFileSync(dashboardSourcePath, stagedDashboardPath, fs.constants.COPYFILE_EXCL);
    requireRegularFile(stagedDashboardPath, "PackagedCollectorDashboardCopyInvalid");
    fs.chmodSync(stagedDashboardPath, 0o644);
    stagedRuntimeDependencies =
      stagePackagedRuntimeDependencies(stagedOutputDirectory);

    if (fs.existsSync(outputDirectory)) {
      fs.renameSync(outputDirectory, backupOutputDirectory);
      backupMoved = true;
    }
    if (options.injectPublicationFailureAfterBackup) {
      buildFailure("PackagedCollectorPublicationInjectedFailure");
    }
    fs.renameSync(stagedOutputDirectory, outputDirectory);
    publicationCompleted = true;
  } catch (error) {
    try {
      if (publicationCompleted && fs.existsSync(outputDirectory)) {
        fs.renameSync(outputDirectory, failedOutputDirectory);
      }
      if (backupMoved && fs.existsSync(backupOutputDirectory)) {
        fs.renameSync(backupOutputDirectory, outputDirectory);
        backupMoved = false;
      }
    } catch {
      buildFailure("PackagedCollectorPublicationRollbackFailed");
    } finally {
      for (const disposable of [stagedOutputDirectory, failedOutputDirectory]) {
        try {
          fs.rmSync(disposable, { recursive: true, force: true });
        } catch {
          // Preserve the original path-free publication failure.
        }
      }
    }
    if (error instanceof PackagedCollectorBuildError) throw error;
    buildFailure("PackagedCollectorPublishFailed");
  }
  if (backupMoved) {
    try {
      fs.rmSync(backupOutputDirectory, { recursive: true, force: true });
      backupMoved = false;
    } catch {
      // The new complete directory is already authoritative; a stale uniquely
      // named backup cannot be observed as the packaged collector path.
    }
  }

  for (const disposable of [stagedOutputDirectory, failedOutputDirectory]) {
    if (!fs.existsSync(disposable)) continue;
    try {
      fs.rmSync(disposable, { recursive: true, force: true });
    } catch {
      buildFailure("PackagedCollectorStagingCleanupFailed");
    }
  }

  const cliPath = path.join(outputDirectory, "cli.mjs");
  const dashboardPath = path.join(outputDirectory, "dashboard.html");
  try {
    requireRegularDirectory(outputDirectory, "PackagedCollectorOutputDirectoryInvalid");
  } catch (error) {
    for (const disposable of [stagedOutputDirectory, failedOutputDirectory]) {
      try {
        fs.rmSync(disposable, { recursive: true, force: true });
      } catch {
        // Preserve the original symbolic validation failure.
      }
    }
    throw error;
  }
  const canonicalOutputDirectory = fs.realpathSync.native(outputDirectory);
  const exactPackagePath =
    path.resolve(cliPath) === path.resolve(outputDirectory, "cli.mjs") &&
    fs.realpathSync.native(cliPath) ===
      path.join(canonicalOutputDirectory, "cli.mjs");
  const executable = (fs.statSync(cliPath).mode & 0o111) !== 0;
  const dashboardExactPackagePath =
    path.resolve(dashboardPath) === path.resolve(outputDirectory, "dashboard.html") &&
    fs.realpathSync.native(dashboardPath) ===
      path.join(canonicalOutputDirectory, "dashboard.html");
  const dashboardMode = fs.statSync(dashboardPath).mode & 0o777;
  const dashboardCopiedExactly =
    createHash("sha256").update(fs.readFileSync(dashboardSourcePath)).digest("hex") ===
    createHash("sha256").update(fs.readFileSync(dashboardPath)).digest("hex");
  if (!stagedRuntimeDependencies) {
    buildFailure("PackagedCollectorRuntimeDependencyUnavailable");
  }
  const observedRuntimeDependencyManifest =
    runtimeDependencyManifest(outputDirectory);
  const runtimeDependencyManifestExact = sameManifest(
    stagedRuntimeDependencies.expectedManifest,
    observedRuntimeDependencyManifest,
  );
  const runtimeDependencyTreeDigest = manifestDigest(
    observedRuntimeDependencyManifest,
  );
  if (
    !exactPackagePath ||
    !executable ||
    !dashboardExactPackagePath ||
    dashboardMode !== 0o644 ||
    !dashboardCopiedExactly ||
    !runtimeDependencyManifestExact ||
    runtimeDependencyTreeDigest !== stagedRuntimeDependencies.digest
  ) {
    buildFailure("PackagedCollectorPathInvalid");
  }
  const fixtureManifestAfterBuild = resourceFixtureManifest(sandbox.root);
  const outputRelative = path.relative(sandbox.root, outputDirectory);
  const fixtureManifestDeltaExact = usedDefaultSandboxOutput
    ? manifestHasExactAdditions(
        fixtureManifestBeforeBuild,
        fixtureManifestAfterBuild,
        [
          outputRelative,
          path.join(outputRelative, "cli.mjs"),
          path.join(outputRelative, "dashboard.html"),
          ...[...stagedRuntimeDependencies.expectedManifest.keys()].map((name) =>
            path.join(outputRelative, name),
          ),
        ],
      )
    : fixtureManifestBeforeBuild.size === fixtureManifestAfterBuild.size &&
      [...fixtureManifestBeforeBuild].every(
        ([name, fingerprint]) =>
          fixtureManifestAfterBuild.get(name) === fingerprint,
      );
  const homeEntryCountAfterBuild = fs.readdirSync(sandbox.home).length;
  return {
    cliPath,
    buildExitCode: build.status,
    exactPackagePath,
    executable,
    dashboardExactPackagePath,
    dashboardMode,
    dashboardCopiedExactly,
    runtimeDependencyManifestExact,
    runtimeDependencyTreeDigest,
    runtimeDependencyManifestEntryCount:
      observedRuntimeDependencyManifest.size,
    runtimeDependencyFileCount: stagedRuntimeDependencies.fileCount,
    runtimeDependencyVersions: stagedRuntimeDependencies.versions,
    exactNodeExecutable: process.execPath,
    exactBuilderPath: path.resolve(builderPath) === path.resolve(repositoryBuilderPath),
    builderDigest: validatedBuilder.digest,
    nativeBinaryPlatform: resolvedNative.platformKey,
    nativeBinaryDigest: validatedNativeBinary.digest,
    expectedNativeBinaryDigest: expectedNativeBinarySha256,
    usedDefaultSandboxOutput,
    fixtureManifestEntryCountBeforeBuild: fixtureManifestBeforeBuild.size,
    fixtureManifestEntryCountAfterBuild: fixtureManifestAfterBuild.size,
    fixtureManifestDeltaExact,
    homeEntryCountBeforeBuild,
    homeEntryCountAfterBuild,
    buildPathWasUnset: buildEnvironment.PATH === undefined,
    buildEnvironmentKeyCount: Object.keys(buildEnvironment).length,
    buildInvocationExecutableBasename: path.basename(process.execPath),
    buildInvocationArgumentCount: buildArguments.length,
    buildInvocationArgumentZeroMatchedValidatedBuilder:
      path.resolve(buildArguments[0]!) === path.resolve(builderPath),
  };
}

function spawnCollectorCli(
  sandbox: ResourceSandbox,
  packagedCliPath: string,
  command: "start" | "stop",
): CapturedChild {
  const child = spawn(process.execPath, [packagedCliPath, command], {
    cwd: repoRoot,
    env: buildAllowlistedChildEnvironment(sandbox),
    stdio: ["ignore", "pipe", "pipe"],
  });
  const output = { stdout: "", stderr: "" };
  let resolveActive!: (body: Record<string, unknown> | null) => void;
  let activeSettled = false;
  const active = new Promise<Record<string, unknown> | null>((resolve) => {
    resolveActive = resolve;
  });
  child.stdout.setEncoding("utf8");
  child.stderr.setEncoding("utf8");
  child.stdout.on("data", (chunk: string) => {
    output.stdout = (output.stdout + chunk).slice(-128 * 1024);
    for (const line of output.stdout.split("\n")) {
      if (!line.startsWith('{"status":"active"')) continue;
      try {
        const body = JSON.parse(line) as Record<string, unknown>;
        if (!activeSettled) {
          activeSettled = true;
          resolveActive(body);
        }
      } catch {
        // A complete active record is one line; wait for the next data event.
      }
    }
  });
  child.stderr.on("data", (chunk: string) => {
    output.stderr = (output.stderr + chunk).slice(-128 * 1024);
  });
  const exit = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>(
    (resolve) => {
      // `exit` may precede the final stdout data event. Resolve on `close` so
      // callers never classify a partially delivered JSON receipt.
      child.once("close", (code, signal) => {
        if (!activeSettled) {
          activeSettled = true;
          resolveActive(null);
        }
        resolve({ code, signal });
      });
    },
  );
  return { child, output, active, exit };
}

function parseCapturedJson(output: string) {
  return JSON.parse(output.trim()) as Record<string, unknown>;
}

function processIdIsLive(pid: number) {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

type FixtureChildRole = "candidate_owner" | "candidate_loser" | "stop_command" | "fixture_child";

/**
 * Boundedly reap every fixture child and record one symbolic ReapOutcome per
 * role. Unlike the previous silent swallow, a child that survives SIGKILL is
 * reported as CleanupFailure so residue can never pass unnoticed (#162).
 */
async function reapFixtureChildren(
  children: Array<{ role: FixtureChildRole; handle: CapturedChild }>,
): Promise<Record<string, ReapOutcome>> {
  const outcomes: Record<string, ReapOutcome> = {};
  await Promise.all(
    children.map(async (entry) => {
      outcomes[entry.role] = await reapFixtureChild(entry.handle.child, entry.handle.exit);
    }),
  );
  return outcomes;
}

function chooseActiveCandidate(candidates: CapturedChild[]) {
  return new Promise<{ index: number; body: Record<string, unknown> }>((resolve, reject) => {
    let settled = false;
    let unavailable = 0;
    candidates.forEach((candidate, index) => {
      candidate.active.then((body) => {
        if (settled) return;
        if (body) {
          settled = true;
          resolve({ index, body });
          return;
        }
        unavailable += 1;
        if (unavailable === candidates.length) {
          settled = true;
          reject(new Error("NoActiveCandidate"));
        }
      });
    });
  });
}

/** Real #76 CLI ownership, listener, already-running, status, and stop path. */
export async function runDuplicateStartSingleOwnerContract(
  sandbox: ResourceSandbox,
  buildOptions: PackagedCollectorBuildOptions = {},
): Promise<ScenarioReceipt> {
  const counters = emptyWorkCounters();
  const childEntries: Array<{ role: FixtureChildRole; handle: CapturedChild }> = [];
  let receipt: ScenarioReceipt;
  let reapOutcomes: Record<string, ReapOutcome> = {};
  try {
    receipt = await attemptDuplicateStartSingleOwnerContract(
      sandbox,
      buildOptions,
      counters,
      childEntries,
    );
  } finally {
    // Await and reap every fixture child on every failure path (#162).
    reapOutcomes = await reapFixtureChildren(childEntries);
  }
  const cleanupFailureRoles = Object.entries(reapOutcomes)
    .filter(([, outcome]) => outcome === "CleanupFailure")
    .map(([role]) => role);
  const allChildrenReapedCleanly = cleanupFailureRoles.length === 0;
  receipt.measurements = {
    ...receipt.measurements,
    ...reapOutcomes,
    fixtureChildrenTracked: childEntries.length,
    fixtureChildrenReapedCleanly: allChildrenReapedCleanly,
  };
  if (!allChildrenReapedCleanly && receipt.status === "pass") {
    // Residue must never pass silently: a fixture child that survived
    // SIGKILL fails the scenario regardless of the assertion outcomes.
    receipt.status = "fail";
    receipt.detail =
      "Duplicate-start production contract passed its assertions but left fixture child residue (CleanupFailure); failed closed.";
  }
  return receipt;
}

async function attemptDuplicateStartSingleOwnerContract(
  sandbox: ResourceSandbox,
  buildOptions: PackagedCollectorBuildOptions,
  counters: ReturnType<typeof emptyWorkCounters>,
  childEntries: Array<{ role: FixtureChildRole; handle: CapturedChild }>,
): Promise<ScenarioReceipt> {
  const started = performance.now();
  let lifecycleStage = "build";
  try {
    const packagedCli = buildPackagedCollectorCli(sandbox, buildOptions);
    lifecycleStage = "start_race";
    const port = await assignLoopbackPort();
    fs.writeFileSync(
      path.join(sandbox.plimsollHome, "collector.config.json"),
      `${JSON.stringify({ port })}\n`,
      { mode: 0o600 },
    );
    const pidPath = path.join(sandbox.plimsollHome, "collector.pid");
    const candidates = [
      spawnCollectorCli(sandbox, packagedCli.cliPath, "start"),
      spawnCollectorCli(sandbox, packagedCli.cliPath, "start"),
    ];
    childEntries.push(
      { role: "fixture_child", handle: candidates[0]! },
      { role: "fixture_child", handle: candidates[1]! },
    );
    const candidatesUseExactPackagePath = candidates.every(
      (candidate) =>
        candidate.child.spawnfile === process.execPath &&
        candidate.child.spawnargs[1] === packagedCli.cliPath &&
        candidate.child.spawnargs[2] === "start",
    );
    const ownerChoice = await withSymbolicDeadline(
      chooseActiveCandidate(candidates),
      20_000,
      "CollectorReadinessTimeout",
    );
    const loserIndex = ownerChoice.index === 0 ? 1 : 0;
    childEntries[ownerChoice.index]!.role = "candidate_owner";
    childEntries[loserIndex]!.role = "candidate_loser";
    const owner = candidates[ownerChoice.index]!;
    const loser = candidates[loserIndex]!;
    // #162: an owner that exits before any stop command exists has exited
    // early; record that fact the moment it happens, before stop spawn.
    let ownerExitedBeforeStopSpawned = false;
    let stopSpawned = false;
    void owner.exit.then(() => {
      if (!stopSpawned) ownerExitedBeforeStopSpawned = true;
    });

    lifecycleStage = "owner_identity";
    const ownerRead = readCollectorPidFile(pidPath, LAUNCH_AGENT_LABEL);
    if (ownerRead.kind !== "current") throw new Error("OwnerPidRecordMissing");
    const ownerIdentity: CollectorRuntimeIdentity = {
      instanceId: ownerRead.record.instanceId,
      pid: ownerRead.record.pid,
      processStartFingerprint: ownerRead.record.processStartFingerprint,
      processStartFingerprintAlgorithm:
        ownerRead.record.processStartFingerprintAlgorithm,
    };
    const pidRecordBeforeLoser = ownerRead.raw;
    const loserExit = await withSymbolicDeadline(loser.exit, 20_000, "LoserExitTimeout");
    const loserReceipt = parseCapturedJson(loser.output.stdout);
    const pidRecordAfterLoser = fs.readFileSync(pidPath, "utf8");

    const statusResponse = await withSymbolicDeadline(
      fetch(`http://127.0.0.1:${port}/status`, {
        // Issue 0056 (#104): enforcing daemons gate status behind the
        // provisioned management credential from their own home.
        headers: readLocalIngestAuth(sandbox.plimsollHome)
          ? { "x-plimsoll-token": readLocalIngestAuth(sandbox.plimsollHome)!.managementRead }
          : {},
      }),
      10_000,
      "StatusReadinessTimeout",
    );
    const statusBody = (await statusResponse.json()) as {
      ok?: unknown;
      runtimeIdentity?: CollectorRuntimeIdentity;
    };
    const activeIdentity = ownerChoice.body.runtimeIdentity as
      | CollectorRuntimeIdentity
      | undefined;
    const loserIdentity = loserReceipt.runtimeIdentity as
      | CollectorRuntimeIdentity
      | undefined;
    const activeIdentityMatches = runtimeIdentityMatches(activeIdentity, ownerIdentity);
    const loserIdentityMatches = runtimeIdentityMatches(loserIdentity, ownerIdentity);
    const statusIdentityMatches = runtimeIdentityMatches(
      statusBody.runtimeIdentity,
      ownerIdentity,
    );
    const ownerProcessLive = processIdIsLive(ownerIdentity.pid);
    const ownerChildPidMatches = owner.child.pid === ownerIdentity.pid;
    const identityProved =
      statusResponse.ok &&
      statusBody.ok === true &&
      activeIdentityMatches &&
      loserIdentityMatches &&
      statusIdentityMatches &&
      ownerProcessLive &&
      ownerChildPidMatches;
    const ownerPidRecordUnchanged = pidRecordBeforeLoser === pidRecordAfterLoser;
    const loserHonest =
      loserExit.code === 0 &&
      loserExit.signal === null &&
      loserReceipt.status === "already_running";
    const startLockReleased = !fs.existsSync(`${pidPath}.start.lock`);
    const startOutcomes = [
      {
        accepted:
          ownerChoice.body.status === "active" &&
          activeIdentityMatches &&
          statusIdentityMatches &&
          ownerProcessLive,
        status: ownerChoice.body.status,
      },
      {
        accepted: loserHonest && loserIdentityMatches,
        status: loserReceipt.status,
      },
    ];
    counters.listenersCreated = startOutcomes.filter(
      (outcome) => outcome.status === "active",
    ).length;
    counters.restartRequests = startOutcomes.filter((outcome) => !outcome.accepted).length;
    const counterProvenanceProved =
      startOutcomes.length === 2 &&
      counters.listenersCreated === 1 &&
      counters.restartRequests === 0;

    lifecycleStage = "stop_command";
    stopSpawned = true;
    const stopper = spawnCollectorCli(sandbox, packagedCli.cliPath, "stop");
    childEntries.push({ role: "stop_command", handle: stopper });
    const stopperUsesExactPackagePath =
      stopper.child.spawnfile === process.execPath &&
      stopper.child.spawnargs[1] === packagedCli.cliPath &&
      stopper.child.spawnargs[2] === "stop";
    // #162: the 20 second bounded budgets are preserved verbatim; only the
    // failure handling changes. Timeouts become symbolic classes instead of
    // unclassified throws.
    const stopperObservation = await observeChildExit(stopper.exit, 20_000);
    let stopperReceipt: Record<string, unknown> | null = null;
    if (stopperObservation.settled) {
      try {
        stopperReceipt = parseCapturedJson(stopper.output.stdout);
      } catch {
        stopperReceipt = null;
      }
    }
    lifecycleStage = "owner_shutdown";
    const ownerObservation = await observeChildExit(owner.exit, 20_000);
    const stopClassification = classifyStopCommand({
      stopperSettled: stopperObservation.settled,
      stopperExitCode: stopperObservation.settled ? stopperObservation.code : null,
      stopperSignal: stopperObservation.settled ? stopperObservation.signal : null,
      stopperReceiptParsed: stopperReceipt !== null,
      stopReceiptReportedStopped: stopperReceipt?.stopped === true,
    });
    const shutdownClassification = classifyOwnerShutdown({
      ownerExitedBeforeStopSpawned,
      stopFailed: stopClassification.failed,
      stopFailureReason: stopClassification.failed ? stopClassification.reason : null,
      ownerExitSettled: ownerObservation.settled,
    });
    const stopCommandExitedCleanly =
      stopperObservation.settled &&
      stopperObservation.code === 0 &&
      stopperObservation.signal === null;
    const stopReceiptReportedStopped = stopperReceipt?.stopped === true;
    const ownerExitedCleanly =
      ownerObservation.settled &&
      ownerObservation.code === 0 &&
      ownerObservation.signal === null;
    const stoppedThroughCli =
      stopCommandExitedCleanly && stopReceiptReportedStopped && ownerExitedCleanly;
    const pidRecordRemoved =
      readCollectorPidFile(pidPath, LAUNCH_AGENT_LABEL).kind === "missing";

    const passed =
      packagedCli.buildExitCode === 0 &&
      packagedCli.exactPackagePath &&
      packagedCli.executable &&
      packagedCli.dashboardExactPackagePath &&
      packagedCli.dashboardMode === 0o644 &&
      packagedCli.dashboardCopiedExactly &&
      packagedCli.runtimeDependencyManifestExact &&
      /^[a-f0-9]{64}$/.test(packagedCli.runtimeDependencyTreeDigest) &&
      packagedCli.runtimeDependencyManifestEntryCount > 0 &&
      packagedCli.runtimeDependencyFileCount > 0 &&
      packagedCli.runtimeDependencyVersions.betterSqlite === "12.10.0" &&
      packagedCli.runtimeDependencyVersions.bindings === "1.5.0" &&
      packagedCli.runtimeDependencyVersions.fileUriToPath === "1.0.0" &&
      packagedCli.exactNodeExecutable === process.execPath &&
      packagedCli.exactBuilderPath &&
      packagedCli.nativeBinaryDigest === packagedCli.expectedNativeBinaryDigest &&
      packagedCli.usedDefaultSandboxOutput &&
      packagedCli.fixtureManifestDeltaExact &&
      packagedCli.homeEntryCountBeforeBuild === 0 &&
      packagedCli.homeEntryCountAfterBuild === 0 &&
      packagedCli.buildPathWasUnset &&
      packagedCli.buildInvocationExecutableBasename === path.basename(process.execPath) &&
      packagedCli.buildInvocationArgumentCount > 0 &&
      packagedCli.buildInvocationArgumentZeroMatchedValidatedBuilder &&
      candidatesUseExactPackagePath &&
      stopperUsesExactPackagePath &&
      identityProved &&
      ownerPidRecordUnchanged &&
      loserHonest &&
      startLockReleased &&
      counterProvenanceProved &&
      stoppedThroughCli &&
      pidRecordRemoved &&
      shutdownClassification.failureClass === null;
    return {
      id: "duplicate_start_single_owner",
      required: true,
      status: passed ? "pass" : "fail",
      detail: passed
        ? "Two real packaged CLI starts raced against one temporary home/port: one owner listened, one exited already_running, the owner record stayed unchanged, and the packaged stop path cleaned up that owner."
        : "Duplicate-start production contract failed one or more ownership assertions.",
      durationMs: Math.round((performance.now() - started) * 100) / 100,
      counters,
      measurements: {
        candidatesRaced: 2,
        activeOwners: counters.listenersCreated,
        alreadyRunningCandidates: startOutcomes.filter(
          (outcome) => outcome.status === "already_running",
        ).length,
        packagedCliBuildExitCode: packagedCli.buildExitCode,
        packagedCliExactPath: packagedCli.exactPackagePath,
        packagedCliExecutable: packagedCli.executable,
        packagedDashboardExactPath: packagedCli.dashboardExactPackagePath,
        packagedDashboardMode: packagedCli.dashboardMode,
        packagedDashboardCopiedExactly: packagedCli.dashboardCopiedExactly,
        runtimeDependencyManifestExact:
          packagedCli.runtimeDependencyManifestExact,
        runtimeDependencyTreeDigest:
          packagedCli.runtimeDependencyTreeDigest,
        runtimeDependencyManifestEntryCount:
          packagedCli.runtimeDependencyManifestEntryCount,
        runtimeDependencyFileCount: packagedCli.runtimeDependencyFileCount,
        betterSqliteRuntimeVersion:
          packagedCli.runtimeDependencyVersions.betterSqlite,
        bindingsRuntimeVersion:
          packagedCli.runtimeDependencyVersions.bindings,
        fileUriToPathRuntimeVersion:
          packagedCli.runtimeDependencyVersions.fileUriToPath,
        buildUsedExactNodeExecutable: packagedCli.exactNodeExecutable === process.execPath,
        buildUsedExactRepositoryBuilder: packagedCli.exactBuilderPath,
        nativeBinaryPlatform: packagedCli.nativeBinaryPlatform,
        nativeBinaryDigestMatchedExpected:
          packagedCli.nativeBinaryDigest === packagedCli.expectedNativeBinaryDigest,
        packagedOutputInsideTemporaryFixture:
          packagedCli.usedDefaultSandboxOutput,
        packagedOutputFixtureManifestExact:
          packagedCli.fixtureManifestDeltaExact,
        fixtureManifestEntryCountBeforeBuild:
          packagedCli.fixtureManifestEntryCountBeforeBuild,
        fixtureManifestEntryCountAfterBuild:
          packagedCli.fixtureManifestEntryCountAfterBuild,
        buildHomeEntryCountBefore: packagedCli.homeEntryCountBeforeBuild,
        buildHomeEntryCountAfter: packagedCli.homeEntryCountAfterBuild,
        buildHomeUnchanged:
          packagedCli.homeEntryCountBeforeBuild ===
          packagedCli.homeEntryCountAfterBuild,
        buildPathWasUnset: packagedCli.buildPathWasUnset,
        buildEnvironmentKeyCount: packagedCli.buildEnvironmentKeyCount,
        buildInvocationExecutableBasename:
          packagedCli.buildInvocationExecutableBasename,
        buildInvocationArgumentCount:
          packagedCli.buildInvocationArgumentCount,
        buildInvocationArgumentZeroMatchedValidatedBuilder:
          packagedCli.buildInvocationArgumentZeroMatchedValidatedBuilder,
        startCandidatesUsePackagedCli: candidatesUseExactPackagePath,
        stopperUsesPackagedCli: stopperUsesExactPackagePath,
        ownerIdentityProved: identityProved,
        activeIdentityMatches,
        loserIdentityMatches,
        statusIdentityMatches,
        ownerProcessLive,
        ownerChildPidMatches,
        ownerPidRecordUnchanged,
        startLockReleased,
        stopCommandExitedCleanly,
        stopReceiptReportedStopped,
        stopReceiptReason:
          typeof stopperReceipt?.reason === "string" ? stopperReceipt.reason : "none",
        ownerExitedCleanly,
        ownerExitCode: ownerObservation.settled ? ownerObservation.code : null,
        ownerExitSignal:
          ownerObservation.settled ? (ownerObservation.signal ?? "none") : "unsettled",
        stoppedThroughCli,
        pidRecordRemoved,
        shutdownFailureClass: shutdownClassification.failureClass ?? "none",
        shutdownFailureReason: shutdownClassification.reason,
        stopCommandFailureReason: stopClassification.failed
          ? stopClassification.reason
          : "none",
        stopperExitObserved: stopperObservation.settled,
        ownerExitObserved: ownerObservation.settled,
        candidateRestartRequests: counters.restartRequests,
        counterProvenanceProved,
        listenerCounterSource: "packaged CLI active start outputs",
        restartCounterSource: "unaccepted packaged CLI start outcomes",
      },
    };
  } catch (error) {
    const errorClass =
      error instanceof PackagedCollectorBuildError
        ? error.name
        : error instanceof Error
          ? error.name
          : "UnknownError";
    return {
      id: "duplicate_start_single_owner",
      required: true,
      status: "fail",
      detail: `Duplicate-start production contract raised ${errorClass}; child output and error text are omitted from the receipt.`,
      durationMs: Math.round((performance.now() - started) * 100) / 100,
      counters,
      measurements: {
        candidatesRaced: childEntries.length,
        lifecycleStage,
        errorClass,
      },
    };
  }
}

export function loadUnwiredIntegrationScenarios(
  wiredScenarioIds: ReadonlySet<string> = new Set(),
): ScenarioReceipt[] {
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
  return fixture.scenarios
    .filter((scenario) => !wiredScenarioIds.has(scenario.id))
    .map((scenario) => {
      const unknownCounters = scenario.requiredCounters.filter(
        (name) => !knownCounters.has(name),
      );
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
