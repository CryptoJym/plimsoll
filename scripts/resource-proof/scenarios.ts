import { spawn, spawnSync, type ChildProcessByStdio } from "node:child_process";
import fs from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import type { Readable } from "node:stream";
import { fileURLToPath } from "node:url";

import { LocalEventBuffer } from "../../packages/collector-cli/src/buffer";
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
import {
  CoalescingMaintenanceScheduler,
  CollectorMaintenance,
  type CollectorMaintenanceRunResult,
} from "../../packages/collector-cli/src/maintenance";
import { RolloutTailer } from "../../packages/collector-cli/src/rollout-tailer";
import {
  readCollectorPidFile,
  runtimeIdentityMatches,
  type CollectorRuntimeIdentity,
} from "../../packages/collector-cli/src/runtime-ownership";
import { TranscriptTailer } from "../../packages/collector-cli/src/transcript-tailer";
import { uploadBufferedEvents } from "../../packages/collector-cli/src/upload";
import { createCollectorServer } from "../../packages/collector-cli/src/server";
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

function writeNoChangeFixtures(sandbox: ResourceSandbox) {
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
  fs.writeFileSync(
    path.join(rolloutDay, `rollout-resource-proof-${rolloutSession}.jsonl`),
    `${rollout.map((entry) => JSON.stringify(entry)).join("\n")}\n`,
    { mode: 0o600 },
  );

  const transcriptSession = "019e6000-0000-7000-8000-000000000002";
  const transcriptDirectory = path.join(sandbox.claudeProjects, "resource-proof");
  fs.mkdirSync(transcriptDirectory, { recursive: true, mode: 0o700 });
  fs.writeFileSync(
    path.join(transcriptDirectory, `${transcriptSession}.jsonl`),
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
  unregistered: boolean;
  restorationVerified: boolean;
};

const directoryEnumerationObservers = new Map<string, DirectoryEnumerationRecord>();
let directoryEnumerationOriginal: typeof fs.readdirSync | undefined;
let directoryEnumerationWrapper: typeof fs.readdirSync | undefined;

function readdirPath(value: Parameters<typeof fs.readdirSync>[0]) {
  if (typeof value === "string") return value;
  if (Buffer.isBuffer(value)) return value.toString();
  if (value instanceof URL && value.protocol === "file:") return fileURLToPath(value);
  return null;
}

function installDirectoryEnumerationWrapper() {
  if (directoryEnumerationWrapper) {
    if (fs.readdirSync !== directoryEnumerationWrapper) {
      throw new Error("DirectoryObserverIntegrityLost");
    }
    return;
  }
  directoryEnumerationOriginal = fs.readdirSync;
  const original = directoryEnumerationOriginal;
  directoryEnumerationWrapper = ((...args: Parameters<typeof fs.readdirSync>) => {
    const result = original(...args);
    const directory = readdirPath(args[0]);
    if (directory) {
      for (const observer of directoryEnumerationObservers.values()) {
        if (!observer.unregistered && within(observer.root, directory)) {
          observer.calls += 1;
          observer.entries += result.length;
        }
      }
    }
    return result;
  }) as typeof fs.readdirSync;
  fs.readdirSync = directoryEnumerationWrapper;
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
    unregistered: false,
    restorationVerified: false,
  };
  directoryEnumerationObservers.set(resolvedRoot, record);
  return {
    snapshot: () => ({ calls: record.calls, entries: record.entries }),
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
        const original = directoryEnumerationOriginal;
        const wrapper = directoryEnumerationWrapper;
        if (original && wrapper && fs.readdirSync === wrapper) {
          fs.readdirSync = original;
        }
        record.restorationVerified = Boolean(original && fs.readdirSync === original);
        directoryEnumerationOriginal = undefined;
        directoryEnumerationWrapper = undefined;
      } else {
        record.restorationVerified = fs.readdirSync === directoryEnumerationWrapper;
      }
    },
    status: () => ({
      calls: record.calls,
      entries: record.entries,
      restored: record.unregistered && record.restorationVerified,
    }),
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
        return new Response(JSON.stringify({ accepted: ids.length }), {
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
 * Production #77 path: both real JSONL tailers, the real maintenance worker,
 * and its real coalescing scheduler. A promise handshake holds the first job
 * while concurrent recent/full triggers queue; no wall-clock delay determines
 * correctness.
 */
export async function runNoChangeConstantWorkContract(
  sandbox: ResourceSandbox,
  options: { injectFailureAfterObserverRegistration?: boolean } = {},
): Promise<ScenarioReceipt> {
  const started = performance.now();
  const counters = emptyWorkCounters();
  let buffer: LocalEventBuffer | undefined;
  let directoryObserver: ReturnType<typeof observeDirectoryEnumeration> | undefined;
  try {
    writeNoChangeFixtures(sandbox);
    buffer = new LocalEventBuffer(sandbox.ledger);
    installTemporaryEventMutationAudit(buffer);
    const maintenance = new CollectorMaintenance(
      buffer,
      new RolloutTailer(buffer, sandbox.codexSessions, () => []),
      new TranscriptTailer(buffer, sandbox.claudeProjects),
    );

    let firstStarted!: () => void;
    let releaseFirst!: () => void;
    const firstStartedSignal = new Promise<void>((resolve) => {
      firstStarted = resolve;
    });
    const firstReleaseSignal = new Promise<void>((resolve) => {
      releaseFirst = resolve;
    });
    const runModes: boolean[] = [];
    const mutationDeltas: EventMutationCounts[] = [];
    const directoryEntryDeltas: number[] = [];
    const activeDirectoryObserver = observeDirectoryEnumeration(sandbox.root);
    directoryObserver = activeDirectoryObserver;
    const scheduler = new CoalescingMaintenanceScheduler(async (recentOnly) => {
      const invocation = runModes.push(recentOnly);
      if (invocation === 1) {
        firstStarted();
        await firstReleaseSignal;
      }
      const before = eventMutationCounts(buffer!);
      const directoryBefore = activeDirectoryObserver.snapshot();
      const result = await maintenance.run(recentOnly);
      mutationDeltas.push(eventMutationDelta(before, eventMutationCounts(buffer!)));
      directoryEntryDeltas.push(
        activeDirectoryObserver.snapshot().entries - directoryBefore.entries,
      );
      return result;
    });

    let initialDrain: CollectorMaintenanceRunResult[] = [];
    let thirdDrain: CollectorMaintenanceRunResult[] = [];
    let queuedStatus = scheduler.status();
    let finalStatus = scheduler.status();
    try {
      if (options.injectFailureAfterObserverRegistration) {
        throw new Error("InjectedDirectoryObserverFailure");
      }
      const initial = scheduler.trigger(true);
      await firstStartedSignal;
      const concurrentRecent = scheduler.trigger(true);
      const concurrentFull = scheduler.trigger(false);
      queuedStatus = scheduler.status();
      releaseFirst();
      [initialDrain] = await Promise.all([initial, concurrentRecent, concurrentFull]);
      thirdDrain = await scheduler.trigger(true);
      finalStatus = scheduler.status();
    } finally {
      activeDirectoryObserver.unregister();
    }
    const directoryObservation = activeDirectoryObserver.status();

    const firstRun = initialDrain[0];
    const secondRun = initialDrain[1];
    const thirdRun = thirdDrain[0];
    const secondMutations = mutationDeltas[1];
    const thirdMutations = mutationDeltas[2];
    if (!firstRun || !secondRun || !thirdRun || !secondMutations || !thirdMutations) {
      throw new Error("MaintenanceResultMissing");
    }

    const fullHistoryFileReads =
      secondRun.rollout.filesRead + secondRun.transcript.filesRead;
    const fileBytesRead =
      secondRun.rollout.bytesRead +
      secondRun.transcript.bytesRead +
      thirdRun.rollout.bytesRead +
      thirdRun.transcript.bytesRead;
    const rawEventWrites = secondMutations.inserted + thirdMutations.inserted;
    const rawEventRewrites =
      secondMutations.updated +
      secondMutations.deleted +
      thirdMutations.updated +
      thirdMutations.deleted;
    const repriceRowsVisited =
      secondRun.repricing.rowsVisited + thirdRun.repricing.rowsVisited;
    const reconciliationRowsVisited =
      secondRun.reconciliation.rowsVisited + thirdRun.reconciliation.rowsVisited;
    const enrichmentRowsVisited =
      secondRun.enrichment.rowsVisited + thirdRun.enrichment.rowsVisited;

    counters.filesOpened =
      secondRun.rollout.filesRead +
      secondRun.transcript.filesRead +
      thirdRun.rollout.filesRead +
      thirdRun.transcript.filesRead;
    counters.fileBytesRead = fileBytesRead;
    counters.fullHistoryFileReads = fullHistoryFileReads;
    counters.rawEventWrites = rawEventWrites;
    counters.rawEventRewrites = rawEventRewrites;
    counters.repriceRowsVisited = repriceRowsVisited;
    counters.reconciliationRowsVisited = reconciliationRowsVisited;
    counters.enrichmentRowsVisited = enrichmentRowsVisited;
    counters.maintenanceRuns = finalStatus.runCount;
    counters.overlappingJobs = finalStatus.overlappingJobs;
    counters.filesystemEntriesScanned = directoryObservation.entries;

    const firstRunBounded =
      firstRun.rollout.filesRead === 1 &&
      firstRun.transcript.filesRead === 1 &&
      firstRun.rawEventWrites === 2 &&
      firstRun.reconciliation.backfillComplete &&
      firstRun.repricing.backfillComplete &&
      firstRun.enrichment.backfillComplete;
    const deterministicIdleCounters =
      unchangedMaintenanceResult(secondRun) &&
      unchangedMaintenanceResult(thirdRun) &&
      secondMutations.inserted === 0 &&
      secondMutations.updated === 0 &&
      secondMutations.deleted === 0 &&
      thirdMutations.inserted === 0 &&
      thirdMutations.updated === 0 &&
      thirdMutations.deleted === 0;
    const coalescingProved =
      queuedStatus.inFlight &&
      queuedStatus.pending &&
      queuedStatus.triggerCount === 3 &&
      queuedStatus.coalescedTriggerCount === 2 &&
      runModes.join(",") === "true,false,true" &&
      finalStatus.triggerCount === 4 &&
      finalStatus.runCount === 3 &&
      finalStatus.maxConcurrentJobs === 1 &&
      finalStatus.overlappingJobs === 0 &&
      finalStatus.failedRuns === 0;
    const counterProvenanceProved =
      directoryObservation.restored &&
      directoryEntryDeltas.length === finalStatus.runCount &&
      directoryEntryDeltas.reduce((total, count) => total + count, 0) ===
        counters.filesystemEntriesScanned &&
      counters.maintenanceRuns === finalStatus.runCount;
    const passed =
      firstRunBounded &&
      deterministicIdleCounters &&
      coalescingProved &&
      counterProvenanceProved &&
      fullHistoryFileReads === 0 &&
      fileBytesRead === 0 &&
      rawEventWrites === 0 &&
      rawEventRewrites === 0 &&
      repriceRowsVisited === 0 &&
      reconciliationRowsVisited === 0 &&
      enrichmentRowsVisited === 0;

    return {
      id: "no_change_constant_work",
      required: true,
      status: passed ? "pass" : "fail",
      detail: passed
        ? "Real rollout/transcript maintenance completed one bounded migration, then two unchanged cycles performed no history reads, event mutations, Codex reconciliation/repricing/enrichment visits, or overlapping work."
        : "No-change production contract failed one or more deterministic work assertions.",
      durationMs: Math.round((performance.now() - started) * 100) / 100,
      counters,
      measurements: {
        firstRunBounded,
        initialEventsAppended: firstRun.rawEventWrites,
        unchangedCycles: 2,
        deterministicIdleCounters,
        concurrentTriggersQueued: 2,
        runModes: runModes.map((recentOnly) => (recentOnly ? "recent" : "full")).join(","),
        triggerCount: finalStatus.triggerCount,
        coalescedTriggerCount: finalStatus.coalescedTriggerCount,
        schedulerRunCount: finalStatus.runCount,
        maxConcurrentJobs: finalStatus.maxConcurrentJobs,
        filesystemEnumerationCalls: directoryObservation.calls,
        setupFilesystemEntriesScanned: directoryEntryDeltas[0] ?? 0,
        unchangedFilesystemEntriesScanned:
          (directoryEntryDeltas[1] ?? 0) + (directoryEntryDeltas[2] ?? 0),
        filesystemObserverRestored: directoryObservation.restored,
        counterProvenanceProved,
        filesystemCounterSource: "observed fs.readdirSync returned entries",
        maintenanceRunCounterSource: "scheduler runCount",
      },
    };
  } catch (error) {
    directoryObserver?.unregister();
    const directoryObservation = directoryObserver?.status();
    if (directoryObservation) {
      counters.filesystemEntriesScanned = directoryObservation.entries;
    }
    return {
      id: "no_change_constant_work",
      required: true,
      status: "fail",
      detail: `No-change production contract raised ${
        error instanceof Error ? error.name : "UnknownError"
      }; error text is omitted from the receipt.`,
      durationMs: Math.round((performance.now() - started) * 100) / 100,
      counters,
      measurements: {
        deterministicIdleCounters: false,
        filesystemObserverRestored: directoryObservation?.restored ?? false,
        counterProvenanceProved: directoryObservation?.restored ?? false,
      },
    };
  } finally {
    directoryObserver?.unregister();
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
      warmP95 <= 500;
    return {
      id: "dashboard_projection_budget",
      required: true,
      status: passed ? "pass" : "fail",
      detail: passed
        ? "One coherent production snapshot served all five dashboard surfaces; twenty warm refreshes performed zero raw/filesystem scans and no snapshot rebuild."
        : "Dashboard snapshot coherence, deterministic no-scan counters, unchanged-refresh build count, or warm p95 exceeded the production gate.",
      durationMs: Math.round((performance.now() - started) * 100) / 100,
      counters,
      measurements: {
        generation: generation ?? -1,
        coherent,
        warmRequests: durations.length,
        warmP95Ms: Math.round(warmP95 * 100) / 100,
        snapshotBuildsDuringRefresh: after.snapshotBuilds - buildsBefore,
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
  const tsxCli = path.join(repoRoot, "node_modules", "tsx", "dist", "cli.mjs");
  const worker = path.join(repoRoot, "scripts", "resource-proof", "integrated-worker.ts");
  const workerRoot = path.join(sandbox.root, `worker-${mode}`);
  fs.mkdirSync(workerRoot, { recursive: true, mode: 0o700 });
  const result = spawnSync(
    process.execPath,
    [
      tsxCli,
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
  const childNode22 = parsed?.measurements.nodeMajor === 22;
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
      ? "A minimal Node 22 child proved loopback OTLP admission, atomic raw/projection/outbox capture, coherent snapshot, reopen, duplicate and rollback safety, and one fake-transport acknowledgement."
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
      ? "Full and prefix sentinels plus the operator-home path were absent from live and closed SQLite surfaces, upload bytes, request logs/responses, child output, and the worker receipt."
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

type CapturedChild = {
  child: ChildProcessByStdio<null, Readable, Readable>;
  output: { stdout: string; stderr: string };
  active: Promise<Record<string, unknown> | null>;
  exit: Promise<{ code: number | null; signal: NodeJS.Signals | null }>;
};

function withDeadline<T>(promise: Promise<T>, milliseconds: number, code: string) {
  return new Promise<T>((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error(code)), milliseconds);
    promise.then(
      (value) => {
        clearTimeout(timeout);
        resolve(value);
      },
      (error) => {
        clearTimeout(timeout);
        reject(error);
      },
    );
  });
}

async function assignLoopbackPort() {
  const reservation = await holdLoopbackPort();
  const port = reservation.port;
  await new Promise<void>((resolve) => reservation.server.close(() => resolve()));
  return port;
}

function buildPackagedCollectorCli(sandbox: ResourceSandbox) {
  const packageDirectory = path.join(repoRoot, "packages", "collector-cli");
  const cliPath = path.join(packageDirectory, "dist", "cli.mjs");
  const build = spawnSync("pnpm", ["--dir", packageDirectory, "build"], {
    cwd: repoRoot,
    env: buildAllowlistedChildEnvironment(sandbox),
    encoding: "utf8",
    timeout: 120_000,
    maxBuffer: 16 * 1024 * 1024,
  });
  if (build.status !== 0 || build.error || !fs.existsSync(cliPath)) {
    throw new Error("PackagedCollectorBuildFailed");
  }
  const exactPackagePath =
    path.resolve(cliPath) ===
      path.resolve(repoRoot, "packages", "collector-cli", "dist", "cli.mjs") &&
    fs.realpathSync(cliPath) === path.resolve(cliPath);
  const executable = (fs.statSync(cliPath).mode & 0o111) !== 0;
  if (!exactPackagePath || !executable) throw new Error("PackagedCollectorPathInvalid");
  return {
    cliPath,
    buildExitCode: build.status,
    exactPackagePath,
    executable,
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
      child.once("exit", (code, signal) => {
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

async function boundedChildCleanup(candidate: CapturedChild) {
  if (candidate.child.exitCode !== null || candidate.child.signalCode !== null) return;
  candidate.child.kill("SIGTERM");
  try {
    await withDeadline(candidate.exit, 3_000, "ChildCleanupTimeout");
  } catch {
    candidate.child.kill("SIGKILL");
    await withDeadline(candidate.exit, 3_000, "ChildKillTimeout").catch(() => undefined);
  }
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
): Promise<ScenarioReceipt> {
  const started = performance.now();
  const counters = emptyWorkCounters();
  const children: CapturedChild[] = [];
  try {
    const packagedCli = buildPackagedCollectorCli(sandbox);
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
    children.push(...candidates);
    const candidatesUseExactPackagePath = candidates.every(
      (candidate) =>
        candidate.child.spawnfile === process.execPath &&
        candidate.child.spawnargs[1] === packagedCli.cliPath &&
        candidate.child.spawnargs[2] === "start",
    );
    const ownerChoice = await withDeadline(
      chooseActiveCandidate(candidates),
      20_000,
      "CollectorReadinessTimeout",
    );
    const owner = candidates[ownerChoice.index]!;
    const loser = candidates[ownerChoice.index === 0 ? 1 : 0]!;

    const ownerRead = readCollectorPidFile(pidPath, LAUNCH_AGENT_LABEL);
    if (ownerRead.kind !== "current") throw new Error("OwnerPidRecordMissing");
    const ownerIdentity: CollectorRuntimeIdentity = {
      instanceId: ownerRead.record.instanceId,
      pid: ownerRead.record.pid,
      processStartFingerprint: ownerRead.record.processStartFingerprint,
    };
    const pidRecordBeforeLoser = ownerRead.raw;
    const loserExit = await withDeadline(loser.exit, 20_000, "LoserExitTimeout");
    const loserReceipt = parseCapturedJson(loser.output.stdout);
    const pidRecordAfterLoser = fs.readFileSync(pidPath, "utf8");

    const statusResponse = await withDeadline(
      fetch(`http://127.0.0.1:${port}/status`),
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

    const stopper = spawnCollectorCli(sandbox, packagedCli.cliPath, "stop");
    children.push(stopper);
    const stopperUsesExactPackagePath =
      stopper.child.spawnfile === process.execPath &&
      stopper.child.spawnargs[1] === packagedCli.cliPath &&
      stopper.child.spawnargs[2] === "stop";
    const stopperExit = await withDeadline(stopper.exit, 20_000, "StopCommandTimeout");
    const stopperReceipt = parseCapturedJson(stopper.output.stdout);
    const ownerExit = await withDeadline(owner.exit, 20_000, "OwnerShutdownTimeout");
    const stoppedThroughCli =
      stopperExit.code === 0 &&
      stopperExit.signal === null &&
      stopperReceipt.stopped === true &&
      ownerExit.code === 0 &&
      ownerExit.signal === null;
    const pidRecordRemoved =
      readCollectorPidFile(pidPath, LAUNCH_AGENT_LABEL).kind === "missing";

    const passed =
      packagedCli.buildExitCode === 0 &&
      packagedCli.exactPackagePath &&
      packagedCli.executable &&
      candidatesUseExactPackagePath &&
      stopperUsesExactPackagePath &&
      identityProved &&
      ownerPidRecordUnchanged &&
      loserHonest &&
      startLockReleased &&
      counterProvenanceProved &&
      stoppedThroughCli &&
      pidRecordRemoved;
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
        stoppedThroughCli,
        pidRecordRemoved,
        candidateRestartRequests: counters.restartRequests,
        counterProvenanceProved,
        listenerCounterSource: "packaged CLI active start outputs",
        restartCounterSource: "unaccepted packaged CLI start outcomes",
      },
    };
  } catch (error) {
    return {
      id: "duplicate_start_single_owner",
      required: true,
      status: "fail",
      detail: `Duplicate-start production contract raised ${
        error instanceof Error ? error.name : "UnknownError"
      }; child output and error text are omitted from the receipt.`,
      durationMs: Math.round((performance.now() - started) * 100) / 100,
      counters,
      measurements: { candidatesRaced: children.filter((child) => child !== undefined).length },
    };
  } finally {
    await Promise.all(children.map((child) => boundedChildCleanup(child)));
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
