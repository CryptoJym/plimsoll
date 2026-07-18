import path from "node:path";

import { PLIMSOLL_VERSION } from "./version";

export const LIFECYCLE_SCHEMA_VERSION = 1 as const;
export const SUPPORTED_NODE_RANGE = { minimum: 20, maximumExclusive: 25 } as const;
export const PURGE_CONFIRMATION = "PURGE PLIMSOLL LOCAL DATA" as const;

export type SupportedArchitecture = "arm64" | "x64";
export type LifecycleOperationKind = "update" | "rollback";
export type LifecyclePhase =
  | "prepared"
  | "snapshotted"
  | "staged"
  | "switched"
  | "verified"
  | "rollback_required"
  | "rollback_complete";

export type LifecyclePurgeOnlyTarget =
  | "collector_config"
  | "workspace_credentials"
  | "ledger"
  | "history"
  | "lifecycle_snapshots";

export type LifecycleRetainedTarget = LifecyclePurgeOnlyTarget | "workspace_membership";

export const LIFECYCLE_PURGE_ONLY_TARGETS = [
  "collector_config",
  "workspace_credentials",
  "ledger",
  "history",
  "lifecycle_snapshots",
] as const satisfies readonly LifecyclePurgeOnlyTarget[];

export const LIFECYCLE_UNINSTALL_RETAINED_TARGETS = [
  ...LIFECYCLE_PURGE_ONLY_TARGETS,
  "workspace_membership",
] as const satisfies readonly LifecycleRetainedTarget[];

export type RuntimeArtifact = {
  version: string;
  platform: "darwin";
  architecture: SupportedArchitecture;
  nodeMajor: number;
  sha256: `sha256:${string}`;
  sourcePath: string;
};

export type LifecycleJournal = {
  schemaVersion: typeof LIFECYCLE_SCHEMA_VERSION;
  operationId: string;
  kind: LifecycleOperationKind;
  fromVersion: string | null;
  toVersion: string;
  phase: LifecyclePhase;
  snapshotId: string;
};

export type LifecycleReadiness = {
  ready: boolean;
  runtimeVersion: string | null;
  serviceReady: boolean;
  configCompatible: boolean;
  databaseCompatible: boolean;
  reason: "ready" | "runtime_mismatch" | "service_unready" | "config_incompatible" | "database_incompatible";
};

export type LifecycleReceipt = {
  schemaVersion: typeof LIFECYCLE_SCHEMA_VERSION;
  toolVersion: string;
  operationId: string;
  operation: LifecycleOperationKind | "uninstall" | "purge" | "support_bundle";
  status: "completed" | "rolled_back" | "rollback_required" | "preview" | "purged" | "generated";
  fromVersion: string | null;
  toVersion: string | null;
  restoredVersion: string | null;
  health: LifecycleReadiness | null;
  ownedTargets: readonly string[];
  retainedTargets: readonly LifecycleRetainedTarget[];
  purgeOnlyTargets: readonly LifecyclePurgeOnlyTarget[];
  preserved: readonly ("ledger" | "history" | "credentials" | "workspace_membership")[];
};

export type LifecycleSupportSnapshot = {
  installedVersion: string | null;
  runtimeVersion: string | null;
  platform: string;
  architecture: string;
  nodeMajor: number;
  readiness: LifecycleReadiness;
  counters: {
    activeDelivery: number;
    deadDelivery: number;
    tokenAttributedEvents: number;
    maintenancePending: number;
  };
  boundedLogs: readonly {
    source: "collector_stdout" | "collector_stderr" | "lifecycle";
    severity: "info" | "warn" | "error";
    code: string;
    count: number;
  }[];
};

export type LifecycleAdapter = {
  acquireLock(operationId: string): Promise<boolean>;
  releaseLock(operationId: string): Promise<void>;
  readJournal(): Promise<LifecycleJournal | null>;
  writeJournal(journal: LifecycleJournal): Promise<void>;
  clearJournal(operationId: string): Promise<void>;
  operationIdExists(operationId: string): Promise<boolean>;
  installedVersion(): Promise<string | null>;
  snapshot(operationId: string): Promise<string>;
  stage(artifact: RuntimeArtifact): Promise<void>;
  switchTo(artifact: RuntimeArtifact): Promise<void>;
  readiness(expectedVersion: string, input: { signal: AbortSignal; deadlineMs: number }): Promise<LifecycleReadiness>;
  restore(snapshotId: string): Promise<void>;
  persistReceipt(receipt: LifecycleReceipt): Promise<void>;
  uninstallOwned(input: { apply: boolean }): Promise<readonly string[]>;
  purgeOwnedData(input: { apply: boolean; confirmation: string | null }): Promise<readonly string[]>;
  supportSnapshot(): Promise<LifecycleSupportSnapshot>;
};

export class LifecycleInterruption extends Error {
  readonly code = "LIFECYCLE_INTERRUPTED";
}

function assertBoundedIdentifier(value: string, label: string) {
  if (!/^[A-Za-z0-9][A-Za-z0-9._-]{0,95}$/.test(value) || value.includes("..")) {
    throw new Error(`${label} must be a bounded identifier`);
  }
}

export function validateRuntimeArtifact(artifact: RuntimeArtifact) {
  assertBoundedIdentifier(artifact.version, "version");
  if (artifact.platform !== "darwin") throw new Error("only darwin artifacts are supported");
  if (artifact.architecture !== "arm64" && artifact.architecture !== "x64") {
    throw new Error("unsupported architecture");
  }
  if (!Number.isInteger(artifact.nodeMajor) ||
      artifact.nodeMajor < SUPPORTED_NODE_RANGE.minimum ||
      artifact.nodeMajor >= SUPPORTED_NODE_RANGE.maximumExclusive) {
    throw new Error(`unsupported Node major ${artifact.nodeMajor}`);
  }
  if (!path.isAbsolute(artifact.sourcePath)) throw new Error("artifact source must be absolute");
  if (!/^sha256:[a-f0-9]{64}$/.test(artifact.sha256)) throw new Error("artifact digest must be sha256");
}

export function immutableRuntimeRelativePath(artifact: RuntimeArtifact) {
  validateRuntimeArtifact(artifact);
  return path.join("versions", artifact.version, `${artifact.platform}-${artifact.architecture}`, "bin", "plimsoll");
}

function phaseAtLeast(phase: LifecyclePhase, expected: LifecyclePhase) {
  return ["prepared", "snapshotted", "staged", "switched", "verified"].indexOf(phase) >=
    ["prepared", "snapshotted", "staged", "switched", "verified"].indexOf(expected);
}

const READINESS_REASONS: LifecycleReadiness["reason"][] = [
  "ready", "runtime_mismatch", "service_unready", "config_incompatible", "database_incompatible",
];

function ownPlainRecord(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  try {
    const prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) return null;
    return value as Record<string, unknown>;
  } catch {
    return null;
  }
}

function ownDataValue(record: Record<string, unknown> | null, key: string): unknown {
  if (!record) return undefined;
  try {
    const descriptor = Object.getOwnPropertyDescriptor(record, key);
    return descriptor && "value" in descriptor ? descriptor.value : undefined;
  } catch {
    return undefined;
  }
}

function safeVersion(value: unknown) {
  return typeof value === "string" && !value.includes("..") && /^[A-Za-z0-9][A-Za-z0-9._-]{0,95}$/.test(value)
    ? value
    : null;
}

export function sanitizeLifecycleReadiness(readiness: unknown): LifecycleReadiness {
  const record = ownPlainRecord(readiness);
  const reason = ownDataValue(record, "reason");
  return {
    ready: ownDataValue(record, "ready") === true,
    runtimeVersion: safeVersion(ownDataValue(record, "runtimeVersion")),
    serviceReady: ownDataValue(record, "serviceReady") === true,
    configCompatible: ownDataValue(record, "configCompatible") === true,
    databaseCompatible: ownDataValue(record, "databaseCompatible") === true,
    reason: READINESS_REASONS.includes(reason as LifecycleReadiness["reason"])
      ? reason as LifecycleReadiness["reason"]
      : "service_unready",
  };
}

function assertReadiness(readiness: LifecycleReadiness, version: string) {
  if (!readiness.ready || readiness.runtimeVersion !== version || !readiness.serviceReady ||
      !readiness.configCompatible || !readiness.databaseCompatible || readiness.reason !== "ready") {
    throw new Error(`readiness failed: ${readiness.reason}`);
  }
}

/**
 * Deterministic lifecycle transaction coordinator. It has no filesystem,
 * process, service-manager, network, or credential access of its own; callers
 * must inject those boundaries through LifecycleAdapter.
 */
export class LifecycleManager {
  private readonly readinessTimeoutMs: number;

  constructor(
    private readonly adapter: LifecycleAdapter,
    options: { readinessTimeoutMs?: number } = {},
  ) {
    const requested = options.readinessTimeoutMs ?? 10_000;
    if (!Number.isSafeInteger(requested) || requested < 10 || requested > 60_000) {
      throw new Error("readiness timeout must be between 10 and 60000 milliseconds");
    }
    this.readinessTimeoutMs = requested;
  }

  private async assertFreshOperation(operationId: string) {
    if (await this.adapter.operationIdExists(operationId)) {
      throw new Error("operationId was already completed; use a fresh operationId");
    }
  }

  private async boundedReadiness(expectedVersion: string) {
    const controller = new AbortController();
    let timer: NodeJS.Timeout | undefined;
    const deadline = new Promise<never>((_resolve, reject) => {
      timer = setTimeout(() => {
        controller.abort();
        reject(new Error("readiness deadline exceeded"));
      }, this.readinessTimeoutMs);
    });
    try {
      const result = await Promise.race([
        this.adapter.readiness(expectedVersion, {
          signal: controller.signal,
          deadlineMs: this.readinessTimeoutMs,
        }),
        deadline,
      ]);
      return sanitizeLifecycleReadiness(result);
    } finally {
      if (timer) clearTimeout(timer);
      controller.abort();
    }
  }

  private rollbackReceipt(journal: LifecycleJournal, status: "rolled_back" | "rollback_required"): LifecycleReceipt {
    return {
      schemaVersion: LIFECYCLE_SCHEMA_VERSION,
      toolVersion: PLIMSOLL_VERSION,
      operationId: journal.operationId,
      operation: journal.kind,
      status,
      fromVersion: journal.fromVersion,
      toVersion: journal.toVersion,
      restoredVersion: status === "rolled_back" ? journal.fromVersion : null,
      health: null,
      ownedTargets: ["runtime", "config", "database", "service_manifest"],
      retainedTargets: LIFECYCLE_UNINSTALL_RETAINED_TARGETS,
      purgeOnlyTargets: LIFECYCLE_PURGE_ONLY_TARGETS,
      preserved: ["ledger", "history", "credentials", "workspace_membership"],
    };
  }

  private async finishRequiredRollback(journal: LifecycleJournal) {
    if (journal.phase === "rollback_required") {
      try {
        await this.adapter.restore(journal.snapshotId);
      } catch {
        const blocked = this.rollbackReceipt(journal, "rollback_required");
        await this.adapter.persistReceipt(blocked);
        throw new Error("rollback required: restore failed; retry the same operationId");
      }
      journal.phase = "rollback_complete";
      await this.adapter.writeJournal(journal);
    }
    if (journal.phase !== "rollback_complete") {
      throw new Error("rollback recovery state is invalid");
    }
    const receipt = this.rollbackReceipt(journal, "rolled_back");
    await this.adapter.persistReceipt(receipt);
    await this.adapter.clearJournal(journal.operationId);
    return receipt;
  }

  async update(input: {
    operationId: string;
    artifact: RuntimeArtifact;
    kind?: LifecycleOperationKind;
  }): Promise<LifecycleReceipt> {
    const { operationId, artifact } = input;
    const kind = input.kind ?? "update";
    assertBoundedIdentifier(operationId, "operationId");
    validateRuntimeArtifact(artifact);

    if (!(await this.adapter.acquireLock(operationId))) {
      throw new Error("another lifecycle operation owns the lock");
    }

    let journal: LifecycleJournal | null = null;
    try {
      const existing = await this.adapter.readJournal();
      const fromVersion = await this.adapter.installedVersion();
      if (existing) {
        if (existing.operationId !== operationId || existing.kind !== kind || existing.toVersion !== artifact.version) {
          throw new Error("a different interrupted lifecycle operation requires recovery");
        }
        journal = existing;
        if (journal.phase === "rollback_required" || journal.phase === "rollback_complete") {
          return await this.finishRequiredRollback(journal);
        }
      } else {
        await this.assertFreshOperation(operationId);
        journal = {
          schemaVersion: LIFECYCLE_SCHEMA_VERSION,
          operationId,
          kind,
          fromVersion,
          toVersion: artifact.version,
          phase: "prepared",
          snapshotId: operationId,
        };
        await this.adapter.writeJournal(journal);
      }

      if (!phaseAtLeast(journal.phase, "snapshotted")) {
        journal.snapshotId = await this.adapter.snapshot(operationId);
        journal.phase = "snapshotted";
        await this.adapter.writeJournal(journal);
      }
      if (!phaseAtLeast(journal.phase, "staged")) {
        await this.adapter.stage(artifact);
        journal.phase = "staged";
        await this.adapter.writeJournal(journal);
      }
      if (!phaseAtLeast(journal.phase, "switched")) {
        await this.adapter.switchTo(artifact);
        journal.phase = "switched";
        await this.adapter.writeJournal(journal);
      }
      const health = await this.boundedReadiness(artifact.version);
      assertReadiness(health, artifact.version);
      journal.phase = "verified";
      await this.adapter.writeJournal(journal);

      const receipt: LifecycleReceipt = {
        schemaVersion: LIFECYCLE_SCHEMA_VERSION,
        toolVersion: PLIMSOLL_VERSION,
        operationId,
        operation: kind,
        status: "completed",
        fromVersion: journal.fromVersion,
        toVersion: artifact.version,
        restoredVersion: null,
        health,
        ownedTargets: ["runtime", "service_manifest"],
        retainedTargets: LIFECYCLE_UNINSTALL_RETAINED_TARGETS,
        purgeOnlyTargets: LIFECYCLE_PURGE_ONLY_TARGETS,
        preserved: ["ledger", "history", "credentials", "workspace_membership"],
      };
      await this.adapter.persistReceipt(receipt);
      await this.adapter.clearJournal(operationId);
      return receipt;
    } catch (error) {
      if (error instanceof LifecycleInterruption) throw error;
      if (journal?.phase === "rollback_required" || journal?.phase === "rollback_complete" || journal?.phase === "verified") {
        throw error;
      }
      if (journal && phaseAtLeast(journal.phase, "snapshotted")) {
        journal.phase = "rollback_required";
        await this.adapter.writeJournal(journal);
        await this.finishRequiredRollback(journal);
      }
      throw error;
    } finally {
      await this.adapter.releaseLock(operationId);
    }
  }

  rollback(input: { operationId: string; artifact: RuntimeArtifact }) {
    return this.update({ ...input, kind: "rollback" });
  }

  async uninstall(input: { operationId: string; apply?: boolean }): Promise<LifecycleReceipt> {
    assertBoundedIdentifier(input.operationId, "operationId");
    const apply = input.apply === true;
    if (!(await this.adapter.acquireLock(input.operationId))) {
      throw new Error("another lifecycle operation owns the lock");
    }
    try {
      if (await this.adapter.readJournal()) throw new Error("lifecycle recovery is required before uninstall");
      await this.assertFreshOperation(input.operationId);
      const fromVersion = await this.adapter.installedVersion();
      const ownedTargets = await this.adapter.uninstallOwned({ apply });
      const receipt: LifecycleReceipt = {
        schemaVersion: LIFECYCLE_SCHEMA_VERSION,
        toolVersion: PLIMSOLL_VERSION,
        operationId: input.operationId,
        operation: "uninstall",
        status: apply ? "completed" : "preview",
        fromVersion,
        toVersion: null,
        restoredVersion: null,
        health: null,
        ownedTargets,
        retainedTargets: LIFECYCLE_UNINSTALL_RETAINED_TARGETS,
        purgeOnlyTargets: LIFECYCLE_PURGE_ONLY_TARGETS,
        preserved: ["ledger", "history", "credentials", "workspace_membership"],
      };
      await this.adapter.persistReceipt(receipt);
      return receipt;
    } finally {
      await this.adapter.releaseLock(input.operationId);
    }
  }

  async purge(input: { operationId: string; apply?: boolean; confirmation?: string }): Promise<LifecycleReceipt> {
    assertBoundedIdentifier(input.operationId, "operationId");
    const apply = input.apply === true;
    if (apply && input.confirmation !== PURGE_CONFIRMATION) {
      throw new Error(`purge requires exact confirmation: ${PURGE_CONFIRMATION}`);
    }
    if (!(await this.adapter.acquireLock(input.operationId))) {
      throw new Error("another lifecycle operation owns the lock");
    }
    try {
      if (await this.adapter.readJournal()) throw new Error("lifecycle recovery is required before purge");
      await this.assertFreshOperation(input.operationId);
      const targets = await this.adapter.purgeOwnedData({
        apply,
        confirmation: apply ? input.confirmation ?? null : null,
      });
      const receipt: LifecycleReceipt = {
        schemaVersion: LIFECYCLE_SCHEMA_VERSION,
        toolVersion: PLIMSOLL_VERSION,
        operationId: input.operationId,
        operation: "purge",
        status: apply ? "purged" : "preview",
        fromVersion: await this.adapter.installedVersion(),
        toVersion: null,
        restoredVersion: null,
        health: null,
        ownedTargets: targets,
        retainedTargets: apply
          ? ["workspace_membership"]
          : LIFECYCLE_UNINSTALL_RETAINED_TARGETS,
        purgeOnlyTargets: LIFECYCLE_PURGE_ONLY_TARGETS,
        preserved: apply
          ? ["workspace_membership"]
          : ["ledger", "history", "credentials", "workspace_membership"],
      };
      await this.adapter.persistReceipt(receipt);
      return receipt;
    } finally {
      await this.adapter.releaseLock(input.operationId);
    }
  }

  async supportBundle(operationId: string): Promise<{ receipt: LifecycleReceipt; bundle: LifecycleSupportSnapshot }> {
    assertBoundedIdentifier(operationId, "operationId");
    if (!(await this.adapter.acquireLock(operationId))) {
      throw new Error("another lifecycle operation owns the lock");
    }
    try {
      if (await this.adapter.readJournal()) throw new Error("lifecycle recovery is required before support bundle");
      await this.assertFreshOperation(operationId);
      const bundle = sanitizeSupportSnapshot(await this.adapter.supportSnapshot());
      const receipt: LifecycleReceipt = {
        schemaVersion: LIFECYCLE_SCHEMA_VERSION,
        toolVersion: PLIMSOLL_VERSION,
        operationId,
        operation: "support_bundle",
        status: "generated",
        fromVersion: bundle.installedVersion,
        toVersion: null,
        restoredVersion: null,
        health: bundle.readiness,
        ownedTargets: ["support_bundle"],
        retainedTargets: LIFECYCLE_UNINSTALL_RETAINED_TARGETS,
        purgeOnlyTargets: LIFECYCLE_PURGE_ONLY_TARGETS,
        preserved: ["ledger", "history", "credentials", "workspace_membership"],
      };
      await this.adapter.persistReceipt(receipt);
      return { receipt, bundle };
    } finally {
      await this.adapter.releaseLock(operationId);
    }
  }
}

const SAFE_LOG_CODE = /^[a-z0-9][a-z0-9._-]{0,63}$/;

export function sanitizeSupportSnapshot(snapshot: unknown): LifecycleSupportSnapshot {
  const record = ownPlainRecord(snapshot);
  const nonnegative = (value: number) => Number.isSafeInteger(value) && value >= 0 ? value : 0;
  const logsCandidate = ownDataValue(record, "boundedLogs");
  const boundedLogs: Array<LifecycleSupportSnapshot["boundedLogs"][number]> = [];
  let logsArray: unknown[] | null = null;
  if (Array.isArray(logsCandidate)) {
    try {
      logsArray = Object.getPrototypeOf(logsCandidate) === Array.prototype ? logsCandidate : null;
    } catch {
      logsArray = null;
    }
  }
  if (logsArray) {
    const lengthDescriptor = Object.getOwnPropertyDescriptor(logsArray, "length");
    const rawLength = lengthDescriptor && "value" in lengthDescriptor ? lengthDescriptor.value : 0;
    const length = Number.isSafeInteger(rawLength) ? Math.min(Math.max(rawLength as number, 0), 32) : 0;
    for (let index = 0; index < length; index += 1) {
      const itemDescriptor = Object.getOwnPropertyDescriptor(logsArray, String(index));
      if (!itemDescriptor || !("value" in itemDescriptor)) continue;
      const row = ownPlainRecord(itemDescriptor.value);
      const source = ownDataValue(row, "source");
      const severity = ownDataValue(row, "severity");
      const code = ownDataValue(row, "code");
      const count = ownDataValue(row, "count");
      if (typeof code !== "string" || !SAFE_LOG_CODE.test(code)) continue;
      if (source !== "collector_stdout" && source !== "collector_stderr" && source !== "lifecycle") continue;
      if (severity !== "info" && severity !== "warn" && severity !== "error") continue;
      // Rebuild from approved scalar fields. Unknown own, prototype,
      // accessor, nested, case-alias, and Unicode-alias keys have no output.
      boundedLogs.push({
        source,
        severity,
        code,
        count: Math.min(nonnegative(typeof count === "number" ? count : 0), 1_000_000),
      });
    }
  }
  const counters = ownPlainRecord(ownDataValue(record, "counters"));
  const safeReadiness = sanitizeLifecycleReadiness(ownDataValue(record, "readiness"));
  const architecture = ownDataValue(record, "architecture");
  const nodeMajor = ownDataValue(record, "nodeMajor");
  return {
    installedVersion: safeVersion(ownDataValue(record, "installedVersion")),
    runtimeVersion: safeVersion(ownDataValue(record, "runtimeVersion")),
    platform: ownDataValue(record, "platform") === "darwin" ? "darwin" : "unsupported",
    architecture: architecture === "arm64" || architecture === "x64" ? architecture : "unsupported",
    nodeMajor: typeof nodeMajor === "number" && Number.isInteger(nodeMajor) ? nodeMajor : 0,
    readiness: safeReadiness,
    counters: {
      activeDelivery: nonnegative(typeof ownDataValue(counters, "activeDelivery") === "number" ? ownDataValue(counters, "activeDelivery") as number : 0),
      deadDelivery: nonnegative(typeof ownDataValue(counters, "deadDelivery") === "number" ? ownDataValue(counters, "deadDelivery") as number : 0),
      tokenAttributedEvents: nonnegative(typeof ownDataValue(counters, "tokenAttributedEvents") === "number" ? ownDataValue(counters, "tokenAttributedEvents") as number : 0),
      maintenancePending: nonnegative(typeof ownDataValue(counters, "maintenancePending") === "number" ? ownDataValue(counters, "maintenancePending") as number : 0),
    },
    boundedLogs,
  };
}

export function lifecycleBoundaryStatement() {
  return {
    leave: "distinct_operation_not_performed",
    revoke: "hosted_owner_operation_not_performed",
    credentialsMoved: false,
    liveServiceTouched: false,
  } as const;
}
