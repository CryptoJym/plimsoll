import path from "node:path";

import { PLIMSOLL_VERSION } from "./version";

export const LIFECYCLE_SCHEMA_VERSION = 1 as const;
export const SUPPORTED_NODE_RANGE = { minimum: 20, maximumExclusive: 25 } as const;
export const PURGE_CONFIRMATION = "PURGE PLIMSOLL LOCAL DATA" as const;

export type SupportedArchitecture = "arm64" | "x64";
export type LifecycleOperationKind = "update" | "rollback";
export type LifecyclePhase = "prepared" | "snapshotted" | "staged" | "switched" | "verified";

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
  status: "completed" | "rolled_back" | "preview" | "purged" | "generated";
  fromVersion: string | null;
  toVersion: string | null;
  restoredVersion: string | null;
  health: LifecycleReadiness | null;
  ownedTargets: readonly string[];
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
  installedVersion(): Promise<string | null>;
  snapshot(operationId: string): Promise<string>;
  stage(artifact: RuntimeArtifact): Promise<void>;
  switchTo(artifact: RuntimeArtifact): Promise<void>;
  readiness(expectedVersion: string): Promise<LifecycleReadiness>;
  restore(snapshotId: string): Promise<void>;
  persistReceipt(receipt: LifecycleReceipt): Promise<void>;
  uninstallOwned(input: { apply: boolean }): Promise<readonly string[]>;
  purgeOwnedData(input: { confirmation: string }): Promise<readonly string[]>;
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

export function sanitizeLifecycleReadiness(readiness: LifecycleReadiness): LifecycleReadiness {
  const safeVersion = (value: string | null) => value !== null && !value.includes("..") && /^[A-Za-z0-9][A-Za-z0-9._-]{0,95}$/.test(value) ? value : null;
  return {
    ready: Boolean(readiness.ready),
    runtimeVersion: safeVersion(readiness.runtimeVersion),
    serviceReady: Boolean(readiness.serviceReady),
    configCompatible: Boolean(readiness.configCompatible),
    databaseCompatible: Boolean(readiness.databaseCompatible),
    reason: READINESS_REASONS.includes(readiness.reason) ? readiness.reason : "service_unready",
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
  constructor(private readonly adapter: LifecycleAdapter) {}

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
      } else {
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
      const health = sanitizeLifecycleReadiness(await this.adapter.readiness(artifact.version));
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
        preserved: ["ledger", "history", "credentials", "workspace_membership"],
      };
      await this.adapter.persistReceipt(receipt);
      await this.adapter.clearJournal(operationId);
      return receipt;
    } catch (error) {
      if (error instanceof LifecycleInterruption) throw error;
      if (journal && phaseAtLeast(journal.phase, "snapshotted")) {
        await this.adapter.restore(journal.snapshotId);
        const receipt: LifecycleReceipt = {
          schemaVersion: LIFECYCLE_SCHEMA_VERSION,
          toolVersion: PLIMSOLL_VERSION,
          operationId,
          operation: kind,
          status: "rolled_back",
          fromVersion: journal.fromVersion,
          toVersion: artifact.version,
          restoredVersion: journal.fromVersion,
          health: null,
          ownedTargets: ["runtime", "config", "database", "service_manifest"],
          preserved: ["ledger", "history", "credentials", "workspace_membership"],
        };
        await this.adapter.persistReceipt(receipt);
        await this.adapter.clearJournal(operationId);
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
        preserved: ["ledger", "history", "credentials", "workspace_membership"],
      };
      await this.adapter.persistReceipt(receipt);
      return receipt;
    } finally {
      await this.adapter.releaseLock(input.operationId);
    }
  }

  async purge(input: { operationId: string; confirmation: string }): Promise<LifecycleReceipt> {
    assertBoundedIdentifier(input.operationId, "operationId");
    if (input.confirmation !== PURGE_CONFIRMATION) {
      throw new Error(`purge requires exact confirmation: ${PURGE_CONFIRMATION}`);
    }
    if (!(await this.adapter.acquireLock(input.operationId))) {
      throw new Error("another lifecycle operation owns the lock");
    }
    try {
      const targets = await this.adapter.purgeOwnedData({ confirmation: input.confirmation });
      const receipt: LifecycleReceipt = {
        schemaVersion: LIFECYCLE_SCHEMA_VERSION,
        toolVersion: PLIMSOLL_VERSION,
        operationId: input.operationId,
        operation: "purge",
        status: "purged",
        fromVersion: await this.adapter.installedVersion(),
        toVersion: null,
        restoredVersion: null,
        health: null,
        ownedTargets: targets,
        preserved: ["workspace_membership"],
      };
      await this.adapter.persistReceipt(receipt);
      return receipt;
    } finally {
      await this.adapter.releaseLock(input.operationId);
    }
  }

  async supportBundle(operationId: string): Promise<{ receipt: LifecycleReceipt; bundle: LifecycleSupportSnapshot }> {
    assertBoundedIdentifier(operationId, "operationId");
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
      preserved: ["ledger", "history", "credentials", "workspace_membership"],
    };
    await this.adapter.persistReceipt(receipt);
    return { receipt, bundle };
  }
}

const SAFE_LOG_CODE = /^[a-z0-9][a-z0-9._-]{0,63}$/;

export function sanitizeSupportSnapshot(snapshot: LifecycleSupportSnapshot): LifecycleSupportSnapshot {
  const nonnegative = (value: number) => Number.isSafeInteger(value) && value >= 0 ? value : 0;
  const boundedLogs = snapshot.boundedLogs.slice(0, 32).flatMap((row) => {
    if (!SAFE_LOG_CODE.test(row.code)) return [];
    if (!["collector_stdout", "collector_stderr", "lifecycle"].includes(row.source)) return [];
    if (!["info", "warn", "error"].includes(row.severity)) return [];
    return [{ ...row, count: Math.min(nonnegative(row.count), 1_000_000) }];
  });
  const safeVersion = (value: string | null) => value !== null && !value.includes("..") && /^[A-Za-z0-9][A-Za-z0-9._-]{0,95}$/.test(value) ? value : null;
  const safeReadiness = sanitizeLifecycleReadiness(snapshot.readiness);
  return {
    installedVersion: safeVersion(snapshot.installedVersion),
    runtimeVersion: safeVersion(snapshot.runtimeVersion),
    platform: snapshot.platform === "darwin" ? "darwin" : "unsupported",
    architecture: snapshot.architecture === "arm64" || snapshot.architecture === "x64" ? snapshot.architecture : "unsupported",
    nodeMajor: Number.isInteger(snapshot.nodeMajor) ? snapshot.nodeMajor : 0,
    readiness: safeReadiness,
    counters: {
      activeDelivery: nonnegative(snapshot.counters.activeDelivery),
      deadDelivery: nonnegative(snapshot.counters.deadDelivery),
      tokenAttributedEvents: nonnegative(snapshot.counters.tokenAttributedEvents),
      maintenancePending: nonnegative(snapshot.counters.maintenancePending),
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
