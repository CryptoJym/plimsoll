import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import Database from "better-sqlite3";
import { z } from "zod";

import { DEFAULT_POLICY, LOCAL_TENANT_ID, policyConfigSchema } from "../../shared/src/index";
import { captureRootSchema, validateCaptureRoots } from "./capture-root-inventory";
import { resolveCollectorHome } from "./collector-home";

export const DEFAULT_COLLECTOR_PORT = 48271;

export const PRIVACY_MODE = "metadata_only" as const;
export const EVIDENCE_VAULT_STATE = "not_implemented" as const;
export const LEGACY_EVIDENCE_DISPOSITION =
  "local_quarantine_migration_required" as const;

const RAW_CAPTURE_ENV = [
  "PLIMSOLL_EVIDENCE_MODE",
  "OTEL_LOG_USER_PROMPTS",
  "OTEL_LOG_TOOL_DETAILS",
  "OTEL_LOG_TOOL_CONTENT",
  "OTEL_LOG_RAW_API_BODIES",
] as const;

function envEnablesRawCapture(name: string, value: string | undefined) {
  if (value === undefined) return false;
  if (name === "PLIMSOLL_EVIDENCE_MODE") {
    return !["", "0", "false", "off", "metadata", "metadata_only"].includes(
      value.trim().toLowerCase(),
    );
  }
  return ["1", "true", "yes", "on", "evidence"].includes(value.trim().toLowerCase());
}

export class CollectorPrivacyModeError extends Error {
  readonly code = "raw_evidence_mode_unavailable";

  constructor(readonly context: string, detail: string) {
    super(
      `${context}: ${detail} Raw evidence capture is unavailable in the ordinary Plimsoll ledger; ` +
        "the encrypted evidence vault is not implemented.",
    );
    this.name = "CollectorPrivacyModeError";
  }
}

export const collectorConfigSchema = z
  .object({
    port: z.number().int().min(1024).max(65535).default(DEFAULT_COLLECTOR_PORT),
    ingestKey: z.string().trim().min(1).optional(),
    uploadSigningSecret: z.string().trim().min(16).optional(),
    uploadUrl: z.string().url().optional(),
    accountActorSaltEndpoint: z.string().url().optional(),
    tenantId: z.string().trim().min(1).default(LOCAL_TENANT_ID),
    installKey: z.string().trim().min(1).default("local-dev"),
    /** Stable local activation identity; never replaced by a hosted id. */
    deviceId: z.string().trim().min(1).optional(),
    /** Hosted DeviceInstall UUID used only by the tenant salt endpoint. */
    cloudDeviceId: z.string().uuid().optional(),
    keyId: z.string().trim().min(1).optional(),
    /** Explicit fleet-management marker. Older joined configs are also
     * recognized from their upload/tenant/install credentials. */
    managed: z.boolean().default(false),
    retentionDays: z.number().int().min(1).max(3650).default(90),
    startupWalCheckpointBytes: z.number().int().min(1).max(100 * 1024 * 1024 * 1024)
      .default(1024 * 1024 * 1024),
    captureRoots: z.array(captureRootSchema).max(64).optional().superRefine((roots, ctx) => {
      if (!roots) return;
      try { validateCaptureRoots(roots); } catch { ctx.addIssue({ code: "custom", message: "Invalid or overlapping capture root inventory" }); }
    }),
    subscriptions: z
      .array(
        z.object({
          account: z.string().trim().min(1), // account label OR sha256: hash
          plan: z.string().trim().min(1),
          usdPerMonth: z.number().nonnegative(),
          vendor: z.enum(["anthropic", "openai", "other"]).default("other"),
        }),
      )
      .default([]),
    syncIntervalSeconds: z.number().int().min(30).max(86400).default(300),
    delivery: z
      .object({
        maxActiveRows: z.number().int().min(1).max(10_000_000).default(50_000),
        maxActiveBytes: z.number().int().min(1).max(100 * 1024 * 1024 * 1024).default(512 * 1024 * 1024),
        maxOldestAgeDays: z.number().int().min(1).max(3650).default(90),
        maxItemBytes: z.number().int().min(1_024).max(1_500_000).default(256 * 1024),
        migrationBatchRows: z.number().int().min(1).max(5_000).default(5_000),
        migrationBatchBytes: z.number().int().min(1_024).max(128 * 1024 * 1024).default(32 * 1024 * 1024),
        maxBatchesPerCycle: z.number().int().min(1).max(100).default(20),
        leaseSeconds: z.number().int().min(10).max(3600).default(120),
        requestTimeoutSeconds: z.number().int().min(1).max(300).default(30),
        maxBackoffSeconds: z.number().int().min(30).max(86_400).default(3600),
        maxProbesPerCycle: z.number().int().min(1).max(255).default(31),
      })
      .default({
        maxActiveRows: 50_000,
        maxActiveBytes: 512 * 1024 * 1024,
        maxOldestAgeDays: 90,
        maxItemBytes: 256 * 1024,
        migrationBatchRows: 5_000,
        migrationBatchBytes: 32 * 1024 * 1024,
        maxBatchesPerCycle: 20,
        leaseSeconds: 120,
        requestTimeoutSeconds: 30,
        maxBackoffSeconds: 3600,
        maxProbesPerCycle: 31,
      }),
    policy: policyConfigSchema.default(DEFAULT_POLICY),
    repoContextDrain: z
      .object({
        enabled: z.boolean().default(false),
        scanSliceMs: z.number().int().min(1).max(50).default(50),
        maxContextsPerRun: z.number().int().min(1).max(64).default(64),
        maxDistinctCwdsPerRun: z.number().int().min(1).max(8).default(8),
        expireEnabled: z.boolean().default(false),
        expireAfterCompletePasses: z.number().int().min(2).max(64).default(2),
        expireLinksPerRun: z.number().int().min(1).max(256).default(256),
      })
      .default({
        enabled: false,
        scanSliceMs: 50,
        maxContextsPerRun: 64,
        maxDistinctCwdsPerRun: 8,
        expireEnabled: false,
        expireAfterCompletePasses: 2,
        expireLinksPerRun: 256,
      }),
  })
  .superRefine((config, context) => {
    if (config.policy.dataMode === "evidence") {
      context.addIssue({
        code: "custom",
        message:
          "Raw evidence mode is unavailable: the encrypted evidence vault is not implemented.",
        path: ["policy", "dataMode"],
      });
    }
  });

export type CollectorConfig = z.infer<typeof collectorConfigSchema>;

const CLOUD_DEVICE_ID_CONFLICT_INTERVAL_MS = 5 * 60 * 1_000;
const CONFIG_MUTATION_LOCK_WAIT_MS = 5_000;
const cloudDeviceIdConflictAt = new Map<string, number>();

export function isManagedOrUploadEnabled(config: CollectorConfig) {
  return (
    config.managed ||
    Boolean(config.uploadUrl || config.ingestKey || config.uploadSigningSecret) ||
    config.tenantId !== LOCAL_TENANT_ID ||
    config.installKey !== "local-dev"
  );
}

export function assertPrivacyEnvironment(
  context: string,
  env: NodeJS.ProcessEnv = process.env,
) {
  const requestedMode = env.PLIMSOLL_DATA_MODE?.trim().toLowerCase();
  if (requestedMode && !["metadata", "metadata_only"].includes(requestedMode)) {
    throw new CollectorPrivacyModeError(
      context,
      `PLIMSOLL_DATA_MODE requested ${JSON.stringify(requestedMode)}.`,
    );
  }
  const rawEnv = RAW_CAPTURE_ENV.find((name) => envEnablesRawCapture(name, env[name]));
  if (rawEnv) {
    throw new CollectorPrivacyModeError(context, `${rawEnv} requested raw capture.`);
  }
}

export function assertCollectorPrivacyMode(
  config: CollectorConfig,
  context: string,
  options: { willEnableUpload?: boolean; checkEnvironment?: boolean } = {},
) {
  if (options.checkEnvironment !== false) assertPrivacyEnvironment(context);
  if (config.policy.dataMode === "evidence") {
    throw new CollectorPrivacyModeError(context, "policy.dataMode=evidence is rejected.");
  }
  if (
    (options.willEnableUpload || isManagedOrUploadEnabled(config)) &&
    config.policy.dataMode !== "metadata"
  ) {
    throw new CollectorPrivacyModeError(
      context,
      `Managed/upload-enabled collectors require metadata_only; received ${config.policy.dataMode}.`,
    );
  }
  return config;
}

export function collectorPrivacyReadiness(config: CollectorConfig) {
  return {
    mode: PRIVACY_MODE,
    configuredDataMode: config.policy.dataMode,
    rawEvidenceCapture: "disabled" as const,
    evidenceVault: EVIDENCE_VAULT_STATE,
    legacyEvidenceDisposition: LEGACY_EVIDENCE_DISPOSITION,
    liveLedgerInspection: "not_performed" as const,
  };
}

export type CollectorConfigReadResult =
  | {
      status: "valid";
      path: string;
      config: CollectorConfig;
    }
  | {
      status: "missing" | "invalid";
      path: string;
      config: null;
    };

export function collectorHome(homeDir = os.homedir()) {
  // Issue #135: the single canonical resolver. A custom PLIMSOLL_HOME is
  // validated (absolute, user-owned, private, non-symlink) or the command
  // fails closed — it never silently falls back to the default home.
  return resolveCollectorHome({ homeDir }).home;
}

export function collectorConfigPath(homeDir = os.homedir()) {
  return path.join(collectorHome(homeDir), "collector.config.json");
}

export function collectorBufferPath(homeDir = os.homedir()) {
  return path.join(collectorHome(homeDir), "work-ledger.sqlite");
}

/** Local non-secret identity metadata; credentials never live in this file. */
export function collectorDeviceIdentityPath(homeDir = os.homedir()) {
  return path.join(collectorHome(homeDir), "device.identity.json");
}

export function collectorLogPath(name: string, homeDir = os.homedir()) {
  return path.join(collectorHome(homeDir), name);
}

export function ensureCollectorHome(homeDir = os.homedir()) {
  const directory = collectorHome(homeDir);
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  return directory;
}

export function saveCollectorConfig(config: CollectorConfig, homeDir = os.homedir()) {
  return writeCollectorConfigTransactionally(config, collectorConfigPath(homeDir));
}

function withCollectorConfigMutationLock<T>(configPath: string, action: () => T) {
  const directory = path.dirname(configPath);
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  const lockPath = path.join(directory, `.${path.basename(configPath)}.mutation.lock.sqlite`);
  try {
    fs.closeSync(fs.openSync(lockPath, "wx", 0o600));
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
  }
  const lockDatabase = new Database(lockPath, { timeout: CONFIG_MUTATION_LOCK_WAIT_MS });
  try {
    fs.chmodSync(lockPath, 0o600);
    lockDatabase.pragma(`busy_timeout = ${CONFIG_MUTATION_LOCK_WAIT_MS}`);
    try {
      lockDatabase.exec("BEGIN IMMEDIATE");
    } catch (error) {
      if ((error as { code?: string }).code === "SQLITE_BUSY") {
        throw new Error("collector_config_mutation_lock_timeout");
      }
      throw error;
    }
    let commit = false;
    try {
      const result = action();
      commit = true;
      return result;
    } finally {
      if (lockDatabase.inTransaction) {
        lockDatabase.exec(commit ? "COMMIT" : "ROLLBACK");
      }
    }
  } finally {
    lockDatabase.close();
  }
}

function writeCollectorConfigTransactionallyUnlocked(
  validated: CollectorConfig,
  configPath: string,
) {
  const directory = path.dirname(configPath);
  const temporaryPath = path.join(
    directory,
    `.collector.config-${process.pid}-${crypto.randomUUID()}.tmp`,
  );
  let descriptor: number | undefined;
  try {
    descriptor = fs.openSync(temporaryPath, "wx", 0o600);
    fs.writeFileSync(descriptor, `${JSON.stringify(validated, null, 2)}\n`);
    fs.fsyncSync(descriptor);
    fs.closeSync(descriptor);
    descriptor = undefined;
    fs.renameSync(temporaryPath, configPath);
    const directoryDescriptor = fs.openSync(directory, "r");
    try { fs.fsyncSync(directoryDescriptor); } finally { fs.closeSync(directoryDescriptor); }
  } finally {
    if (descriptor !== undefined) fs.closeSync(descriptor);
    fs.rmSync(temporaryPath, { force: true });
  }
  return validated;
}

/** Validate and atomically publish a complete collector config under its mutation lock. */
export function writeCollectorConfigTransactionally(
  config: CollectorConfig,
  configPath = collectorConfigPath(),
) {
  const validated = collectorConfigSchema.parse(config);
  assertCollectorPrivacyMode(validated, "config write");
  return withCollectorConfigMutationLock(configPath, () =>
    writeCollectorConfigTransactionallyUnlocked(validated, configPath),
  );
}

/**
 * Learn the hosted DeviceInstall UUID only from a validated ingest
 * acknowledgement. The active config file is authoritative: equal echoes are
 * no-ops, while a different stored value is never replaced or disclosed.
 */
export function reconcileCloudDeviceIdFromIngest(
  config: CollectorConfig,
  responseDeviceId: unknown,
  options: { homeDir?: string; now?: Date } = {},
) {
  const parsed = z.string().uuid().safeParse(responseDeviceId);
  if (!parsed.success) return "ignored" as const;
  const configPath = collectorConfigPath(options.homeDir);
  return withCollectorConfigMutationLock(configPath, () => {
    const current = readCollectorConfig(options.homeDir);
    if (current.status !== "valid") return "ignored" as const;
    const stored = current.config.cloudDeviceId;
    if (stored === parsed.data) {
      config.cloudDeviceId = stored;
      return "unchanged" as const;
    }
    if (stored) {
      const now = (options.now ?? new Date()).getTime();
      const last = cloudDeviceIdConflictAt.get(current.path) ?? Number.NEGATIVE_INFINITY;
      if (now - last >= CLOUD_DEVICE_ID_CONFLICT_INTERVAL_MS) {
        cloudDeviceIdConflictAt.set(current.path, now);
        console.warn(JSON.stringify({ status: "cloud_device_id_conflict" }));
      }
      return "conflict" as const;
    }
    const updated = collectorConfigSchema.parse({ ...current.config, cloudDeviceId: parsed.data });
    assertCollectorPrivacyMode(updated, "config write");
    writeCollectorConfigTransactionallyUnlocked(updated, current.path);
    config.cloudDeviceId = updated.cloudDeviceId;
    return "stored" as const;
  });
}

/**
 * Inspect the collector config without creating the Plimsoll home or a default
 * config. Diagnostic callers deliberately get a small status instead of parse
 * errors that could echo values from a malformed file.
 */
export function readCollectorConfig(homeDir = os.homedir()): CollectorConfigReadResult {
  const configPath = collectorConfigPath(homeDir);
  if (!fs.existsSync(configPath)) {
    return { status: "missing", path: configPath, config: null };
  }

  try {
    return {
      status: "valid",
      path: configPath,
      config: collectorConfigSchema.parse(JSON.parse(fs.readFileSync(configPath, "utf8"))),
    };
  } catch {
    return { status: "invalid", path: configPath, config: null };
  }
}

export function loadCollectorConfig(homeDir = os.homedir()): CollectorConfig {
  assertPrivacyEnvironment("config load");
  ensureCollectorHome(homeDir);
  const configPath = collectorConfigPath(homeDir);

  if (!fs.existsSync(configPath)) {
    const created = collectorConfigSchema.parse({});
    assertCollectorPrivacyMode(created, "default config write", { checkEnvironment: false });
    fs.writeFileSync(configPath, `${JSON.stringify(created, null, 2)}\n`, {
      mode: 0o600,
    });
    return created;
  }

  const parsed = collectorConfigSchema.parse(JSON.parse(fs.readFileSync(configPath, "utf8")));
  return assertCollectorPrivacyMode(parsed, "config load", { checkEnvironment: false });
}
