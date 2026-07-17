import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { z } from "zod";

import { DEFAULT_POLICY, LOCAL_TENANT_ID, policyConfigSchema } from "../../shared/src/index";

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
    tenantId: z.string().trim().min(1).default(LOCAL_TENANT_ID),
    installKey: z.string().trim().min(1).default("local-dev"),
    /** Explicit fleet-management marker. Older joined configs are also
     * recognized from their upload/tenant/install credentials. */
    managed: z.boolean().default(false),
    retentionDays: z.number().int().min(1).max(3650).default(90),
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
  if (process.env.PLIMSOLL_HOME) return process.env.PLIMSOLL_HOME;
  return path.join(homeDir, "Library", "Application Support", "Plimsoll");
}

export function collectorConfigPath(homeDir = os.homedir()) {
  return path.join(collectorHome(homeDir), "collector.config.json");
}

export function collectorBufferPath(homeDir = os.homedir()) {
  return path.join(collectorHome(homeDir), "work-ledger.sqlite");
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
  const validated = collectorConfigSchema.parse(config);
  assertCollectorPrivacyMode(validated, "config write");
  ensureCollectorHome(homeDir);
  fs.writeFileSync(collectorConfigPath(homeDir), `${JSON.stringify(validated, null, 2)}\n`, {
    mode: 0o600,
  });
  return validated;
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
