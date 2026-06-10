import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { z } from "zod";

import { DEFAULT_POLICY, LOCAL_TENANT_ID, policyConfigSchema } from "../../shared/src/index";

export const DEFAULT_COLLECTOR_PORT = 48271;

export const collectorConfigSchema = z.object({
  port: z.number().int().min(1024).max(65535).default(DEFAULT_COLLECTOR_PORT),
  ingestKey: z.string().trim().min(1).optional(),
  uploadSigningSecret: z.string().trim().min(16).optional(),
  uploadUrl: z.string().url().optional(),
  tenantId: z.string().trim().min(1).default(LOCAL_TENANT_ID),
  installKey: z.string().trim().min(1).default("local-dev"),
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
  policy: policyConfigSchema.default(DEFAULT_POLICY),
});

export type CollectorConfig = z.infer<typeof collectorConfigSchema>;

export function collectorHome(homeDir = os.homedir()) {
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

export function loadCollectorConfig(homeDir = os.homedir()): CollectorConfig {
  ensureCollectorHome(homeDir);
  const configPath = collectorConfigPath(homeDir);

  if (!fs.existsSync(configPath)) {
    const created = collectorConfigSchema.parse({});
    fs.writeFileSync(configPath, `${JSON.stringify(created, null, 2)}\n`, {
      mode: 0o600,
    });
    return created;
  }

  return collectorConfigSchema.parse(
    JSON.parse(fs.readFileSync(configPath, "utf8")),
  );
}
