import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { collectorDeviceIdentityPath, ensureCollectorHome } from "./config";

export const DEVICE_IDENTITY_SCHEMA_VERSION = 1 as const;
export type LocalDeviceStatus = "pending" | "active" | "suspended" | "revoked";

export type LocalDeviceIdentity = {
  version: typeof DEVICE_IDENTITY_SCHEMA_VERSION;
  deviceId: string;
  keyId: string;
  createdAt: string;
  lastSeenAt: string | null;
  lastUploadAt: string | null;
  status: LocalDeviceStatus;
};

const DEVICE_ID_PATTERN = /^dev_[A-Za-z0-9][A-Za-z0-9._:-]{7,127}$/;
const KEY_ID_PATTERN = /^key_[A-Za-z0-9][A-Za-z0-9._:-]{3,127}$/;

function timestamp(value: string, field: string) {
  if (
    typeof value !== "string" ||
    Number.isNaN(Date.parse(value)) ||
    !/(?:Z|[+-]\d{2}:\d{2})$/.test(value)
  ) {
    throw new Error(`device_identity_invalid_${field}`);
  }
  return value;
}

function parseIdentity(value: unknown): LocalDeviceIdentity {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("device_identity_invalid_document");
  }
  const input = value as Record<string, unknown>;
  if (
    input.version !== DEVICE_IDENTITY_SCHEMA_VERSION ||
    typeof input.deviceId !== "string" ||
    !DEVICE_ID_PATTERN.test(input.deviceId) ||
    typeof input.keyId !== "string" ||
    !KEY_ID_PATTERN.test(input.keyId) ||
    typeof input.createdAt !== "string" ||
    !["pending", "active", "suspended", "revoked"].includes(String(input.status))
  ) {
    throw new Error("device_identity_invalid_document");
  }
  const lastSeenAt = input.lastSeenAt === null ? null : timestamp(String(input.lastSeenAt), "last_seen_at");
  const lastUploadAt = input.lastUploadAt === null ? null : timestamp(String(input.lastUploadAt), "last_upload_at");
  return {
    version: DEVICE_IDENTITY_SCHEMA_VERSION,
    deviceId: input.deviceId,
    keyId: input.keyId,
    createdAt: timestamp(input.createdAt, "created_at"),
    lastSeenAt,
    lastUploadAt,
    status: input.status as LocalDeviceStatus,
  };
}

function writeIdentity(file: string, identity: LocalDeviceIdentity) {
  fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
  const temporary = `${file}.${process.pid}.${crypto.randomUUID()}.tmp`;
  try {
    fs.writeFileSync(temporary, `${JSON.stringify(identity, null, 2)}\n`, {
      flag: "wx",
      mode: 0o600,
    });
    fs.renameSync(temporary, file);
  } finally {
    fs.rmSync(temporary, { force: true });
  }
}

function generatedIdentity(now = new Date()): LocalDeviceIdentity {
  const stamp = now.toISOString();
  return {
    version: DEVICE_IDENTITY_SCHEMA_VERSION,
    deviceId: `dev_${crypto.randomUUID()}`,
    keyId: `key_${crypto.randomUUID()}`,
    createdAt: stamp,
    lastSeenAt: null,
    lastUploadAt: null,
    status: "pending",
  };
}

/**
 * Load the one stable identity for this collector home, creating it once.
 * This file contains no bearer credential and is never copied from another
 * machine. A malformed existing identity is a hard error, not a reset.
 */
export function loadOrCreateDeviceIdentity(
  homeDir = os.homedir(),
  options: { seed?: Partial<Pick<LocalDeviceIdentity, "deviceId" | "keyId">>; now?: Date } = {},
) {
  const file = collectorDeviceIdentityPath(homeDir);
  if (fs.existsSync(file)) return parseIdentity(JSON.parse(fs.readFileSync(file, "utf8")));
  const generated = generatedIdentity(options.now);
  const seeded = {
    ...generated,
    ...(options.seed?.deviceId ? { deviceId: options.seed.deviceId } : {}),
    ...(options.seed?.keyId ? { keyId: options.seed.keyId } : {}),
  };
  const identity = parseIdentity(seeded);
  ensureCollectorHome(homeDir);
  writeIdentity(file, identity);
  return identity;
}

export function readDeviceIdentity(homeDir = os.homedir()) {
  const file = collectorDeviceIdentityPath(homeDir);
  if (!fs.existsSync(file)) return null;
  return parseIdentity(JSON.parse(fs.readFileSync(file, "utf8")));
}

/** Restore the exact pre-transaction identity state after activation fails. */
export function restoreDeviceIdentity(
  homeDir: string,
  identity: LocalDeviceIdentity | null,
) {
  const file = collectorDeviceIdentityPath(homeDir);
  if (identity === null) {
    fs.rmSync(file, { force: true });
    return null;
  }
  writeIdentity(file, identity);
  return identity;
}

function updateIdentity(
  homeDir: string,
  update: (identity: LocalDeviceIdentity) => LocalDeviceIdentity,
) {
  const current = loadOrCreateDeviceIdentity(homeDir);
  const next = parseIdentity(update(current));
  if (current.status === "revoked" && next.status !== "revoked") {
    throw new Error("device_identity_revoked_is_terminal");
  }
  writeIdentity(collectorDeviceIdentityPath(homeDir), next);
  return next;
}

export function recordDeviceSeen(homeDir = os.homedir(), at = new Date()) {
  const seenAt = timestamp(at.toISOString(), "last_seen_at");
  return updateIdentity(homeDir, (identity) => ({ ...identity, lastSeenAt: seenAt }));
}

export function recordDeviceUpload(homeDir = os.homedir(), at = new Date()) {
  const uploadedAt = timestamp(at.toISOString(), "last_upload_at");
  return updateIdentity(homeDir, (identity) => ({ ...identity, lastUploadAt: uploadedAt }));
}

export function setDeviceKey(
  homeDir = os.homedir(),
  keyId: string,
  at = new Date(),
) {
  if (!KEY_ID_PATTERN.test(keyId)) throw new Error("device_identity_invalid_key_id");
  if (loadOrCreateDeviceIdentity(homeDir).status === "revoked") {
    throw new Error("device_identity_revoked_is_terminal");
  }
  const changedAt = timestamp(at.toISOString(), "last_seen_at");
  return updateIdentity(homeDir, (identity) => ({
    ...identity,
    keyId,
    lastSeenAt: changedAt,
  }));
}

export function setDeviceStatus(
  homeDir = os.homedir(),
  status: LocalDeviceStatus,
  at = new Date(),
) {
  const changedAt = timestamp(at.toISOString(), "last_seen_at");
  return updateIdentity(homeDir, (identity) => ({
    ...identity,
    status: identity.status === "revoked" ? "revoked" : status,
    lastSeenAt: changedAt,
  }));
}
