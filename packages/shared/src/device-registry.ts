import crypto from "node:crypto";

/**
 * Paired collector/hosted device registry contract.
 *
 * The registry stores identifiers and sanitized operational facts only. The
 * install/signing credentials are deliberately not part of this shape: the
 * authenticated server-side credential binding is the authority for tenant
 * and device authorization.
 */
export const DEVICE_REGISTRY_SCHEMA_VERSION = 1 as const;

export const DEVICE_STATUSES = [
  "pending",
  "active",
  "suspended",
  "revoked",
] as const;
export type DeviceStatus = (typeof DEVICE_STATUSES)[number];

export type DeviceRegistryEntry = {
  schemaVersion: typeof DEVICE_REGISTRY_SCHEMA_VERSION;
  deviceId: string;
  tenantId: string;
  keyId: string;
  appVersion: string;
  policyVersion: string;
  createdAt: string;
  lastSeenAt: string | null;
  lastUploadAt: string | null;
  queueAgeSeconds: number | null;
  status: DeviceStatus;
};

export type DeviceRegistry = {
  schemaVersion: typeof DEVICE_REGISTRY_SCHEMA_VERSION;
  devices: readonly DeviceRegistryEntry[];
};

export type DeviceRegistryResult =
  | {
      ok: true;
      registry: DeviceRegistry;
      entry: DeviceRegistryEntry;
      changed: boolean;
      reason?: DeviceRegistrySuccessReason;
    }
  | {
      ok: false;
      registry: DeviceRegistry;
      reason: string;
    };

type DeviceMetadata = Pick<
  DeviceRegistryEntry,
  "deviceId" | "tenantId" | "keyId" | "appVersion" | "policyVersion" | "createdAt"
>;

const DEVICE_ID_PATTERN = /^dev_[A-Za-z0-9][A-Za-z0-9._:-]{7,127}$/;
const KEY_ID_PATTERN = /^key_[A-Za-z0-9][A-Za-z0-9._:-]{3,127}$/;

type DeviceRegistrySuccessReason =
  | "created"
  | "activated"
  | "already_current"
  | "rotated"
  | "reassigned"
  | "suspended"
  | "revoked";

function requireText(value: string, field: string, pattern?: RegExp) {
  if (typeof value !== "string" || !value.trim() || (pattern && !pattern.test(value.trim()))) {
    throw new Error(`device_registry_invalid_${field}`);
  }
  return value.trim();
}

function requireTimestamp(value: string, field: string) {
  const normalized = requireText(value, field);
  if (Number.isNaN(Date.parse(normalized)) || !/(?:Z|[+-]\d{2}:\d{2})$/.test(normalized)) {
    throw new Error(`device_registry_invalid_${field}`);
  }
  return normalized;
}

function validateMetadata(input: DeviceMetadata): DeviceMetadata {
  return {
    deviceId: requireText(input.deviceId, "device_id", DEVICE_ID_PATTERN),
    tenantId: requireText(input.tenantId, "tenant_id"),
    keyId: requireText(input.keyId, "key_id", KEY_ID_PATTERN),
    appVersion: requireText(input.appVersion, "app_version"),
    policyVersion: requireText(input.policyVersion, "policy_version"),
    createdAt: requireTimestamp(input.createdAt, "created_at"),
  };
}

function cloneEntry(entry: DeviceRegistryEntry): DeviceRegistryEntry {
  return { ...entry };
}

function replaceEntry(registry: DeviceRegistry, entry: DeviceRegistryEntry): DeviceRegistry {
  return {
    schemaVersion: DEVICE_REGISTRY_SCHEMA_VERSION,
    devices: registry.devices.map((candidate) =>
      candidate.deviceId === entry.deviceId ? cloneEntry(entry) : cloneEntry(candidate),
    ),
  };
}

function result(
  registry: DeviceRegistry,
  entry: DeviceRegistryEntry,
  changed: boolean,
  reason: DeviceRegistrySuccessReason,
): DeviceRegistryResult {
  return { ok: true, registry, entry: cloneEntry(entry), changed, reason };
}

export function createDeviceRegistry(devices: readonly DeviceRegistryEntry[] = []): DeviceRegistry {
  const seen = new Set<string>();
  const normalized = devices.map((entry) => {
    if (entry.schemaVersion !== DEVICE_REGISTRY_SCHEMA_VERSION) {
      throw new Error("device_registry_invalid_schema_version");
    }
    if (seen.has(entry.deviceId)) throw new Error("device_registry_duplicate_device_id");
    seen.add(entry.deviceId);
    const metadata = validateMetadata(entry);
    if (!DEVICE_STATUSES.includes(entry.status)) throw new Error("device_registry_invalid_status");
    const lastSeenAt = entry.lastSeenAt === null || entry.lastSeenAt === undefined
      ? null
      : requireTimestamp(entry.lastSeenAt, "last_seen_at");
    const lastUploadAt = entry.lastUploadAt === null || entry.lastUploadAt === undefined
      ? null
      : requireTimestamp(entry.lastUploadAt, "last_upload_at");
    if (
      entry.queueAgeSeconds !== null &&
      (!Number.isSafeInteger(entry.queueAgeSeconds) || entry.queueAgeSeconds < 0)
    ) {
      throw new Error("device_registry_invalid_queue_age");
    }
    return {
      ...metadata,
      schemaVersion: DEVICE_REGISTRY_SCHEMA_VERSION,
      lastSeenAt,
      lastUploadAt,
      queueAgeSeconds: entry.queueAgeSeconds ?? null,
      status: entry.status,
    };
  });
  return { schemaVersion: DEVICE_REGISTRY_SCHEMA_VERSION, devices: normalized };
}

function findDevice(registry: DeviceRegistry, deviceId: string) {
  const normalized = requireText(deviceId, "device_id", DEVICE_ID_PATTERN);
  return registry.devices.find((entry) => entry.deviceId === normalized);
}

function notFound(registry: DeviceRegistry): DeviceRegistryResult {
  return { ok: false, registry, reason: "device_registry_device_not_found" };
}

export function enrollDevice(registry: DeviceRegistry, input: DeviceMetadata): DeviceRegistryResult {
  const metadata = validateMetadata(input);
  const existing = registry.devices.find((entry) => entry.deviceId === metadata.deviceId);
  if (existing) {
    if (existing.status === "revoked") return { ok: false, registry, reason: "device_registry_revoked_enroll" };
    if (
      existing.tenantId === metadata.tenantId &&
      existing.keyId === metadata.keyId &&
      existing.appVersion === metadata.appVersion &&
      existing.policyVersion === metadata.policyVersion
    ) {
      return result(registry, existing, false, "already_current");
    }
    return { ok: false, registry, reason: "device_registry_existing_identity_requires_rotation_or_reassign" };
  }
  const entry: DeviceRegistryEntry = {
    ...metadata,
    schemaVersion: DEVICE_REGISTRY_SCHEMA_VERSION,
    lastSeenAt: null,
    lastUploadAt: null,
    queueAgeSeconds: null,
    status: "pending",
  };
  return result(
    { schemaVersion: DEVICE_REGISTRY_SCHEMA_VERSION, devices: [...registry.devices.map(cloneEntry), entry] },
    entry,
    true,
    "created",
  );
}

export function activateDevice(registry: DeviceRegistry, deviceId: string, lastSeenAt: string): DeviceRegistryResult {
  const entry = findDevice(registry, deviceId);
  if (!entry) return notFound(registry);
  if (entry.status === "revoked") return { ok: false, registry, reason: "device_registry_revoked_activate" };
  const seen = requireTimestamp(lastSeenAt, "last_seen_at");
  if (entry.status === "active" && entry.lastSeenAt === seen) return result(registry, entry, false, "already_current");
  if (entry.status !== "pending" && entry.status !== "active") {
    return { ok: false, registry, reason: `device_registry_${entry.status}_cannot_activate` };
  }
  const updated = { ...entry, status: "active" as const, lastSeenAt: seen };
  return result(replaceEntry(registry, updated), updated, true, "activated");
}

export function rotateDeviceKey(
  registry: DeviceRegistry,
  deviceId: string,
  keyId: string,
  at: string,
): DeviceRegistryResult {
  const entry = findDevice(registry, deviceId);
  if (!entry) return notFound(registry);
  if (entry.status === "revoked") return { ok: false, registry, reason: "device_registry_revoked_rotate" };
  const nextKeyId = requireText(keyId, "key_id", KEY_ID_PATTERN);
  const timestamp = requireTimestamp(at, "last_seen_at");
  if (entry.keyId === nextKeyId && entry.status === "pending") return result(registry, entry, false, "already_current");
  const updated = { ...entry, keyId: nextKeyId, status: "pending" as const, lastSeenAt: timestamp };
  return result(replaceEntry(registry, updated), updated, true, "rotated");
}

export function leaveDevice(registry: DeviceRegistry, deviceId: string, at: string): DeviceRegistryResult {
  const entry = findDevice(registry, deviceId);
  if (!entry) return notFound(registry);
  if (entry.status === "revoked") return { ok: false, registry, reason: "device_registry_revoked_leave" };
  const timestamp = requireTimestamp(at, "last_seen_at");
  if (entry.status === "suspended") return result(registry, entry, false, "already_current");
  const updated = { ...entry, status: "suspended" as const, lastSeenAt: timestamp };
  return result(replaceEntry(registry, updated), updated, true, "suspended");
}

export function revokeDevice(registry: DeviceRegistry, deviceId: string, at: string): DeviceRegistryResult {
  const entry = findDevice(registry, deviceId);
  if (!entry) return notFound(registry);
  const timestamp = requireTimestamp(at, "last_seen_at");
  if (entry.status === "revoked") return result(registry, entry, false, "already_current");
  const updated = { ...entry, status: "revoked" as const, lastSeenAt: timestamp };
  return result(replaceEntry(registry, updated), updated, true, "revoked");
}

export function previewDeviceReassignment(registry: DeviceRegistry, deviceId: string, toTenantId: string) {
  const entry = findDevice(registry, deviceId);
  if (!entry) return { ok: false as const, reason: "device_registry_device_not_found" };
  const to = requireText(toTenantId, "tenant_id");
  if (entry.status === "revoked") return { ok: false as const, reason: "device_registry_revoked_reassign" };
  return {
    ok: true as const,
    deviceId: entry.deviceId,
    fromTenantId: entry.tenantId,
    toTenantId: to,
    requiresConfirmation: entry.tenantId !== to,
  };
}

export function reassignDevice(
  registry: DeviceRegistry,
  deviceId: string,
  toTenantId: string,
  options: { confirmed?: boolean; at: string },
): DeviceRegistryResult {
  const entry = findDevice(registry, deviceId);
  if (!entry) return notFound(registry);
  if (entry.status === "revoked") return { ok: false, registry, reason: "device_registry_revoked_reassign" };
  const to = requireText(toTenantId, "tenant_id");
  const at = requireTimestamp(options.at, "last_seen_at");
  if (entry.tenantId === to) return result(registry, entry, false, "already_current");
  if (options.confirmed !== true) return { ok: false, registry, reason: "device_registry_reassign_confirmation_required" };
  const updated = {
    ...entry,
    tenantId: to,
    status: "pending" as const,
    lastSeenAt: at,
    lastUploadAt: null,
    queueAgeSeconds: null,
  };
  return result(replaceEntry(registry, updated), updated, true, "reassigned");
}

/** A reinstall gets a new identity; it never changes a revoked identity back. */
export function reinstallDevice(
  registry: DeviceRegistry,
  oldDeviceId: string,
  input: DeviceMetadata,
  at: string,
): DeviceRegistryResult {
  const old = findDevice(registry, oldDeviceId);
  if (!old) return notFound(registry);
  const metadata = validateMetadata(input);
  if (metadata.deviceId === old.deviceId) {
    return { ok: false, registry, reason: "device_registry_reinstall_requires_new_identity" };
  }
  const existingNewIdentity = registry.devices.find((entry) => entry.deviceId === metadata.deviceId);
  if (existingNewIdentity) {
    if (
      old.status === "revoked" &&
      existingNewIdentity.status !== "revoked" &&
      existingNewIdentity.tenantId === metadata.tenantId &&
      existingNewIdentity.keyId === metadata.keyId &&
      existingNewIdentity.appVersion === metadata.appVersion &&
      existingNewIdentity.policyVersion === metadata.policyVersion &&
      existingNewIdentity.createdAt === metadata.createdAt
    ) {
      return result(registry, existingNewIdentity, false, "already_current");
    }
    return {
      ok: false,
      registry,
      reason: existingNewIdentity.status === "revoked"
        ? "device_registry_revoked_enroll"
        : "device_registry_existing_identity_requires_rotation_or_reassign",
    };
  }
  if (old.status !== "revoked") {
    const revoked = revokeDevice(registry, old.deviceId, at);
    if (!revoked.ok) return revoked;
    registry = revoked.registry;
  }
  return enrollDevice(registry, metadata);
}

export type DeviceTelemetry = {
  lastSeenAt?: string | null;
  lastUploadAt?: string | null;
  queueAgeSeconds?: number | null;
};

export type DeviceRegistryView = DeviceRegistryEntry;

export function renderDeviceRegistry(input: {
  registry: DeviceRegistry;
  telemetry?: Readonly<Record<string, DeviceTelemetry>>;
  now?: string;
}): DeviceRegistryView[] {
  if (input.now !== undefined) requireTimestamp(input.now, "now");
  return input.registry.devices
    .slice()
    .sort((left, right) => left.deviceId.localeCompare(right.deviceId))
    .map((entry) => {
      const telemetry = input.telemetry?.[entry.deviceId];
      const lastSeenAt = telemetry?.lastSeenAt ?? entry.lastSeenAt;
      const lastUploadAt = telemetry?.lastUploadAt ?? entry.lastUploadAt;
      const queueAgeSeconds = telemetry?.queueAgeSeconds ?? entry.queueAgeSeconds;
      if (lastSeenAt !== null && lastSeenAt !== undefined) requireTimestamp(lastSeenAt, "last_seen_at");
      if (lastUploadAt !== null && lastUploadAt !== undefined) requireTimestamp(lastUploadAt, "last_upload_at");
      if (queueAgeSeconds !== null && queueAgeSeconds !== undefined && (!Number.isSafeInteger(queueAgeSeconds) || queueAgeSeconds < 0)) {
        throw new Error("device_registry_invalid_queue_age");
      }
      return { ...entry, lastSeenAt: lastSeenAt ?? null, lastUploadAt: lastUploadAt ?? null, queueAgeSeconds: queueAgeSeconds ?? null };
    });
}

/** Stable inside one tenant; a different tenant gets a different digest. */
export function tenantScopedActorPseudonym(actorIdentifier: string, tenantId: string) {
  const actor = requireText(actorIdentifier, "actor_identifier");
  const tenant = requireText(tenantId, "tenant_id");
  return `sha256:${crypto.createHash("sha256").update(`plimsoll-actor:v1\0${tenant}\0${actor}`).digest("hex")}`;
}
