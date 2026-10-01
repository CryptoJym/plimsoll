import { execFileSync } from "node:child_process";
import os from "node:os";

import type Database from "better-sqlite3";

import { captureBaselineStatus } from "./capture-baseline";
import type { CollectorConfig } from "./config";
import { authenticatedJsonPost, validatedTransportUrl, type JsonPostResult } from "./http-transport";
import { PLIMSOLL_VERSION } from "./version";

/** The optional, non-event collector contact endpoint introduced in 0.7.46. */
export const INSTALL_CONTACT_PATH = "/api/work-intelligence/install-contact";
export const INSTALL_CONTACT_INTERVAL_MS = 15 * 60_000;
export const INSTALL_CONTACT_FRESH_MS = 60 * 60_000;
export const INSTALL_CONTACT_MAX_BACKOFF_MS = 60 * 60_000;

export type CaptureContactState = "covered" | "fault" | "gap_open";

/** The complete bounded contact wire. No path, account, serial, or user field is permitted. */
export const INSTALL_CONTACT_WIRE_FIELDS = [
  "tenantId", "deviceId", "installKey", "appVersion", "lastActivityAt", "captureState", "machineName",
] as const;

/** macOS computer name, intentionally collected with a read-only command. */
export function reportedMachineName(options: {
  platform?: string;
  hostname?: string;
  runScutil?: () => string;
} = {}): string | undefined {
  const platform = options.platform ?? process.platform;
  let value = "";
  let usedHostnameFallback = false;
  if (platform === "darwin") {
    try {
      value = options.runScutil
        ? options.runScutil()
        : execFileSync("scutil", ["--get", "ComputerName"], {
            encoding: "utf8", stdio: ["ignore", "pipe", "ignore"], timeout: 500,
          });
    } catch {
      value = "";
    }
  }
  if (!value.trim()) {
    value = options.hostname ?? os.hostname();
    usedHostnameFallback = true;
  }
  if (usedHostnameFallback) value = value.replace(/\.local$/i, "");
  const cleaned = Array.from(value.replace(/[\u0000-\u001f\u007f]/g, "").trim())
    .slice(0, 64).join("");
  if (cleaned) return cleaned;
  return undefined;
}

/** A hostname fallback must not expose the conventional mDNS suffix. */
export function fallbackMachineName(hostname = os.hostname()): string | undefined {
  return reportedMachineName({ platform: "other", hostname });
}

export type InstallContactPayload = {
  tenantId: string;
  deviceId: string;
  installKey: string;
  appVersion: string;
  lastActivityAt: string | null;
  captureState: CaptureContactState;
  machineName?: string;
};

function contactEndpoint(config: CollectorConfig): string | null {
  if (config.installContactEndpoint) return config.installContactEndpoint;
  if (!config.uploadUrl) return null;
  const endpoint = validatedTransportUrl(config.uploadUrl, "Configured upload URL");
  endpoint.pathname = INSTALL_CONTACT_PATH;
  endpoint.search = "";
  endpoint.hash = "";
  return endpoint.toString();
}

function latestActivityAt(database: Database.Database): string | null {
  try {
    const row = database.prepare("select max(observed_at) as at from buffered_events").get() as { at?: unknown } | undefined;
    return typeof row?.at === "string" && Number.isFinite(Date.parse(row.at)) ? new Date(row.at).toISOString() : null;
  } catch {
    return null;
  }
}

function captureState(database: Database.Database): CaptureContactState {
  try {
    const status = captureBaselineStatus(database);
    if (status.status === "complete") return "covered";
    const hasGap = status.sources.some((source) => source.unresolvedObservationErrors > 0 ||
      /gap|unresolved|missing|continuity/i.test(source.reason ?? ""));
    if (hasGap) return "gap_open";
    return "fault";
  } catch {
    return "fault";
  }
}

export function buildInstallContactPayload(
  config: CollectorConfig,
  database: Database.Database,
  options: { appVersion?: string; now?: Date; machineName?: string } = {},
): InstallContactPayload {
  const payload: InstallContactPayload = {
    tenantId: config.tenantId,
    deviceId: config.cloudDeviceId ?? config.deviceId ?? "",
    installKey: config.installKey,
    appVersion: options.appVersion ?? PLIMSOLL_VERSION,
    lastActivityAt: latestActivityAt(database),
    captureState: captureState(database),
  };
  if (config.reportMachineName && options.machineName) payload.machineName = options.machineName;
  return payload;
}

export type InstallContactResult =
  | { kind: "accepted"; response: JsonPostResult }
  | { kind: "not_available"; response: JsonPostResult }
  | { kind: "failed"; error: unknown };

/**
 * Send one authenticated contact. A 404 is a capability response from an
 * older cloud and is deliberately distinct from a transient failure.
 */
export async function postInstallContact(input: {
  config: CollectorConfig;
  payload: InstallContactPayload;
  endpoint?: string;
  fetchImpl?: typeof fetch;
  now?: () => Date;
}): Promise<InstallContactResult> {
  const endpoint = input.endpoint ?? contactEndpoint(input.config);
  if (!endpoint || !input.config.deviceId || !input.config.cloudDeviceId) {
    return { kind: "failed", error: new Error("install_contact_missing_binding") };
  }
  const body = JSON.stringify(input.payload);
  try {
    const response = await authenticatedJsonPost({
      url: endpoint,
      body,
      installKey: input.config.installKey,
      ingestKey: input.config.ingestKey,
      signingSecret: input.config.uploadSigningSecret,
      now: input.now,
      fetchImpl: input.fetchImpl,
      timeoutMs: input.config.delivery.requestTimeoutSeconds * 1_000,
      maxRequestBytes: 64 * 1024,
    });
    if (response.status === 404) return { kind: "not_available", response };
    if (response.ok) return { kind: "accepted", response };
    return { kind: "failed", error: new Error(`install_contact_http_${response.status}`) };
  } catch (error) {
    return { kind: "failed", error };
  }
}

export type InstallContactScheduler = {
  stop: () => void;
  disabled: () => boolean;
};

/**
 * Owns only its contact timer. It never runs on the upload promise and a
 * failed request schedules a later retry; 404 disables this scheduler until
 * the daemon is restarted.
 */
export function startInstallContactScheduler(input: {
  config: CollectorConfig;
  database: Database.Database;
  appVersion?: string;
  fetchImpl?: typeof fetch;
  machineName?: string;
  now?: () => Date;
  setTimer?: (callback: () => void, delayMs: number) => ReturnType<typeof setTimeout>;
  clearTimer?: (timer: ReturnType<typeof setTimeout>) => void;
}): InstallContactScheduler {
  let timer: ReturnType<typeof setTimeout> | undefined;
  let stopped = false;
  let unavailable = false;
  let failures = 0;
  const setTimer = input.setTimer ?? ((callback, delay) => setTimeout(callback, delay));
  const clearTimer = input.clearTimer ?? ((handle) => clearTimeout(handle));
  const schedule = (delay: number) => {
    if (stopped || unavailable) return;
    timer = setTimer(() => { timer = undefined; void tick(); }, delay);
    timer.unref?.();
  };
  const tick = async () => {
    if (stopped || unavailable) return;
    const payload = buildInstallContactPayload(input.config, input.database, {
      appVersion: input.appVersion,
      machineName: input.machineName ?? (input.config.reportMachineName ? reportedMachineName() : undefined),
    });
    const result = await postInstallContact({
      config: input.config, payload, fetchImpl: input.fetchImpl, now: input.now,
    });
    if (stopped) return;
    if (result.kind === "not_available") {
      unavailable = true;
      return;
    }
    if (result.kind === "accepted") {
      failures = 0;
      schedule(INSTALL_CONTACT_INTERVAL_MS);
      return;
    }
    failures += 1;
    const delay = Math.min(INSTALL_CONTACT_MAX_BACKOFF_MS,
      Math.max(60_000, INSTALL_CONTACT_INTERVAL_MS * 2 ** Math.min(failures, 6)));
    schedule(delay);
  };
  void tick();
  return {
    stop: () => {
      stopped = true;
      if (timer !== undefined) clearTimer(timer);
      timer = undefined;
    },
    disabled: () => unavailable,
  };
}
