import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { z } from "zod";

import collectorPackage from "../package.json";
import { LocalEventBuffer } from "./buffer";
import {
  collectorConfigPath,
  collectorConfigSchema,
  type CollectorConfig,
} from "./config";
import { appendForwardedHook } from "./forwarder";
import { uploadBufferedEvents } from "./upload";

/**
 * Fleet join is transactional: redeem into memory, prove only a fresh
 * synthetic event in an isolated ledger, then atomically activate the staged
 * config. The active ledger and config are never part of the handshake.
 */
export const CLOUD_JOIN_PATH = "/api/work-intelligence/join";
export const COLLECTOR_APP_VERSION = collectorPackage.version;

export type JoinTarget = { token: string; baseUrl: string | null };

/**
 * Accepts the share-ready form the admin copies — `https://cloud.example#pljt_…`
 * (the token rides the URL FRAGMENT, so it never appears in server logs) — or
 * a bare `pljt_…` token plus an explicit base URL (--url / PLIMSOLL_CLOUD_URL).
 */
export function parseJoinTarget(raw: string, explicitBaseUrl?: string): JoinTarget {
  const trimmed = raw.trim();
  if (/^https?:\/\//i.test(trimmed)) {
    const url = new URL(trimmed);
    return {
      token: url.hash.replace(/^#/, "").trim(),
      baseUrl: explicitBaseUrl?.trim() || url.origin,
    };
  }
  return { token: trimmed, baseUrl: explicitBaseUrl?.trim() || null };
}

/** The grant replaces the complete hosted credential set, including absence. */
const joinGrantSchema = z.object({
  ok: z.literal(true),
  tenantId: z.string().trim().min(1),
  installKey: z.string().trim().min(1),
  uploadUrl: z.string().url(),
  ingestKey: z.string().trim().min(1).optional(),
  uploadSigningSecret: z.string().trim().min(16).optional(),
});

export const JOIN_REFUSAL_MESSAGES: Record<string, string> = {
  malformed:
    "That join token is malformed (expected pljt_…). Re-copy the join URL from your workspace admin.",
  unknown:
    "The server does not recognize this join token. Check you are pointing at the right workspace URL, or ask your admin for a fresh token.",
  used: "This join token was already used — join tokens are single-use. Ask your workspace admin to mint a new one.",
  expired:
    "This join token has expired — they only live ~30 minutes. Ask your workspace admin to mint a new one.",
  revoked: "This join token was revoked by your workspace admin.",
  signing_unconfigured:
    "The workspace server requires signed uploads but has no signing secret configured. Your token was NOT consumed — ask the workspace owner to fix the server, then retry.",
};

export type JoinResult =
  | {
      joined: true;
      configPath: string;
      tenantId: string;
      uploadUrl: string;
      uploadSigningConfigured: boolean;
      handshake: {
        uploadedEvents: 1;
        selfTestEventId: string;
        signedUpload: boolean;
        response: unknown;
      };
    }
  | {
      joined: false;
      reason: string;
      message: string;
      httpStatus: number;
      configTouched: false;
    };

function isLoopbackHostname(hostname: string) {
  const normalized = hostname.toLowerCase().replace(/^\[|\]$/g, "");
  return (
    normalized === "localhost" ||
    normalized.endsWith(".localhost") ||
    normalized === "::1" ||
    /^127(?:\.\d{1,3}){3}$/.test(normalized)
  );
}

function validatedTransportUrl(raw: string, label: string) {
  let url: URL;
  try {
    url = new URL(raw);
  } catch {
    throw new Error(`${label} must be a valid absolute URL.`);
  }
  if (url.username || url.password) {
    throw new Error(`${label} must not contain embedded credentials.`);
  }
  if (url.protocol === "https:") return url;
  if (url.protocol === "http:" && isLoopbackHostname(url.hostname)) return url;
  throw new Error(`${label} must use HTTPS (HTTP is allowed only for an explicit loopback development URL).`);
}

function assertNoRedirect(response: Response, label: string, expectedOrigin: string) {
  if (response.redirected || (response.status >= 300 && response.status < 400)) {
    throw new Error(`${label} redirects are rejected.`);
  }
  if (response.url && new URL(response.url).origin !== expectedOrigin) {
    throw new Error(`${label} response escaped its authenticated origin.`);
  }
}

function readConfigWithoutCreating(configPath: string): CollectorConfig {
  if (!fs.existsSync(configPath)) return collectorConfigSchema.parse({});
  return collectorConfigSchema.parse(JSON.parse(fs.readFileSync(configPath, "utf8")));
}

function stageGrant(existing: CollectorConfig, grant: z.infer<typeof joinGrantSchema>) {
  // Credential absence is meaningful. Destructure every stale credential out
  // before applying the grant so a rejoin cannot inherit an old secret/key.
  const {
    ingestKey: _staleIngestKey,
    installKey: _staleInstallKey,
    tenantId: _staleTenantId,
    uploadSigningSecret: _staleSigningSecret,
    uploadUrl: _staleUploadUrl,
    ...localSettings
  } = existing;
  return collectorConfigSchema.parse({
    ...localSettings,
    tenantId: grant.tenantId,
    installKey: grant.installKey,
    uploadUrl: grant.uploadUrl,
    ...(grant.ingestKey ? { ingestKey: grant.ingestKey } : {}),
    ...(grant.uploadSigningSecret
      ? { uploadSigningSecret: grant.uploadSigningSecret }
      : {}),
  });
}

function activateConfigAtomically(config: CollectorConfig, configPath: string) {
  const directory = path.dirname(configPath);
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  const temporaryPath = path.join(
    directory,
    `.collector.config.join-${process.pid}-${crypto.randomUUID()}.tmp`,
  );
  try {
    fs.writeFileSync(temporaryPath, `${JSON.stringify(config, null, 2)}\n`, {
      flag: "wx",
      mode: 0o600,
    });
    fs.renameSync(temporaryPath, configPath);
  } finally {
    fs.rmSync(temporaryPath, { force: true });
  }
}

function inputUrl(input: Parameters<typeof fetch>[0]) {
  if (typeof input === "string") return new URL(input);
  if (input instanceof URL) return new URL(input.href);
  return new URL(input.url);
}

export async function performJoin(options: {
  target: string;
  baseUrl?: string;
  appVersion?: string;
  homeDir?: string;
  fetchImpl?: typeof fetch;
  /** Test/proof seam only; the temporary ledger is still deleted on every path. */
  temporaryRoot?: string;
}): Promise<JoinResult> {
  const { token, baseUrl } = parseJoinTarget(options.target, options.baseUrl);
  if (!token) {
    throw new Error(
      'No token found. Use the full join URL from your admin ("https://…#pljt_…") or provide it with --token-stdin.',
    );
  }
  if (!baseUrl) {
    throw new Error(
      'No workspace URL. Use the full join URL from your admin ("https://…#pljt_…"), or pass --url <cloud-base-url> (or set PLIMSOLL_CLOUD_URL).',
    );
  }

  const fetchImpl = options.fetchImpl ?? fetch;
  const homeDir = options.homeDir ?? os.homedir();
  const appVersion = options.appVersion ?? COLLECTOR_APP_VERSION;
  const activeConfigPath = collectorConfigPath(homeDir);
  const existingConfig = readConfigWithoutCreating(activeConfigPath);
  const joinBase = validatedTransportUrl(baseUrl, "Workspace join URL");
  const joinUrl = new URL(CLOUD_JOIN_PATH, joinBase.origin);

  const response = await fetchImpl(joinUrl, {
    method: "POST",
    redirect: "manual",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      token,
      platform: process.platform === "darwin" ? "macos" : process.platform,
      appVersion,
    }),
  });
  assertNoRedirect(response, "Workspace join", joinUrl.origin);
  const body = (await response.json().catch(() => ({}))) as Record<string, unknown>;

  if (!response.ok) {
    // Never include an untrusted response body: it may contain token or grant
    // material. Refusals leave the active config byte-for-byte untouched.
    const reason = typeof body.reason === "string" ? body.reason : "unknown_error";
    return {
      joined: false,
      reason,
      message:
        JOIN_REFUSAL_MESSAGES[reason] ??
        `Join refused with HTTP ${response.status} (${reason}).`,
      httpStatus: response.status,
      configTouched: false,
    };
  }

  const grant = joinGrantSchema.parse(body);
  const uploadUrl = validatedTransportUrl(grant.uploadUrl, "Granted upload URL");
  if (uploadUrl.origin !== joinUrl.origin) {
    throw new Error("Granted upload URL must use the same origin as the workspace join URL.");
  }
  const stagedConfig = stageGrant(existingConfig, grant);

  // The active ledger is deliberately unreachable from this handshake. A
  // fresh, one-event SQLite ledger proves the grant and is then deleted.
  const temporaryParent = options.temporaryRoot ?? os.tmpdir();
  fs.mkdirSync(temporaryParent, { recursive: true, mode: 0o700 });
  const temporaryDirectory = fs.mkdtempSync(path.join(temporaryParent, "plimsoll-join-handshake-"));
  const temporaryLedgerPath = path.join(temporaryDirectory, "handshake.sqlite");
  let buffer: LocalEventBuffer | undefined;
  let selfTestEventId = "";
  try {
    buffer = new LocalEventBuffer(temporaryLedgerPath, {
      delivery: { enabled: true, limits: stagedConfig.delivery },
    });
    const normalized = appendForwardedHook(
      {
        id: `join_handshake_${crypto.randomUUID()}`,
        source: "claude_code",
        event_type: "UserPromptSubmit",
      },
      { config: stagedConfig, buffer, source: "claude_code" },
    );
    selfTestEventId = normalized.event.id;

    const handshakeFetch = (async (input, init) => {
      const requestedUrl = inputUrl(input);
      if (requestedUrl.href !== uploadUrl.href) {
        throw new Error("Handshake upload attempted an unexpected URL.");
      }
      const uploadResponse = await fetchImpl(input, { ...init, redirect: "manual" });
      assertNoRedirect(uploadResponse, "Handshake upload", uploadUrl.origin);
      return uploadResponse;
    }) as typeof fetch;

    const uploaded = await uploadBufferedEvents(stagedConfig, buffer, {
      appVersion,
      fetchImpl: handshakeFetch,
    });
    const uploadedEvent = uploaded.batch?.events[0]?.event.id;
    const accepted =
      uploaded.response && typeof uploaded.response === "object"
        ? (uploaded.response as { accepted?: unknown }).accepted
        : undefined;
    if (
      uploaded.uploadedEvents !== 1 ||
      uploaded.batch?.events.length !== 1 ||
      uploadedEvent !== selfTestEventId ||
      (accepted !== true && accepted !== 1)
    ) {
      throw new Error("Join handshake did not explicitly acknowledge exactly its one synthetic probe.");
    }

    // This is the only active-config mutation in the join flow, and it occurs
    // only after the isolated probe has been acknowledged.
    activateConfigAtomically(stagedConfig, activeConfigPath);
    return {
      joined: true,
      configPath: activeConfigPath,
      tenantId: grant.tenantId,
      uploadUrl: grant.uploadUrl,
      uploadSigningConfigured: Boolean(stagedConfig.uploadSigningSecret),
      handshake: {
        uploadedEvents: 1,
        selfTestEventId,
        signedUpload: uploaded.signedUpload,
        response: uploaded.response,
      },
    };
  } catch (error) {
    throw new Error(
      `Join grant was not activated because its isolated handshake failed: ${
        error instanceof Error ? error.message : String(error)
      }`,
      { cause: error },
    );
  } finally {
    buffer?.close();
    fs.rmSync(temporaryDirectory, { recursive: true, force: true });
  }
}
