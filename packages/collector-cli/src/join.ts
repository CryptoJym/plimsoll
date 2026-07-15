import os from "node:os";

import { z } from "zod";

import { LocalEventBuffer } from "./buffer";
import {
  collectorBufferPath,
  collectorConfigPath,
  loadCollectorConfig,
  saveCollectorConfig,
} from "./config";
import { appendForwardedHook } from "./forwarder";
import { uploadBufferedEvents } from "./upload";

/**
 * Fleet join (issue 0016): one command takes a teammate Mac from installed to
 * syncing. The workspace admin mints a single-use token in the hosted product
 * and shares a join URL; this module redeems it, writes the returned sync
 * credentials into collector.config.json (ONLY after the server accepts), and
 * proves the credentials with a handshake upload through the real sync path.
 *
 * The open/paid boundary holds: this side only ever sees its own install
 * credentials — no cloud secrets, no comparative analytics.
 */
export const CLOUD_JOIN_PATH = "/api/work-intelligence/join";

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

/** The grant is exactly the config's sync fields; tenantId keeps uploads attributed. */
const joinGrantSchema = z.object({
  ok: z.literal(true),
  tenantId: z.string().trim().min(1),
  installKey: z.string().trim().min(1),
  uploadUrl: z.string().url(),
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
      installKey: string;
      uploadUrl: string;
      uploadSigningConfigured: boolean;
      handshake: {
        uploadedEvents: number;
        selfTestEventId: string | null;
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

export async function performJoin(options: {
  target: string;
  baseUrl?: string;
  appVersion?: string;
  homeDir?: string;
  fetchImpl?: typeof fetch;
}): Promise<JoinResult> {
  const { token, baseUrl } = parseJoinTarget(options.target, options.baseUrl);
  if (!token) {
    throw new Error(
      'No token found. Use the full join URL from your admin ("https://…#pljt_…") or pass the pljt_… token directly.',
    );
  }
  if (!baseUrl) {
    throw new Error(
      'No workspace URL. Use the full join URL from your admin ("https://…#pljt_…"), or pass --url <cloud-base-url> (or set PLIMSOLL_CLOUD_URL).',
    );
  }

  const fetchImpl = options.fetchImpl ?? fetch;
  const homeDir = options.homeDir ?? os.homedir();
  const appVersion = options.appVersion ?? "0.1.0";

  const response = await fetchImpl(new URL(CLOUD_JOIN_PATH, baseUrl), {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      token,
      platform: process.platform === "darwin" ? "macos" : process.platform,
      appVersion,
    }),
  });
  const body = (await response.json().catch(() => ({}))) as Record<string, unknown>;

  if (!response.ok) {
    // Refused tokens never touch the config — the collector keeps whatever
    // sync state it had (usually none).
    const reason = typeof body.reason === "string" ? body.reason : "unknown_error";
    return {
      joined: false,
      reason,
      message:
        JOIN_REFUSAL_MESSAGES[reason] ??
        `Join refused with HTTP ${response.status}: ${JSON.stringify(body)}`,
      httpStatus: response.status,
      configTouched: false,
    };
  }

  const grant = joinGrantSchema.parse(body);

  // Written ONLY now that the server accepted the token. Everything else in
  // the existing config (port, policy, subscriptions…) is preserved.
  const config = saveCollectorConfig(
    {
      ...loadCollectorConfig(homeDir),
      installKey: grant.installKey,
      tenantId: grant.tenantId,
      uploadUrl: grant.uploadUrl,
      ...(grant.uploadSigningSecret ? { uploadSigningSecret: grant.uploadSigningSecret } : {}),
    },
    homeDir,
  );

  // Handshake: prove the issued credentials round-trip through the REAL sync
  // path. Drain whatever is buffered; on a box with nothing captured yet,
  // emit one self-test event (the self-test-hook shape) so the upload is real.
  const buffer = new LocalEventBuffer(collectorBufferPath(homeDir), {
    delivery: { enabled: true, limits: config.delivery },
  });
  let selfTestEventId: string | null = null;
  try {
    if (buffer.stats().unuploadedCount === 0) {
      const normalized = appendForwardedHook(
        {
          id: `join_handshake_${Date.now()}`,
          source: "claude_code",
          event_type: "UserPromptSubmit",
          project: "plimsoll-join-handshake",
        },
        { config, buffer, source: "claude_code" },
      );
      selfTestEventId = normalized.event.id;
    }

    let uploaded: Awaited<ReturnType<typeof uploadBufferedEvents>>;
    try {
      uploaded = await uploadBufferedEvents(config, buffer, { appVersion });
    } catch (error) {
      throw new Error(
        `Joined the workspace and wrote ${collectorConfigPath(homeDir)}, but the handshake upload failed: ` +
          `${error instanceof Error ? error.message : String(error)}. ` +
          "The collector will retry on its sync interval; check the uploadUrl is reachable.",
      );
    }

    return {
      joined: true,
      configPath: collectorConfigPath(homeDir),
      tenantId: grant.tenantId,
      installKey: grant.installKey,
      uploadUrl: grant.uploadUrl,
      uploadSigningConfigured: Boolean(config.uploadSigningSecret),
      handshake: {
        uploadedEvents: uploaded.uploadedEvents,
        selfTestEventId,
        signedUpload: uploaded.signedUpload,
        response: uploaded.response,
      },
    };
  } finally {
    buffer.close();
  }
}
