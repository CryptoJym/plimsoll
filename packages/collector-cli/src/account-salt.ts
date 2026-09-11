import fs from "node:fs";
import path from "node:path";
import Database from "better-sqlite3";
import { z } from "zod";

import { recordAccountAssertionSalt, storeAccountAssertionSalt } from "./account-assertion";
import { authenticatedJsonPost, validatedTransportUrl } from "./http-transport";

/** Hosted tenant endpoint; the collector never reads adjacent credential stores. */
export const CLOUD_ACCOUNT_SALT_PATH = "/api/work-intelligence/account-actor-salt";
const responseSchema = z.object({
  ok: z.literal(true),
  schema: z.literal("account-actor-salt/v1"),
  tenantId: z.string().min(1),
  salt: z.string().min(1),
  saltVersion: z.string().min(1),
}).strict();

export type AccountSaltSyncResult = {
  synced: boolean;
  tenantId: string;
  saltVersion: string | null;
  reason: "synced" | "unallocated" | "refused";
};

function saltBytes(encoded: string) {
  let value: Buffer;
  try { value = Buffer.from(encoded, "base64"); } catch { throw new Error("account_salt_encoding_invalid"); }
  if (value.length !== 32 || value.toString("base64") !== encoded) throw new Error("account_salt_encoding_invalid");
  return value;
}

/**
 * Fetch once over the same authenticated device channel used for uploads.
 * The response is intentionally reduced to a boolean/version receipt; the
 * raw salt never reaches stdout, events, receipts, or logs.
 */
export async function syncAccountActorSalt(options: {
  collectorHome: string;
  tenantId: string;
  deviceId: string;
  uploadUrl: string;
  installKey: string;
  ingestKey?: string;
  signingSecret?: string;
  fetchImpl?: typeof fetch;
  endpointUrl?: string;
}): Promise<AccountSaltSyncResult> {
  const refused = (): AccountSaltSyncResult => ({
    synced: false, tenantId: options.tenantId, saltVersion: null, reason: "refused",
  });
  let response: Awaited<ReturnType<typeof authenticatedJsonPost>>;
  try {
    const origin = validatedTransportUrl(options.uploadUrl, "account salt channel").origin;
    const url = options.endpointUrl ? validatedTransportUrl(options.endpointUrl, "account salt endpoint").href :
      new URL(CLOUD_ACCOUNT_SALT_PATH, origin).href;
    if (new URL(url).origin !== origin) throw new Error("account_salt_origin_mismatch");
    const body = JSON.stringify({ schema: "account-actor-salt-request/v1", tenantId: options.tenantId, deviceId: options.deviceId });
    response = await authenticatedJsonPost({
      url, body, fetchImpl: options.fetchImpl, installKey: options.installKey,
      ingestKey: options.ingestKey, signingSecret: options.signingSecret,
      maxRequestBytes: 4 * 1024, maxResponseBytes: 4 * 1024,
    });
  } catch {
    // Remote URL, transport, redirect, size, deadline and JSON failures are
    // refusals. Keep their diagnostics value-blind and never create a salt.
    return refused();
  }
  if (!response.ok) return { synced: false, tenantId: options.tenantId, saltVersion: null,
    reason: response.status === 404 ? "unallocated" : "refused" };
  const parsed = responseSchema.safeParse(response.body);
  if (!parsed.success || parsed.data.tenantId !== options.tenantId) return refused();
  let bytes: Buffer;
  try { bytes = saltBytes(parsed.data.salt); }
  catch { return refused(); }
  storeAccountAssertionSalt(options.collectorHome, bytes, { tenantId: options.tenantId, version: parsed.data.saltVersion });
  // Maintenance state is additive and records only tenant/version metadata.
  const ledgerPath = path.join(options.collectorHome, "work-ledger.sqlite");
  if (fs.existsSync(ledgerPath)) {
    // A separate bounded writer records only the tenant/version maintenance
    // receipt; the raw salt remains solely in its owner-only file.
    const db = new Database(ledgerPath, { timeout: 5_000 });
    try { recordAccountAssertionSalt(db, options.tenantId, parsed.data.saltVersion); } finally { db.close(); }
  }
  return { synced: true, tenantId: options.tenantId, saltVersion: parsed.data.saltVersion, reason: "synced" };
}

export { responseSchema as accountSaltResponseSchema };
