import crypto from "node:crypto";

import type { BufferedEventRow, LocalEventBuffer } from "./buffer";
import type { CollectorConfig } from "./config";
import {
  aiWorkIngestBatchSchema,
  type AiInteractionEvent,
  type AiWorkIngestBatch,
} from "../../shared/src/index";

/**
 * Project attribution parity (issue 0036): the ledger's per-event repo
 * linkage lives in COLUMNS (repo_hash/branch_hash — issue 0008 stitching),
 * not in the payload, so uploads historically dropped it and the workspace
 * could not map events to projects. This forwards the linkage as
 * event.projectKey — the privacy-preserving hash DESIGNED to cross
 * boundaries (raw URLs/branches never leave the machine). branchHash rides
 * in metadata for later use. A projectKey already present in the payload is
 * never overwritten.
 */
export function attachRepoLinkage(
  payload: AiInteractionEvent,
  repoHash: string | null | undefined,
  branchHash?: string | null,
): AiInteractionEvent {
  if (!repoHash || payload.projectKey) return payload;
  return {
    ...payload,
    projectKey: repoHash,
    ...(branchHash
      ? { metadata: { ...payload.metadata, branchHash } }
      : {}),
  };
}

export function buildIngestBatch(
  config: CollectorConfig,
  buffer: LocalEventBuffer,
  options: { limit?: number; maxBytes?: number; appVersion?: string } = {},
): { batch: AiWorkIngestBatch | null; rows: BufferedEventRow[] } {
  const rows = buffer.listUnuploaded({
    maxRows: options.limit ?? 500,
    maxBytes: options.maxBytes,
  });

  if (rows.length === 0) {
    return { batch: null, rows };
  }

  const batch = aiWorkIngestBatchSchema.parse({
    tenantId: config.tenantId,
    installKey: config.installKey,
    appVersion: options.appVersion ?? "0.1.0",
    events: rows.map((row) => ({
      event: attachRepoLinkage(row.payload, row.repoHash, row.branchHash),
      suppressedFields: row.suppressedFields,
    })),
  });

  return { batch, rows };
}

export async function uploadBufferedEvents(
  config: CollectorConfig,
  buffer: LocalEventBuffer,
  options: {
    appVersion?: string;
    ingestKey?: string;
    limit?: number;
    maxBytes?: number;
    markUploaded?: boolean;
    signingSecret?: string;
    url?: string;
  } = {},
) {
  const url = options.url ?? config.uploadUrl;
  if (!url) {
    throw new Error("No upload URL configured. Pass --url or set uploadUrl in collector.config.json.");
  }

  const { batch, rows } = buildIngestBatch(config, buffer, options);
  if (!batch) {
    return {
      batch: null,
      markedUploaded: 0,
      remainingUnuploaded: buffer.stats().unuploadedCount,
      response: null,
      signedUpload: false,
      uploadedEvents: 0,
    };
  }

  const ingestKey = options.ingestKey ?? config.ingestKey;
  const signingSecret = options.signingSecret ?? config.uploadSigningSecret;
  const body = JSON.stringify(batch);
  const headers: Record<string, string> = {
    "content-type": "application/json",
    "x-plimsoll-install-key": config.installKey,
  };

  if (ingestKey) {
    headers["x-plimsoll-ingest-key"] = ingestKey;
  }

  if (signingSecret) {
    const timestamp = new Date().toISOString();
    const digest = crypto
      .createHmac("sha256", signingSecret)
      .update(`${timestamp}.${body}`)
      .digest("hex");
    headers["x-plimsoll-upload-timestamp"] = timestamp;
    headers["x-plimsoll-upload-signature"] = `sha256=${digest}`;
  }

  const response = await fetch(url, {
    method: "POST",
    headers,
    body,
  });

  const responseBody = (await response.json().catch(() => ({}))) as unknown;

  if (!response.ok) {
    throw new Error(`Upload failed with ${response.status}: ${JSON.stringify(responseBody)}`);
  }

  const markedUploaded =
    options.markUploaded === false ? 0 : buffer.markUploaded(rows.map((row) => row.id));

  return {
    batch,
    markedUploaded,
    remainingUnuploaded: buffer.stats().unuploadedCount,
    response: responseBody,
    signedUpload: Boolean(signingSecret),
    uploadedEvents: batch.events.length,
  };
}
