import crypto from "node:crypto";

import type { BufferedEventRow, LocalEventBuffer } from "./buffer";
import type { CollectorConfig } from "./config";
import type { LeasedDeliveryItem, DeliveryFailureClass } from "./outbox";
import {
  aiWorkIngestBatchSchema,
  type AiInteractionEvent,
  type AiWorkIngestBatch,
} from "../../shared/src/index";

/**
 * Project attribution parity (issue 0036): the ledger's per-event repo
 * linkage lives in columns, not in the payload. Fill it only when the payload
 * has no projectKey. The outbox freezes this result before its first attempt.
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

/** Legacy/stateless snapshot builder retained for `upload --no-mark`. */
export function buildIngestBatch(
  config: CollectorConfig,
  buffer: LocalEventBuffer,
  options: { limit?: number; maxBytes?: number; appVersion?: string } = {},
): { batch: AiWorkIngestBatch | null; rows: BufferedEventRow[] } {
  const rows = buffer.listUnuploaded({
    maxRows: options.limit ?? 500,
    maxBytes: options.maxBytes,
  });

  if (rows.length === 0) return { batch: null, rows };
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

export class DeliveryUploadError extends Error {
  constructor(
    readonly failureClass: Exclude<DeliveryFailureClass, "none">,
    readonly httpStatusClass: string,
  ) {
    super(`Upload deferred: ${failureClass} (${httpStatusClass})`);
    this.name = "DeliveryUploadError";
  }
}

type SafeResponseSummary = {
  accepted?: boolean | number;
  inserted?: number;
  matched?: number;
  updated?: number;
};

type ProbeResult = {
  ok: boolean;
  status: number;
  statusClass: string;
  summary: SafeResponseSummary;
};

function safeResponseSummary(value: unknown): SafeResponseSummary {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  const source = value as Record<string, unknown>;
  const summary: SafeResponseSummary = {};
  if (typeof source.accepted === "boolean" || (typeof source.accepted === "number" && Number.isFinite(source.accepted))) {
    summary.accepted = source.accepted;
  }
  for (const key of ["inserted", "matched", "updated"] as const) {
    const field = source[key];
    if (typeof field === "number" && Number.isFinite(field)) summary[key] = field;
  }
  return summary;
}

function statusClass(status: number) {
  if (status === 0) return "network";
  if (status >= 200 && status < 300) return "remote_2xx";
  if (status === 400 || status === 422) return "remote_validation";
  if (status === 401 || status === 403) return "remote_auth";
  if (status === 408 || status === 429 || status >= 500) return "remote_transient";
  return "remote_contract";
}

function uploadContractHash(config: CollectorConfig, url: string, appVersion: string) {
  return `sha256:${crypto
    .createHash("sha256")
    .update(JSON.stringify([url, config.tenantId, config.installKey, appVersion]))
    .digest("hex")}`;
}

function bodyForItems(
  config: CollectorConfig,
  items: LeasedDeliveryItem[],
  appVersion: string,
) {
  // Preserve each once-sealed item byte-for-byte; only the deterministic batch
  // wrapper is rebuilt for bounded poison isolation.
  const body =
    `{"tenantId":${JSON.stringify(config.tenantId)},` +
    `"installKey":${JSON.stringify(config.installKey)},` +
    `"appVersion":${JSON.stringify(appVersion)},` +
    `"events":[${items.map((item) => item.envelopeJson).join(",")}]}`;
  const batch = aiWorkIngestBatchSchema.parse(JSON.parse(body));
  return { body, batch };
}

async function postItems(input: {
  config: CollectorConfig;
  items: LeasedDeliveryItem[];
  appVersion: string;
  url: string;
  ingestKey?: string;
  signingSecret?: string;
  fetchImpl: typeof fetch;
  timeoutSeconds: number;
  now: () => Date;
}): Promise<ProbeResult> {
  const { body } = bodyForItems(input.config, input.items, input.appVersion);
  const headers: Record<string, string> = {
    "content-type": "application/json",
    "x-plimsoll-install-key": input.config.installKey,
  };
  if (input.ingestKey) headers["x-plimsoll-ingest-key"] = input.ingestKey;
  if (input.signingSecret) {
    const timestamp = input.now().toISOString();
    const digest = crypto
      .createHmac("sha256", input.signingSecret)
      .update(`${timestamp}.${body}`)
      .digest("hex");
    headers["x-plimsoll-upload-timestamp"] = timestamp;
    headers["x-plimsoll-upload-signature"] = `sha256=${digest}`;
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), input.timeoutSeconds * 1_000);
  timeout.unref();
  try {
    const response = await input.fetchImpl(input.url, {
      method: "POST",
      headers,
      body,
      signal: controller.signal,
    });
    if (!response.ok) {
      await response.body?.cancel().catch(() => undefined);
      return {
        ok: false,
        status: response.status,
        statusClass: statusClass(response.status),
        summary: {},
      };
    }
    const parsed = await response.json().catch(() => ({}));
    return {
      ok: true,
      status: response.status,
      statusClass: statusClass(response.status),
      summary: response.ok ? safeResponseSummary(parsed) : {},
    };
  } catch {
    return { ok: false, status: 0, statusClass: "network", summary: {} };
  } finally {
    clearTimeout(timeout);
  }
}

function failureForProbe(result: ProbeResult): Exclude<DeliveryFailureClass, "none"> {
  if (result.statusClass === "remote_auth") return "remote_auth";
  if (result.statusClass === "remote_transient" || result.statusClass === "network") {
    return "remote_transient";
  }
  if (result.statusClass === "remote_validation") return "remote_validation";
  return "remote_contract";
}

async function uploadStateless(
  config: CollectorConfig,
  buffer: LocalEventBuffer,
  options: UploadOptions,
) {
  const url = options.url ?? config.uploadUrl;
  if (!url) throw new Error("No upload URL configured. Pass --url or set uploadUrl in collector.config.json.");
  const { batch, rows } = buildIngestBatch(config, buffer, options);
  if (!batch) {
    return {
      batch: null,
      markedUploaded: 0,
      remainingUnuploaded: buffer.stats().unuploadedCount,
      remainingDelivery: buffer.delivery.status().remainingDelivery,
      response: null,
      signedUpload: false,
      uploadedEvents: 0,
      delivery: { mode: "stateless_no_mark" as const, attempts: 0, deadLetters: 0 },
    };
  }
  const items = batch.events.map((envelope) => ({
    deliveryId: envelope.event.id,
    rawRowid: null,
    envelope,
    envelopeJson: JSON.stringify(envelope),
    attemptCount: 0,
  }));
  const result = await postItems({
    config,
    items,
    appVersion: options.appVersion ?? "0.1.0",
    url,
    ingestKey: options.ingestKey ?? config.ingestKey,
    signingSecret: options.signingSecret ?? config.uploadSigningSecret,
    fetchImpl: options.fetchImpl ?? fetch,
    timeoutSeconds: config.delivery.requestTimeoutSeconds,
    now: options.now ?? (() => new Date()),
  });
  if (!result.ok) throw new DeliveryUploadError(failureForProbe(result), result.statusClass);
  return {
    batch,
    markedUploaded: 0,
    remainingUnuploaded: buffer.stats().unuploadedCount,
    remainingDelivery: buffer.delivery.status().remainingDelivery,
    response: result.summary,
    signedUpload: Boolean(options.signingSecret ?? config.uploadSigningSecret),
    uploadedEvents: rows.length,
    delivery: { mode: "stateless_no_mark" as const, attempts: 0, deadLetters: 0 },
  };
}

export type UploadOptions = {
  appVersion?: string;
  ingestKey?: string;
  limit?: number;
  maxBytes?: number;
  markUploaded?: boolean;
  signingSecret?: string;
  url?: string;
  fetchImpl?: typeof fetch;
  now?: () => Date;
  leaseId?: string;
  maxProbes?: number;
  /** Test-only crash seam after HTTP effects but before local acknowledgement. */
  afterRemote?: () => void;
  /** Test-only crash seam after a sibling acknowledgement is durable but before poison settlement. */
  afterSiblingAcknowledgement?: () => void;
};

export async function uploadBufferedEvents(
  config: CollectorConfig,
  buffer: LocalEventBuffer,
  options: UploadOptions = {},
) {
  if (options.markUploaded === false) return uploadStateless(config, buffer, options);
  const url = options.url ?? config.uploadUrl;
  if (!url) throw new Error("No upload URL configured. Pass --url or set uploadUrl in collector.config.json.");

  buffer.delivery.configure({ enabled: true, limits: config.delivery });
  const nowFn = options.now ?? (() => new Date());
  buffer.delivery.migrateLegacy({ now: nowFn() });
  const appVersion = options.appVersion ?? "0.1.0";
  const contractHash = uploadContractHash(config, url, appVersion);
  const outputLimit = Math.max(
    1,
    Math.min(
      Number.isFinite(options.limit) ? Math.trunc(options.limit!) : 500,
      500,
    ),
  );
  const provenDead = buffer.delivery.settleProvenValidationCandidates(contractHash, {
    maxRows: outputLimit,
    now: nowFn(),
  });
  const lease = buffer.delivery.lease({
    maxRows: outputLimit,
    maxBytes: options.maxBytes,
    now: nowFn(),
    leaseId: options.leaseId,
  });
  const statusBefore = buffer.delivery.status(nowFn());
  if (lease.items.length === 0) {
    return {
      batch: null,
      markedUploaded: 0,
      remainingUnuploaded: buffer.stats().unuploadedCount,
      remainingDelivery: statusBefore.remainingDelivery,
      response:
        lease.blockedBy === "none"
          ? null
          : { status: "circuit_open", circuit: lease.blockedBy },
      signedUpload: false,
      uploadedEvents: 0,
      delivery: {
        mode: "durable_outbox" as const,
        attempts: 0,
        deadLetters: provenDead + lease.locallyDead,
        circuit: lease.blockedBy,
      },
    };
  }

  const maxProbes = Math.max(
    1,
    Math.min(options.maxProbes ?? config.delivery.maxProbesPerCycle, config.delivery.maxProbesPerCycle),
  );
  let probes = 0;
  let lastSummary: SafeResponseSummary = {};
  const succeeded = new Map<string, LeasedDeliveryItem>();
  const validationSingletons = new Map<string, LeasedDeliveryItem>();
  const unresolved = new Map<string, LeasedDeliveryItem>();
  const attemptedActive = new Set<string>();
  let fatal: ProbeResult | null = null;
  let validationWitnessProven = false;
  let validationWitnessRejected = false;
  let locallyDead = provenDead + lease.locallyDead;
  const queue: LeasedDeliveryItem[][] = [lease.items];

  while (queue.length > 0 && probes < maxProbes && !fatal) {
    const group = queue.shift()!;
    probes += 1;
    for (const item of group) attemptedActive.add(item.deliveryId);
    const result = await postItems({
      config,
      items: group,
      appVersion,
      url,
      ingestKey: options.ingestKey ?? config.ingestKey,
      signingSecret: options.signingSecret ?? config.uploadSigningSecret,
      fetchImpl: options.fetchImpl ?? fetch,
      timeoutSeconds: config.delivery.requestTimeoutSeconds,
      now: nowFn,
    });
    if (result.ok) {
      lastSummary = result.summary;
      for (const item of group) succeeded.set(item.deliveryId, item);
      continue;
    }
    if (result.status === 400 || result.status === 422) {
      if (group.length === 1) {
        validationSingletons.set(group[0].deliveryId, group[0]);
        buffer.delivery.markValidationCandidate(
          lease.leaseId,
          group[0].deliveryId,
          contractHash,
          nowFn(),
        );
        // A singleton validation response is ambiguous until a sibling proves
        // the endpoint contract. When the caller's output limit is one, lease
        // one bounded lookahead under the same lease and probe it without ever
        // acknowledging more than that limit. If there is no active lookahead,
        // re-probe the one durable sanitized witness from the same contract.
        if (queue.length === 0 && succeeded.size === 0 && probes < maxProbes) {
          const lookahead = buffer.delivery.lease({
            maxRows: Math.max(1, outputLimit - succeeded.size),
            maxBytes: options.maxBytes,
            now: nowFn(),
            leaseId: lease.leaseId,
          });
          locallyDead += lookahead.locallyDead;
          if (lookahead.items.length > 0) {
            queue.push(lookahead.items);
          } else {
            const witness = buffer.delivery.validationWitness(contractHash);
            if (witness && probes < maxProbes) {
              probes += 1;
              const witnessResult = await postItems({
                config,
                items: [witness.item],
                appVersion,
                url,
                ingestKey: options.ingestKey ?? config.ingestKey,
                signingSecret: options.signingSecret ?? config.uploadSigningSecret,
                fetchImpl: options.fetchImpl ?? fetch,
                timeoutSeconds: config.delivery.requestTimeoutSeconds,
                now: nowFn,
              });
              if (witnessResult.ok) {
                validationWitnessProven = true;
              } else if (witnessResult.status === 400 || witnessResult.status === 422) {
                validationWitnessRejected = true;
              } else if (witnessResult.status !== 400 && witnessResult.status !== 422) {
                fatal = witnessResult;
              }
            }
          }
        }
      } else {
        const midpoint = Math.floor(group.length / 2);
        queue.push(group.slice(0, midpoint), group.slice(midpoint));
      }
      continue;
    }
    fatal = result;
    for (const item of group) unresolved.set(item.deliveryId, item);
  }
  for (const group of queue) {
    for (const item of group) unresolved.set(item.deliveryId, item);
  }

  options.afterRemote?.();

  const acknowledged = buffer.delivery.acknowledge(
    lease.leaseId,
    [...succeeded.keys()],
    nowFn(),
    succeeded.size > 0
      ? { contractHash, item: [...succeeded.values()][0] }
      : undefined,
  );
  if (acknowledged.acknowledged > 0 && validationSingletons.size > 0) {
    options.afterSiblingAcknowledgement?.();
  }
  let deadLetters = locallyDead;
  if ((succeeded.size > 0 || validationWitnessProven) && validationSingletons.size > 0) {
    deadLetters += buffer.delivery.deadLetterRemote(
      lease.leaseId,
      [...validationSingletons.keys()],
      nowFn(),
    );
  } else {
    for (const item of validationSingletons.values()) unresolved.set(item.deliveryId, item);
  }

  let failure: Exclude<DeliveryFailureClass, "none"> | null = null;
  if (fatal) {
    failure = failureForProbe(fatal);
  } else if (succeeded.size === 0 && unresolved.size > 0) {
    failure = validationSingletons.size > 0 && attemptedActive.size < 2 && !validationWitnessRejected
      ? "remote_validation"
      : "remote_contract";
  } else if (unresolved.size > 0) {
    failure = "remote_validation";
  }

  if (failure && unresolved.size > 0) {
    buffer.delivery.retry(lease.leaseId, [...unresolved.values()], failure, nowFn());
    if (failure === "remote_auth") buffer.delivery.openCircuit("auth_blocked", nowFn());
    if (failure === "remote_contract") buffer.delivery.openCircuit("contract_blocked", nowFn());
  } else if (succeeded.size > 0) {
    buffer.delivery.clearCircuit(nowFn());
  }

  const deliveryStatus = buffer.delivery.status(nowFn());
  const acknowledgedBatch =
    succeeded.size > 0
      ? bodyForItems(config, [...succeeded.values()], appVersion).batch
      : null;
  const result = {
    // Session sync consumes this batch. It must name only events actually
    // acknowledged in this cycle, never unresolved or dead-lettered leases.
    batch: acknowledgedBatch,
    markedUploaded: acknowledged.markedUploaded,
    remainingUnuploaded: buffer.stats().unuploadedCount,
    remainingDelivery: deliveryStatus.remainingDelivery,
    response:
      failure === null
        ? lastSummary
        : { status: succeeded.size > 0 ? "partial" : "deferred", failureClass: failure },
    signedUpload: Boolean(options.signingSecret ?? config.uploadSigningSecret),
    uploadedEvents: succeeded.size,
    delivery: {
      mode: "durable_outbox" as const,
      attempts: probes,
      deadLetters,
      circuit: deliveryStatus.circuit.kind,
      rootLeaseEvents: lease.items.length,
    },
  };
  if (failure && succeeded.size === 0) {
    throw new DeliveryUploadError(failure, fatal?.statusClass ?? "remote_validation");
  }
  return result;
}
