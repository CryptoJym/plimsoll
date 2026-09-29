import type { LocalEventBuffer } from "./buffer";
import { assertCollectorPrivacyMode, type CollectorConfig } from "./config";
import { normalizeHookPayload } from "./normalizer";
import {
  canonicalizeSuppressionReceipts,
  type ToolSource,
} from "../../shared/src/index";
import { sealOutboundEvent } from "./outbound-envelope";
import { attachRepoContextSidecar, extractRepoContextCwd } from "./repo-context";
import { recordMaintenanceHookAdmission } from "./maintenance-hook-admission";

type ForwardedHookOptions = {
  config: CollectorConfig;
  source: ToolSource;
  transportPath?: string;
  now?: () => number;
  /** Local-only receipt clock stored atomically with a producer-ID hook row. */
  firstReceivedAt?: string;
  producerEventId?: string;
  fallbackEventId?: string;
  /** Raw caller body before spool replay supplies a receive-time alias. */
  originalHookPayload?: unknown;
};

export function appendForwardedHook(
  payload: unknown,
  options: ForwardedHookOptions & { buffer: LocalEventBuffer },
) {
  return appendNormalizedHook(options.buffer, normalizeForwardedHook(payload, options),
    options.firstReceivedAt, options.originalHookPayload ?? payload);
}

/**
 * The normalized event and receipts `appendForwardedHook` appends, without the
 * append: the OTLP fallback normalizes once so a spooled copy keeps its id.
 */
export function normalizeForwardedHook(payload: unknown, options: ForwardedHookOptions) {
  assertCollectorPrivacyMode(options.config, "hook capture");
  // Hook admission must not touch caller-selected filesystem paths. Repository
  // linkage is intentionally UNKNOWN here; bounded maintenance may enrich it
  // later without making event capture depend on local filesystem latency.
  const normalized = normalizeHookPayload(payload, {
    policy: options.config.policy,
    source: options.source,
    transportPath: options.transportPath,
    now: options.now,
    producerEventId: options.producerEventId,
    fallbackEventId: options.fallbackEventId,
  });
  // Successful hook/fallback responses are public proof surfaces before the
  // durable outbox runs. Include the same deterministic local-only omissions
  // the outbound sealer will add later so response, ledger and wire receipts
  // cannot diverge while field values remain local-only.
  const presealed = sealOutboundEvent(normalized.event);
  const canonical = {
    ...normalized,
    suppressedFields: canonicalizeSuppressionReceipts([
      ...normalized.suppressedFields,
      ...(presealed.ok ? presealed.omittedFields : []),
    ]),
  };

  const cwd = extractRepoContextCwd(payload);
  if (cwd) attachRepoContextSidecar(canonical.event, canonical.event.id, cwd);
  return canonical;
}

export function appendNormalizedHook(
  buffer: LocalEventBuffer,
  canonical: ReturnType<typeof normalizeForwardedHook>,
  firstReceivedAt?: string,
  rawPayload?: unknown,
) {
  const appended = buffer.append(
    canonical.event,
    canonical.suppressedFields,
    { integrityReceipt: true, firstReceivedAt,
      onCommittedAppend: rawPayload === undefined ? undefined :
        (db, event, inserted) => recordMaintenanceHookAdmission(db, rawPayload, event, inserted) },
  );
  return {
    ...canonical,
    ...(appended.deduplicated ? { deduplicated: true as const } : {}),
    ...(appended.collisionQuarantined
      ? { collisionQuarantined: true as const }
      : {}),
  };
}
