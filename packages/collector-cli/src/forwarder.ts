import type { LocalEventBuffer } from "./buffer";
import { assertCollectorPrivacyMode, type CollectorConfig } from "./config";
import { normalizeHookPayload } from "./normalizer";
import {
  canonicalizeSuppressionReceipts,
  type ToolSource,
} from "../../shared/src/index";
import { sealOutboundEvent } from "./outbound-envelope";
import { attachRepoContextSidecar, extractRepoContextCwd } from "./repo-context";
import { claudeBindingForUnrootedEvent, countClaudeReplayTimeout, currentDispatchBindingSnapshot,
  dispatchBindingMetadata, durableClaudeRootSessionSightings,
  type DispatchBindingSnapshot } from "./capture-root-inventory";

type ForwardedHookOptions = {
  config: CollectorConfig;
  source: ToolSource;
  transportPath?: string;
  now?: () => number;
  /** Local-only receipt clock stored atomically with a producer-ID hook row. */
  firstReceivedAt?: string;
  producerEventId?: string;
  fallbackEventId?: string;
  buffer?: LocalEventBuffer;
  dispatchSnapshot?: DispatchBindingSnapshot;
};

export function appendForwardedHook(
  payload: unknown,
  options: ForwardedHookOptions & { buffer: LocalEventBuffer },
) {
  return appendNormalizedHook(options.buffer, normalizeForwardedHook(payload, options), options.firstReceivedAt);
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
  if (options.source === "claude_code" && normalized.event.sessionId) {
    const replay=options.buffer?.claudeReplayBarrierState();
    if(replay==="pending") options.buffer!.deferClaudeHookUntilReplay(normalized.event.id);
    if(replay==="timed_out") countClaudeReplayTimeout();
    const binding = replay==="pending"||replay==="timed_out" ? null
      : claudeBindingForUnrootedEvent(normalized.event.sessionId,
        normalized.event.observedAt,options.dispatchSnapshot??currentDispatchBindingSnapshot(),
        options.buffer ? durableClaudeRootSessionSightings(options.buffer.database,normalized.event.sessionId) : undefined);
    if (binding) normalized.event.metadata = {
      ...normalized.event.metadata, ...dispatchBindingMetadata(binding),
    };
  }
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
) {
  const appended = buffer.append(
    canonical.event,
    canonical.suppressedFields,
    { integrityReceipt: true, firstReceivedAt },
  );
  return {
    ...canonical,
    ...(appended.deduplicated ? { deduplicated: true as const } : {}),
    ...(appended.collisionQuarantined
      ? { collisionQuarantined: true as const }
      : {}),
  };
}
