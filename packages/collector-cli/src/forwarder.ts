import type { LocalEventBuffer } from "./buffer";
import { assertCollectorPrivacyMode, type CollectorConfig } from "./config";
import { normalizeHookPayload } from "./normalizer";
import {
  canonicalizeSuppressionReceipts,
  type ToolSource,
} from "../../shared/src/index";
import { sealOutboundEvent } from "./outbound-envelope";
import { attachRepoContextSidecar, extractRepoContextCwd } from "./repo-context";

export function appendForwardedHook(
  payload: unknown,
  options: {
    config: CollectorConfig;
    buffer: LocalEventBuffer;
    source: ToolSource;
    transportPath?: string;
  },
) {
  assertCollectorPrivacyMode(options.config, "hook capture");
  // Hook admission must not touch caller-selected filesystem paths. Repository
  // linkage is intentionally UNKNOWN here; bounded maintenance may enrich it
  // later without making event capture depend on local filesystem latency.
  const normalized = normalizeHookPayload(payload, {
    policy: options.config.policy,
    source: options.source,
    transportPath: options.transportPath,
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

  options.buffer.append(canonical.event, canonical.suppressedFields);
  return canonical;
}
