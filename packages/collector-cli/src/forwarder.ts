import type { LocalEventBuffer } from "./buffer";
import type { CollectorConfig } from "./config";
import { resolveGitContext } from "./git-context";
import { asRecord, normalizeHookPayload, stringField } from "./normalizer";
import {
  canonicalizeSuppressionReceipts,
  type ToolSource,
} from "../../shared/src/index";
import { sealOutboundEvent } from "./outbound-envelope";

export function appendForwardedHook(
  payload: unknown,
  options: {
    config: CollectorConfig;
    buffer: LocalEventBuffer;
    source: ToolSource;
    transportPath?: string;
  },
) {
  // Resolve git linkage keys from the hook's cwd before sanitization hashes it.
  const cwd = stringField(asRecord(payload), ["cwd", "current_working_directory", "workdir"]);
  const resolved = resolveGitContext(cwd);
  if (resolved?.remoteUrlHash && resolved.remoteLabel) {
    options.buffer.recordRepoLabel(resolved.remoteUrlHash, resolved.remoteLabel);
  }
  const { remoteLabel: _localOnly, ...gitContext } = resolved ?? {};

  const normalized = normalizeHookPayload(payload, {
    policy: options.config.policy,
    source: options.source,
    gitContext: resolved ? gitContext : undefined,
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

  options.buffer.append(canonical.event, canonical.suppressedFields);
  return canonical;
}
