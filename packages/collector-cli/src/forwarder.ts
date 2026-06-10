import type { LocalEventBuffer } from "./buffer";
import type { CollectorConfig } from "./config";
import { resolveGitContext } from "./git-context";
import { asRecord, normalizeHookPayload, stringField } from "./normalizer";
import type { ToolSource } from "../../shared/src/index";

export function appendForwardedHook(
  payload: unknown,
  options: {
    config: CollectorConfig;
    buffer: LocalEventBuffer;
    source: ToolSource;
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
  });

  options.buffer.append(normalized.event, normalized.suppressedFields);
  return normalized;
}
