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
  const gitContext = resolveGitContext(cwd);

  const normalized = normalizeHookPayload(payload, {
    policy: options.config.policy,
    source: options.source,
    gitContext,
  });

  options.buffer.append(normalized.event, normalized.suppressedFields);
  return normalized;
}
