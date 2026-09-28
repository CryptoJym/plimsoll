import path from "node:path";

/** The tailer and enrollment discovery must agree on Codex rollout identity. */
const UUID_EXACT_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
// The tailer has always accepted both rollout-<uuid>.jsonl and
// rollout-<timestamp>-<uuid>.jsonl. Discovery must share that identity rule.
const ROLLOUT_FILE_RE = /^rollout-(?:.+-)?([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\.jsonl$/i;

export function isCodexUuid(value: unknown): value is string {
  return typeof value === "string" && UUID_EXACT_RE.test(value);
}

export function codexRolloutIdFromFilename(file: string): string | undefined {
  const match = ROLLOUT_FILE_RE.exec(path.basename(file));
  return match && isCodexUuid(match[1]) ? match[1].toLowerCase() : undefined;
}

export function verifiedCodexSessionMetaId(row: unknown): string | undefined {
  if (!row || typeof row !== "object" || Array.isArray(row)) return undefined;
  const record = row as Record<string, unknown>;
  if (record.type !== "session_meta") return undefined;
  const payload = record.payload;
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) return undefined;
  const id = (payload as Record<string, unknown>).id;
  const timestamp = record.timestamp ?? (payload as Record<string, unknown>).timestamp;
  // Codex rollouts can omit the timestamp on session_meta. The tailer has
  // always accepted those records, so enrollment must use the same rule.
  if (!isCodexUuid(id) ||
      (timestamp !== undefined &&
        (typeof timestamp !== "string" || !Number.isFinite(Date.parse(timestamp))))) return undefined;
  return id.toLowerCase();
}

export function verifiedCodexRolloutSessionId(file: string, row: unknown): string | undefined {
  const filenameId = codexRolloutIdFromFilename(file);
  const metadataId = verifiedCodexSessionMetaId(row);
  return filenameId && filenameId === metadataId ? filenameId : undefined;
}
