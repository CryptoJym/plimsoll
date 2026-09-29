/** The shared privacy normalization used by the client spool and the private
 * maintenance refusal fingerprint. Keep one rule for both paths. */
import { isProtectedMetadataFieldName, isSafeSuppressionSourceKey,
  isSensitiveMetadataSemanticKey } from "../../shared/src/index";

export const SPOOL_DERIVATION_INPUT_KEYS = [
  "cwd", "current_working_directory", "workdir", "working_directory", "hookEventName",
] as const;
const derivationInputKeys = new Set<string>(SPOOL_DERIVATION_INPUT_KEYS);

function collectorStripsKeyOutright(key: string) {
  return !isSafeSuppressionSourceKey(key) || isSensitiveMetadataSemanticKey(key);
}

/** Values the ledger hashes as protected identities stay available to its
 * normalizer; every value it drops is blanked before a spool write. */
export function spoolKeepsProtectedIdentityRaw(key: string) {
  return !collectorStripsKeyOutright(key) && isProtectedMetadataFieldName(key);
}

function spoolSuppressedKey(key: string) {
  if (derivationInputKeys.has(key)) return false;
  if (spoolKeepsProtectedIdentityRaw(key)) return false;
  return collectorStripsKeyOutright(key);
}

/** Preserve key names and only the values the collector can retain. The
 * OTLP attribute shape has a semantic key in `key` and its content in `value`.
 * This function is deliberately idempotent for an already blanked retry. */
export function blankForbiddenRawContent(
  body: string,
): { text: string; blanked: number } | null {
  let parsed: unknown;
  try { parsed = JSON.parse(body); }
  catch { return null; }
  let blanked = 0;
  const blank = (value: unknown): unknown => {
    if (Array.isArray(value)) return value.map((item) => blank(item));
    if (!value || typeof value !== "object") return value;
    const record = value as Record<string, unknown>;
    const semanticKey = typeof record.key === "string" ? record.key : undefined;
    if (semanticKey && "value" in record && spoolSuppressedKey(semanticKey)) {
      blanked += 1;
      return { ...record, value: "" };
    }
    const next: Record<string, unknown> = {};
    for (const [key, nested] of Object.entries(record)) {
      if (spoolSuppressedKey(key)) {
        blanked += 1;
        next[key] = "";
        continue;
      }
      next[key] = blank(nested);
    }
    return next;
  };
  let text: string | undefined;
  try { text = JSON.stringify(blank(parsed)); }
  catch { return null; }
  return text === undefined ? null : { text, blanked };
}
