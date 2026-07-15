import {
  hasPrivateMetadataKeyConcept,
  isSensitiveMetadataSemanticKey,
} from "./analytical-metadata";

/**
 * Canonical privacy-safe suppression receipts shared by capture, durable
 * storage, and outbound sealing. Receipts contain field names only: values
 * are never accepted by this contract.
 */
export const SUPPRESSION_RECEIPT_MAX_LENGTH = 96;
export const SUPPRESSION_RECEIPT_MAX_COUNT = 128;
export const GENERIC_SUPPRESSION_RECEIPT = "suppression.non_ascii_or_unbounded_key";
export const GENERIC_ATTRIBUTE_SUPPRESSION_RECEIPT =
  "attributes.non_ascii_or_unbounded_key";
export const SUPPRESSION_RECEIPT_OVERFLOW = "suppression.additional_fields";

const ATTRIBUTE_PREFIX = "attributes.";
export const SUPPRESSION_ATTRIBUTE_KEY_MAX_LENGTH =
  SUPPRESSION_RECEIPT_MAX_LENGTH - ATTRIBUTE_PREFIX.length;
const SAFE_RECEIPT_CHARACTERS = /^[a-zA-Z0-9_.:+-]+$/;

/** Syntactic key safety used before an attacker-controlled key is retained. */
export function isSafeSuppressionSourceKey(value: unknown): value is string {
  return (
    typeof value === "string" &&
    value.length >= 1 &&
    value.length <= SUPPRESSION_RECEIPT_MAX_LENGTH &&
    SAFE_RECEIPT_CHARACTERS.test(value)
  );
}

/** Credential-like names are themselves sensitive and never enter receipts. */
export function hasSensitiveSuppressionConcept(value: string) {
  return hasPrivateMetadataKeyConcept(value);
}

function isSafeReceipt(value: string) {
  return (
    value.length >= 1 &&
    value.length <= SUPPRESSION_RECEIPT_MAX_LENGTH &&
    SAFE_RECEIPT_CHARACTERS.test(value) &&
    !hasSensitiveSuppressionConcept(value)
  );
}

/**
 * Canonicalize a pre-existing receipt. Valid legacy bounded ASCII receipts
 * survive (including historical surrounding whitespace after trim). Unsafe
 * legacy names become one privacy-safe generic instead of being echoed or
 * silently discarded.
 */
export function canonicalSuppressionReceipt(value: unknown) {
  if (typeof value !== "string") return GENERIC_SUPPRESSION_RECEIPT;
  const candidate = value.trim();
  return isSafeReceipt(candidate) ? candidate : GENERIC_SUPPRESSION_RECEIPT;
}

export function isCanonicalSuppressionReceipt(value: unknown): value is string {
  return typeof value === "string" && canonicalSuppressionReceipt(value) === value;
}

/**
 * Format an attacker-controlled OTLP attribute key under the same total
 * 96-character receipt grammar used by the outbound sealer. Unsafe names,
 * including non-ASCII, control, path, URL, email and over-budget shapes,
 * collapse to one stable category receipt.
 */
export function suppressionReceiptForAttributeKey(key: unknown) {
  if (typeof key !== "string") return GENERIC_ATTRIBUTE_SUPPRESSION_RECEIPT;
  if (
    key.length < 1 ||
    key.length > SUPPRESSION_ATTRIBUTE_KEY_MAX_LENGTH ||
    !SAFE_RECEIPT_CHARACTERS.test(key) ||
    hasSensitiveSuppressionConcept(key) ||
    isSensitiveMetadataSemanticKey(key)
  ) {
    return GENERIC_ATTRIBUTE_SUPPRESSION_RECEIPT;
  }
  const receipt = `${ATTRIBUTE_PREFIX}${key}`;
  return isSafeReceipt(receipt) ? receipt : GENERIC_ATTRIBUTE_SUPPRESSION_RECEIPT;
}

/**
 * Stable first-seen dedupe bounds generic collisions and total cardinality.
 * On a 129th unique receipt the last named slot becomes an explicit overflow
 * category, so work stays bounded without silently claiming full fidelity.
 */
export function canonicalizeSuppressionReceipts(values: readonly unknown[]) {
  const receipts: string[] = [];
  const seen = new Set<string>();
  for (const value of values) {
    const receipt = canonicalSuppressionReceipt(value);
    if (seen.has(receipt)) continue;
    if (receipts.length < SUPPRESSION_RECEIPT_MAX_COUNT) {
      receipts.push(receipt);
      seen.add(receipt);
      continue;
    }
    if (!seen.has(SUPPRESSION_RECEIPT_OVERFLOW)) {
      const removed = receipts[SUPPRESSION_RECEIPT_MAX_COUNT - 1];
      if (removed !== undefined) seen.delete(removed);
      receipts[SUPPRESSION_RECEIPT_MAX_COUNT - 1] = SUPPRESSION_RECEIPT_OVERFLOW;
      seen.add(SUPPRESSION_RECEIPT_OVERFLOW);
    }
    break;
  }
  return receipts;
}
