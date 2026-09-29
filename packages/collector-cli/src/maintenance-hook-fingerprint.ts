import { createHash } from "node:crypto";
import { HOOK_AUTHORITY_CONTRACT } from "./hook-authority";
import { blankForbiddenRawContent } from "./hook-spool-privacy";
import { isUuid } from "./normalizer";

const canonicalIdKeys = new Set<string>([
  ...HOOK_AUTHORITY_CONTRACT.eventId.aliases,
  "sessionId", "session_id", "conversation.id", "conversation_id", "thread_id",
  "session.id", "gen_ai.session.id",
]);

function canonicalBody(value: unknown, depth = 0): unknown {
  if (Array.isArray(value)) return value.map((entry) => canonicalBody(entry, depth + 1));
  if (!value || typeof value !== "object") return value;
  const record = value as Record<string, unknown>;
  // The normalizer reads OTLP {key, value} attributes at any nesting depth.
  // Only its identity aliases have case-insensitive UUID semantics; retained
  // nonidentity attributes remain byte-for-byte significant after key sorting.
  const attributeIdentity = typeof record.key === "string" && canonicalIdKeys.has(record.key);
  return Object.fromEntries(Object.entries(record)
    .sort(([left], [right]) => left < right ? -1 : left > right ? 1 : 0)
    .map(([key, entry]) => [key, depth === 0 && canonicalIdKeys.has(key) &&
      typeof entry === "string" && isUuid(entry) ? entry.toLowerCase() :
      key === "value" && attributeIdentity ? canonicalAttributeIdentity(entry, depth + 1) :
      canonicalBody(entry, depth + 1)]));
}

function canonicalAttributeIdentity(value: unknown, depth: number): unknown {
  if (typeof value === "string" && isUuid(value)) return value.toLowerCase();
  if (value && typeof value === "object" && !Array.isArray(value)) {
    const record = value as Record<string, unknown>;
    if (typeof record.stringValue === "string" && isUuid(record.stringValue)) {
      return canonicalBody({ ...record, stringValue: record.stringValue.toLowerCase() }, depth);
    }
  }
  return canonicalBody(value, depth);
}

/** Fingerprint the caller's privacy-blanked JSON, before receive-time
 * clamping, spool receive-time aliases or repository enrichment. The pause
 * listener records the same blanked body the 0.7.44-compatible client spools.
 * Key order and valid UUID spelling are the only further canonicalizations;
 * every other retained body value is exact.
 * A retry must resend this same body to prove the refused event was admitted. */
export function hookBodyDigest(payload: unknown): string {
  const raw = JSON.stringify(payload);
  if (raw === undefined) throw new Error("maintenance_hook_body_invalid");
  const blanked = blankForbiddenRawContent(raw);
  if (!blanked) throw new Error("maintenance_hook_body_invalid");
  return createHash("sha256").update(JSON.stringify(canonicalBody(JSON.parse(blanked.text)))).digest("hex");
}

export function hookReceiptFileName(source: string, digest: string) {
  return `${createHash("sha256").update(`hook\0${source}\0${digest}`).digest("hex")}.receipt`;
}

export function hookBodyFromWire(body: string | Buffer): unknown {
  return JSON.parse(String(body)) as unknown;
}

/** A 0.7.44-compatible client spool injects only its minted ID into an ID-less
 * refused body. Probe those three bounded alternatives, then require the
 * receipt's exact event ID before accepting the stripped candidate. */
export function hookBodyDigestCandidates(payload: unknown) {
  const candidates = [{ digest: hookBodyDigest(payload), injectedId: null as string | null }];
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) return candidates;
  const record = payload as Record<string, unknown>;
  for (const key of HOOK_AUTHORITY_CONTRACT.eventId.aliases) {
    const id = record[key];
    if (typeof id !== "string" || !isUuid(id)) continue;
    const without = { ...record };
    delete without[key];
    candidates.push({ digest: hookBodyDigest(without), injectedId: id });
  }
  return candidates;
}
