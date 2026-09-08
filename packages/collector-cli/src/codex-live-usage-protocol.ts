import crypto from "node:crypto";

export const LIVE_SCHEMA = "codex.app-server.usage.v1" as const;
export const LIVE_BODY_BYTES = 4096;
export const LIVE_ID = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;
export const LIVE_DIGEST = /^[a-f0-9]{64}$/;
export const LIVE_COUNTERS = ["inputTokens", "cachedInputTokens", "cacheWriteInputTokens",
  "outputTokens", "reasoningOutputTokens", "totalTokens"] as const;
export type LiveTotals = Record<(typeof LIVE_COUNTERS)[number], number>;
type Common = { schema: typeof LIVE_SCHEMA; producerId: string; credentialId: string;
  attachmentId: string; threadId: string; capturedAt: string };
export type LiveUsagePacket = Common & { kind: "usage"; observationSeq: number;
  previousDigest: string | null; turnId: string; total: LiveTotals };
export type LiveGapPacket = Common & { kind: "gap"; controlId: string; gapSeq: number;
  lastObservedSeq: number; reason: "queue_overflow" | "connection_lost" | "attachment_changed" |
    "invalid_notification" | "projection_error" };
export type LivePacket = LiveUsagePacket | LiveGapPacket;
export type LiveDisposition = "stored" | "baseline_only" | "gap" | "counter_reset" |
  "authority_conflict" | "collision" | "enrollment_rejected" | "unsupported" | "retryable";
export type LiveReceipt = { schema: "codex.app-server.receipt.v1"; producerId: string;
  credentialId: string; threadId: string; attachmentId: string; kind: LivePacket["kind"];
  packetDigest: string; disposition: LiveDisposition; replayed: boolean; committed: boolean;
  committedObservationSeq: number | null; observationSeq?: number; controlId?: string };

export function canonicalJson(value: unknown): string {
  if (value === null || typeof value === "string" || typeof value === "boolean") return JSON.stringify(value);
  if (typeof value === "number" && Number.isFinite(value) && !Object.is(value, -0)) return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  if (value && typeof value === "object" && Object.getPrototypeOf(value) === Object.prototype) {
    return `{${Object.keys(value).sort().map(key => `${JSON.stringify(key)}:${canonicalJson((value as Record<string, unknown>)[key])}`).join(",")}}`;
  }
  throw new Error("live_noncanonical_value");
}
export const liveSha256 = (bytes: string | Buffer) => crypto.createHash("sha256").update(bytes).digest("hex");
export const liveEventId = (p: LiveUsagePacket) => "codex-live:" + liveSha256(canonicalJson([
  "plimsoll.codex.live-event.v1", p.producerId, p.attachmentId, p.observationSeq,
]));
export const liveControlId = (attachmentId: string, gapSeq: number) => liveSha256(canonicalJson([
  "plimsoll.codex.gap.v1", attachmentId, gapSeq,
]));
export function liveInteger(value: unknown, minimum = 0): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= minimum && !Object.is(value, -0);
}
export function liveTimestamp(value: unknown): value is string {
  return typeof value === "string" && /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/.test(value) &&
    Number.isFinite(Date.parse(value)) && new Date(value).toISOString() === value;
}
export function exactKeys(value: unknown, keys: readonly string[]): value is Record<string, unknown> {
  return Boolean(value && typeof value === "object" && !Array.isArray(value) &&
    Object.keys(value).sort().join(",") === [...keys].sort().join(","));
}
export function parseLivePacket(bytes: Buffer): LivePacket {
  if (bytes.length === 0 || bytes.length > LIVE_BODY_BYTES) throw new Error("live_body_invalid");
  const text = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  const p: unknown = JSON.parse(text);
  const common = ["schema", "kind", "producerId", "credentialId", "attachmentId", "threadId", "capturedAt"];
  const usage = p && typeof p === "object" && "kind" in p && p.kind === "usage";
  if (!exactKeys(p, [...common, ...(usage ? ["observationSeq", "previousDigest", "turnId", "total"] :
    ["controlId", "gapSeq", "lastObservedSeq", "reason"])])) throw new Error("live_schema_invalid");
  if (p.schema !== LIVE_SCHEMA || !["usage", "gap"].includes(p.kind as string) ||
      ![p.producerId, p.credentialId, p.threadId].every(v => typeof v === "string" && LIVE_ID.test(v)) ||
      typeof p.attachmentId !== "string" || !LIVE_DIGEST.test(p.attachmentId) || !liveTimestamp(p.capturedAt)) {
    throw new Error("live_schema_invalid");
  }
  if (usage) {
    const total = p.total;
    if (!liveInteger(p.observationSeq, 1) || typeof p.turnId !== "string" || !LIVE_ID.test(p.turnId) ||
        !(p.previousDigest === null || (typeof p.previousDigest === "string" && LIVE_DIGEST.test(p.previousDigest))) ||
        !exactKeys(total, LIVE_COUNTERS) || !LIVE_COUNTERS.every(k => liveInteger(total[k]))) {
      throw new Error("live_usage_invalid");
    }
  } else if (!liveInteger(p.gapSeq, 1) || !liveInteger(p.lastObservedSeq) ||
      !["queue_overflow", "connection_lost", "attachment_changed", "invalid_notification", "projection_error"].includes(p.reason as string) ||
      p.controlId !== liveControlId(p.attachmentId, p.gapSeq)) throw new Error("live_gap_invalid");
  if (!Buffer.from(canonicalJson(p)).equals(bytes)) throw new Error("live_body_noncanonical");
  return p as LivePacket;
}
export function liveReceipt(p: LivePacket, digest: string, disposition: LiveDisposition,
  committed: boolean, committedObservationSeq: number | null): LiveReceipt {
  return { schema: "codex.app-server.receipt.v1", producerId: p.producerId, credentialId: p.credentialId,
    threadId: p.threadId, attachmentId: p.attachmentId, kind: p.kind, packetDigest: digest,
    ...(p.kind === "usage" ? { observationSeq: p.observationSeq } : { controlId: p.controlId }),
    disposition, replayed: false, committed, committedObservationSeq };
}
export function validLiveTotals(total: LiveTotals) {
  return total.cachedInputTokens <= total.inputTokens && total.reasoningOutputTokens <= total.outputTokens;
}

const observerKeys = new Set(["liveobservationkind", "liveintervalstart", "liveintervalend",
  "liveattributionstate", "livefinanceeligibility", "livetotaltokens", "livereasoningoutputtokens"]);
const normalizedKey = (key: string) => key.replace(/[_.-]/g, "").toLowerCase();
/** Reject client claims of generated observer authority, including OTLP key/value attributes. */
export function hasLiveUsageClaim(value: unknown): boolean {
  const stack = [value];
  while (stack.length) {
    const item = stack.pop();
    if (!item || typeof item !== "object") continue;
    if (Array.isArray(item)) { stack.push(...item); continue; }
    for (const [key, child] of Object.entries(item)) {
      const k = normalizedKey(key);
      if (observerKeys.has(k) ||
          (k === "key" && typeof child === "string" && observerKeys.has(normalizedKey(child))) ||
          (["schema", "sourceversion", "stringvalue"].includes(k) && child === LIVE_SCHEMA) ||
          (["eventtype", "stringvalue"].includes(k) && child === "usage_live") ||
          (["sourceidentityevidenceref", "stringvalue"].includes(k) && child === "native_runtime_observed_interval_v1")) return true;
      if (child && typeof child === "object") stack.push(child);
    }
  }
  return false;
}
