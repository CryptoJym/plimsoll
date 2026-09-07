import crypto from "node:crypto";

// Wire contract v1. Kept identical in collector and cloud; see delivery-ack-v1.md.
export const DELIVERY_ACK_HEADER = "x-plimsoll-ack-version";
export class DeliveryAcknowledgementError extends Error {
  constructor() { super("Delivery failed: invalid_acknowledgement"); this.name = "DeliveryAcknowledgementError"; }
}
const digest = (value: string) => `sha256:${crypto.createHash("sha256").update(value).digest("hex")}`;
const record = (value: unknown): Record<string, unknown> =>
  value !== null && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};
const normalizedId = (id: string) => /^[\da-f]{8}-(?:[\da-f]{4}-){3}[\da-f]{12}$/i.test(id) ? id.toLowerCase() : id;
export const deliveryItemId = (type: string, id: string) => digest(JSON.stringify([type, normalizedId(id)]));
export type DeliveryExpectation = {
  version: 1; kind: string; requestDigest: string; scopeDigest: string;
  tenantId: string; installKey: string; itemIds: string[]; counts: Record<string, number>;
};

export function deliveryExpectation(rawBody: string, installKey: string): DeliveryExpectation {
  let payload: Record<string, unknown>;
  try { payload = record(JSON.parse(rawBody)); } catch { throw new DeliveryAcknowledgementError(); }
  if (typeof payload.tenantId !== "string" || !payload.tenantId || !installKey ||
      (payload.installKey !== undefined && payload.installKey !== installKey)) throw new DeliveryAcknowledgementError();
  let kind: string;
  const itemIds: string[] = [];
  const counts: Record<string, number> = {};
  const add = (type: string, rows: unknown, nested?: string) => {
    if (!Array.isArray(rows)) throw new DeliveryAcknowledgementError();
    counts[type] = rows.length;
    for (const row of rows) {
      const value = nested ? record(record(row)[nested]) : record(row);
      if (typeof value.id !== "string" || !value.id) throw new DeliveryAcknowledgementError();
      itemIds.push(deliveryItemId(type, value.id));
    }
  };
  if (payload.kind === "session_sync") { kind = "sessions"; add("session", payload.sessions, "session"); }
  else if (payload.kind === "attribution_repair") { kind = "attribution_repair"; add("event", payload.rows); }
  else if (Array.isArray(payload.events)) { kind = "events"; add("event", payload.events, "event"); }
  else if (Array.isArray(payload.artifacts)) { kind = "outcomes"; add("artifact", payload.artifacts); add("outcome", payload.outcomes ?? []); }
  else throw new DeliveryAcknowledgementError();
  if (itemIds.length === 0 || itemIds.length > 1500 || new Set(itemIds).size !== itemIds.length) throw new DeliveryAcknowledgementError();
  return { version: 1, kind, requestDigest: digest(rawBody), scopeDigest: digest(JSON.stringify([payload.tenantId, installKey])),
    tenantId: payload.tenantId, installKey, itemIds, counts };
}

/** acceptedIds must come from completed tenant-scoped storage, including held
 * duplicates/superseded snapshots. Never create this from the request alone.
 */
export function deliveryAcknowledgement(expected: DeliveryExpectation, acceptedIds: string[]) {
  const accepted = new Set(acceptedIds);
  if (accepted.size !== acceptedIds.length || acceptedIds.some(id => !expected.itemIds.includes(id))) throw new DeliveryAcknowledgementError();
  return { version: 1, kind: expected.kind, requestDigest: expected.requestDigest, scopeDigest: expected.scopeDigest,
    acceptedIds, rejectedIds: expected.itemIds.filter(id => !accepted.has(id)) };
}

export function validateDeliveryAcknowledgement(body: unknown, expected: DeliveryExpectation) {
  const response = record(body);
  const ack = record(response.ack);
  const fail = () => { throw new DeliveryAcknowledgementError(); };
  if ((response.ok !== undefined && response.ok !== true) || "error" in response ||
      (response.tenantId !== undefined && response.tenantId !== expected.tenantId) ||
      (response.installKey !== undefined && response.installKey !== expected.installKey) ||
      ack.version !== 1 || ack.kind !== expected.kind || ack.requestDigest !== expected.requestDigest || ack.scopeDigest !== expected.scopeDigest ||
      !Array.isArray(ack.acceptedIds) || !Array.isArray(ack.rejectedIds) || ack.rejectedIds.length !== 0) fail();
  const ids = ack.acceptedIds as unknown[];
  if (ids.length !== expected.itemIds.length || new Set(ids).size !== ids.length || ids.some(id => typeof id !== "string" || !expected.itemIds.includes(id))) fail();
  const count = expected.itemIds.length;
  const counter = (name: string, max: number, exact = false) => {
    const value = response[name];
    if (value !== undefined && (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0 || (exact ? value !== max : value > max))) fail();
  };
  for (const name of ["accepted", "inserted", "matched", "updated", "skippedStale"]) counter(name, count, name === "accepted" || name === "matched");
  counter("acceptedArtifacts", expected.counts.artifact ?? 0, true);
  counter("acceptedOutcomes", expected.counts.outcome ?? 0, true);
  for (const name of ["detachedActorRefs", "detachedSessionRefs"]) counter(name, count);
  if (typeof response.inserted === "number" && typeof response.updated === "number" && response.inserted + response.updated > count) fail();
  if (typeof response.inserted === "number" && typeof response.updated === "number" && typeof response.skippedStale === "number" && response.inserted + response.updated + response.skippedStale !== count) fail();
  return count;
}
