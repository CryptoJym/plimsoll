/** Design prototype only. No CLI, scheduler, ledger connection or production deletes. */
export type RetentionCopy = {
  key: string;
  createdAt: string;
  bytes: number;
  payloadDigest: string;
  audienceDigest: string;
  pendingDelivery: boolean;
  replayHold: boolean;
  disputeHold: boolean;
  receipt: null | {
    verified: boolean;
    rawKey: string;
    payloadDigest: string;
    audienceDigest: string;
    acknowledgedAt: string;
  };
};
export type RetentionCopyPolicy = {
  enabled?: boolean;
  retentionDays: number;
  maxLocalBytes: number;
  maxRows: number;
  now: string;
  /** Complete metadata inventory, including the bytes of state that must remain. */
  complete: boolean;
  retainedStateBytes: number;
};

/** A candidate must preserve every surviving implicit identity and its referent. */
export function retentionRowIdentitiesMatch(before: readonly { table: string; rowid: number; key: string }[],
  after: readonly { table: string; rowid: number; key: string }[]) {
  const signature = (rows: typeof before) => rows.map(row => JSON.stringify([row.table, row.rowid, row.key])).sort();
  return JSON.stringify(signature(before)) === JSON.stringify(signature(after));
}

export function planRetentionCopies(rows: readonly RetentionCopy[], policy: RetentionCopyPolicy) {
  const integer = (n: number) => Number.isSafeInteger(n) && n >= 0;
  const now = Date.parse(policy.now);
  if (!Number.isFinite(now) || !integer(policy.retentionDays) || policy.retentionDays < 1 || policy.retentionDays > 3650 ||
      !integer(policy.maxLocalBytes) || policy.maxLocalBytes < 1 || !integer(policy.maxRows) || policy.maxRows < 1 ||
      policy.maxRows > 10_000 || !integer(policy.retainedStateBytes) ||
      rows.some(row => !integer(row.bytes)) || new Set(rows.map(row => row.key)).size !== rows.length) {
    throw new Error("invalid retention prototype inventory or policy");
  }
  const cutoff = now - policy.retentionDays * 86_400_000;
  const reasons: Record<string, number> = {};
  const eligible: RetentionCopy[] = [];
  for (const row of rows) {
    const captured = Date.parse(row.createdAt);
    const receipt = row.receipt;
    const acknowledged = receipt ? Date.parse(receipt.acknowledgedAt) : NaN;
    const digest = (value: string) => /^sha256:[a-f0-9]{64}$/.test(value);
    const reason = !Number.isFinite(captured) || captured >= cutoff ? "age_or_time_unproven" :
      row.pendingDelivery !== false || row.replayHold !== false || row.disputeHold !== false ? "active_hold" :
      !receipt || receipt.verified !== true || receipt.rawKey !== row.key ||
        !digest(row.payloadDigest) || receipt.payloadDigest !== row.payloadDigest ||
        !digest(row.audienceDigest) || receipt.audienceDigest !== row.audienceDigest ? "cloud_receipt_unverified" :
      !Number.isFinite(acknowledged) || acknowledged < captured || acknowledged >= cutoff ? "receipt_window_not_elapsed" : null;
    if (reason) reasons[reason] = (reasons[reason] ?? 0) + 1;
    else eligible.push(row);
  }
  eligible.sort((a, b) => a.createdAt.localeCompare(b.createdAt) || a.key.localeCompare(b.key));
  const candidates = policy.enabled === true ? eligible.slice(0, policy.maxRows) : [];
  const bytes = (items: readonly RetentionCopy[]) => items.reduce((total, row) => total + row.bytes, 0);
  const observedBytes = bytes(rows) + policy.retainedStateBytes;
  if (!integer(observedBytes)) throw new Error("invalid retention prototype byte total");
  const candidateBytes = bytes(candidates);
  return {
    state: policy.enabled === true ? "preview" as const : "disabled" as const,
    coverage: policy.complete ? "complete" as const : "partial" as const,
    candidateKeys: candidates.map(row => row.key), candidateBytes,
    observedBytes, totalBytes: policy.complete ? observedBytes : null,
    projectedBytes: policy.complete ? observedBytes - candidateBytes : null,
    capExceeded: policy.complete ? observedBytes - candidateBytes > policy.maxLocalBytes : null,
    heldReasons: reasons, hasMoreEligible: policy.enabled === true && eligible.length > candidates.length,
  };
}
