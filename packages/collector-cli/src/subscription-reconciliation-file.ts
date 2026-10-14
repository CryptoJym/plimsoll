import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { reconcileSubscriptionCharges, type SubscriptionReconciliationResult } from "./subscription-reconciliation";

function privateJson(file: string, maximum: number): unknown {
  const stat = fs.lstatSync(file);
  if (!stat.isFile() || stat.isSymbolicLink() || stat.size > maximum ||
      (stat.mode & 0o077) !== 0 || (process.getuid && stat.uid !== process.getuid())) throw new Error("private_evidence_unavailable");
  const fd = fs.openSync(file, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
  try {
    const opened = fs.fstatSync(fd);
    if (opened.dev !== stat.dev || opened.ino !== stat.ino || opened.size > maximum) throw new Error("private_evidence_changed");
    return JSON.parse(fs.readFileSync(fd, "utf8"));
  } finally { fs.closeSync(fd); }
}

/** Reprocessing preserves every source clock. It never makes an old bank read current. */
export function readSubscriptionReconciliation(home: string, now: string) {
  const unavailable = (reason: string) => ({ state: "UNKNOWN" as const, reason,
    currentCertifiedSettledTotals: null, result: null as SubscriptionReconciliationResult | null,
    freshness: [] as Array<{ sourceRef: string; state: string; asOf: string | null }>,
    observedPostedBankDebits: [] as Array<{ sourceRef:string;provider:string;currency:string;kind:string;amountCents:number|null;rows:number;asOf:string|null;freshness:string;accountJoin:"UNKNOWN" }>,
    supplemental: { state: "UNKNOWN", hostedBankRows: null as number | null, unknownReceiptCount: null as number | null },
    completeness: "UNKNOWN", cashAllocation: "UNKNOWN", inputDigest: null as string | null });
  try {
    const file = path.join(home, "subscription-reconciliation-input.json");
    const input = privateJson(file, 8 * 1024 * 1024);
    const result = reconcileSubscriptionCharges(input);
    const freshness = result.sources.map(source => {
      const asOf = source.type === "BANK" ? source.lastSyncedAt : source.providerObservedAt;
      const age = asOf ? Date.parse(now) - Date.parse(asOf) : NaN;
      const readAge = Date.parse(now) - Date.parse(source.readAt);
      return { sourceRef: source.sourceRef, asOf, state: Number.isFinite(age) && age >= -300_000 &&
        age <= 86_400_000 && readAge >= -300_000 && readAge <= 86_400_000 ? "fresh" : "UNKNOWN" };
    });
    // Posted bank debits are independently observed cash records. They are not
    // invoice matches or subscription-account totals. Never sum across sources.
    const debitGroups=new Map<string,ReturnType<typeof unavailable>["observedPostedBankDebits"][number]>();
    for (const row of result.bankTransactions) {
      const bank=row.record, source=result.sources.find(source=>source.sourceRef === bank.sourceRef);
      const postedDay=bank.postedAt?.slice(0,10);
      if (row.disposition !== "ACTIVE" || bank.state !== "POSTED" || bank.amountCents<=0 || !bank.provider ||
          !postedDay || !source?.coverage.periodStart || !source.coverage.periodEnd ||
          postedDay<source.coverage.periodStart || postedDay>=source.coverage.periodEnd) continue;
      const kind=bank.kind ?? "UNKNOWN", key=JSON.stringify([bank.sourceRef,bank.provider,bank.currency,kind]);
      const group=debitGroups.get(key) ?? {sourceRef:bank.sourceRef,provider:bank.provider,currency:bank.currency,kind,
        amountCents:0,rows:0,asOf:source.lastSyncedAt,freshness:freshness.find(item=>item.sourceRef === bank.sourceRef)?.state ?? "UNKNOWN",accountJoin:"UNKNOWN" as const};
      const sum=group.amountCents === null ? NaN : group.amountCents+bank.amountCents;
      group.amountCents=Number.isSafeInteger(sum) ? sum : null;group.rows++;debitGroups.set(key,group);
    }
    let supplemental: ReturnType<typeof unavailable>["supplemental"] = unavailable("").supplemental;
    try {
      const index = privateJson(path.join(home, "subscription-reconciliation-evidence-index.json"), 1024 * 1024) as {
        supplementalHostedBankView?: { rows?: unknown }; unknownReceipts?: unknown[] };
      const rows = index.supplementalHostedBankView?.rows;
      supplemental = { state: "UNKNOWN", hostedBankRows: Number.isSafeInteger(rows) && Number(rows) >= 0 ? Number(rows) : null,
        unknownReceiptCount: Array.isArray(index.unknownReceipts) ? index.unknownReceipts.length : null };
    } catch { /* Supplemental observations cannot certify settlement. */ }
    const current = freshness.length > 0 && freshness.every(source => source.state === "fresh");
    const settled = result.certifiedSettledTotals.length > 0;
    return { state: current && settled ? "partial" as const : "UNKNOWN" as const,
      reason: !current ? "SOURCE_FRESHNESS_UNKNOWN" : settled ? "PROVIDED_RECORDS_ONLY" : "NO_VERIFIED_SETTLEMENTS",
      currentCertifiedSettledTotals: current && settled ? result.certifiedSettledTotals : null,
      result, freshness, observedPostedBankDebits:[...debitGroups.values()], supplemental, completeness: "UNKNOWN", cashAllocation: "UNKNOWN",
      inputDigest: `sha256:${crypto.createHash("sha256").update(JSON.stringify(input)).digest("hex")}` };
  } catch { return unavailable("RECONCILIATION_INPUT_UNKNOWN"); }
}
