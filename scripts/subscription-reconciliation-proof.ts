import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import {
  parseSubscriptionReconciliationInput, reconcileSubscriptionCharges,
  type SubscriptionReconciliationInput, type SubscriptionInvoice, type SubscriptionBankTransaction, type ChargeKind,
} from "../packages/collector-cli/src/subscription-reconciliation.ts";

// Synthetic labels only. This script never reads files, accounts, ledgers or the network.
const evidence = (label: string) => `sha256:${createHash("sha256").update(label).digest("hex")}` as const;
const native = (label: string) => `native:${createHash("sha256").update(label).digest("hex").slice(0, 32)}`;
const now = "2026-10-14T18:00:00.000Z", paid = "2026-10-01T12:00:00.000Z", posted = "2026-10-02T12:00:00.000Z";
function invoice(patch: Partial<SubscriptionInvoice> = {}): SubscriptionInvoice {
  return { sourceRef: "invoices", invoiceRef: "inv-1", revision: 1, sourceObservedAt: now, provider: "openai",
    accountRef: null, accountIdentityState: "UNKNOWN", amountCents: 2000, currency: "USD", status: "PAID",
    paidAt: paid, invoiceDate: "2026-10-01", kind: "MONTHLY_SUBSCRIPTION", paymentLast4: null,
    paymentReference: "payment-1", evidenceRefs: [evidence("invoice-1")], ...patch };
}
function bank(patch: Partial<SubscriptionBankTransaction> = {}): SubscriptionBankTransaction {
  return { sourceRef: "bank", transactionRef: "txn-1", revision: 1, sourceObservedAt: now, provider: "openai",
    amountCents: 2000, currency: "USD", state: "POSTED", transactedAt: paid, postedAt: posted,
    payerRef: native("payer"), cardLast4: null, paymentReference: "payment-1", kind: null,
    evidenceRefs: [evidence("bank-1")], ...patch };
}
function dayInvoice(patch: Partial<SubscriptionInvoice> = {}): SubscriptionInvoice {
  return invoice({ paidAt: "2026-10-01", ...patch });
}
function dayBank(patch: Partial<SubscriptionBankTransaction> = {}): SubscriptionBankTransaction {
  return bank({ transactedAt: "2026-10-01", postedAt: "2026-10-02", ...patch });
}
function input(invoices = [invoice()], bankTransactions = [bank()]): SubscriptionReconciliationInput {
  return { schema: "subscription-reconciliation/v1", observedAt: now,
    sources: [
      { sourceRef: "invoices", type: "INVOICES", readAt: now, lastSyncedAt: null,
        providerObservedAt: "2026-10-14T17:00:00.000Z", coverage: { state: "PARTIAL", periodStart: "2026-09-01", periodEnd: "2026-10-15" }, evidenceRefs: [evidence("invoice-export")] },
      { sourceRef: "bank", type: "BANK", readAt: now, lastSyncedAt: "2026-10-14T16:00:00.000Z",
        providerObservedAt: null, coverage: { state: "PARTIAL", periodStart: "2026-09-01", periodEnd: "2026-10-15" }, evidenceRefs: [evidence("bank-export")] },
    ], invoices, bankTransactions };
}
const tests: Array<[string, () => void]> = [];
function test(name: string, run: () => void) { tests.push([name, run]); }
const verified = (v: unknown) => reconcileSubscriptionCharges(v).candidates.filter((c) => c.state === "VERIFIED");
function noSettlement(v: unknown) { assert.deepEqual(reconcileSubscriptionCharges(v).certifiedSettledTotals, []); }

test("exact reference settles the invoice but leaves subscription-account identity UNKNOWN", () => {
  const result = reconcileSubscriptionCharges(input());
  assert.equal(result.certifiedSettledTotals.length, 1);
  assert.deepEqual(result.certifiedSettledTotals[0], { provider: "openai", currency: "USD", kind: "MONTHLY_SUBSCRIPTION", amountCents: 2000, matchCount: 1 });
  assert.equal(result.candidates[0].basis, "EXACT_REFERENCE");
  assert.equal(result.candidates[0].state, "VERIFIED");
  assert.equal(result.candidates[0].accountIdentityState, "UNKNOWN");
  assert.equal(result.candidates[0].accountRef, null);
  assert.deepEqual(result.spendAllocation.allocations, []);
  assert.equal(result.spendAllocation.state, "UNKNOWN");
});
test("a bank invoice reference can match the opaque invoiceRef exactly", () => {
  assert.equal(verified(input([invoice({ paymentReference: null })], [bank({ paymentReference: "inv-1" })])).length, 1);
});
test("evidence refs and source read/sync/provider timestamps survive without mutation", () => {
  const fixture = input(), original = JSON.stringify(fixture);
  const result = reconcileSubscriptionCharges(fixture);
  assert.deepEqual(result.sources, parseSubscriptionReconciliationInput(fixture).sources);
  assert.deepEqual(result.invoices[0].record.evidenceRefs, fixture.invoices[0].evidenceRefs);
  assert.deepEqual(result.bankTransactions[0].record.evidenceRefs, fixture.bankTransactions[0].evidenceRefs);
  assert.ok(result.candidates[0].evidenceRefs.includes(evidence("invoice-export")));
  assert.ok(result.candidates[0].evidenceRefs.includes(evidence("bank-export")));
  assert.equal(result.sources[1].lastSyncedAt, "2026-10-14T16:00:00.000Z");
  result.invoices[0].record.evidenceRefs.push(evidence("output-only"));
  assert.equal(JSON.stringify(fixture), original);
  assert.deepEqual(reconcileSubscriptionCharges(fixture), reconcileSubscriptionCharges(JSON.parse(original)));
});
test("amount/date/provider alone remains an UNKNOWN candidate", () => {
  const r = reconcileSubscriptionCharges(input([invoice({ paymentReference: null })], [bank({ paymentReference: null })]));
  assert.equal(r.candidates.length, 1);
  assert.equal(r.candidates[0].state, "UNKNOWN");
  assert.ok(r.candidates[0].reasons.includes("PAYMENT_IDENTITY_UNKNOWN"));
  assert.deepEqual(r.certifiedSettledTotals, []);
});
test("matching card evidence can settle; explicit card, payer or reference conflicts cannot", () => {
  const i = invoice({ paymentReference: null, paymentLast4: "4242", payerRef: native("payer") });
  const b = bank({ paymentReference: null, cardLast4: "4242" });
  assert.equal(verified(input([i], [b]))[0]?.basis, "PAYMENT_INSTRUMENT");
  for (const patch of [{ cardLast4: "1111" }, { payerRef: native("different-payer") }, { paymentReference: "wrong-reference" }]) {
    noSettlement(input([invoice({ ...i, paymentReference: "payment-1" })], [bank({ ...b, ...patch })]));
  }
  noSettlement(input([invoice({ paymentLast4: "1111" })], [bank({ cardLast4: "4242" })]));
});
test("a documented card alias is scoped to provider, payer and both dates", () => {
  const fixture = input([invoice({ paymentReference: null, paymentLast4: "1111", payerRef: native("payer") })], [bank({ paymentReference: null, cardLast4: "4242" })]);
  noSettlement(fixture);
  fixture.cardAliases = [{ provider: "openai", payerRef: native("payer"), invoiceLast4: "1111", bankLast4: "4242", validFrom: "2026-09-01T00:00:00.000Z", validThrough: "2026-10-10T00:00:00.000Z", evidenceRefs: [evidence("documented-tokenized-card-alias")] }];
  const c = verified(fixture)[0];
  assert.equal(c?.basis, "DOCUMENTED_CARD_ALIAS");
  assert.ok(c.evidenceRefs.includes(evidence("documented-tokenized-card-alias")));
  for (const patch of [{ provider: "anthropic" }, { payerRef: native("other") }, { validThrough: "2026-09-30T00:00:00.000Z" }]) {
    noSettlement({ ...fixture, cardAliases: [{ ...fixture.cardAliases[0], ...patch }] });
  }
});
test("equal amounts remain ambiguous across all possible one-to-one mappings", () => {
  const invoices = [invoice({ paymentReference: null, paymentLast4: "4242" }), invoice({ invoiceRef: "inv-2", paymentReference: null, paymentLast4: "4242" })];
  const banks = [bank({ paymentReference: null, cardLast4: "4242" }), bank({ transactionRef: "txn-2", paymentReference: null, cardLast4: "4242" })];
  const r = reconcileSubscriptionCharges(input(invoices, banks));
  assert.equal(r.candidates.length, 4);
  assert.ok(r.candidates.every((c) => c.reasons.includes("AMBIGUOUS_MAPPING")));
  assert.deepEqual(r.certifiedSettledTotals, []);
});
test("a weak candidate blocks a greedy strong match in either direction", () => {
  const strongI = invoice({ paymentLast4: "4242" }), strongB = bank({ cardLast4: "4242" });
  const weakB = bank({ transactionRef: "txn-weak", paymentReference: null, cardLast4: null });
  const weakI = invoice({ invoiceRef: "inv-weak", paymentReference: null, paymentLast4: null });
  noSettlement(input([strongI], [strongB, weakB]));
  noSettlement(input([strongI, weakI], [strongB]));
});
test("a graph with a forced assignment still abstains when a candidate has multiple neighbors", () => {
  const invoices = [invoice({ paymentLast4: "4242" }), invoice({ invoiceRef: "inv-2", paymentReference: "payment-2", paymentLast4: "1111" })];
  const banks = [bank({ cardLast4: "4242" }), bank({ transactionRef: "txn-weak", paymentReference: null, cardLast4: null })];
  noSettlement(input(invoices, banks));
});
test("distinct exact references disambiguate equal-amount payments", () => {
  const fixture = input([invoice(), invoice({ invoiceRef: "inv-2", paymentReference: "payment-2" })], [bank(), bank({ transactionRef: "txn-2", paymentReference: "payment-2" })]);
  assert.equal(verified(fixture).length, 2);
  assert.equal(reconcileSubscriptionCharges(fixture).certifiedSettledTotals[0].amountCents, 4000);
});
test("exact replay is visible once as ACTIVE and once as REPLAY and is never added twice", () => {
  const fixture = input([invoice(), invoice()], [bank(), bank()]);
  const r = reconcileSubscriptionCharges(fixture);
  assert.equal(r.invoices.length, 2);
  assert.equal(r.bankTransactions.length, 2);
  assert.deepEqual(r.invoices.map((v) => v.disposition), ["ACTIVE", "REPLAY"]);
  assert.deepEqual(r.bankTransactions.map((v) => v.disposition), ["ACTIVE", "REPLAY"]);
  assert.equal(r.certifiedSettledTotals[0]?.amountCents, 2000);
  assert.deepEqual(reconcileSubscriptionCharges(fixture), r);
});
test("conflicting duplicates and non-monotone revisions fail closed", () => {
  for (const changed of [bank({ amountCents: 2001 }), bank({ state: "REMOVED" }), bank({ revision: 2, sourceObservedAt: "2026-10-13T18:00:00.000Z" })]) {
    const r = reconcileSubscriptionCharges(input([invoice()], [bank(), changed]));
    assert.deepEqual(r.bankTransactions.map((v) => v.disposition), ["REVISION_CONFLICT", "REVISION_CONFLICT"]);
    assert.deepEqual(r.certifiedSettledTotals, []);
  }
  noSettlement(input([invoice(), invoice({ amountCents: 2001 })], [bank()]));
});
test("pending-to-posted revisions use source revision and preserve older records in any order", () => {
  const pending = bank({ revision: 1, state: "PENDING", postedAt: null, sourceObservedAt: "2026-10-01T18:00:00.000Z" });
  const current = bank({ revision: 2 });
  for (const rows of [[pending, current], [current, pending]]) {
    const r = reconcileSubscriptionCharges(input([invoice()], rows));
    assert.equal(r.certifiedSettledTotals[0]?.amountCents, 2000);
    assert.equal(r.bankTransactions.find((v) => v.record.revision === 1)?.disposition, "SUPERSEDED");
  }
});
test("removed is authoritative even after posted; pending and removed rows remain visible", () => {
  const fixture = input([invoice()], [bank({ revision: 1, sourceObservedAt: "2026-10-02T18:00:00.000Z" }), bank({ revision: 2, state: "REMOVED" }), bank({ transactionRef: "pending", state: "PENDING", postedAt: null })]);
  const r = reconcileSubscriptionCharges(fixture);
  assert.equal(r.bankTransactions.length, 3);
  assert.deepEqual(r.certifiedSettledTotals, []);
  assert.equal(r.bankTransactions[1].record.state, "REMOVED");
  noSettlement(input([invoice()], [bank({ state: "PENDING" })]));
});
test("a pending-to-removed revision remains a visible non-settled record", () => {
  const r = reconcileSubscriptionCharges(input([invoice()], [
    bank({ state: "PENDING", postedAt: null, sourceObservedAt: "2026-10-01T18:00:00.000Z" }),
    bank({ revision: 2, state: "REMOVED", postedAt: null }),
  ]));
  assert.deepEqual(r.bankTransactions.map((b) => b.disposition), ["SUPERSEDED", "ACTIVE"]);
  assert.ok(r.bankTransactions[1].reasons.includes("BANK_REMOVED"));
  assert.deepEqual(r.certifiedSettledTotals, []);
});
test("a new posted id never guesses that it supersedes a pending or removed id", () => {
  const r = reconcileSubscriptionCharges(input([invoice()], [bank(), bank({ transactionRef: "old-id", state: "REMOVED", postedAt: null }), bank({ transactionRef: "other-id", state: "PENDING", postedAt: null })]));
  assert.ok(r.bankTransactions.every((v) => v.disposition === "ACTIVE"));
  assert.equal(r.certifiedSettledTotals[0]?.amountCents, 2000);
});
test("revised invoice amount/status is authoritative, never the import array order", () => {
  const old = invoice({ revision: 1, sourceObservedAt: "2026-10-02T18:00:00.000Z" });
  noSettlement(input([old, invoice({ revision: 2, amountCents: 3000 })], [bank()]));
  noSettlement(input([invoice({ revision: 2, status: "VOID" }), old], [bank()]));
});
test("cross-source copies are not deduplicated by merchant, amount or dates", () => {
  const fixture = input([invoice()], [bank(), bank({ sourceRef: "other-bank" })]);
  fixture.sources.push({ ...fixture.sources[1], sourceRef: "other-bank", evidenceRefs: [evidence("other-export")] });
  const r = reconcileSubscriptionCharges(fixture);
  assert.equal(r.bankTransactions.length, 2);
  assert.ok(r.bankTransactions.every((v) => v.disposition === "ACTIVE"));
  assert.equal(r.crossSourceDedupeState, "UNKNOWN");
  assert.deepEqual(r.certifiedSettledTotals, []);
});
test("duplicate invoices across sources and conflicting bank revisions still block competing matches", () => {
  const fixture = input([invoice(), invoice({ sourceRef: "other-invoices" })], [bank()]);
  fixture.sources.push({ ...fixture.sources[0], sourceRef: "other-invoices", evidenceRefs: [evidence("other-invoice-export")] });
  noSettlement(fixture);
  noSettlement(input([invoice()], [bank(), bank({ transactionRef: "conflicting" }), bank({ transactionRef: "conflicting", state: "PENDING" })]));
});
test("provider, exact integer cents, currency and signed amounts are hard boundaries", () => {
  for (const patch of [{ provider: "anthropic" }, { provider: null }, { amountCents: 1999 }, { amountCents: -2000 }, { currency: "EUR" }]) noSettlement(input([invoice()], [bank(patch)]));
  const fixture = input([invoice(), invoice({ invoiceRef: "inv-eur", currency: "EUR", paymentReference: "eur-payment" })], [bank(), bank({ transactionRef: "eur", currency: "EUR", paymentReference: "eur-payment" })]);
  assert.equal(reconcileSubscriptionCharges(fixture).certifiedSettledTotals.length, 2);
});
test("dates must be present, nearby, chronological and no later than the record observation", () => {
  for (const patch of [{ postedAt: null }, { transactedAt: null }, { postedAt: "2026-10-12T12:00:00.000Z" }, { postedAt: "2026-09-30T12:00:00.000Z" }, { postedAt: "2026-10-15T12:00:00.000Z" }]) noSettlement(input([invoice()], [bank(patch)]));
  for (const patch of [{ paidAt: null }, { invoiceDate: null }, { invoiceDate: "2026-10-03" }, { paidAt: "2026-10-14T19:00:00.000Z" }]) noSettlement(input([invoice(patch)], [bank()]));
});
test("unknown dates or provider on another posted candidate cannot make a strong pair unique", () => {
  noSettlement(input([invoice()], [bank(), bank({ transactionRef: "date-unknown", postedAt: null, paymentReference: null })]));
  noSettlement(input([invoice()], [bank(), bank({ transactionRef: "provider-unknown", provider: null, paymentReference: null })]));
});
test("only paid invoice evidence can settle; unknown/refunded/credit kinds do not become charges", () => {
  for (const status of ["OPEN", "VOID", "REFUNDED", "CREDIT", "UNKNOWN"] as const) noSettlement(input([invoice({ status })], [bank()]));
  for (const kind of ["REFUND", "CREDIT", "UNKNOWN"] as const) noSettlement(input([invoice({ kind })], [bank({ kind })]));
  const r = reconcileSubscriptionCharges(input([invoice({ amountCents: -2000, status: "REFUNDED", kind: "REFUND" })], [bank({ amountCents: -2000, kind: "REFUND" })]));
  assert.equal(r.invoices[0].record.kind, "REFUND");
  assert.equal(r.bankTransactions[0].record.amountCents, -2000);
  assert.deepEqual(r.certifiedSettledTotals, []);
});
test("monthly, upgrade/proration, reset and API-console charges remain separate", () => {
  const kinds: ChargeKind[] = ["MONTHLY_SUBSCRIPTION", "UPGRADE_PRORATION", "USAGE_RESET", "API_CONSOLE"];
  const r = reconcileSubscriptionCharges(input(kinds.map((kind, n) => invoice({ invoiceRef: `inv-${n}`, kind, paymentReference: `pay-${n}` })), kinds.map((kind, n) => bank({ transactionRef: `txn-${n}`, kind, paymentReference: `pay-${n}` }))));
  assert.equal(r.certifiedSettledTotals.length, 4);
  assert.deepEqual(r.certifiedSettledTotals.map((v) => v.kind).sort(), kinds.sort());
  noSettlement(input([invoice()], [bank({ kind: "API_CONSOLE" })]));
});
test("native account hints stay UNKNOWN; verified scoped identity requires its own evidence", () => {
  let fixture = input([invoice({ accountRef: native("hint") })], [bank()]);
  assert.equal(verified(fixture)[0]?.accountIdentityState, "UNKNOWN");
  fixture = input([invoice({ accountRef: native("known"), accountIdentityState: "VERIFIED", accountEvidenceRefs: [evidence("account-link")] })], [bank()]);
  assert.equal(verified(fixture)[0]?.accountRef, native("known"));
  assert.ok(verified(fixture)[0]?.accountEvidenceRefs.includes(evidence("account-link")));
  noSettlement(input(fixture.invoices, [bank({ accountRef: native("different"), accountIdentityState: "VERIFIED", accountEvidenceRefs: [evidence("other-account-link")] })]));
});
test("invalid or unknown contract fields are rejected without echoing their values", () => {
  const cases: Array<(v: any) => void> = [
    (v) => v.schema = "v0", (v) => v.invoices[0].amountCents = 20.01,
    (v) => v.bankTransactions[0].amountCents = Number.MAX_SAFE_INTEGER + 1,
    (v) => v.invoices[0].currency = "usd", (v) => v.invoices[0].invoiceDate = "2026-02-30",
    (v) => v.bankTransactions[0].postedAt = "not-a-date", (v) => v.invoices[0].evidenceRefs = ["unhashed"],
    (v) => v.invoices[0].accountRef = "private@example.invalid", (v) => v.invoices[0].accountIdentityState = "VERIFIED",
    (v) => v.invoices[0].paymentLast4 = "4242424242424242", (v) => v.invoices[0].currentPriceCents = 2000,
    (v) => v.invoices[0].projectedCostCents = 2000, (v) => v.invoices[0].status = "PROJECTED",
    (v) => v.invoices[0].sourceRef = "missing", (v) => v.bankTransactions[0].sourceRef = "invoices",
    (v) => v.sources.push(structuredClone(v.sources[0])), (v) => v.sources[0].readAt = "2026-10-01T00:00:00.000Z",
    (v) => v.sources[0].coverage = { state: "COMPLETE", periodStart: null, periodEnd: null },
    (v) => v.sources[1].lastSyncedAt = "2026-10-15T00:00:00.000Z", (v) => v.invoices[0].revision = 0,
    (v) => v.sources[0].provider = "anthropic", (v) => v.invoices[0].evidenceRefs = [],
  ];
  for (const change of cases) {
    const fixture = input(); change(fixture);
    assert.throws(() => parseSubscriptionReconciliationInput(fixture), (e: any) => e.name === "SubscriptionReconciliationInputError" && !e.message.includes("private@example.invalid"));
    assert.throws(() => reconcileSubscriptionCharges(fixture));
  }
});
test("optional fields reject explicit null when their typed contract is non-nullable", () => {
  for (const patch of [{ cardAliases: null }, { invoices: [invoice({ accountIdentityState: null as any })] }, { invoices: [invoice({ accountEvidenceRefs: null as any })] }]) {
    assert.throws(() => parseSubscriptionReconciliationInput({ ...input(), ...patch }));
  }
});
test("the synthetic 65-row bank-only export keeps all states and zero certified matches", () => {
  const rows = Array.from({ length: 65 }, (_, n) => bank({ transactionRef: `synthetic-${n}`, provider: null,
    state: n < 40 ? "POSTED" : n < 63 ? "REMOVED" : "PENDING", postedAt: n < 40 ? posted : null, paymentReference: null }));
  const r = reconcileSubscriptionCharges(input([], rows));
  assert.equal(r.bankTransactions.length, 65);
  assert.equal(r.bankTransactions.filter((b) => b.record.state === "POSTED").length, 40);
  assert.equal(r.bankTransactions.filter((b) => b.record.state === "REMOVED").length, 23);
  assert.equal(r.bankTransactions.filter((b) => b.record.state === "PENDING").length, 2);
  assert.ok(r.bankTransactions.every((b) => b.state === "UNKNOWN"));
  assert.deepEqual(r.certifiedSettledTotals, []);
});
test("complete source coverage and a known account still cannot allocate spend to chats or projects", () => {
  const fixture = input([invoice({ accountRef: native("known"), accountIdentityState: "VERIFIED", accountEvidenceRefs: [evidence("account-link")] })], [bank()]);
  fixture.sources.forEach((s) => s.coverage = { state: "COMPLETE", periodStart: "2026-09-01", periodEnd: "2026-10-14" });
  const r = reconcileSubscriptionCharges(fixture);
  assert.equal(r.candidates[0].state, "VERIFIED");
  assert.equal(r.candidates[0].accountIdentityState, "VERIFIED");
  assert.equal(r.spendAllocation.state, "UNKNOWN");
  assert.deepEqual(r.spendAllocation.allocations, []);
});
test("empty evidence-bound export yields no inferred charges or completeness", () => {
  const r = reconcileSubscriptionCharges(input([], []));
  assert.deepEqual(r.certifiedSettledTotals, []);
  assert.equal(r.totalsScope, "PROVIDED_RECORDS_ONLY");
  assert.equal(r.crossSourceDedupeState, "UNKNOWN");
  assert.equal(r.spendAllocation.state, "UNKNOWN");
});
test("safe-integer overflow never produces an imprecise certified total", () => {
  const max = Number.MAX_SAFE_INTEGER;
  const fixture = input([invoice({ amountCents: max }), invoice({ invoiceRef: "two", amountCents: 1, paymentReference: "two" })], [bank({ amountCents: max }), bank({ transactionRef: "two", amountCents: 1, paymentReference: "two" })]);
  assert.throws(() => reconcileSubscriptionCharges(fixture), /certified_total_overflow/);
});

// Calendar-precision fixtures extend the original 31 without changing their assertions.
test("calendar strings and precision survive parsing, unmatched rows and detached output", () => {
  const fixture = input([dayInvoice()], [dayBank({ amountCents: 3000 })]);
  const original = JSON.stringify(fixture), parsed = parseSubscriptionReconciliationInput(fixture);
  assert.equal(parsed.invoices[0].paidAt, "2026-10-01");
  assert.equal(parsed.bankTransactions[0].transactedAt, "2026-10-01");
  assert.equal(parsed.bankTransactions[0].postedAt, "2026-10-02");
  const r = reconcileSubscriptionCharges(fixture);
  assert.deepEqual(r.invoices[0].datePrecision, { paidAt: "DAY", invoiceDate: "DAY" });
  assert.deepEqual(r.bankTransactions[0].datePrecision, { transactedAt: "DAY", postedAt: "DAY" });
  assert.equal(r.candidates.length, 0);
  r.invoices[0].datePrecision.paidAt = "INSTANT";
  assert.equal(JSON.stringify(fixture), original);
  assert.equal(reconcileSubscriptionCharges(fixture).invoices[0].datePrecision.paidAt, "DAY");
});
test("a unique paid receipt day and posted bank day with card evidence verify without a paid instant", () => {
  const fixture = input([dayInvoice({ paymentReference: null, paymentLast4: "4242", payerRef: native("payer") })],
    [dayBank({ postedAt: "2026-10-01", paymentReference: null, cardLast4: "4242" })]);
  const r = reconcileSubscriptionCharges(fixture), c = r.candidates[0];
  assert.equal(c.state, "VERIFIED");
  assert.equal(c.basis, "PAYMENT_INSTRUMENT");
  assert.equal(r.invoices[0].record.paidAt, "2026-10-01");
  assert.equal(r.invoices[0].datePrecision.paidAt, "DAY");
  assert.equal(r.bankTransactions[0].datePrecision.postedAt, "DAY");
  assert.ok(c.evidenceRefs.includes(evidence("invoice-1")));
  assert.ok(c.evidenceRefs.includes(evidence("bank-1")));
  assert.deepEqual(r.certifiedSettledTotals, [{ provider: "openai", currency: "USD", kind: "MONTHLY_SUBSCRIPTION", amountCents: 2000, matchCount: 1 }]);
  assert.equal(c.accountIdentityState, "UNKNOWN");
  assert.equal(r.crossSourceDedupeState, "UNKNOWN");
  assert.equal(r.spendAllocation.state, "UNKNOWN");
  assert.deepEqual(r.spendAllocation.allocations, []);
});
test("different precise instants on one day retain their exact order and source spelling", () => {
  const paidAt = "2026-10-01T12:00:00Z", transactedAt = "2026-10-01T12:30:00Z";
  const r = reconcileSubscriptionCharges(input([invoice({ paidAt })], [bank({ transactedAt, postedAt: "2026-10-01T13:00:00Z" })]));
  assert.equal(r.candidates[0].state, "VERIFIED");
  assert.equal(r.invoices[0].record.paidAt, paidAt);
  assert.equal(r.bankTransactions[0].record.transactedAt, transactedAt);
  assert.deepEqual(r.bankTransactions[0].datePrecision, { transactedAt: "INSTANT", postedAt: "INSTANT" });
  const reversed = reconcileSubscriptionCharges(input([invoice({ paidAt })], [bank({ transactedAt, postedAt: "2026-10-01T12:29:59.999Z" })]));
  assert.ok(reversed.candidates[0].reasons.includes("DATE_ORDER_CONFLICT"));
  assert.equal(reversed.candidates[0].competesForMapping, false);
  assert.deepEqual(reversed.certifiedSettledTotals, []);
});
test("mixed day and instant evidence can be compatible on the same day or ordered days", () => {
  const fixtures = [
    input([dayInvoice()], [bank({ transactedAt: "2026-10-01", postedAt: "2026-10-01T13:00:00Z" })]),
    input([invoice()], [dayBank({ transactedAt: paid, postedAt: "2026-10-01" })]),
    input([dayInvoice()], [dayBank({ transactedAt: "2026-10-02T12:00:00Z", postedAt: "2026-10-03" })]),
    input([invoice()], [dayBank()]),
  ];
  for (const fixture of fixtures) {
    const r = reconcileSubscriptionCharges(fixture);
    assert.equal(r.candidates[0].state, "VERIFIED");
    assert.equal(r.invoices[0].record.paidAt, fixture.invoices[0].paidAt);
    assert.equal(r.bankTransactions[0].record.postedAt, fixture.bankTransactions[0].postedAt);
  }
  const r = reconcileSubscriptionCharges(fixtures[0]);
  assert.deepEqual(r.bankTransactions[0].datePrecision, { transactedAt: "DAY", postedAt: "INSTANT" });
});
test("missing charge dates retain UNKNOWN precision and required date fields cannot be omitted", () => {
  const fixture = input([dayInvoice({ paidAt: null, invoiceDate: null })], [dayBank({ transactedAt: null, postedAt: null })]);
  const r = reconcileSubscriptionCharges(fixture);
  assert.deepEqual(r.invoices[0].datePrecision, { paidAt: "UNKNOWN", invoiceDate: "UNKNOWN" });
  assert.deepEqual(r.bankTransactions[0].datePrecision, { transactedAt: "UNKNOWN", postedAt: "UNKNOWN" });
  assert.ok(r.candidates[0].reasons.includes("DATE_EVIDENCE_MISSING"));
  assert.equal(r.candidates[0].competesForMapping, true);
  assert.deepEqual(r.certifiedSettledTotals, []);
  for (const fixture of [input([dayInvoice({ paidAt: undefined })]), input([dayInvoice()], [dayBank({ transactedAt: undefined })]), input([dayInvoice()], [dayBank({ postedAt: undefined })])]) {
    assert.throws(() => parseSubscriptionReconciliationInput(fixture));
  }
});
test("leap days validate and impossible calendar dates fail in every newly accepted charge field", () => {
  const leap = input([dayInvoice({ invoiceDate: "2024-02-29", paidAt: "2024-02-29" })], [dayBank({ transactedAt: "2024-02-29", postedAt: "2024-02-29" })]);
  assert.equal(verified(leap).length, 1);
  for (const value of ["2026-02-29", "2026-02-30", "2026-04-31", "2026-13-01", "2026-10-00", "2026-1-01", "2026-10-01T00:00:00+00:00"]) {
    for (const fixture of [input([dayInvoice({ paidAt: value })]), input([dayInvoice()], [dayBank({ transactedAt: value })]), input([dayInvoice()], [dayBank({ postedAt: value })])]) {
      assert.throws(() => parseSubscriptionReconciliationInput(fixture), (e: any) => e.name === "SubscriptionReconciliationInputError" && !e.message.includes(value));
      assert.throws(() => reconcileSubscriptionCharges(fixture));
    }
  }
});
test("observation, source freshness and card-alias validity still require exact UTC timestamps", () => {
  const changes: Array<(v: SubscriptionReconciliationInput) => void> = [
    (v) => v.observedAt = "2026-10-14",
    (v) => v.sources[0].readAt = "2026-10-14",
    (v) => v.sources[0].lastSyncedAt = "2026-10-14",
    (v) => v.sources[0].providerObservedAt = "2026-10-14",
    (v) => v.invoices[0].sourceObservedAt = "2026-10-14",
    (v) => v.bankTransactions[0].sourceObservedAt = "2026-10-14",
    (v) => v.cardAliases![0].validFrom = "2026-10-01",
    (v) => v.cardAliases![0].validThrough = "2026-10-02",
  ];
  for (const change of changes) {
    const fixture = input([dayInvoice()], [dayBank()]);
    fixture.cardAliases = [{ provider: "openai", payerRef: native("payer"), invoiceLast4: "1111", bankLast4: "4242", validFrom: paid, validThrough: posted, evidenceRefs: [evidence("alias")] }];
    change(fixture);
    assert.throws(() => parseSubscriptionReconciliationInput(fixture));
  }
});
test("future calendar dates are compared with each record observation, not the later source read", () => {
  const observed = "2026-10-01T18:00:00Z";
  const fixtures = [
    input([dayInvoice({ paidAt: "2026-10-02", sourceObservedAt: observed })], [dayBank()]),
    input([dayInvoice()], [dayBank({ transactedAt: "2026-10-02", sourceObservedAt: observed })]),
    input([dayInvoice()], [dayBank({ sourceObservedAt: observed })]),
  ];
  for (const fixture of fixtures) {
    const r = reconcileSubscriptionCharges(fixture);
    assert.ok(r.candidates[0].reasons.includes("FUTURE_CHARGE_DATE"));
    assert.equal(r.candidates[0].competesForMapping, false);
    assert.deepEqual(r.certifiedSettledTotals, []);
  }
});
test("calendar and mixed chronology rejects a later invoice day or a definitely earlier posting", () => {
  for (const fixture of [
    input([dayInvoice({ invoiceDate: "2026-10-02" })], [dayBank()]),
    input([dayInvoice()], [dayBank({ transactedAt: "2026-10-02", postedAt: "2026-10-01" })]),
    input([dayInvoice()], [dayBank({ transactedAt: "2026-10-02T00:00:00Z", postedAt: "2026-10-01" })]),
    input([dayInvoice()], [dayBank({ transactedAt: "2026-10-02", postedAt: "2026-10-01T23:59:59.999Z" })]),
  ]) {
    const r = reconcileSubscriptionCharges(fixture);
    assert.ok(r.candidates[0].reasons.includes("DATE_ORDER_CONFLICT"));
    assert.equal(r.candidates[0].competesForMapping, false);
    assert.deepEqual(r.certifiedSettledTotals, []);
  }
});
test("a charge day on its observation day is compatible without blessing a future precise instant", () => {
  const observed = "2026-10-01T00:00:00Z";
  const fixture = input([dayInvoice({ sourceObservedAt: observed })], [dayBank({ sourceObservedAt: observed, postedAt: "2026-10-01" })]);
  assert.equal(verified(fixture).length, 1);
  for (const precise of [input([dayInvoice({ sourceObservedAt: observed, paidAt: "2026-10-01T00:00:00.001Z" })], fixture.bankTransactions),
    input(fixture.invoices, [dayBank({ sourceObservedAt: observed, postedAt: "2026-10-01T00:00:00.001Z" })])]) {
    const r = reconcileSubscriptionCharges(precise);
    assert.ok(r.candidates[0].reasons.includes("FUTURE_CHARGE_DATE"));
    assert.deepEqual(r.certifiedSettledTotals, []);
  }
});
test("six calendar days of separation fit wholly inside the existing seven elapsed day bound", () => {
  const r = reconcileSubscriptionCharges(input([dayInvoice()], [dayBank({ postedAt: "2026-10-07" })]));
  assert.equal(r.candidates[0].state, "VERIFIED");
  assert.equal(r.bankTransactions[0].record.postedAt, "2026-10-07");
  assert.equal(r.bankTransactions[0].datePrecision.postedAt, "DAY");
});
test("seven calendar days of separation are uncertain; eight days are definitely outside", () => {
  const uncertain = reconcileSubscriptionCharges(input([dayInvoice()], [dayBank({ postedAt: "2026-10-08" })]));
  assert.ok(uncertain.candidates[0].reasons.includes("DATE_WINDOW_UNCERTAIN"));
  assert.ok(!uncertain.candidates[0].reasons.includes("DATE_OUTSIDE_WINDOW"));
  assert.equal(uncertain.candidates[0].competesForMapping, true);
  assert.deepEqual(uncertain.certifiedSettledTotals, []);
  const outside = reconcileSubscriptionCharges(input([dayInvoice()], [dayBank({ postedAt: "2026-10-09" })]));
  assert.ok(outside.candidates[0].reasons.includes("DATE_OUTSIDE_WINDOW"));
  assert.equal(outside.candidates[0].competesForMapping, false);
  assert.deepEqual(outside.certifiedSettledTotals, []);
});
test("exactly seven days between precise instants still verify and an extra millisecond does not", () => {
  assert.equal(verified(input([invoice()], [bank({ postedAt: "2026-10-08T12:00:00.000Z" })])).length, 1);
  const r = reconcileSubscriptionCharges(input([invoice()], [bank({ postedAt: "2026-10-08T12:00:00.001Z" })]));
  assert.ok(r.candidates[0].reasons.includes("DATE_OUTSIDE_WINDOW"));
  assert.deepEqual(r.certifiedSettledTotals, []);
});
test("mixed precision at the seven-day boundary uses all bounds instead of invented midnight points", () => {
  assert.equal(verified(input([dayInvoice()], [dayBank({ postedAt: "2026-10-08T00:00:00Z" })])).length, 1);
  const uncertain = reconcileSubscriptionCharges(input([invoice({ paidAt: "2026-10-01T00:00:00Z" })], [dayBank({ postedAt: "2026-10-08" })]));
  assert.ok(uncertain.candidates[0].reasons.includes("DATE_WINDOW_UNCERTAIN"));
  assert.equal(uncertain.candidates[0].competesForMapping, true);
  assert.deepEqual(uncertain.certifiedSettledTotals, []);
  const outside = reconcileSubscriptionCharges(input([dayInvoice()], [dayBank({ postedAt: "2026-10-09T00:00:00Z" })]));
  assert.ok(outside.candidates[0].reasons.includes("DATE_OUTSIDE_WINDOW"));
  assert.equal(outside.candidates[0].competesForMapping, false);
});
test("window-uncertain neighbors preserve one-to-one ambiguity in both directions", () => {
  const uncertainBank = dayBank({ transactionRef: "uncertain-bank", postedAt: "2026-10-08", paymentReference: null });
  const uncertainInvoice = dayInvoice({ invoiceRef: "uncertain-invoice", invoiceDate: "2026-09-25", paidAt: "2026-09-25", paymentReference: null });
  for (const fixture of [input([invoice()], [bank(), uncertainBank]), input([invoice(), uncertainInvoice], [bank()])]) {
    const r = reconcileSubscriptionCharges(fixture);
    assert.ok(r.candidates.some((c) => c.reasons.includes("DATE_WINDOW_UNCERTAIN") && c.competesForMapping));
    assert.ok(r.candidates[0].reasons.includes("AMBIGUOUS_MAPPING"));
    assert.deepEqual(r.certifiedSettledTotals, []);
  }
  assert.equal(verified(input([invoice()], [bank(), dayBank({ ...uncertainBank, postedAt: "2026-10-10" })])).length, 1);
});
test("date-only equal amounts remain ambiguous until independent exact references disambiguate", () => {
  const invoices = [dayInvoice({ paymentReference: null, paymentLast4: "4242" }), dayInvoice({ invoiceRef: "inv-2", paymentReference: null, paymentLast4: "4242" })];
  const banks = [dayBank({ paymentReference: null, cardLast4: "4242" }), dayBank({ transactionRef: "txn-2", paymentReference: null, cardLast4: "4242" })];
  const ambiguous = reconcileSubscriptionCharges(input(invoices, banks));
  assert.ok(ambiguous.candidates.every((c) => c.reasons.includes("AMBIGUOUS_MAPPING")));
  assert.deepEqual(ambiguous.certifiedSettledTotals, []);
  invoices.forEach((i, n) => i.paymentReference = `day-payment-${n}`);
  banks.forEach((b, n) => b.paymentReference = `day-payment-${n}`);
  assert.equal(verified(input(invoices, banks)).length, 2);
});
test("card aliases must cover whole possible days and retain inclusive precise validity endpoints", () => {
  const fixture = input([dayInvoice({ paymentReference: null, paymentLast4: "1111", payerRef: native("payer") })], [dayBank({ paymentReference: null, cardLast4: "4242" })]);
  fixture.cardAliases = [{ provider: "openai", payerRef: native("payer"), invoiceLast4: "1111", bankLast4: "4242", validFrom: "2026-10-01T00:00:00Z", validThrough: "2026-10-02T23:59:59.999Z", evidenceRefs: [evidence("calendar-card-alias")] }];
  assert.equal(verified(fixture)[0]?.basis, "DOCUMENTED_CARD_ALIAS");
  for (const patch of [{ validFrom: "2026-10-01T00:00:00.001Z" }, { validThrough: "2026-10-02T12:00:00Z" }]) {
    const r = reconcileSubscriptionCharges({ ...fixture, cardAliases: [{ ...fixture.cardAliases[0], ...patch }] });
    assert.ok(r.candidates[0].reasons.includes("CARD_ALIAS_DATE_UNCERTAIN"));
    assert.equal(r.candidates[0].competesForMapping, true);
    assert.ok(r.candidates[0].evidenceRefs.includes(evidence("calendar-card-alias")));
    assert.deepEqual(r.certifiedSettledTotals, []);
  }
  const outside = reconcileSubscriptionCharges({ ...fixture, cardAliases: [{ ...fixture.cardAliases[0], validThrough: "2026-09-30T23:59:59.999Z", validFrom: "2026-09-01T00:00:00Z" }] });
  assert.ok(outside.candidates[0].reasons.includes("CARD_CONFLICT"));
  assert.equal(outside.candidates[0].competesForMapping, false);
});
test("an uncertain card-alias day cannot make another posted pair uniquely settle", () => {
  const fixture = input([dayInvoice({ paymentReference: null, paymentLast4: "1111", payerRef: native("payer") })], [
    dayBank({ cardLast4: "1111", paymentReference: null }), dayBank({ transactionRef: "alias-uncertain", cardLast4: "4242", paymentReference: null }),
  ]);
  fixture.cardAliases = [{ provider: "openai", payerRef: native("payer"), invoiceLast4: "1111", bankLast4: "4242", validFrom: "2026-10-01T12:00:00Z", validThrough: "2026-10-03T00:00:00Z", evidenceRefs: [evidence("partial-day-alias")] }];
  const r = reconcileSubscriptionCharges(fixture);
  assert.ok(r.candidates[1].reasons.includes("CARD_ALIAS_DATE_UNCERTAIN"));
  assert.equal(r.candidates[1].competesForMapping, true);
  assert.ok(r.candidates[0].reasons.includes("AMBIGUOUS_MAPPING"));
  assert.deepEqual(r.certifiedSettledTotals, []);
});
test("day precision preserves replay, revision conflicts, posted transitions and removal authority", () => {
  const replay = reconcileSubscriptionCharges(input([dayInvoice(), dayInvoice()], [dayBank(), dayBank()]));
  assert.deepEqual(replay.invoices.map((i) => i.disposition), ["ACTIVE", "REPLAY"]);
  assert.equal(replay.certifiedSettledTotals[0]?.amountCents, 2000);
  const conflict = reconcileSubscriptionCharges(input([dayInvoice(), invoice()], [dayBank()]));
  assert.ok(conflict.invoices.every((i) => i.disposition === "REVISION_CONFLICT"));
  assert.deepEqual(conflict.invoices.map((i) => i.datePrecision.paidAt), ["DAY", "INSTANT"]);
  assert.deepEqual(conflict.certifiedSettledTotals, []);
  const pending = dayBank({ state: "PENDING", postedAt: null, sourceObservedAt: "2026-10-01T18:00:00Z" });
  const posted = dayBank({ revision: 2, sourceObservedAt: "2026-10-02T18:00:00Z" });
  assert.equal(verified(input([dayInvoice()], [pending, posted])).length, 1);
  const removed = dayBank({ revision: 3, state: "REMOVED" });
  for (const rows of [[pending, posted, removed], [removed, posted, pending]]) {
    const r = reconcileSubscriptionCharges(input([dayInvoice()], rows));
    assert.ok(r.bankTransactions.find((b) => b.record.revision === 3)?.reasons.includes("BANK_REMOVED"));
    assert.deepEqual(r.certifiedSettledTotals, []);
  }
});
test("calendar matching preserves payment, financial, account and allocation holds", () => {
  const i = dayInvoice({ paymentLast4: "4242", payerRef: native("payer") });
  const b = dayBank({ cardLast4: "4242" });
  for (const patch of [{ provider: "anthropic" }, { provider: null }, { currency: "EUR" }, { amountCents: 2001 }, { amountCents: -2000 },
    { state: "PENDING" as const }, { state: "REMOVED" as const }, { kind: "API_CONSOLE" as const }, { paymentReference: "conflict" }, { cardLast4: "1111" }, { payerRef: native("other-payer") }]) {
    noSettlement(input([i], [dayBank({ ...b, ...patch })]));
  }
  for (const patch of [{ status: "OPEN" as const }, { status: "UNKNOWN" as const }, { kind: "UNKNOWN" as const }, { kind: "CREDIT" as const }]) noSettlement(input([dayInvoice({ ...i, ...patch })], [b]));
  noSettlement(input([dayInvoice({ paymentReference: null })], [dayBank({ paymentReference: null })]));
  noSettlement(input([dayInvoice({ ...i, accountRef: native("one"), accountIdentityState: "VERIFIED", accountEvidenceRefs: [evidence("one-account")] })],
    [dayBank({ ...b, accountRef: native("two"), accountIdentityState: "VERIFIED", accountEvidenceRefs: [evidence("two-account")] })]));
  const known = input([dayInvoice({ accountRef: native("known"), accountIdentityState: "VERIFIED", accountEvidenceRefs: [evidence("day-account-link")] })], [dayBank()]);
  known.sources.forEach((s) => s.coverage = { state: "COMPLETE", periodStart: "2026-09-01", periodEnd: "2026-10-14" });
  const r = reconcileSubscriptionCharges(known);
  assert.equal(r.candidates[0].state, "VERIFIED");
  assert.equal(r.candidates[0].accountIdentityState, "VERIFIED");
  assert.equal(r.spendAllocation.state, "UNKNOWN");
  assert.deepEqual(r.spendAllocation.allocations, []);
});

let failed = 0;
for (const [name, run] of tests) {
  try { run(); console.log(`PASS ${name}`); }
  catch (error) { failed += 1; console.error(`FAIL ${name}\n${error instanceof Error ? error.stack : String(error)}`); }
}
console.log(JSON.stringify({ schema: "subscription-reconciliation-proof/v1", fixtures: tests.length, passed: tests.length - failed, failed, data: "SYNTHETIC_ONLY" }));
if (failed) process.exitCode = 1;
