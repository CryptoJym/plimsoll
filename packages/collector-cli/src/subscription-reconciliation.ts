/** Local evidence assertions only. No I/O, clock, account discovery or usage allocation. */
export type EvidenceRef = `sha256:${string}`;
export type IdentityState = "VERIFIED" | "UNKNOWN";
export type ChargeDatePrecision = "DAY" | "INSTANT" | "UNKNOWN";
export type ChargeKind = "MONTHLY_SUBSCRIPTION" | "UPGRADE_PRORATION" | "USAGE_RESET" | "API_CONSOLE" | "REFUND" | "CREDIT" | "UNKNOWN";
export interface ReconciliationSource {
  sourceRef: string;
  type: "INVOICES" | "BANK";
  provider?: string | null;
  accountRef?: string | null;
  readAt: string;
  lastSyncedAt: string | null;
  providerObservedAt: string | null;
  coverage: { state: "COMPLETE" | "PARTIAL" | "UNKNOWN"; periodStart: string | null; periodEnd: string | null };
  evidenceRefs: EvidenceRef[];
}
export interface AccountIdentity {
  /** Native pseudonym, never an email. UNKNOWN hints do not establish identity. */
  accountRef?: string | null;
  accountIdentityState?: IdentityState;
  accountEvidenceRefs?: EvidenceRef[];
}
export interface SubscriptionInvoice extends AccountIdentity {
  sourceRef: string;
  invoiceRef: string;
  revision: number;
  sourceObservedAt: string;
  provider: string;
  /** Signed integer cents: positive debit; negative refund/credit. */
  amountCents: number;
  currency: string;
  status: "PAID" | "OPEN" | "VOID" | "REFUNDED" | "CREDIT" | "UNKNOWN";
  /** Source calendar day or exact UTC instant; never synthesize a paid instant. */
  paidAt: string | null;
  invoiceDate: string | null;
  kind: ChargeKind;
  payerRef?: string | null;
  paymentLast4?: string | null;
  paymentReference?: string | null;
  evidenceRefs: EvidenceRef[];
}
export interface SubscriptionBankTransaction extends AccountIdentity {
  sourceRef: string;
  transactionRef: string;
  revision: number;
  sourceObservedAt: string;
  provider: string | null;
  amountCents: number;
  currency: string;
  state: "POSTED" | "PENDING" | "REMOVED";
  /** Source calendar days or exact UTC instants, independently per field. */
  transactedAt: string | null;
  postedAt: string | null;
  payerRef?: string | null;
  cardLast4?: string | null;
  paymentReference?: string | null;
  kind?: ChargeKind | null;
  evidenceRefs: EvidenceRef[];
}
export interface DocumentedCardAlias {
  provider: string;
  payerRef: string;
  invoiceLast4: string;
  bankLast4: string;
  validFrom: string;
  validThrough: string;
  evidenceRefs: EvidenceRef[];
}
export interface SubscriptionReconciliationInput {
  schema: "subscription-reconciliation/v1";
  observedAt: string;
  sources: ReconciliationSource[];
  invoices: SubscriptionInvoice[];
  bankTransactions: SubscriptionBankTransaction[];
  cardAliases?: DocumentedCardAlias[];
}
export type RecordDisposition = "ACTIVE" | "REPLAY" | "SUPERSEDED" | "REVISION_CONFLICT";
export type RecordDatePrecision<T> = T extends SubscriptionInvoice
  ? { paidAt: ChargeDatePrecision; invoiceDate: "DAY" | "UNKNOWN" }
  : { transactedAt: ChargeDatePrecision; postedAt: ChargeDatePrecision };
export interface ReconciledRecord<T> {
  index: number;
  record: T;
  disposition: RecordDisposition;
  state: IdentityState;
  reasons: string[];
  candidateIndices: number[];
  /** Derived from preserved source strings, not an assertion of a payment time. */
  datePrecision: RecordDatePrecision<T>;
}
export interface ChargeCandidate {
  invoiceIndex: number;
  bankIndex: number;
  state: IdentityState;
  basis: "EXACT_REFERENCE" | "PAYMENT_INSTRUMENT" | "DOCUMENTED_CARD_ALIAS" | null;
  /** A plausible current paid/posted pair participates even with weak identity evidence. */
  competesForMapping: boolean;
  reasons: string[];
  evidenceRefs: EvidenceRef[];
  accountRef: string | null;
  accountIdentityState: IdentityState;
  accountEvidenceRefs: EvidenceRef[];
}
export interface CertifiedSettledTotal {
  provider: string;
  currency: string;
  kind: ChargeKind;
  amountCents: number;
  matchCount: number;
}
export interface SubscriptionReconciliationResult {
  schema: "subscription-reconciliation-result/v1";
  observedAt: string;
  sources: ReconciliationSource[];
  cardAliases: DocumentedCardAlias[];
  invoices: ReconciledRecord<SubscriptionInvoice>[];
  bankTransactions: ReconciledRecord<SubscriptionBankTransaction>[];
  candidates: ChargeCandidate[];
  certifiedSettledTotals: CertifiedSettledTotal[];
  totalsScope: "PROVIDED_RECORDS_ONLY";
  crossSourceDedupeState: "UNKNOWN";
  spendAllocation: { state: "UNKNOWN"; reason: "USAGE_ACCOUNT_JOINS_AND_COMPLETE_PERIOD_NOT_PROVED"; allocations: never[] };
}

export class SubscriptionReconciliationInputError extends Error {
  constructor(path: string, reason: string) {
    // Paths are constructed locally; never echo an input value or an unknown key.
    super(`subscription_reconciliation_input:${path}:${reason}`);
    this.name = "SubscriptionReconciliationInputError";
  }
}
const kinds = ["MONTHLY_SUBSCRIPTION", "UPGRADE_PRORATION", "USAGE_RESET", "API_CONSOLE", "REFUND", "CREDIT", "UNKNOWN"] as const;
const dayMs = 86_400_000;
const windowMs = 7 * dayMs;
type ObjectValue = Record<string, unknown>;
function fail(path: string, reason: string): never { throw new SubscriptionReconciliationInputError(path, reason); }
function object(value: unknown, path: string, keys: string): ObjectValue {
  if (value === null || typeof value !== "object" || Array.isArray(value) ||
      ![Object.prototype, null].includes(Object.getPrototypeOf(value))) fail(path, "object_required");
  if (Object.keys(value).some((key) => !keys.split(" ").includes(key))) fail(path, "unknown_field");
  return value as ObjectValue;
}
function list<T>(value: unknown, path: string, parse: (v: unknown, p: string) => T, max = 2000): T[] {
  if (!Array.isArray(value) || value.length > max) fail(path, "bounded_array_required");
  return Array.from(value, (v, n) => parse(v, `${path}[${n}]`));
}
function text(value: unknown, path: string, pattern: RegExp): string {
  if (typeof value !== "string" || value.length > 192 || !pattern.test(value)) fail(path, "invalid_string");
  return value;
}
function ref(value: unknown, path: string): string { return text(value, path, /^[A-Za-z0-9][A-Za-z0-9._:-]*$/); }
function provider(value: unknown, path: string): string { return text(value, path, /^[a-z][a-z0-9_-]{0,63}$/); }
function nativeRef(value: unknown, path: string): string { return text(value, path, /^native:(?:[a-f0-9]{32}|[a-f0-9]{64})$/); }
function payerRef(value: unknown, path: string): string { return text(value, path, /^(?:native|payer|bank):(?:[a-f0-9]{32}|[a-f0-9]{64})$/); }
function last4(value: unknown, path: string): string { return text(value, path, /^[0-9]{4}$/); }
function nullable<T>(value: unknown, path: string, parse: (v: unknown, p: string) => T): T | null {
  return value === null ? null : parse(value, path);
}
function optional<T>(value: unknown, path: string, parse: (v: unknown, p: string) => T): T | null {
  return value === undefined ? null : nullable(value, path, parse);
}
function choice<T extends string>(value: unknown, path: string, values: readonly T[]): T {
  if (typeof value !== "string" || !values.includes(value as T)) fail(path, "invalid_enum");
  return value as T;
}
function integer(value: unknown, path: string): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value)) fail(path, "safe_integer_required");
  return value;
}
function timestamp(value: unknown, path: string): string {
  const v = text(value, path, /^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{3})?Z$/);
  const ms = Date.parse(v);
  if (!Number.isFinite(ms) || new Date(ms).toISOString() !== (v.length === 20 ? v.replace("Z", ".000Z") : v)) fail(path, "invalid_timestamp");
  return v;
}
function date(value: unknown, path: string): string {
  const v = text(value, path, /^\d{4}-\d\d-\d\d$/);
  if (!Number.isFinite(Date.parse(v)) || new Date(v).toISOString().slice(0, 10) !== v) fail(path, "invalid_date");
  return v;
}
function chargeDate(value: unknown, path: string): string {
  return typeof value === "string" && value.length === 10 ? date(value, path) : timestamp(value, path);
}
function datePrecision(value: string | null): ChargeDatePrecision {
  return value === null ? "UNKNOWN" : value.length === 10 ? "DAY" : "INSTANT";
}
/** Inclusive bounds at the contract's millisecond resolution, for comparison only. */
function chargeBounds(value: string): { earliest: number; latest: number } {
  const earliest = Date.parse(value);
  return { earliest, latest: value.length === 10 ? earliest + dayMs - 1 : earliest };
}
function followsObservation(value: string, observedAt: string): boolean {
  // A day on the observation day is compatible; its within-day instant is unknown.
  return value.length === 10 ? value > observedAt.slice(0, 10) : Date.parse(value) > Date.parse(observedAt);
}
function evidenceRefs(value: unknown, path: string, allowEmpty = false): EvidenceRef[] {
  const refs = list(value, path, (v, p) => text(v, p, /^sha256:[a-f0-9]{64}$/) as EvidenceRef, 64);
  if (!allowEmpty && refs.length === 0) fail(path, "evidence_required");
  return [...new Set(refs)].sort();
}
function account(value: ObjectValue, path: string): Required<AccountIdentity> {
  const accountRef = optional(value.accountRef, `${path}.accountRef`, nativeRef);
  const accountIdentityState = choice(value.accountIdentityState === undefined ? "UNKNOWN" : value.accountIdentityState, `${path}.accountIdentityState`, ["VERIFIED", "UNKNOWN"]);
  const accountEvidenceRefs = evidenceRefs(value.accountEvidenceRefs === undefined ? [] : value.accountEvidenceRefs, `${path}.accountEvidenceRefs`, true);
  if (accountIdentityState === "VERIFIED" && (!accountRef || !accountEvidenceRefs.length)) fail(path, "account_identity_evidence_required");
  return { accountRef, accountIdentityState, accountEvidenceRefs };
}
function common(value: ObjectValue, path: string) {
  const revision = integer(value.revision, `${path}.revision`);
  if (revision < 1) fail(`${path}.revision`, "positive_revision_required");
  return {
    ...account(value, path), sourceRef: ref(value.sourceRef, `${path}.sourceRef`), revision,
    sourceObservedAt: timestamp(value.sourceObservedAt, `${path}.sourceObservedAt`),
    amountCents: integer(value.amountCents, `${path}.amountCents`), currency: text(value.currency, `${path}.currency`, /^[A-Z]{3}$/),
    payerRef: optional(value.payerRef, `${path}.payerRef`, payerRef),
    paymentReference: optional(value.paymentReference, `${path}.paymentReference`, ref),
    evidenceRefs: evidenceRefs(value.evidenceRefs, `${path}.evidenceRefs`),
  };
}
/** Strict versioned JSON contract. Returns a detached copy; throws path-only validation errors. */
export function parseSubscriptionReconciliationInput(value: unknown): SubscriptionReconciliationInput {
  const v = object(value, "$", "schema observedAt sources invoices bankTransactions cardAliases");
  const observedAt = timestamp(v.observedAt, "$.observedAt");
  const sources = list(v.sources, "$.sources", (entry, p): ReconciliationSource => {
    const s = object(entry, p, "sourceRef type provider accountRef readAt lastSyncedAt providerObservedAt coverage evidenceRefs");
    const c = object(s.coverage, `${p}.coverage`, "state periodStart periodEnd");
    const coverage = {
      state: choice(c.state, `${p}.coverage.state`, ["COMPLETE", "PARTIAL", "UNKNOWN"]),
      periodStart: nullable(c.periodStart, `${p}.coverage.periodStart`, date),
      periodEnd: nullable(c.periodEnd, `${p}.coverage.periodEnd`, date),
    };
    if (coverage.state === "COMPLETE" && (!coverage.periodStart || !coverage.periodEnd)) fail(`${p}.coverage`, "complete_period_required");
    if (coverage.periodStart && coverage.periodEnd && coverage.periodStart >= coverage.periodEnd) fail(`${p}.coverage`, "invalid_period");
    const source = {
      sourceRef: ref(s.sourceRef, `${p}.sourceRef`), type: choice(s.type, `${p}.type`, ["INVOICES", "BANK"]),
      provider: optional(s.provider, `${p}.provider`, provider), accountRef: optional(s.accountRef, `${p}.accountRef`, nativeRef),
      readAt: timestamp(s.readAt, `${p}.readAt`), lastSyncedAt: nullable(s.lastSyncedAt, `${p}.lastSyncedAt`, timestamp),
      providerObservedAt: nullable(s.providerObservedAt, `${p}.providerObservedAt`, timestamp), coverage,
      evidenceRefs: evidenceRefs(s.evidenceRefs, `${p}.evidenceRefs`),
    };
    if (Date.parse(source.readAt) > Date.parse(observedAt) || [source.lastSyncedAt, source.providerObservedAt].some((t) => t && Date.parse(t) > Date.parse(source.readAt))) fail(p, "source_timestamp_order");
    return source;
  }, 256);
  if (new Set(sources.map((s) => s.sourceRef)).size !== sources.length) fail("$.sources", "duplicate_source_ref");
  const commonKeys = "sourceRef revision sourceObservedAt provider accountRef accountIdentityState accountEvidenceRefs amountCents currency kind payerRef paymentReference evidenceRefs";
  const invoices = list(v.invoices, "$.invoices", (entry, p): SubscriptionInvoice => {
    const i = object(entry, p, `${commonKeys} invoiceRef status paidAt invoiceDate paymentLast4`);
    return { ...common(i, p), invoiceRef: ref(i.invoiceRef, `${p}.invoiceRef`), provider: provider(i.provider, `${p}.provider`),
      status: choice(i.status, `${p}.status`, ["PAID", "OPEN", "VOID", "REFUNDED", "CREDIT", "UNKNOWN"]),
      paidAt: nullable(i.paidAt, `${p}.paidAt`, chargeDate), invoiceDate: nullable(i.invoiceDate, `${p}.invoiceDate`, date),
      kind: choice(i.kind, `${p}.kind`, kinds), paymentLast4: optional(i.paymentLast4, `${p}.paymentLast4`, last4) };
  });
  const bankTransactions = list(v.bankTransactions, "$.bankTransactions", (entry, p): SubscriptionBankTransaction => {
    const b = object(entry, p, `${commonKeys} transactionRef state transactedAt postedAt cardLast4`);
    return { ...common(b, p), transactionRef: ref(b.transactionRef, `${p}.transactionRef`), provider: nullable(b.provider, `${p}.provider`, provider),
      state: choice(b.state, `${p}.state`, ["POSTED", "PENDING", "REMOVED"]),
      transactedAt: nullable(b.transactedAt, `${p}.transactedAt`, chargeDate), postedAt: nullable(b.postedAt, `${p}.postedAt`, chargeDate),
      kind: optional(b.kind, `${p}.kind`, (v, p) => choice(v, p, kinds)), cardLast4: optional(b.cardLast4, `${p}.cardLast4`, last4) };
  });
  for (const [rows, type, name] of [[invoices, "INVOICES", "invoices"], [bankTransactions, "BANK", "bankTransactions"]] as const) {
    rows.forEach((row, n) => {
      const source = sources.find((s) => s.sourceRef === row.sourceRef), p = `$.${name}[${n}]`;
      if (!source || source.type !== type) fail(p, "source_scope_invalid");
      if (Date.parse(row.sourceObservedAt) > Date.parse(source.readAt)) fail(p, "record_observed_after_read");
      if (source.provider && row.provider && source.provider !== row.provider || source.accountRef && row.accountRef && source.accountRef !== row.accountRef) fail(p, "source_scope_conflict");
    });
  }
  const cardAliases = list(v.cardAliases === undefined ? [] : v.cardAliases, "$.cardAliases", (entry, p): DocumentedCardAlias => {
    const a = object(entry, p, "provider payerRef invoiceLast4 bankLast4 validFrom validThrough evidenceRefs");
    const alias = { provider: provider(a.provider, `${p}.provider`), payerRef: payerRef(a.payerRef, `${p}.payerRef`),
      invoiceLast4: last4(a.invoiceLast4, `${p}.invoiceLast4`), bankLast4: last4(a.bankLast4, `${p}.bankLast4`),
      validFrom: timestamp(a.validFrom, `${p}.validFrom`), validThrough: timestamp(a.validThrough, `${p}.validThrough`),
      evidenceRefs: evidenceRefs(a.evidenceRefs, `${p}.evidenceRefs`) };
    if (Date.parse(alias.validFrom) >= Date.parse(alias.validThrough) || alias.invoiceLast4 === alias.bankLast4) fail(p, "invalid_card_alias");
    return alias;
  }, 256);
  return { schema: choice(v.schema, "$.schema", ["subscription-reconciliation/v1"]), observedAt, sources, invoices, bankTransactions, cardAliases };
}

type VersionedRecord = (SubscriptionInvoice | SubscriptionBankTransaction);
function recordDatePrecision<T extends VersionedRecord>(row: T): RecordDatePrecision<T> {
  return ("invoiceRef" in row
    ? { paidAt: datePrecision(row.paidAt), invoiceDate: row.invoiceDate === null ? "UNKNOWN" : "DAY" }
    : { transactedAt: datePrecision(row.transactedAt), postedAt: datePrecision(row.postedAt) }) as RecordDatePrecision<T>;
}
function currentRecords<T extends VersionedRecord>(rows: T[], identity: (row: T) => string): ReconciledRecord<T>[] {
  const result = rows.map((record, index): ReconciledRecord<T> => ({ index, record, disposition: "ACTIVE", state: "UNKNOWN", reasons: [], candidateIndices: [], datePrecision: recordDatePrecision(record) }));
  const groups = new Map<string, ReconciledRecord<T>[]>();
  for (const row of result) {
    const key = JSON.stringify([row.record.sourceRef, identity(row.record)]);
    const group = groups.get(key) ?? []; group.push(row); groups.set(key, group);
  }
  for (const group of groups.values()) {
    const revisions = [...new Set(group.map((r) => r.record.revision))].sort((a, b) => a - b);
    let priorObserved = -Infinity, conflict = false;
    for (const revision of revisions) {
      const same = group.filter((r) => r.record.revision === revision);
      // Normalized object keys and evidence sets make JSON comparison order-independent.
      if (new Set(same.map((r) => JSON.stringify(r.record))).size !== 1) conflict = true;
      const observed = Date.parse(same[0].record.sourceObservedAt);
      if (observed < priorObserved) conflict = true;
      priorObserved = observed;
    }
    const latest = group.find((r) => r.record.revision === revisions[revisions.length - 1])!;
    for (const row of group) {
      row.disposition = conflict ? "REVISION_CONFLICT" : row === latest ? "ACTIVE" : row.record.revision === latest.record.revision ? "REPLAY" : "SUPERSEDED";
      if (row.disposition !== "ACTIVE") row.reasons.push(row.disposition);
    }
  }
  return result;
}
function isAdjustment(kind: ChargeKind | null | undefined): boolean { return kind === "REFUND" || kind === "CREDIT"; }
function invoiceReasons(i: SubscriptionInvoice): string[] {
  const reasons: string[] = [];
  if (i.status !== "PAID") reasons.push("INVOICE_NOT_PAID");
  if (i.amountCents <= 0 || isAdjustment(i.kind) || i.status === "REFUNDED" || i.status === "CREDIT") reasons.push("REFUND_CREDIT_OR_NON_DEBIT");
  if (i.kind === "UNKNOWN") reasons.push("CHARGE_KIND_UNKNOWN");
  if (!i.paidAt || !i.invoiceDate) reasons.push("DATE_EVIDENCE_MISSING");
  if (i.paidAt && i.invoiceDate && i.invoiceDate > i.paidAt.slice(0, 10)) reasons.push("DATE_ORDER_CONFLICT");
  if (i.paidAt && followsObservation(i.paidAt, i.sourceObservedAt) || i.invoiceDate && i.invoiceDate > i.sourceObservedAt.slice(0, 10)) reasons.push("FUTURE_CHARGE_DATE");
  return reasons;
}
function bankReasons(b: SubscriptionBankTransaction): string[] {
  const reasons: string[] = [];
  if (b.state !== "POSTED") reasons.push(b.state === "REMOVED" ? "BANK_REMOVED" : "BANK_PENDING");
  if (b.amountCents <= 0 || isAdjustment(b.kind)) reasons.push("REFUND_CREDIT_OR_NON_DEBIT");
  if (!b.provider) reasons.push("PROVIDER_UNKNOWN");
  if (!b.transactedAt || !b.postedAt) reasons.push("DATE_EVIDENCE_MISSING");
  if (b.postedAt && b.transactedAt && chargeBounds(b.postedAt).latest < chargeBounds(b.transactedAt).earliest) reasons.push("DATE_ORDER_CONFLICT");
  if ([b.transactedAt, b.postedAt].some((t) => t && followsObservation(t, b.sourceObservedAt))) reasons.push("FUTURE_CHARGE_DATE");
  return reasons;
}
function union(...refs: EvidenceRef[][]): EvidenceRef[] { return [...new Set(refs.flat())].sort(); }
function candidate(
  i: ReconciledRecord<SubscriptionInvoice>, b: ReconciledRecord<SubscriptionBankTransaction>, input: SubscriptionReconciliationInput,
): ChargeCandidate {
  const invoice = i.record, bank = b.record;
  const reasons = [...i.reasons, ...b.reasons];
  const dates = [invoice.paidAt, bank.transactedAt, bank.postedAt].filter((t): t is string => t !== null).map(chargeBounds);
  if (dates.length > 1) {
    const minimumSpan = Math.max(...dates.map((t) => t.earliest)) - Math.min(...dates.map((t) => t.latest));
    const maximumSpan = Math.max(...dates.map((t) => t.latest)) - Math.min(...dates.map((t) => t.earliest));
    if (minimumSpan > windowMs) reasons.push("DATE_OUTSIDE_WINDOW");
    else if (maximumSpan > windowMs) reasons.push("DATE_WINDOW_UNCERTAIN");
  }
  const exactReference = Boolean(bank.paymentReference && (bank.paymentReference === invoice.paymentReference || bank.paymentReference === invoice.invoiceRef));
  if (invoice.paymentReference && bank.paymentReference && !exactReference) reasons.push("PAYMENT_REFERENCE_CONFLICT");
  if (invoice.payerRef && bank.payerRef && invoice.payerRef !== bank.payerRef) reasons.push("PAYER_CONFLICT");
  if (invoice.accountIdentityState === "VERIFIED" && bank.accountIdentityState === "VERIFIED" && invoice.accountRef !== bank.accountRef) reasons.push("SUBSCRIPTION_ACCOUNT_CONFLICT");
  if (bank.kind && bank.kind !== "UNKNOWN" && invoice.kind !== "UNKNOWN" && bank.kind !== invoice.kind) reasons.push("CHARGE_KIND_CONFLICT");
  const possibleAliases = (input.cardAliases ?? []).filter((a) =>
    a.provider === invoice.provider && a.payerRef === invoice.payerRef && a.payerRef === bank.payerRef &&
    a.invoiceLast4 === invoice.paymentLast4 && a.bankLast4 === bank.cardLast4 && dates.length === 3 &&
    dates.every((t) => t.latest >= Date.parse(a.validFrom) && t.earliest <= Date.parse(a.validThrough)));
  const aliases = possibleAliases.filter((a) =>
    dates.every((t) => t.earliest >= Date.parse(a.validFrom) && t.latest <= Date.parse(a.validThrough)));
  const sameCard = Boolean(invoice.paymentLast4 && bank.cardLast4 && invoice.paymentLast4 === bank.cardLast4);
  if (invoice.paymentLast4 && bank.cardLast4 && !sameCard && aliases.length === 0) reasons.push(possibleAliases.length ? "CARD_ALIAS_DATE_UNCERTAIN" : "CARD_CONFLICT");
  const basis = exactReference ? "EXACT_REFERENCE" : sameCard ? "PAYMENT_INSTRUMENT" : aliases.length ? "DOCUMENTED_CARD_ALIAS" : null;
  if (!basis) reasons.push("PAYMENT_IDENTITY_UNKNOWN");
  const hardConflicts = ["DATE_ORDER_CONFLICT", "FUTURE_CHARGE_DATE", "DATE_OUTSIDE_WINDOW", "PAYMENT_REFERENCE_CONFLICT", "PAYER_CONFLICT", "SUBSCRIPTION_ACCOUNT_CONFLICT", "CHARGE_KIND_CONFLICT", "CARD_CONFLICT", "REFUND_CREDIT_OR_NON_DEBIT"];
  return {
    invoiceIndex: i.index, bankIndex: b.index, state: "UNKNOWN", basis,
    competesForMapping: invoice.status === "PAID" && bank.state === "POSTED" && !reasons.some((r) => hardConflicts.includes(r)),
    reasons: [...new Set(reasons)], evidenceRefs: union(invoice.evidenceRefs, bank.evidenceRefs,
      ...input.sources.filter((s) => s.sourceRef === invoice.sourceRef || s.sourceRef === bank.sourceRef).map((s) => s.evidenceRefs),
      ...possibleAliases.map((a) => a.evidenceRefs)),
    accountRef: null, accountIdentityState: "UNKNOWN", accountEvidenceRefs: [],
  };
}
/** Recomputes from a validated snapshot; never mutates, fetches or persists anything. */
export function reconcileSubscriptionCharges(value: unknown): SubscriptionReconciliationResult {
  const input = parseSubscriptionReconciliationInput(value);
  const invoices = currentRecords(input.invoices, (i) => i.invoiceRef);
  const bankTransactions = currentRecords(input.bankTransactions, (b) => b.transactionRef);
  for (const i of invoices) i.reasons.push(...invoiceReasons(i.record));
  for (const b of bankTransactions) b.reasons.push(...bankReasons(b.record));
  const candidates: ChargeCandidate[] = [];
  // Replays and superseded versions cannot compete. Conflicting current facts still
  // compete conservatively, so quarantining them cannot make another pair unique.
  const included = (r: ReconciledRecord<VersionedRecord>) => r.disposition === "ACTIVE" || r.disposition === "REVISION_CONFLICT";
  const bankBuckets = new Map<string, ReconciledRecord<SubscriptionBankTransaction>[]>();
  const bucketKey = (r: VersionedRecord) => JSON.stringify([r.currency, r.amountCents]);
  for (const bank of bankTransactions.filter(included)) {
    const key = bucketKey(bank.record), bucket = bankBuckets.get(key) ?? [];
    bucket.push(bank); bankBuckets.set(key, bucket);
  }
  for (const invoice of invoices.filter(included)) {
    for (const bank of bankBuckets.get(bucketKey(invoice.record)) ?? []) {
      if (bank.record.provider !== null && bank.record.provider !== invoice.record.provider) continue;
      if (candidates.length >= 100_000) throw new Error("subscription_reconciliation_candidate_limit");
      const c = candidate(invoice, bank, input), index = candidates.length;
      candidates.push(c); invoice.candidateIndices.push(index); bank.candidateIndices.push(index);
    }
  }
  const invoiceDegrees = new Map<number, number>(), bankDegrees = new Map<number, number>();
  for (const c of candidates.filter((c) => c.competesForMapping)) {
    invoiceDegrees.set(c.invoiceIndex, (invoiceDegrees.get(c.invoiceIndex) ?? 0) + 1);
    bankDegrees.set(c.bankIndex, (bankDegrees.get(c.bankIndex) ?? 0) + 1);
  }
  const totals = new Map<string, CertifiedSettledTotal>();
  for (const c of candidates) {
    if (c.competesForMapping && (invoiceDegrees.get(c.invoiceIndex) !== 1 || bankDegrees.get(c.bankIndex) !== 1)) c.reasons.push("AMBIGUOUS_MAPPING");
    if (c.reasons.length !== 0) continue;
    const i = invoices[c.invoiceIndex], b = bankTransactions[c.bankIndex];
    c.state = "VERIFIED"; i.state = "VERIFIED"; b.state = "VERIFIED";
    const known = [i.record, b.record].filter((r) => r.accountIdentityState === "VERIFIED");
    if (known.length) {
      c.accountIdentityState = "VERIFIED"; c.accountRef = known[0].accountRef!;
      c.accountEvidenceRefs = union(...known.map((r) => r.accountEvidenceRefs ?? []));
      c.evidenceRefs = union(c.evidenceRefs, c.accountEvidenceRefs);
    }
    const key = JSON.stringify([i.record.provider, i.record.currency, i.record.kind]);
    const total = totals.get(key) ?? { provider: i.record.provider, currency: i.record.currency, kind: i.record.kind, amountCents: 0, matchCount: 0 };
    const next = total.amountCents + i.record.amountCents;
    if (!Number.isSafeInteger(next)) throw new Error("certified_total_overflow");
    total.amountCents = next; total.matchCount += 1; totals.set(key, total);
  }
  for (const row of [...invoices, ...bankTransactions]) {
    if (row.state === "VERIFIED") continue;
    row.reasons = [...new Set([...row.reasons, ...row.candidateIndices.flatMap((n) => candidates[n].reasons)])];
    if (!row.candidateIndices.length) row.reasons.push("NO_CANDIDATE");
  }
  return {
    schema: "subscription-reconciliation-result/v1", observedAt: input.observedAt,
    sources: input.sources, cardAliases: input.cardAliases ?? [], invoices, bankTransactions, candidates,
    certifiedSettledTotals: [...totals.entries()].sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0).map(([, t]) => t),
    totalsScope: "PROVIDED_RECORDS_ONLY", crossSourceDedupeState: "UNKNOWN",
    spendAllocation: { state: "UNKNOWN", reason: "USAGE_ACCOUNT_JOINS_AND_COMPLETE_PERIOD_NOT_PROVED", allocations: [] },
  };
}
