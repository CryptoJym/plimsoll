# Subscription invoice / bank reconciliation v1

`packages/collector-cli/src/subscription-reconciliation.ts` exports the exact
TypeScript input/output interfaces, `parseSubscriptionReconciliationInput(unknown)`
and `reconcileSubscriptionCharges(unknown)`. Both functions validate the input;
both return detached data. Reconciliation is deterministic and has no imports,
I/O, current clock, persistence, network, credentials, scheduler or usage allocation.
This module is supplied for parent integration against the brief's `cba41795`
source snapshot; it does not change the existing report or refresh code.

```ts
import {
  reconcileSubscriptionCharges,
  type SubscriptionReconciliationInput,
} from "./subscription-reconciliation";

// The parent reads its own local JSON and retains custody of the evidence bytes.
const result = reconcileSubscriptionCharges(JSON.parse(localJson));
// Optional typed construction: const input: SubscriptionReconciliationInput = ...;
const matches = result.candidates.filter((c) => c.state === "VERIFIED");
// A VERIFIED charge can still have accountIdentityState === "UNKNOWN".
```

## Input fields

Unmarked fields below are required. Unknown object keys, including prices,
projected costs, quota percentages, emails and chat/project allocation fields,
are rejected. `?` means optional; nullable fields explicitly accept `null`.

| Object | Required fields | Optional fields and defaults |
| --- | --- | --- |
| Root | `schema: "subscription-reconciliation/v1"`, `observedAt`, `sources[]`, `invoices[]`, `bankTransactions[]` | `cardAliases[]` defaults to `[]` |
| Source | `sourceRef`, `type: "INVOICES" \| "BANK"`, `readAt`, `lastSyncedAt: timestamp \| null`, `providerObservedAt: timestamp \| null`, `coverage`, `evidenceRefs[]` | `provider?: provider \| null`, `accountRef?: nativeRef \| null`, both default `null` |
| Source coverage | `state: "COMPLETE" \| "PARTIAL" \| "UNKNOWN"`, `periodStart: date \| null`, `periodEnd: date \| null` | None |
| Invoice | `sourceRef`, `invoiceRef`, `revision`, `sourceObservedAt`, `provider`, `amountCents`, `currency`, `status`, `paidAt: date \| timestamp \| null`, `invoiceDate: date \| null`, `kind`, `evidenceRefs[]` | Account identity fields below; `payerRef?`, `paymentLast4?`, `paymentReference?`, each nullable and default `null` |
| Bank transaction | `sourceRef`, `transactionRef`, `revision`, `sourceObservedAt`, `provider: provider \| null`, `amountCents`, `currency`, `state`, `transactedAt: date \| timestamp \| null`, `postedAt: date \| timestamp \| null`, `evidenceRefs[]` | Account identity fields below; `payerRef?`, `cardLast4?`, `paymentReference?`, `kind?`, each nullable and default `null` |
| Invoice/bank account identity | None | `accountRef?: nativeRef \| null` defaults `null`; `accountIdentityState?: "VERIFIED" \| "UNKNOWN"` defaults `"UNKNOWN"`; `accountEvidenceRefs?: evidenceRef[]` defaults `[]` |
| Documented card alias | `provider`, `payerRef`, `invoiceLast4`, `bankLast4`, `validFrom`, `validThrough`, `evidenceRefs[]` | None |

`status` is `PAID | OPEN | VOID | REFUNDED | CREDIT | UNKNOWN`.
Bank `state` is `POSTED | PENDING | REMOVED`; removal is a state, including when
the previous revision was posted. Invoice `kind` is one of:

```text
MONTHLY_SUBSCRIPTION
UPGRADE_PRORATION
USAGE_RESET
API_CONSOLE
REFUND
CREDIT
UNKNOWN
```

- An evidence ref is `sha256:` followed by 64 lowercase hexadecimal characters.
  Required evidence arrays contain 1–64 refs. Identity evidence may be empty
  only for `UNKNOWN`. Refs are copied, sorted and deduplicated. The engine checks
  syntax; the parent must hash and verify the real bytes and establish which
  source facts they support. A well-formed hash is not authentication.
- A native account ref is `native:` plus 32 or 64 lowercase hexadecimal
  characters, compatible with existing collector refs. It identifies the
  provider subscription account. A payer ref is a separate bank/card owner
  pseudonym with prefix `native:`, `payer:` or `bank:` and the same hash lengths.
  It never supplies a subscription account link.
- `accountIdentityState: VERIFIED` requires a non-null account ref and nonempty
  `accountEvidenceRefs`. An account ref alone is an unverified hint. Source
  account scope also never promotes an account identity claim. When a supplied
  row and its source both declare provider/account scope, they must agree.
- Other refs (`sourceRef`, `invoiceRef`, `transactionRef`, `paymentReference`)
  are opaque, case-sensitive ASCII identifiers: an initial letter/digit,
  followed by letters, digits, `.`, `_`, `:`, or `-`, up to 192 characters.
  Use consistent private pseudonyms across matching reference fields. Do not
  put emails, credentials, card numbers, file paths or document text in refs.
- Providers are exact lowercase identifiers (`[a-z][a-z0-9_-]{0,63}`). The parent
  supplies evidence-backed canonical provider names; no merchant/provider,
  `codex/openai`, `claude/anthropic`, or other alias is inferred here.
- Cents are signed safe integers: positive bank debit/spend, negative bank
  refund/credit. Invoices use the same sign convention. The parent must convert
  its export's sign convention explicitly. Currency is exactly three uppercase
  letters; no FX conversion or currency mixing occurs. Zero is not a paid debit.
- Timestamps are valid UTC `YYYY-MM-DDTHH:mm:ssZ` or
  `YYYY-MM-DDTHH:mm:ss.sssZ`. Dates are valid `YYYY-MM-DD`. Their supplied strings
  are preserved; malformed dates and rollovers such as February 30 are rejected.
  Missing charge dates use `null`; do not invent dates or payment timestamps.
  Only `paidAt`, `transactedAt` and `postedAt` accept either precision, independently
  per field. Observation, source read/sync and card-alias validity fields still
  require exact UTC timestamps. `invoiceDate` and coverage bounds remain dates.
- `observedAt` is reconciliation observation time. `readAt` is when the parent
  read the source, separate from the last bank sync or provider observation.
  `sourceObservedAt` is the source's observation/update time for that record
  revision. Never replace a missing bank sync with local read time. Source
  observation/sync must not follow its read, records must not be observed after
  their source read, and reads must not follow the root observation.
- Coverage uses a half-open date range `[periodStart, periodEnd)`. `COMPLETE`
  requires both ordered bounds. Other states may have unknown bounds. Coverage
  is retained as a source assertion, never a proof of estate-wide completeness,
  account joins or spend allocation.
- Limits fail closed: 256 sources, 2,000 invoices, 2,000 bank rows, 256 aliases,
  64 refs per evidence array, 100,000 candidate pairs. No silent truncation.
  Validation errors name only a constructed field path and reason.

## Matching and totals

An invoice/bank pair can become `VERIFIED` only when all these checks hold:

1. The authoritative invoice revision is `PAID`, positive, and has a known
   non-adjustment kind; the authoritative bank revision is positive `POSTED`.
   Provider, currency and integer cents agree exactly. A missing bank provider
   remains `UNKNOWN` even with an exact payment reference.
2. Invoice date, paid date, bank transaction date and bank posting date exist.
   Invoice date is on/before the paid day. Posting and transaction evidence is
   chronologically compatible, and no charge date definitely follows its record
   observation. The entire possible span of paid, transaction and posting dates
   fits within seven elapsed days, using the precision rules below.
3. Either the bank payment reference equals the invoice payment reference or
   `invoiceRef` exactly, or both records report the same last four card digits,
   or a documented alias links their reported last four digits. Equal amount,
   date and provider alone supply no payment identity evidence. Matching card
   digits do not establish subscription account identity.
4. No supplied payment-reference, card, payer, verified account or known kind
   evidence contradicts the pairing. Contradictory cards require a documented
   alias even when the payment reference matches. A card alias is directional,
   non-transitive, scoped to an exact provider and a payer known on both rows.
   The full possible bounds of all three charge dates must fall within its
   inclusive validity interval. Partial day coverage cannot certify an alias.
5. Both endpoints have exactly one plausible competing neighbor. All current
   paid/posted candidates compete, including those with missing provider/date
   or payment identity evidence, seven-day boundary uncertainty, or a possibly
   applicable card alias whose validity covers only part of a charge day.
   Hard contradictions rule a pair out. Pending,
   removed, unpaid, refund/credit, superseded and replay records cannot compete
   for settlement. Conflicting revisions still compete where plausibly posted
   or paid, but cannot themselves verify. No greedy tie-break, confidence score,
   maximum-matching inference or assignment by input order is used.

`candidates[]` retains compatible-provider (or provider-unknown), exact-amount,
same-currency pairs, including rejected pairs with reasons. Each invoice and
bank output row retains its detached `record`, original `index`, `disposition`,
charge match `state`, reasons and candidate indices. Each row additionally has
derived `datePrecision` metadata, including on unmatched, superseded, replay and
conflicting records:

| Output row | `datePrecision` fields |
| --- | --- |
| Invoice | `paidAt: "DAY" \| "INSTANT" \| "UNKNOWN"`, `invoiceDate: "DAY" \| "UNKNOWN"` |
| Bank transaction | `transactedAt: "DAY" \| "INSTANT" \| "UNKNOWN"`, `postedAt: "DAY" \| "INSTANT" \| "UNKNOWN"` |

`UNKNOWN` precision means the required field contains `null`. Non-null dates
use `DAY`; exact UTC timestamps use `INSTANT`. Input and result schema identifiers
remain v1; this output metadata is additive and is not an accepted input field.
`basis` continues to record payment evidence, while `state` determines whether
a candidate actually verified. A `VERIFIED` charge with `paidAt` precision `DAY`
certifies the pairing at source calendar precision; it supplies no paid instant.
Candidate evidence includes record, source and fully or possibly applicable
alias hashes. Precision alone never supplies payment or account identity.

## Calendar precision and conservative bounds

Calendar labels are compared on a common calendar-day axis. For interval
arithmetic only, a date bounds every millisecond of that day (00:00:00.000 through
23:59:59.999), while an exact UTC timestamp is a single point. UTC day boundaries
provide the arithmetic convention; they are not inferred payment timestamps
or an assertion of the source's timezone. The parent must supply comparable
source calendar days. No bound is substituted for a source string or emitted
as a paid/transacted/posted instant.

- Invoice-day chronology remains a calendar check. For bank chronology, a
  posting whose latest possible value precedes the transaction's earliest
  possible value is `DATE_ORDER_CONFLICT`. Two different precise instants on
  the same day retain their exact ordering. Overlapping day/mixed bounds are
  chronologically compatible; their within-day ordering remains unknown.
- A calendar charge day later than its record's observation day is
  `FUTURE_CHARGE_DATE`; the same day is compatible without asserting an instant.
  Exact charge timestamps later than `sourceObservedAt` are still rejected for
  settlement, even on the same day. Source observation/read/sync ordering is
  unchanged and uses exact timestamps. Observation never narrows a charge day
  into a fabricated point for the window or alias checks.
- Across the available paid/transaction/posting bounds, the maximum possible
  span is `max(latest) - min(earliest)`. Verification requires this to be at most
  seven elapsed days. If the minimum possible span
  `max(0, max(earliest) - min(latest))` exceeds seven days, the pair has the hard
  contradiction `DATE_OUTSIDE_WINDOW`. Otherwise an excessive maximum produces
  `DATE_WINDOW_UNCERTAIN`: UNKNOWN, still a plausible competing neighbor.
  Missing dates separately remain `DATE_EVIDENCE_MISSING` and UNKNOWN.
- For example, October 1 and October 7 calendar days fit wholly within the
  bound. October 1 and October 8 days straddle it and remain UNKNOWN; October 1
  and October 9 days are definitely outside. Precise instants exactly seven
  days apart remain eligible; an additional millisecond does not. Mixed
  precision uses the same full bounds rather than rounding either endpoint.
- A scoped card alias must contain every possible millisecond of each charge
  day. If it intersects every charge interval but does not cover all of them,
  contradictory reported cards remain `CARD_ALIAS_DATE_UNCERTAIN` and UNKNOWN
  while still competing. An alias wholly outside any charge interval cannot
  resolve `CARD_CONFLICT`. Provider, payer, direction and evidence requirements
  are unchanged, including when an exact payment reference is available.

A normalized paid receipt may therefore supply `paidAt: "2026-10-01"` and paid
status, while an original invoice supplies amount, currency and invoice date.
The parent must retain evidence for each assertion; an original amount-due
document alone does not prove payment. With unique posted bank day and matching
card evidence, the receipt can verify without assigning a midnight paid time.

Candidate `accountIdentityState` is separate from charge `state`. Only an
independently evidenced account assertion on a verified match supplies its
`accountRef` and `accountEvidenceRefs`; otherwise those remain `null` / `[]` /
`UNKNOWN`. Differing verified account assertions block the financial pairing.

`certifiedSettledTotals[]` groups verified positive matches by exact provider,
currency and invoice kind, with integer `amountCents` and `matchCount`. A matched
unknown-account charge may contribute at provider level; these are not account
totals. Refunds, credits, zero/non-debits and unknown invoice kinds stay visible
outside these totals; they are never netted against charges. Bank kind `null`
or `UNKNOWN` supplies no classification; a known bank kind must agree with the
invoice's known kind. An overflowing sum throws `certified_total_overflow`.

Totals have `totalsScope: PROVIDED_RECORDS_ONLY`. They are certified matched
records in this snapshot, not complete bank spend, a recurring-price forecast,
current plan price, entitlement value or API-equivalent usage cost.
`crossSourceDedupeState` remains `UNKNOWN`. `spendAllocation` always returns
`UNKNOWN`, the reason `USAGE_ACCOUNT_JOINS_AND_COMPLETE_PERIOD_NOT_PROVED` and
an empty allocation list. This contract accepts no usage/account or
chat/project allocation proof, so even complete source coverage cannot release
that hold. Thread/account integration remains the parent's separate work.

## Replay, revisions and source boundaries

Identity is `(sourceRef, invoiceRef)` or `(sourceRef, transactionRef)`. The parent
must retain stable source-native refs and assign a positive monotone `revision`
for changed authoritative facts. Use the same revision when re-reading identical
facts; do not increment it merely to bless conflicting snapshots. If an export
does not establish revision order, keep the conflict unresolved.

Within one identity, the highest revision is active, older rows are
`SUPERSEDED`, and identical copies of the active revision are `REPLAY` (one
active representative). Replays contribute zero additional cents. Conflicting
facts/evidence at the same revision, or observation times moving backwards as
revision increases, make the whole identity `REVISION_CONFLICT`. Input array
order cannot resolve a conflict. Every supplied row remains visible. Feed the
known revisions or the authoritative current snapshot on each call; the pure
function has no memory across calls and cannot discover omitted tombstones.

Different transaction IDs are never linked as a pending/posted transition.
Different sources are never deduplicated from merchant, amount, dates or a
shared export hash. Plausible copies instead block one-to-one settlement. The
parent must resolve cross-source identity from explicit evidence before making
broader bank-spend claims.

## Local proof and handoff

Run the standalone synthetic proof on Node 22.18+ with built-in type stripping:

```sh
node --disable-warning=MODULE_TYPELESS_PACKAGE_JSON scripts/subscription-reconciliation-proof.ts
```

The proof has 51 fixtures: the original 31 unchanged plus 20 calendar-precision
fixtures. It includes the synthetic 40-posted / 23-removed / 2-pending shape.
Those counts are a fixture, not a readback of any real export.
No data file, ledger, bank, provider or private account is opened. A focused
static check, using the parent's already installed compiler and Node types:

```sh
pnpm exec tsc --noEmit --strict --target ES2022 --module Preserve --moduleResolution Bundler --allowImportingTsExtensions --skipLibCheck --types node packages/collector-cli/src/subscription-reconciliation.ts scripts/subscription-reconciliation-proof.ts
```

This adaptation changes only the module, proof and this contract. No package
scripts or other sources are changed by this lane. Live evidence contents, real invoices/banks, hosted
sources, native account links and report/refresh integration are **NOT CHECKED**.
The supplied workspace has no `.git`; the parent can copy these three files into
its checkout and stage them there. No commit or Git staging is claimed here.
