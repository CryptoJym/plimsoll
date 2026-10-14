# Subscription-native telemetry

This path is for individual ChatGPT Pro/Max, Claude Pro/Max, Cursor, and Grok
subscriptions. It reuses the existing native account feed and the local agent
ledger. It makes no provider model calls and requires no team Admin API keys.

## What is measured

Keep these four quantities separate:

1. **Native allowance:** provider-reported used/remaining percentages, credits,
   plan labels, and reset times. These are account-level meters, not raw token
   totals, and a percentage cannot be converted into dollars or tokens.
2. **Local tokens:** token-bearing events copied from the existing collector
   with their thread ID, model, native usage authority, and repository hash.
   The bridge starts with a bounded recent window and then advances a rowid
   cursor. Earlier history and fleet completeness remain `UNKNOWN`.
3. **Provider exports:** individual dashboard snapshots. An overlapping export
   is not added to an earlier snapshot. Missing counters remain null/UNKNOWN.
4. **Actual money paid:** invoices and settled bank transactions. Included
   subscription usage and API-equivalent token estimates are not cash charges.
   Account-to-charge allocation remains `UNKNOWN` until supported by billing
   evidence. Anthropic explicitly distinguishes included Pro/Max usage from
   its locally calculated token-cost estimate in its
   [usage documentation](https://code.claude.com/docs/en/costs).

## Verified surfaces, 2026-10-14

| Product | Native source | Dashboard/export | Remaining UNKNOWN |
|---|---|---|---|
| ChatGPT / Codex | Existing account/rate-limit feed; local token events | Personal Usage overview and Analytics inspected in an account-matched browser. Current weekly history showed Not available. | Exact subscription-wide tokens, personal Compliance API entitlement, complete historical capture |
| Claude Max / Claude Code | Existing native usage feed; local transcript/live token events | Account-matched Max 20x Usage screen inspected, with session and weekly bars | Individual token export, exact subscription-wide tokens, invoice allocation |
| Cursor Ultra | Existing native feed, whose quota reading was stale; local conversation/model metadata | Individual dashboard CSV downloaded and imported: 23,421 rows, Sep 15-Oct 14 | Most rows lack a cloud-agent ID; 865 rows lack total tokens; automatic provider-side export refresh |
| SuperGrok Heavy | Existing native included-credit meter | Native local account/billing extension verified by the existing observer | Grok thread tokens, external OpenTelemetry delivery, personal compliance export |

The account feed currently contains 32 configured records: 20 OpenAI, 10 Claude,
1 Cursor, and 1 Grok. Some OpenAI records are free, stale, disabled, signed out,
or unproved; this is not a claim that all 32 are active paid subscriptions.
Every account/window retains its own evidence state and observation time.

OpenAI documents native usage limits and a separate
[Compliance API surface](https://help.openai.com/en/articles/11369540-using-codex-with-your-chatgpt-plan).
That documentation does not prove eligibility for these personal accounts.
Anthropic's [organization Analytics API](https://platform.claude.com/docs/en/manage-claude/claude-code-analytics-api)
and Cursor's [team Admin API](https://cursor.com/docs/account/teams/admin-api)
are separate from this individual-subscription path. Inference-console keys
are never treated as subscription meters.

## The small loop

The existing collector writes
`~/Library/Memory/ops/data/native-account-usage.json`. It contains private native
identity data; this bridge copies only hashed account references, plan/meter
fields, observation times, and bounded error codes.

A separate, one-minute local refresh reads the feed and at most 10,000 new rows
from the live work ledger. The read happens in a child process with a hard
four-second deadline and a forced rowid range, so SQLite cannot choose a full
ledger scan. It preserves the collector's terminal privacy exclusions, native
duplicate dispositions, and live-versus-tailer authority. Source ledgers are
opened read-only; a failed read leaves the cursor in place and reports UNKNOWN.
The same job checks whether an hourly stored-bank refresh is due. Its pinned
private Python helper reads the existing local tracker, preserves invoice source
clocks, and replaces only its bank snapshot. It makes no new Plaid/API calls.
Invoice and provider dashboard acquisition remain separate; the refresh never
makes cached documents current by changing their timestamps.

The bridge writes only its own small local database and status artifacts:

- `~/Library/Application Support/Plimsoll/native-subscription.sqlite`
- `~/Library/Application Support/Plimsoll/subscription-telemetry-status.json`
- `~/Library/Application Support/Plimsoll/subscription-telemetry.html`

The HTML report refreshes itself when open. It shows account allowance windows,
captured usage by subscription account, recent chats and project candidates,
provider export chat IDs, and invoice-to-bank adjudication. Unknown joins remain
visible. Personal email labels appear only in the private HTML; the JSON and
sidecar use account references and protected identity evidence.

## Linking chats to subscription accounts and projects

An account join requires identity captured on the event. The native 0.7.47
collector protects provider account IDs with its boxed SHA-256 derivation;
tenant-salted assertion hashes use a separate derivation. Generic user/actor
hashes are preserved separately. In particular, Claude's generic `user.id`
must not replace its captured `user.account_uuid`.

Codex account IDs come from the authenticated native account feed. Claude
account UUIDs come from an explicitly registered profile's local metadata,
checked against both its native email and organization identity. This does not
assign historical activity from a current login. Missing identities, semantic
conflicts, collisions and multiple account candidates remain `UNKNOWN`.
Coverage is reported per chat and per captured event, including chats that span
accounts. Native allowance freshness remains independent from identity evidence.

Explicit project keys take precedence over repository suggestions. Each
candidate keeps its source; conflicting suggestions remain reviewable. A Git
repository link is an inferred project relationship, not evidence that a
corresponding hosted project was created or accepted.
The refresh also checks repository metadata for up to 256 stored chat groups
across the explicitly registered Claude project folders. Starting folders rotate
under a shared read budget. Repeated directory metadata uses one repository
lookup per refresh; different repository candidates remain separate.

Cursor cloud-agent IDs supplied by its latest individual export retain the
export's account scope. They are separate control observations, not extra
native events. Mapping these IDs to local Cursor conversations or projects is
`UNKNOWN` without further evidence. Rows without IDs stay unattributed.

## Subscription charges and bank reconciliation

The private `subscription-reconciliation-input.json` binds source clocks and
SHA-256 evidence to paid invoices and bank transactions. The strict matcher
requires exact provider, currency and cents, compatible actual payment/posting
dates, payment-instrument or exact-reference proof, and an unambiguous mapping.
Amount and date alone never settle a match. Calendar dates keep day precision;
an invoice's issue date is not substituted for its payment date.

Monthly subscriptions, upgrades/prorations and usage resets remain different
charge kinds. Pending transactions, removals, refunds and revisions are retained
outside certified settlement totals. A matching card does not establish the
subscription login; account identity has its own evidence. Cash allocation to
chats and projects remains `UNKNOWN` until account usage joins and period
coverage support it.

The initial bank source is the existing company tracker's stored transactions.
It is read without DB writes or new Plaid calls. Its importer company label does
not prove the legal payer. Hosted bank-screen observations lacking stable
transaction IDs/current posted state remain supplemental `UNKNOWN` evidence and
are excluded from totals. There is no cross-source summing or guessed card alias.
The tracker derives `paymentInstrument.lastFour` from a financial account's
Plaid mask. That field supplies no independent card proof, so bank `cardLast4`
stays null. Plaid documents that an account's
[mask can be a displayed account mask and can be non-unique](https://plaid.com/docs/api/accounts/).
The initial $214.90 candidate was therefore withdrawn from verified settlement
after this source review. It remains `UNKNOWN` with the other unresolved pairs.
Positive posted bank debits are displayed separately by source, provider,
currency and charge kind, within the source's posting window. They do not become
subscription-account totals. Pending/removed rows, unresolved revisions and
cross-source summing are excluded; absence of verified matches is `UNKNOWN`,
not zero dollars spent.

Invoice PDFs, paid-receipt evidence and the reconciliation evidence index stay
under the private Plimsoll application-support directory. Originals issued with
an amount due are not relabeled unpaid: paid status comes from the separate
receipt or native paid-invoice screen. Unavailable documents/currency stay
`UNKNOWN`. Reprocessing keeps source read/sync times; stale sources mask current
totals while retaining historical matches as of those sources.

```sh
# From the repository root; read-only capability/status inspection.
pnpm collector usage native-status

# Refresh once, preview, or inspect the stored report.
pnpm collector usage native-import --dry-run
pnpm telemetry:refresh
pnpm collector usage native-report

# Install the frozen, tested local runtime and its one-minute LaunchAgent.
pnpm telemetry:install
```

The exact job label is `com.utlyze.plimsoll-subscription-telemetry`. Installation
reuses the existing Node and SQLite library and leaves the active collector
running. It does not change provider login, plan, credit, or reset settings.

## Individual dashboard exports

The supported live-tested shape is Cursor's individual usage CSV:
`Date`, `Cloud Agent ID`, `Automation ID`, `Kind`, `Model`, `Max Mode`,
`Input (w/ Cache Write)`, `Input (w/o Cache Write)`, `Cache Read`,
`Output Tokens`, `Total Tokens`, and `Cost`.

Cache-write input, uncached input, cache-read input, and output remain separate.
`Included` and `Free` costs do not become dollar charges. Numeric `On-Demand`
costs are stored as reported usage costs, not settled bank payments. Rows with
missing counters are retained as UNKNOWN.

```sh
pnpm collector usage dashboard-import --provider cursor --file cursor.csv \
  --account-ref NON_SECRET_ACCOUNT_REF \
  --period-start 2026-09-15T00:00:00Z --period-end 2026-10-15T00:00:00Z
```

For automatic local ingestion, place a CSV and a matching `.metadata.json` in
`~/Library/Application Support/Plimsoll/subscription-exports/cursor/`. The
manifest binds the provider account, selected UTC period, individual-subscription
scope, and SHA-256 of the file. The refresh validates the manifest and deduplicates
replays. Getting a new export from the provider is currently a separate manual
step; reprocessing an old file never makes its provider data fresh.

The first imported CSV sums to 3,198,818,064 observed tokens and reports $146.25
of on-demand usage. The selected dashboard chart displayed approximately 1.9B
tokens. Their scope/counting difference is unverified, and 865 CSV rows lack
total-token counters, so reconciliation remains **UNKNOWN**. The observed CSV
sum is not presented as a certified subscription total.

## Separate API-console lane

The earlier `usage import`, `usage report`, and `apiConsoleCapabilities` support
API organization reports. They are not polled by this subscription refresh and
must not be used as Pro/Max subscription accounting. Generic CSV normalization
is separate from the live-tested individual Cursor adapter.

## Verification

```sh
pnpm typecheck
pnpm proof:native-subscription
pnpm proof:provider-usage
pnpm proof:native-account-links
pnpm proof:subscription-reconciliation
pnpm proof:subscription-reconciliation-file
pnpm proof:subscription-export-links
pnpm proof:subscription-bank-source
```

The native proof checks freshness and missing-data semantics, privacy exclusions,
usage authority, replay-safe incremental capture, inferred project labels, the
actual Cursor CSV shape, and the separation of included usage from cash spend.
Native job state and its newly produced artifacts are verified separately from
these offline checks.
