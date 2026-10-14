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

The bridge writes only its own small local database and status artifacts:

- `~/Library/Application Support/Plimsoll/native-subscription.sqlite`
- `~/Library/Application Support/Plimsoll/subscription-telemetry-status.json`
- `~/Library/Application Support/Plimsoll/subscription-telemetry.html`

The HTML report refreshes itself when open. It shows current account allowance
windows, their reset times, provider export timestamps, and recent chats with
observed or inferred project links. Inferred repository links remain reviewable;
unknown subscription-account joins and settled spend are not filled in.

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
```

The native proof checks freshness and missing-data semantics, privacy exclusions,
usage authority, replay-safe incremental capture, inferred project labels, the
actual Cursor CSV shape, and the separation of included usage from cash spend.
Native job state and its newly produced artifacts are verified separately from
these offline checks.
