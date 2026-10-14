# Read-only subscription bank source refresh

This adapter replaces one BANK snapshot in the existing private
`subscription-reconciliation/v1` input. It reads transactions already stored in
the local company tracker. It does not run a bank collection, call a provider or
payment API, read balances, move money, install software, or install a schedule.
A sourceRef containing a bank name or date does not establish full coverage.

## Run, dependencies, and ownership

Root installs these three files into the existing Plimsoll checkout:

- `scripts/subscription-bank-source-refresh.py`
- `scripts/subscription-bank-source-refresh-proof.py`
- `docs/subscription-bank-source-refresh.md`

Run from that checkout as the owner of the private input, using the owner's
normal home directory (no sudo):

```sh
python3 -B ./scripts/subscription-bank-source-refresh.py --source-ref company-tracker-chase-20261014
```

Omitting `--source-ref` selects that exact default. An explicit value selects
exactly one existing BANK source with that sourceRef; missing, duplicate, or
wrong-type sources fail. There is no discovery, fallback source, or CLI option
to change SQL, helper, database, provider, credentials, or psql executable.
`--home /absolute/fixture/home` changes the fixed home-relative paths for isolated
fixtures; it must never point an offline test at a real home. The proof creates
its own home and mocks the psql subprocess. No other CLI arguments are accepted.

Local runtime dependencies are Python 3.9+ with its standard library, POSIX
`fcntl.flock`/directory-fd/no-follow file operations (macOS target), the existing
approved helper, its existing import dependencies and credential access, and:

```text
~/Library/Memory/ops/manager/bank_collect.py
~/Library/Memory/ops/finance-daily-sync/runtime/company-expense-tracker/.local-runtime/runtime.env
/opt/homebrew/opt/postgresql@15/bin/psql
```

Only `importlib` module loading and `pg_environment(Path(runtime_env))` are used.
The helper's main/collect entrypoints are never invoked. The adapter does not
open or parse runtime.env or retrieve credentials itself. The returned process
environment is passed privately to psql. Diagnostics from helper loading and
execution, Python runtime errors, and psql stderr are suppressed. Root must
retain the approved helper's import-only behavior and verify its target runtime;
those dependencies were not inspected on the worker.

The adapter accepts an explicit loopback PGHOST, a Unix-socket directory, or
libpq's default local socket. Remote hosts/host lists/hostaddr, connection strings
in PGDATABASE, and service-file indirection are refused. It does not rewrite any
configuration. Existing credentials remain only in process memory/environment,
never in arguments, evidence, or output. Root owns invocation, actual DB target
verification and subsequent runtime integration. Call at most hourly; there is
no automatic scheduler or retry loop here.

## Database read and selection

One psql subprocess runs a fixed repeatable-read, READ ONLY transaction with UTC
timezone, `statement_timeout = '10s'`, and a 20-second subprocess timeout. Process
PGOPTIONS also sets default transaction read-only, UTC and a 10-second timeout;
connection timeout is five seconds. `-X`, `-w`, disabled pager, quiet unaligned
JSON output and ON_ERROR_STOP avoid startup scripts, password prompts and partial
success. The transaction ends with ROLLBACK. Only SELECT and transaction-local
settings are issued. No database write or provider client is present.

The existing selected source's `coverage.periodStart` must be a valid ordered
YYYY-MM-DD date. The upper bound is the actual UTC tomorrow computed in the DB
transaction, exclusive: `[periodStart, tomorrow)`. A row qualifies if its stored
transaction day OR posting day intersects that window. Both original dates are
retained even if the other date is outside the window. Rows with neither date
cannot establish membership and are not discovered by this bounded query; this
is one reason coverage is always PARTIAL. If such a row is returned unexpectedly,
it is retained as a sanitized exclusion and the input is left untouched.

Merchant + description filtering is closed and case-insensitive:
`openai|chatgpt|anthropic|claude|cursor|grok|bounded xai`. XAI uses ASCII
alphanumeric/underscore boundaries; e.g. `prexaipost` and `_xai` do not match.
The same terms classify `openai`, `anthropic`, `cursor`, or `xai`; multiple or no
provider matches yield null. Only an explicit bounded RESET word with unambiguous
OpenAI yields `USAGE_RESET`. Every other row has `kind: UNKNOWN`; the invoice
engine adjudicates charge/subscription kind. Generic AI merchant evidence never
becomes a monthly subscription.

The query selects only transaction UUID, joined financial-account UUID, external
transaction ID, numeric amount as text, currency, merchant/description,
transaction/posting dates, pending flag, provider revision/removal marker and
account mask. Sync selection aggregates only plaid_items.status and
last_successful_sync_at. It does not select provider_account_id, importer company
labels, item credentials, metadata, bodies, or balances. The financial-account
join is `transactions.financial_account_id = financial_accounts.id`; the unrelated
provider_account_id UUID never supplies a native account mapping.

SQL has hard LIMIT 5001. Seeing 5001 fails with PROVIDER_ROW_LIMIT, never truncates
or replaces the input. Up to 5000 provider rows can be sanitized. The existing
engine independently limits the combined bankTransactions array to 2000,
including preserved other BANK sources; exceeding that fails CONTRACT_ROW_LIMIT.
Sources/invoices/aliases retain the existing engine's limits. There is also a
16 MiB cap on DB output, sanitized evidence and old/new input. SQL text fields
are capped at 1025 characters to detect an overlong (>1024) field, with an amount
cap of 129 to detect >128; any such data-quality failure blocks replacement.

## Schema and provenance

The existing strict contract is
[`subscription-reconciliation-contract.md`](subscription-reconciliation-contract.md)
and its engine is `packages/collector-cli/src/subscription-reconciliation.ts`.
The adapter validates the wire schema without normalizing preserved values;
it does not implement reconciliation. The proof optionally uses the actual TS
parser and reconciler. No TypeScript file is changed.

| Field | Meaning |
| --- | --- |
| source.readAt | DB transaction/export instant, exact UTC milliseconds |
| row.sourceObservedAt | Same DB export instant, an observation of the stored revision |
| source.lastSyncedAt | Oldest successful sync only if every current item is healthy and has a sync |
| source.providerObservedAt | Always null |
| input.observedAt | Local processing instant after DB read; never used as bank sync time |
| source.coverage | PARTIAL, existing periodStart, actual DB UTC tomorrow |
| transactedAt / postedAt | Validated provider calendar YYYY-MM-DD days, or null; no synthesized midnight instant |
| revision | Actual positive integer provider_revision; no synthetic revision or deduplication |
| state | Non-null removal marker wins as REMOVED; otherwise actual pending true/false gives PENDING/POSTED |
| amountCents | Exact signed Decimal-to-integer scaling by 100; no float, rounding, absolute value, sign flip, FX, or default zero |
| currency | Explicit supported uppercase denomination only; no USD fallback |
| accountRef / accountIdentityState | Always null / UNKNOWN |
| payerRef | Optional `bank:` + SHA256(UTF8(String(joined financial_accounts.id))) |
| transactionRef | `banktx:` + SHA256(UTF8(String(transactions.id))) |
| externalIdDigest | Evidence-only `sha256:` + SHA256(UTF8(String(external_transaction_id))), if present |
| cardLast4 | Always null; a financial-account mask alone does not prove a payment card |

The conservative healthy status is exactly `plaid_items.status = 'healthy'`.
Unknown status spellings, any unhealthy item, any missing successful sync, or no
items gives lastSyncedAt null. Root verified this convention on the actual tracker on 2026-10-14: both current items were `healthy`. DB naive sync timestamps use the supplied documented UTC
convention. Provider charge timestamps are naive calendar dates; their time of
day is discarded without timezone conversion. PostgreSQL's short fractional
seconds are padded only in a Python 3.9 validation copy; original evidence
strings are preserved. The removal timestamp remains a
validated naive string in evidence as providerRemovedAtNaive; no UTC removal
instant is invented. Pending and removed rows, different native IDs, negative
amounts, and actual revision numbers remain visible. A replacement is the current
bounded stored snapshot, not a reconstruction of prior DB revisions. Earlier
sanitized export files retain earlier observations. Missing rows are never
synthesized as tombstones and distinct pending/posted IDs are never linked.

Supported explicit denominations are USD EUR GBP CAD AUD NZD CHF JPY CNY HKD SGD
INR BRL MXN SEK NOK DKK PLN CZK HUF RON ZAR KRW TWD ILS AED SAR TRY THB MYR IDR PHP
VND. Unsupported, missing, lowercase or unknown currencies are held. This is a
closed adapter support set, not a claim about the full currency registry. Integer
cents always means the existing engine's amount-times-100 representation.

Sanitized evidence uses `subscription-bank-source-export/v1`, canonical UTF-8
JSON (sorted keys, compact separators, ASCII escaping, final newline). Each
observation contains only pseudonyms/digests, exact safe amount/cents/currency,
provider/kind, dates, pending/removal/revision state, optional mask4 and fixed issue
codes. Merchant/description text is represented by digests; raw native IDs,
external IDs, arbitrary invalid values, full bank/card numbers, secrets and
provider bodies are not persisted. A valid four-digit account mask is private
evidence only. No payer/legal-company/subscription identity is inferred.

The exact evidence bytes are SHA256-addressed at:

```text
~/Library/Application Support/Plimsoll/subscription-reconciliation-inputs/bank/<64hex>.json
```

The selected BANK source and every new bank row receive the same
`evidenceRefs: ["sha256:<64hex>"]`. Existing content-addressed files are never
overwritten; reuse checks the exact bytes. Immutability is enforced by this
writer's content-address/compare policy, not an OS immutable flag. An unrelated
owner process can still tamper with files, so root must check the hashes.
All account, bank and fleet completeness remains UNKNOWN in evidence and the
engine. This export establishes no bank-wide/account-wide completeness.

## Holds, writes, and failure behavior

Unknown provider, one missing charge date, or missing joined financial account
remains null in an included row; these weak candidates remain available to the
engine, which keeps identity/date uncertainty. `heldRows` counts those rows; all
native subscription-account identities remain UNKNOWN regardless of that count.
Missing/unknown currency, invalid/non-cent/unsafe amounts, invalid IDs, dates,
revisions, state, duplicate IDs or overlong text are sanitized with issue codes
and exclusions.count/state UNKNOWN. Any exclusion blocks the whole replacement
with status UNKNOWN / code ROW_QUALITY. Valid peers are not published as if the
excluded competitor disappeared. Malformed envelopes/schema, missing sync
metadata, inconsistent clocks or row limits fail before publication. No missing
amount becomes zero. A read/validation failure leaves the prior input bytes.

Only the configured BANK source object and its bankTransactions are replaced;
input.observedAt advances. Invoice rows, invoice source clocks/evidence, other
sources/rows and cardAliases retain their original JSON values. Formatting may
change. Both old and proposed inputs pass strict structural validation. The
selected source's previous readAt and root observedAt must not lie in the future
relative to the new read/processing times.

The existing Plimsoll directory must be owner-owned mode 0700. Input and evidence
must be regular owner-owned mode 0600 files, without symlinks/hardlinks; path
components are opened without following symlinks. New evidence directories are
0700 and files are 0600. File sizes, owner, type and permissions are checked.
No existing insecure modes are repaired automatically.

A persistent private `.subscription-bank-source-refresh.lock` uses nonblocking
kernel flock to serialize separate refresher invocations. The lock is never
unlinked. Directory/lock identity and original input inode/timestamps/bytes are
rechecked immediately before replacement to reject noncooperating edits during
the read. A writer deliberately ignoring the lock in the final check/rename
interval cannot be universally excluded by ordinary filesystem operations;
root's other writers should take the same lock. Temp files are fsynced and the
input is atomically renamed in its original directory, followed by directory
fsync and an independent byte reread. New evidence may remain unreferenced if
validation, concurrency, or precommit checks fail; it is never deleted as part
of a refresh. COMMIT_UNCERTAIN means replacement may already have occurred and
root must inspect before retrying; the adapter never rolls back another writer.

CLI stdout is one JSON object with status, counts, sourceRef/exportedAt when a
read was valid, and a stable code on failure. It contains no merchant/card/payer
identity, amount totals, credentials, stderr or traceback. Exit zero is UPDATED;
nonzero is ERROR or an exclusion hold UNKNOWN. Codes include REFRESH_LOCKED,
INPUT_CHANGED, BANK_SOURCE_NOT_UNIQUE, INPUT_INVALID, PRIVATE_MODE,
PRIVATE_OWNER, DB_ENVIRONMENT, DB_NOT_LOCAL, DB_READ_FAILED, DB_READ_TIMEOUT,
DB_EXPORT_INVALID, CLOCK_ORDER, PERIOD_INVALID, PROVIDER_ROW_LIMIT,
CONTRACT_ROW_LIMIT, ROW_QUALITY, EVIDENCE_CONFLICT, INPUT_TOO_LARGE,
EXPORT_TOO_LARGE, PRIVATE_IO and COMMIT_UNCERTAIN. Some OS/path/type errors share
PRIVATE_OR_INVALID_INPUT; unexpected exceptions use REFRESH_FAILED. Failure
codes never include exception text or input values.

## Offline proof and root acceptance

Run the standard-library-only proof without any DB or credentials:

```sh
python3 -B ./scripts/subscription-bank-source-refresh-proof.py
```

To also exercise the actual existing TypeScript parser and reconciler, use an
already-installed Node with native TypeScript type stripping:

```sh
python3 -B ./scripts/subscription-bank-source-refresh-proof.py --engine
```

The --engine run passed 19 test groups, zero failures/errors/skips on Python
3.14.7 / Node v24.18.0. The standard-only run skips just the actual TS check.
The proof imports only a generated fixture helper, asserts runtime.env does not
exist, and replaces psql execution with a synthetic JSON writer. It exercises
penny/sign/safe-integer precision, classification, date precision, tombstones,
pending/revisions, missing sync/no freshness laundering, source isolation,
invoice clocks/aliases, sanitized evidence hashes/modes, 5000/5001 and combined
2000-row bounds, malformed inputs, privacy guards, lock/concurrent edits,
precommit failure and diagnostic suppression. It is not a credential-bound DB
read, hosted acceptance, recovery, completeness, or runtime integration proof.

Root must install and run the refresh command once, then independently verify
that its stdout exportedAt equals the selected source.readAt and that actual
stored data/healthy-status semantics match this adapter's documented contract.
Compare invoice rows, original invoice/source clocks, other source rows, and
cardAliases against the private pre-run input. Do not use advancing
input.observedAt as evidence of a successful provider sync. Separately verify
the published evidence bytes and clocks, printing only counts/status:

```sh
python3 -B - <<'PY'
import hashlib, json, os, pathlib, stat
root = pathlib.Path.home() / 'Library/Application Support/Plimsoll'
value = json.loads((root / 'subscription-reconciliation-input.json').read_bytes())
ref = 'company-tracker-chase-20261014'
sources = [s for s in value['sources'] if s['sourceRef'] == ref and s['type'] == 'BANK']
assert len(sources) == 1
source = sources[0]
assert source['coverage']['state'] == 'PARTIAL' and source['providerObservedAt'] is None
assert len(source['evidenceRefs']) == 1
h = source['evidenceRefs'][0].removeprefix('sha256:')
assert len(h) == 64 and all(c in '0123456789abcdef' for c in h)
path = root / 'subscription-reconciliation-inputs/bank' / (h + '.json')
assert not path.is_symlink() and path.stat().st_uid == os.getuid()
assert stat.S_IMODE(path.stat().st_mode) == 0o600
raw_bytes = path.read_bytes()
assert hashlib.sha256(raw_bytes).hexdigest() == h
raw = json.loads(raw_bytes)
for key in ['readAt', 'lastSyncedAt', 'providerObservedAt', 'coverage']:
    assert source[key] == raw[key]
rows = [r for r in value['bankTransactions'] if r['sourceRef'] == ref]
assert len(rows) == len(raw['observations']) and raw['exclusions']['count'] == 0
assert all(r['evidenceRefs'] == source['evidenceRefs'] and r['sourceObservedAt'] == source['readAt'] for r in rows)
assert all(r['accountRef'] is None and r['accountIdentityState'] == 'UNKNOWN' for r in rows)
print(json.dumps({'status':'EVIDENCE_VERIFIED','rows':len(rows),'exportedAt':source['readAt']}))
PY
```

Root should also pass the actual refreshed input through the existing TS parser
in its normal integration. Worker live read, local helper/dependency readiness,
actual data semantics and root runtime integration: **NOT CHECKED**.

To verify the three delivered file hashes, from the checkout run:

```sh
shasum -a 256 scripts/subscription-bank-source-refresh.py scripts/subscription-bank-source-refresh-proof.py docs/subscription-bank-source-refresh.md
```

The handoff includes these three SHA256s. The document cannot contain its own
stable hash; evidence data files are hashed independently as described above.
