# Subscription-native telemetry result

Verified 2026-10-14T17:34:12.824329+00:00. This is the individual-subscription path, separate from provider API-console accounting.

- Native feed: **32 configured account records and 61 allowance windows**. Configured records include unproved or non-paid accounts; current values are masked when evidence is stale or unknown.
- Local capture: **4801 observed token-bearing events**, with 83 recent thread groups in this snapshot. Project links are observed, inferred, or UNKNOWN. Earlier history, account joins, and fleet completeness remain UNKNOWN.
- Cursor: **23,421 personal dashboard CSV rows** imported, with 865 missing token totals. The CSV totals 3,198,818,064 observed tokens and $146.25 of reported on-demand usage; neither is certified settled payment. The approximately 1.9B dashboard chart does not reconcile to the CSV, so the control comparison remains UNKNOWN.
- Local refresh: **installed and running**, configured every 60 seconds. Native job exit status was 0; 4 successful runs after installation produced fresh artifacts. The frozen runtime matches its installation receipt.

The source checks passed: typecheck, native-subscription proof, provider-usage proof, and diff whitespace. The native proof covers stale/missing data, privacy exclusions, usage authority, incremental replay and failure behavior, project inference, actual Cursor CSV shape, scope validation, and included usage versus cash.

Provider exports must still be acquired from the provider dashboard; the local importer runs automatically. Grok thread tokens, personal compliance entitlement, complete historical/fleet coverage, and bank-charge allocation remain **UNKNOWN**. No plan, credit, reset, or billing setting was changed.

Live local output: `/Users/utlyze/Library/Application Support/Plimsoll/subscription-telemetry.html`. Full machine-readable verification is in [verification.json](verification.json); operating instructions are in [provider-usage-reconciliation.md](../../provider-usage-reconciliation.md).
