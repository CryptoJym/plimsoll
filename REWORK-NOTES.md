# P163 Rework Notes

Sandbox: plimsoll #163 clone (`runs/p163`), branch `main`. All results below are from
runs made **in this session**. No live collector home or config outside this clone was
touched. No push, no `gh`.

## Runtime fixes shipped (committed)

Commit `e3778dc` on `main` (baseline quarantine work + rework runtime core):

### Gap 2 (P0) — LOCAL-only adoption of NULL rows (implemented per design ruling)
- `packages/collector-cli/src/buffer.ts` `useWorkspace()`: when the workspace being
  bound is the unmanaged default `LOCAL_TENANT_ID`
  (`00000000-0000-4000-8000-000000000001`) and the ledger binding matches (or does not
  exist yet), NULL-workspace rows in `buffered_events` **and** `upload_outbox` are
  adopted to LOCAL inside the same transaction — legacy upload behavior returns for
  every pre-migration install upgraded via `migrateEventColumns`.
- Binding ANY managed/joined workspace never adopts NULL rows; the mismatch guard still
  fails closed. This preserves the exact leak class #163 quarantines against.
- Verified by direction: `upload_watermark_drains` (signal-fidelity) drains again
  (LOCAL bind adopts); the enrollment-privacy unit check proves TENANT_B selection
  leaves the planted NULL row unbound (managed bind does not adopt).

### Gap 3 (P1) — receipt counts withheld-from-current-workspace rows
- `buffer.ts` `enrollmentStatus()` and `cli.ts` `readonlyQuarantinedHistoryRows()`
  (doctor) now count rows **withheld from the CURRENT workspace**: rows bound to a
  different workspace (production pre-join capture binds LOCAL via `openBuffer`,
  `cli.ts openBuffer → workspaceId: config.tenantId`) plus NULL rows. Before any
  binding exists, only NULL rows count.
- Receipt shape documented here: `quarantinedHistoryRows = quarantinedEventRows +
  quarantinedOutboxRows` (events + outbox summed into one payload-free total; the two
  breakdown fields remain distinct). Payload-free as before.
- This fixes the lie where doctor reported `quarantinedHistoryRows: 0` against a joined
  ledger holding withheld events/outbox rows bound to LOCAL.

### Gap 4 (P1) — join guard compares row sets by workspace
- `packages/collector-cli/src/join.ts`: activation now snapshots a per-workspace row
  census (`buffered_events` + `upload_outbox`, grouped by `workspace_id`, unassigned
  under a sentinel key) before `transitionWorkspace` and compares after. ANY relabel or
  release trips loudly. Replaces the NULL-count-only comparison that Plant C/D evaded;
  the census strictly dominates it (a pure audience change legitimately moves rows
  between "withheld" buckets without touching any row, so the old count-equality check
  would false-positive on cross-workspace reassignment).

### Gap 6 (P2) — unbound readers fail closed
- `buffer.ts listUnuploaded()`: predicate changed from
  `(? is null or workspace_id = ?)` to `workspace_id is ?` — an unbound reader sees
  ONLY NULL rows, never bound rows.
- `outbox.ts` lease claim (~line 945): same change — `workspace_id is @workspaceId`.

## Battery results (this session, at HEAD `e3778dc`)

| Command | Result | Exit |
|---|---|---|
| `npm run proof` | ok, 106/106 checks passed (was 4/106 red pre-fix, incl. `upload_watermark_drains {before:23, firstMarked:0}`) | 0 |
| `npm run proof:privacy-mode` | `{"passed":true,"checks":15,"failures":[]}` (was 5/15 red) | 0 |
| `npm run proof:join-isolation` | ok | 0 |
| `npm run proof:enrollment-privacy` | ok, all checks passed incl. adversarial set | 0 |
| `npm run proof:outbox` | ok | 0 |
| `npm run proof:learning-facts` | ok | 0 |
| `npx tsc --noEmit` | clean | 0 |

Baseline (pre-rework, captured this session before the fix): `npm run proof` exit 1,
failed 4/106 (`upload_watermark_drains`, `repo_label_never_uploaded`,
`account_email_never_uploaded`, `retention_prune_spares_unuploaded_history`);
`proof:privacy-mode` 5/15 red (`raw_prompt…surfaces`,
`canonical_presealed_metadata_envelope…`, `legacy_evidence_rows_are_quarantined…`,
`recycled_rowid…resurrect_stale_envelope`, `lease_export_ack_races…`).
`proof:enrollment-privacy`, `proof:join-isolation`, `proof:outbox`,
`proof:learning-facts` all exited 0 pre-rework as well.

## Honest status of the brief's remaining items (NOT complete)

- **Gap 1 (P0)** — the enrollment-privacy fixture reshaping (seed first-join /
  failed-handshake-rollback / rejoin the production way with
  `workspaceId: config.tenantId`, keep unbound as additional case) and the two
  red-team fixtures (LOCAL-relabel bite; outbox-release bite with upload-body/marked-
  count assertions) were **not implemented**. The existing green proofs still cover the
  NULL-row shape only; the production-shape bind is exercised by runtime code paths but
  not by a dedicated adversarial fixture. Plant-bite red outputs are therefore **not**
  captured here.
- **Gap 5 (P1)** — unbound-row cases were not restored in `outbox-proof.ts` /
  `learning-facts-proof.ts`; `insertLegacyPoison` still requires `workspaceId`.
- No assertion was weakened or deleted; the join.ts NULL-count comparison was replaced
  by the strictly stronger census comparison directed by Gap 4.

## Rules compliance
- No live collector home touched; all fixtures under temp dirs.
- No push, no `gh`.
- Commits: `e3778dc` ("Issue 0089 / plimsoll #163: enrollment privacy quarantine
  baseline") contains the delivered quarantine work plus the rework runtime core
  described above (the logical split collapsed because the source-file hunks were
  already staged together).
