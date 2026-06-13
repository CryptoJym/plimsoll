# 0036 — Project attribution parity: the workspace maps events to named projects the way the local ledger does

GitHub mirror: https://github.com/CryptoJym/plimsoll/issues/65 (cloud pairing: plimsoll-cloud#20 "map like local")

## TL;DR
- Locally 169k+ events carry `repo_hash` (privacy-preserving linkage; issue 0008 stitching) and humans see names via `repo_labels`; the cloud had `projectKey` on 2 of 832,886 events because neither upload path copied the column into the payload.
- Three lanes: forward (both upload mappers send `event.projectKey = repo_hash`, `branchHash` in metadata), repair (`upload-history --repair-attribution`: bare {id, projectKey} pairs, ONE set-based tenant-scoped FILL-ONLY update per batch cloud-side — the bulk lane is first-writer-wins and can never backfill), and labels (`push-repo-labels`: deliberate owner disclosure of repo slugs, previewed doctor-style before sending; schema refuses anything containing `://`).
- Cloud dashboards grow byProject rollups with name resolution: `WorkProjectMap.projectName` > `WorkRepository` owner/name slug > FULL key (never truncated into collisions).

## Design decisions
- Forward path injects into the EVENT (strict schema validated after injection); a payload-supplied projectKey is never overwritten.
- Repair ids ride the SAME deterministic UUID mapping as uploads (`ensureUuidEventId`), so pairs target exactly the rows uploads created. No resume state: the walk is cheap, the update fill-only — re-running reports `updated: 0`.
- Repair rides the existing ingest route, discriminated by `kind: "attribution_repair"` — same install-key + HMAC auth, no new seam.
- Labels route lives under `/api/work-intelligence/` so both proxy matcher entries already exclude it from Clerk (machine path).
- `repo_labels` slugs win over `priority_repos`-derived names per hash; derived rows are marked in the preview.

## Acceptance Criteria
- [ ] Forward path: ledger row with `repo_hash` uploads with `event.projectKey == repo_hash` (proof: `forward_path_sends_repo_linkage_as_project_key` — 100 linked fixture events observed on the wire).
- [ ] Live repair against prod fills projectKey for every cloud row whose ledger row carries `repo_hash`; coverage before/after reported; re-run → `updated: 0`.
- [ ] `push-repo-labels` previews the exact payload; live push upserts WorkRepository rows for the ~50 labeled repos.
- [ ] Dashboards: admin byProject panel + finance byProject show NAMED projects with money; cross-checked against local `pnpm report -- --repository <slug>`.
- [ ] Proofs green: public 76 → 80 (`attribution_repair_rows_share_upload_id_mapping`, `attribution_repair_fills_once_then_settles`, `repo_labels_disclose_slugs_never_urls`, forward-path); cloud 96 → 102 (repair contract, set-based/fill-only/tenant-scoped SQL grep, labels seam, label precedence + never-truncate, byProject aggregate≡per-event, finance labels).

## Operational Boundaries
- Ledger read-only; daemon untouched (forward path activates when the owner restarts it — the repair covers everything before that, and a top-up repair covers the gap after).
- Only hashes + owner-disclosed slugs cross the wire; raw URLs/branch names never do (schema-refused).
- `pnpm proof` stays green in both repos.

## Notes For Future Agents
- The cloud's `WorkRepository` has NO unique on (tenantId, remoteUrlHash) — labels upsert is find-then-write; if churn ever matters, add the constraint first.
- The repair lane's `matched` counts pairs whose id exists in the tenant; ids for rows the daemon hasn't uploaded yet simply match nothing and will be covered by a later run.
