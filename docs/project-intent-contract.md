# P02: canonical project intent v1

**Use a registered business project key for the project served; keep the observed repo separate.** This contract adds declarations beside the existing session ledger. It neither registers guessed labels nor rewrites usage, owner approvals or acceptance receipts.

**Paths:** `GET /api/work-intelligence/project-intents?tenantId=<uuid>&after=<optional key>` returns up to 200 tenant registry choices: `projectKey`, `projectLabel`, `projectRegistryRevision`, and `nextCursor`. `POST` to the same path sends `{tenantId, expectedRevision, receipt}`. Send `x-plimsoll-install-key`, `x-plimsoll-upload-timestamp` (ISO UTC), and `x-plimsoll-upload-signature: sha256=<hex>`. Sign HMAC-SHA256 over `timestamp.rawBody` using the existing local signing credential; GET signs an empty body and POST signs the exact JSON bytes (`content-type: application/json`). The existing five-minute signature policy applies. Bootstrap/master-key-only admission is refused. Keep credential values out of source code and receipts.

**Minimum outbound receipt:** Every field below is mandatory; “nullable” means send JSON null for unavailable information. Schemas and golden fixtures are identical in both repositories (`src/shared/project-intent.ts` / `packages/shared/src/project-intent.ts`).

| Exact fields | Type / rule |
|---|---|
| `schema` | `plimsoll-project-intent/v1` |
| `receiptId`, `installId`, `sessionId` | Lowercase UUIDs; installId is the authenticated ledger installation; sessionId is the existing collector/cloud join UUID |
| `source` | `codex\|claude_code\|gemini_cli\|grok`; this cloud base refuses Gemini with `unsupported_source` |
| `sourceRootKey`, `nativeSessionKey`, `sessionEpochKey` | `sha256:` plus exactly 64 lowercase hex digits |
| `accountKey`, `workItemKey` | Same linkage format, nullable; hash existing account/work identity locally, never a payer or project default |
| `rootAttemptId`, `attemptId`, `parentAttemptId` | UUIDs; parent is nullable. First attempt equals root and has no parent; rotated attempts reference an existing predecessor |
| `projectKey`, `projectRegistryRevision` | Registered canonical key and positive registry revision, or both null; a supplied unknown hash is retained as unresolved evidence |
| `observedRepoKey` | Existing remoteLinkageHash, nullable; never substituted for projectKey |
| `effectiveFrom`, `effectiveUntil` | ISO UTC timestamps; until is nullable; interval is [from, until) |
| `basis` | `routed_launch\|hand_start\|trusted_folder_default\|repo_observation` |
| `evidenceRef` | Full linkage digest referencing local evidence |
| `adapterId`, `adapterVersion` | `routed-launch`, `hand-start`, or `repo-observation`, matching basis; currently `1.0.0` |

**Authority and history:** A resume retains native/epoch/root identity. Account or source-root rotation needs a new linked attempt. Reused native IDs need a new session epoch and ledger UUID; the server rejects reusing the old incarnation. New explicit choices cut over open prior choices without modifying them. History is bounded to 128 receipts per session; a full history refuses new receipts without ACK. Observations cannot replace intent; defaults remain suggestions. Missing/unregistered choices stay Unknown, including after later registration until a fresh receipt is admitted. Different repo evidence remains visible alongside intent.

POST returns HTTP 202, `schema: plimsoll-project-intent-ack/v1`, `acknowledged`, `receiptId`, `sessionId`, current `revision`, original `receiptRevision`, `replayed`, and `project` (state/key/company/revision/reasons). Exact replay is idempotent even after later revisions; conflicting replay, stale session/registry revisions, invalid lineage and unsupported adapters receive a refusal without acknowledgment. Refetch registry choices and preserve the failed local evidence.

The server rechecks tenant, active install, credential, actor binding and project access under locks. Tenant registration in `expense_project_companies` is the business-project authority; client work and overhead use its existing keys, with `work_project_maps` supplying the existing project registry names. No slug, company-label hash or repo-derived guess is automatically registered. Company comes from the project's effective-dated `company_history`; empty history preserves its existing undated company, while a dated gap stays Unknown. Nothing comes from a payer account. `resolveSessionProjectIntentAt` is P09/P10's read hook inside their transaction snapshot: latest owner parts in `session_project_assignments` win on the exact source/session/UTC-month/model unit, including splits. P02 performs no usage allocation.

**P03 / P04:** Save a durable local pending launch before the first turn (`localState: awaiting_native_binding`, `receiptDraft`, and null local session/native keys), then bind the actual ledger UUID and native key before emitting the strict outbound receipt. Preserve root/attempt lineage, queue exact receipts offline, and prune only after ACK. P03 uses routed_launch. P04 resolves a registry choice once, uses hand_start or trusted_folder_default, and records an explicit null choice when unbound; never reuse a global last project. Native hook qualification and queue implementation belong to those lanes.

**Local only:** Prompts, replies, code, commands, file names, cwd/source-root paths, raw native/account/work IDs, emails, environment values, credential values, and pending-launch state. Use existing linkage functions; raw hook/OTLP envelopes never enter this API.

**Versioning:** Unknown schema versions, fields and adapter versions fail closed. A wire field or semantic change requires a new major schema and coordinated fixtures/server/producer support; producer implementation changes retain v1 only when its semantics remain identical. Schema v1 and adapter 1.0.0 are protocol contracts, not claims that P03/P04 native adapters are deployed.
