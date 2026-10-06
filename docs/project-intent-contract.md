# P02: canonical project intent v1

**Launch summary for P03/P04.** Copy a registered business `projectKey` and its `projectRegistryRevision` from authenticated GET; send intent separately from `observedRepoKey`. Persist a pending local launch before the first turn, bind the existing collector/cloud session UUID and actual native incarnation, then queue an immutable receipt. Start `expectedRevision` at 0; prune only after a matching durable ACK. P03 uses `routed_launch`; P04 uses `hand_start` or records an unconfirmed `trusted_folder_default`. An unbound choice is explicit null; never reuse a global last project. Native hooks and queue implementation belong to P03/P04.

**API and callers.** `GET /api/work-intelligence/project-intents?tenantId=<uuid>&after=<optional canonical key>` returns at most 200 choices and a nullable cursor. Only an active registered install of that tenant may call it. It lists only that tenant's registered business projects from `expense_project_companies`, using names from `work_project_maps`; non-repo work, client work and overhead use the existing canonical keys. Owner-facing P12 pickers use owner-session routes, not this install API. `POST /api/work-intelligence/project-intents` accepts exactly `{tenantId, expectedRevision, receipt}`. Both methods use the existing install-signature authentication and send `Cache-Control: private, no-store`, `CDN-Cache-Control: no-store`, and `Vercel-CDN-Cache-Control: no-store`, including errors. Bootstrap/master-key-only requests are refused.

Send `x-plimsoll-install-key`, `x-plimsoll-upload-timestamp` (ISO UTC), and `x-plimsoll-upload-signature: sha256=<lowercase hex>`. Sign HMAC-SHA256 over UTF-8 `timestamp + "." + rawBody` using the existing local signing credential. GET signs an empty body; POST signs the exact JSON bytes with `Content-Type: application/json`. The existing five-minute signature policy applies. Credentials never enter source code or receipts. The server rechecks tenant, active install, credential, actor binding and session/project access under locks.

**Minimum outbound receipt.** Every field is mandatory; nullable fields use JSON null. UUIDs are lowercase, hashes are `sha256:` plus 64 lowercase hex digits, timestamps are UTC `YYYY-MM-DDTHH:mm:ss.sssZ`. Unknown fields fail closed. Canonicalize before saving/hashing a receipt, rather than relying on server transformations.

| Exact fields | Type / rule |
|---|---|
| `schema` | `plimsoll-project-intent/v1` |
| `receiptId`, `installId`, `sessionId` | UUIDs: unique receipt, authenticated ledger installation, existing collector/cloud session join |
| `source` | `codex`, `claude_code`, `gemini_cli`, `grok`; this cloud base refuses Gemini with 422 `unsupported_source` |
| `sourceRootKey`, `nativeSessionKey`, `sessionEpochKey` | Required full SHA-256 linkage digests; derivation below |
| `accountKey`, `workItemKey` | Full linkage digest or null; derivation below |
| `rootAttemptId`, `attemptId`, `parentAttemptId` | UUIDs; first attempt equals root with null parent; later attempts reference a persisted predecessor |
| `projectKey`, `projectRegistryRevision` | Canonical registered key plus positive registry revision, or both null |
| `observedRepoKey` | Existing `remoteLinkageHash`, or null; never substituted for the served project |
| `effectiveFrom`, `effectiveUntil` | UTC instants; until nullable; half-open interval `[from, until)` |
| `basis` | `routed_launch`, `hand_start`, `trusted_folder_default`, `repo_observation` |
| `evidenceRef` | Full digest of canonical receipt facts, indexed to local evidence; derivation below |
| `adapterId`, `adapterVersion` | `routed-launch`, `hand-start`, `repo-observation` respectively; defaults also use `hand-start`; version `1.0.0` |

**Authority and privacy.** Shape is not registration: unknown hash-shaped keys remain Unknown, even after later registration until a fresh declaration is admitted. Never promote `project:<slug>`, a company-label hash or a repo guess. GET keys are server choices, not locally derived project names. A repo may serve two projects and a project may span two repos. Company follows the project's effective-dated `company_history`, never payer/account/first repo; empty history preserves the existing undated mapping, while a dated gap is Unknown. Explicit intent, observed facts and owner corrections stay distinct. Latest owner parts in `session_project_assignments` win on source/session/UTC-month/model, including splits; `resolveSessionProjectIntentAt` is the downstream read hook. Receipts append on the existing `ai_work_sessions` row, with no second ledger. Raw usage, owner assignments and acceptance receipts are unchanged. History is bounded to 128 receipts; new declarations cut over open intervals at read time without rewriting prior receipts or reviving expired choices. Observations cannot replace declarations; folder defaults remain unconfirmed suggestions.

**Local only.** Prompts, replies, code, commands, file names, cwd/source-root paths, raw native/account/work IDs, emails, environment/credential values, incarnation nonces, binding state, pending launches and raw hook/OTLP envelopes stay on the machine. Required schema, privacy and erasure inventories apply unchanged. Hashes permit linkage; they do not prove raw evidence or confer authority. The server validates their shape and access but cannot independently verify private inputs that never leave the machine.

## Normative producer derivation

For the six fields below use `D(kind, parts)`:

```text
payload = "plimsoll:project-intent:v1:" + kind + U+0000 + JSON.stringify(parts)
D = "sha256:" + lowercase_hex(SHA256(UTF8(payload)))
```

`parts` is an ordered JSON array of the specified strings, integers and nulls, with no whitespace. Use ECMAScript JSON string escaping (including control characters), literal Unicode and UTF-8 without a BOM; reject unpaired surrogates. Ordinary identity strings use Unicode NFC, retain case and do not trim unless their authority's canonical ID specification requires it. UUID identity strings use lowercase canonical UUIDs. Never concatenate ambiguous identity components without the array or omit the domain prefix. The collector's existing `linkageHash(payload)` computes this same full digest. Existing repo/project linkage domains remain unchanged.

| Field / kind | Ordered inputs and normalization |
|---|---|
| `sourceRootKey` / `source-root` | `[installId, source, canonicalRootPath]`. Resolve the provider's actual state root (for example `CODEX_HOME`), using native `realpath` on the configured absolute directory. Remove trailing separators except the filesystem root; NFC; preserve component case. On Windows only, strip the extended-path prefix, replace separators with `/`, and lowercase a drive letter; retain UNC/component case. Use the provider state root, not cwd or the first repo. If unavailable, keep the launch pending instead of inventing a root. |
| `accountKey` / `account` | `[source, providerRealm, providerPrincipalId]`. Realm is the lowercase authority `openai`, `anthropic`, `google` or `xai` matching source. Principal is the provider's authoritative stable opaque ID, NFC/case-preserving or lowercase UUID. No email, token, payer or folder substitute; if no such ID is available, send null. Install ID is deliberately absent so the same principal links across installs. |
| `nativeSessionKey` / `native-session` | `[installId, source, nativeSessionId]`. Use the actual provider ID, NFC/case-preserving or lowercase UUID, never a file name. Scope by ledger installation and source; omit account/root so a proved resume can rotate either. |
| `sessionEpochKey` / `session-epoch` | `[installId, source, nativeSessionKey, incarnationId]`. Incarnation ID is a lowercase random UUIDv4 persisted once per proved native lifetime in the existing local session binding. Rules below define when it changes. |
| `workItemKey` / `work-item` | `[authority, namespace, canonicalWorkItemId]`. Authority is the tracker's lowercase identifier, e.g. `beads`; namespace is its stable program/store identity, e.g. `eco-6hoxj`, not a local path. Use the tracker's canonical ID, NFC/case-preserving (UUID lower), never title/description. If authority, namespace or ID is unavailable, send null. This does not select a business project. |
| `evidenceRef` / `evidence` | Ordered canonical receipt values in the list below. Include nulls and integer registry revisions as JSON values, not strings. No raw content or local identifiers are hash inputs here. Persist local qualification evidence under this reference; do not upload it. |

The exact evidence array field order is:

```text
receiptId, installId, source, sourceRootKey, accountKey, sessionId,
nativeSessionKey, sessionEpochKey, rootAttemptId, attemptId, parentAttemptId,
workItemKey, projectKey, projectRegistryRevision, observedRepoKey,
effectiveFrom, effectiveUntil, basis, adapterId, adapterVersion
```

`schema` is identified by the domain prefix; `evidenceRef` is excluded to avoid a circular hash. Produce the digest only after every other value has its canonical wire representation. A changed receipt requires a new reference; transport retries and a changed `expectedRevision` do not.

### Incarnations and attempts

Before any native ID is available, persist `localState: awaiting_native_binding` and a `receiptDraft`; session/native/epoch fields remain null locally and the strict outbound receipt is not yet emitted. Bind to the actual existing collector/cloud join UUID; do not create a parallel P02 session identity. Coordinate the incarnation nonce and join UUID with that existing binding, including after restarts.

Resume, process restart, retry, offline queue replay and a proved continuation after account/root rotation retain `sessionId`, native key, epoch and root attempt. Account/root changes (including a null-to-known account transition) create a fresh `attemptId` with the previous attempt as parent; first attempt equals `rootAttemptId` with null parent. A new provider native lifetime, a reused native ID, replacement ledger installation or changed source creates a new incarnation nonce, epoch, existing-ledger join UUID and root attempt. A reused ID may keep the scoped native digest but cannot reuse the old epoch/join. When continuity cannot be proved or durable binding is lost, keep the launch pending for fresh native binding; never reconstruct the old epoch from the ID alone. Removing receipt queue files does not change an incarnation.

### Worked examples (synthetic, shared golden fixtures)

Each example gives the complete normalized array and expected digest. `installId` is `24000000-0000-4000-8000-000000000002`; the native UUID is `24000000-0000-4000-8000-000000000003`. The full base receipt and all six edge cases live in both copies of `project-intent-v1.json`. The synthetic business key is seeded in the fixture registry using its existing server scheme; a producer copies it from GET.

**`sourceRootKey`**, domain kind `source-root`:

```json
{
  "normalizedParts": [
    "24000000-0000-4000-8000-000000000002",
    "codex",
    "/Users/fixture/.codex"
  ],
  "digest": "sha256:8c46a54821f957c60b472658ee0de09b286556293c76150210dc36ce6c971aaa"
}
```

**`accountKey`**, domain kind `account`:

```json
{
  "normalizedParts": [
    "codex",
    "openai",
    "acct_p02_demo"
  ],
  "digest": "sha256:0f2a83d7322bce30f199a5852f6bae85303fa593b1ab0a19be028d7f8ffc9727"
}
```

**`nativeSessionKey`**, domain kind `native-session`:

```json
{
  "normalizedParts": [
    "24000000-0000-4000-8000-000000000002",
    "codex",
    "24000000-0000-4000-8000-000000000003"
  ],
  "digest": "sha256:864cfc49b31ee0de16068def156dd810d454e47860ebeee7afba34ea631b1614"
}
```

**`sessionEpochKey`**, domain kind `session-epoch`:

```json
{
  "normalizedParts": [
    "24000000-0000-4000-8000-000000000002",
    "codex",
    "sha256:864cfc49b31ee0de16068def156dd810d454e47860ebeee7afba34ea631b1614",
    "24000000-0000-4000-8000-000000000020"
  ],
  "digest": "sha256:e9d1c39a2c629156d42e1ff4e96d6f0f505250168eaf107fe88e625ffe58cd32"
}
```

**`workItemKey`**, domain kind `work-item`:

```json
{
  "normalizedParts": [
    "beads",
    "eco-6hoxj",
    "eco-6hoxj.165.240"
  ],
  "digest": "sha256:01ecdfe26b61389d19089f66be79b84d7f9ce1a533f3923a931af5e08da75adb"
}
```

**`evidenceRef`**, domain kind `evidence`:

```json
{
  "normalizedParts": [
    "24000000-0000-4000-8000-000000000001",
    "24000000-0000-4000-8000-000000000002",
    "codex",
    "sha256:8c46a54821f957c60b472658ee0de09b286556293c76150210dc36ce6c971aaa",
    "sha256:0f2a83d7322bce30f199a5852f6bae85303fa593b1ab0a19be028d7f8ffc9727",
    "24000000-0000-4000-8000-000000000003",
    "sha256:864cfc49b31ee0de16068def156dd810d454e47860ebeee7afba34ea631b1614",
    "sha256:e9d1c39a2c629156d42e1ff4e96d6f0f505250168eaf107fe88e625ffe58cd32",
    "24000000-0000-4000-8000-000000000004",
    "24000000-0000-4000-8000-000000000004",
    null,
    "sha256:01ecdfe26b61389d19089f66be79b84d7f9ce1a533f3923a931af5e08da75adb",
    "sha256:3e867b783a8b606a0725d6879a683ce851c2ffb3f4bdde1c0e795a7c0fc74c4a",
    1,
    "sha256:9a421f8aa3ad32da7bb79bcb71208922932035b00400202cf95d44bbaaf6a508",
    "2026-10-06T18:00:00.000Z",
    null,
    "routed_launch",
    "routed-launch",
    "1.0.0"
  ],
  "digest": "sha256:e4d8f66dd84dd892f10ce0079bfb2a552d77c931f1cd8b2916b3cbae85ffaf0f"
}
```

## Exact responses and durable recovery

GET 200 has exactly this shape; `nextCursor` is a canonical project key when another page exists, otherwise null. `projectRegistryRevision` here becomes the receipt's field of the same name. It differs from the ACK project's `registryRevision`.

```json
{
  "schema": "plimsoll-project-intent-projects/v1",
  "projects": [
    {
      "projectKey": "sha256:3e867b783a8b606a0725d6879a683ce851c2ffb3f4bdde1c0e795a7c0fc74c4a",
      "projectLabel": "Client work",
      "projectRegistryRevision": 1
    }
  ],
  "nextCursor": null
}
```

POST 202 ACK for the base receipt:

```json
{
  "schema": "plimsoll-project-intent-ack/v1",
  "acknowledged": true,
  "receiptId": "24000000-0000-4000-8000-000000000001",
  "sessionId": "24000000-0000-4000-8000-000000000003",
  "replayed": false,
  "revision": 1,
  "receiptRevision": 1,
  "project": {
    "state": "known",
    "projectKey": "sha256:3e867b783a8b606a0725d6879a683ce851c2ffb3f4bdde1c0e795a7c0fc74c4a",
    "company": "Client company",
    "registryRevision": 1,
    "reason": null,
    "companyReason": null
  }
}
```

The ACK's `revision` is the current session intent revision; `receiptRevision` is this receipt's original admitted revision. Exact replays return `replayed: true` without another write, and may return a newer current revision. `project` always has exactly `state`, `projectKey`, `company`, `registryRevision`, `reason`, `companyReason`. Missing choices return `state: "unknown"` and null key/company/registry revision, with both reasons `needs_project`; unregistered choices use `project_not_registered`. A known key with a dated company gap remains `state: "known"`, keeps its key/registry revision, has null company/reason and `companyReason: "company_mapping_missing"` (malformed stored mapping: `company_mapping_invalid`). ACK admits intent; it is not an owner approval.

POST 409 for a stale session revision, after install/session authorization and locking:

```json
{
  "error": "intent_revision_stale",
  "receiptId": "24000000-0000-4000-8000-000000000005",
  "sessionId": "24000000-0000-4000-8000-000000000003",
  "revision": 1
}
```

POST 422 refusal (no ACK):

```json
{
  "error": "unsupported_adapter"
}
```

Other refusal bodies contain only `{error: <stable code>}`. For example 409 `{error: "project_registry_stale"}`, 403 `{error: "install_tenant_mismatch"}` or `{error: "device_revoked"}`, 400 `{error: "invalid_project_intent"}`, and 422 `{error: "unsupported_source"}`. They disclose no session revision. History, identity, lineage, time or interval conflicts are 409 error-only; transient unavailability is 503 error-only. None acknowledges delivery.

1. **Create and queue.** Start the session's durable observed revision at `0`. Persist native/epoch/attempt bindings and the complete canonical receipt with its unique `receiptId` before transport. Serialize outstanding sends for that join UUID/incarnation. Persist the POST envelope's `expectedRevision` separately from immutable receipt facts.
2. **ACK.** Require HTTP 202, the exact ACK schema, `acknowledged: true`, matching receipt/session IDs, and integer `revision >= receiptRevision >= 1`. Atomically save the ACK, mark that receipt delivered, and set durable observed revision to `max(previous, ack.revision)`. Only then prune the pending queue. A late replay ACK cannot regress a newer revision. Unknown project resolution still acknowledges the receipt; retain a local review item.
3. **Ambiguous delivery.** On a timeout or transport failure, retain and replay the identical canonical receipt/ID using a fresh timestamp/signature. Do not guess that it was accepted, advance a revision, relaunch, generate another receipt or change effective time. Server idempotence permits a successful replay even if the envelope revision is old.
4. **Stale recovery.** Require 409 `intent_revision_stale`, matching pending receipt/session IDs and a nonnegative integer `revision`. Persist the observed revision with `max(previous, response.revision)`; this is not an ACK, so keep the receipt queued. Change only the envelope's `expectedRevision` to the observed revision; send the identical receipt using a fresh transport signature, within the existing bounded retry policy. A later concurrent write can cause another stale refusal. Identity/time/lineage conflicts and exhausted retries remain queued for review; never rewrite receipt facts or loop without a bound. The returned number is current at the locked check, not a reservation.
5. **Registry stale/refusal.** `project_registry_stale` carries no session revision. Refetch signed GET, preserve the refused local evidence and obtain renewed authority for the choice. If changing registry revision or choice, create a new declaration/receipt ID and evidence reference; never overwrite an accepted or ambiguous receipt. Unsupported versions/adapters require a compatible adapter; malformed/auth/identity refusals require fixing their actual cause. Do not fall back to repo, payer, Unknown or a global last project to obtain an ACK.

**Versioning.** Unknown schema versions, fields and adapter versions fail closed. A wire-field or semantic change requires a new major receipt schema and coordinated server/producer/fixture support; the digest prefix includes that major. Implementation changes retain v1 only when semantics remain identical. This round completes the previously unspecified v1 producer derivation before P03/P04 qualification; it does not deploy those adapters. Accepted adapter `1.0.0` is a protocol contract. This stale-recovery body adds authorized metadata to a refusal without changing immutable receipt semantics.
