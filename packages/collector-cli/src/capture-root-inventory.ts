import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { z } from "zod";
import { accountAssertionV1Schema, type AccountAssertionV1 } from "./account-assertion";
const id=z.string().regex(/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/);
const legacyAccountSchema=z.object({
  actorHash: z.string().regex(/^sha256:[a-f0-9]{64}$/),
  validFrom: z.iso.datetime(),validUntil: z.iso.datetime().nullable(),evidenceRef: id
}).strict();
export const captureRootSchema=z.object({
  rootId: id,profileId: id,installationEpochId: id,
  source: z.enum(["codex","claude_code"]),directory: z.string().min(1),
  /** Explicit enrollment attestation; no search of neighboring auth stores. */
  dispatch: z.array(z.object({
    sessionId: id,workItemId: id,projectKey: z.string().regex(/^sha256:[a-f0-9]{64}$/),
    companyRef: id.nullable(),attemptId: id,parentAttemptId: id.nullable(),acceptedOutcomeId: id.nullable(),
    validFrom: z.iso.datetime(),validUntil: z.iso.datetime().nullable(),evidenceRef: id,
  }).strict()).max(1000).optional(),
  /** Legacy account rows remain accepted; new enrollments use the additive V1 contract. */
  account: z.union([legacyAccountSchema,accountAssertionV1Schema]).optional(),
  /** Immutable historical account windows hydrated from the maintenance key. */
  accountAssertions: z.array(accountAssertionV1Schema).max(1024).optional(),
}).strict();
export type CaptureRoot=z.infer<typeof captureRootSchema>;
export type CaptureRootAccount=NonNullable<CaptureRoot["account"]>;
export { accountAssertionV1Schema };
export type { AccountAssertionV1 };
export type CaptureRootCoverage={
  rootId: string;
  profileId: string;
  installationEpochId: string;
  source: CaptureRoot["source"];
  state: "ready"|"missing"|"unreadable"|"unsafe"|"partial";
  observedAt: string;
  reason: string|null;
  pathDigest: string;
};
export function validateCaptureRoots(input: unknown): CaptureRoot[] {
  const roots=z.array(captureRootSchema).max(64).parse(input);
  const ids=new Set<string>();
  for(const root of roots) {
    if(!path.isAbsolute(root.directory))
      throw new Error("capture_root_requires_absolute_path");
    root.directory=path.resolve(root.directory);
    if(ids.has(root.rootId))
      throw new Error("capture_root_duplicate_id");
    ids.add(root.rootId);
    if (root.account && "schema" in root.account && root.account.schema === "account-assertion/v1" &&
        root.account.source !== root.source)
      throw new Error("capture_account_source_mismatch");
    if(root.account?.validUntil&&Date.parse(root.account.validUntil)<=Date.parse(root.account.validFrom))
      throw new Error("capture_identity_window_invalid");
    const assertions = root.accountAssertions ?? [];
    for (const assertion of assertions) {
      if (assertion.source !== root.source || (assertion.validUntil !== null && Date.parse(assertion.validUntil) <= Date.parse(assertion.validFrom)))
        throw new Error("capture_identity_window_invalid");
    }
    for (let i = 1; i < assertions.length; i += 1) {
      if (Date.parse(assertions[i - 1].validFrom) >= Date.parse(assertions[i].validFrom))
        throw new Error("capture_identity_window_invalid");
    }
    for(const binding of root.dispatch??[]) {
      if(binding.validUntil&&Date.parse(binding.validUntil)<=Date.parse(binding.validFrom)) {
        throw new Error("capture_dispatch_window_invalid");
      }
    }
  }
  for(let i=0;i<roots.length;i++)
    for(let j=i+1;j<roots.length;j++) {
      const a=roots[i].directory,b=roots[j].directory;
      if(a===b||a.startsWith(b+path.sep)||b.startsWith(a+path.sep))
        throw new Error("capture_roots_overlap");
    }
  return roots;
}
export function captureRootDigest(root: CaptureRoot): string {
  return crypto.createHash("sha256").update(JSON.stringify([root.source,root.rootId,root.profileId,root.installationEpochId,root.directory])).digest("hex");
}
export function inspectCaptureRoots(roots: readonly CaptureRoot[],now=new Date()): CaptureRootCoverage[] {
  return roots.map(root => {
    let state: CaptureRootCoverage["state"]="ready",reason: string|null=null;
    try {
      const stat=fs.lstatSync(root.directory);
      if(stat.isSymbolicLink()||!stat.isDirectory()||fs.realpathSync(root.directory)!==root.directory) {
        state="unsafe";
        reason="capture_root_not_physical_directory";
      }
      else
        fs.accessSync(root.directory,fs.constants.R_OK|fs.constants.X_OK);
    }
    catch(error) {
      state=(error as NodeJS.ErrnoException).code==="ENOENT"? "missing":"unreadable";
      reason=`capture_root_${state}`;
    }
    return {
      rootId: root.rootId,profileId: root.profileId,installationEpochId: root.installationEpochId,source: root.source,state,
      observedAt: now.toISOString(),reason,pathDigest: captureRootDigest(root)
    };
  });
}
export function rootForFile(roots: readonly CaptureRoot[],file: string): CaptureRoot|undefined {
  return roots.find(root => file.startsWith(root.directory+path.sep));
}
export function rootCursorKey(roots: readonly CaptureRoot[],file: string): string {
  const root=rootForFile(roots,file);
  return root? `${file}\u0000${captureRootDigest(root)}`:file;
}
export function rootEventMetadata(root: CaptureRoot|undefined,sourceEventId: string,observedAt: string,sessionId?: string,
  accountAttributionEnabled=true): Record<string, unknown> {
  if(!root)
    return {};
  const at=Date.parse(observedAt);
  const accountCandidates = accountAttributionEnabled ? [...new Map(
    [ ...(root.accountAssertions ?? []), ...(root.account ? [root.account] : []) ]
      .map(account => [`${account.validFrom}\u0000${account.evidenceRef}`, account] as const),
  ).values()] : [];
  const matchingAccounts = accountCandidates.filter(account => at>=Date.parse(account.validFrom)&&(!account.validUntil||at<Date.parse(account.validUntil)));
  const account = matchingAccounts.length === 1 ? matchingAccounts[0] : null;
  const bindings=(root.dispatch??[]).filter(binding => binding.sessionId===sessionId&&at>=Date.parse(binding.validFrom)&&(!binding.validUntil||at<Date.parse(binding.validUntil)));
  const binding=bindings.length===1? bindings[0]:null;
  return {
    ...(binding? {
      workItemId: binding.workItemId,dispatchProjectKey: binding.projectKey,
      workEvidenceRef: binding.evidenceRef,attemptId: binding.attemptId,
      ...(binding.parentAttemptId? { parentAttemptId: binding.parentAttemptId }:{}),
      ...(binding.companyRef? { companyRef: binding.companyRef }:{}),
      ...(binding.acceptedOutcomeId? { acceptedOutcomeId: binding.acceptedOutcomeId }:{}),
    }:{}),...(bindings.length>1? { workAttributionState: "conflict" }:{}),
    captureRootId: root.rootId,captureProfileId: root.profileId,installationEpochId: root.installationEpochId,
    logicalSourceEventId: sourceEventId,sourceIdentityEvidenceRef: "native_runtime_event_v1",
    ...(account ? { captureAccountHash: account.actorHash,accountEvidenceRef: account.evidenceRef } : {}),
    ...(matchingAccounts.length > 1 ? { accountAttributionState: "conflict" } : {})
  };
}
/** Reopen the existing provider baseline at its original cutoff when the inventory changes.
 * Old generation exclusions and cursors stay intact. No raw event is changed or removed. */
export function bindCaptureInventory(database: import("better-sqlite3").Database,source: CaptureRoot["source"],roots: CaptureRoot[],coverage?: CaptureRootCoverage[]) {
  if(!roots.length)
    return false;
  ensureRootObservationSchema(database);
  const inventoryDigest=crypto.createHash("sha256").update(JSON.stringify([
    roots.map(captureRootDigest).sort(),coverage?.filter(row => row.state==="ready").map(row => row.rootId).sort()??null,
  ])).digest("hex");
  database.exec(`create table if not exists capture_root_inventory_bindings (
    source text primary key, inventory_digest text not null, updated_at text not null)`);
  const prior=database.prepare("select inventory_digest as digest from capture_root_inventory_bindings where source=?").get(source) as {
    digest: string;
  }|undefined;
  if(prior?.digest===inventoryDigest)
    return false;
  database.transaction(() => {
    if(database.prepare("select 1 from sqlite_master where type='table' and name='automatic_capture_baseline_state'").get()) {
      database.prepare(`update automatic_capture_baseline_state set status='in_progress',completed_at=null
        where source=? and status='complete'`).run(source);
    }
    database.prepare(`insert into capture_root_inventory_bindings(source,inventory_digest,updated_at) values(?,?,?)
      on conflict(source) do update set inventory_digest=excluded.inventory_digest,updated_at=excluded.updated_at`)
      .run(source,inventoryDigest,new Date().toISOString());
  }).immediate();
  return true;
}
const initializedObservationDatabases=new WeakSet<object>();
function ensureRootObservationSchema(database: import("better-sqlite3").Database) {
  if(initializedObservationDatabases.has(database))
    return;
  database.exec(`create table if not exists capture_root_observations (
    root_digest text not null,event_id text not null,payload_digest text not null,
    observed_at text not null,state text not null check(state in ('admitted','duplicate','conflict')),
    primary key(root_digest,event_id));
    create index if not exists idx_capture_root_event on capture_root_observations(event_id)`);
  // A rolled-back transaction must not leave a cached schema assertion.
  if(!database.inTransaction)
    initializedObservationDatabases.add(database);
}
/** Root sightings live beside immutable events; replay/failover never changes the first receipt. */
export function appendRootObservation(buffer: import("./buffer").LocalEventBuffer,event: import("../../shared/src/schemas").AiInteractionEvent,root: CaptureRoot|undefined): boolean {
  if (buffer.eventAdmissionReason(event.observedAt, root?.installationEpochId ?? event.metadata?.installationEpochId))
    return false;
  if(!root)
    return buffer.append(event,[]);
  const database=buffer.database;
  ensureRootObservationSchema(database);
  const nativeSignature=(value: typeof event) => crypto.createHash("sha256").update(JSON.stringify([
    value.source,value.id,value.sessionId,value.observedAt,value.model,value.inputTokens??null,value.outputTokens??null,
    value.cacheReadTokens??null,value.cacheCreationTokens??null,value.costUsd??null,
  ])).digest("hex");
  const payloadDigest=nativeSignature(event),rootDigest=captureRootDigest(root);
  const existing=database.prepare("select payload_json as payload from buffered_events where id=?").get(event.id) as {
    payload: string;
  }|undefined;
  const priorSightings=database.prepare(`select payload_digest as digest,state
    from capture_root_observations where event_id=?`).all(event.id) as Array<{
    digest: string;
    state: string;
  }>;
  // Compact source receipts survive raw retention so an epoch change cannot
  // revive already observed consumption. They contain no paths or payloads.
  const priorConflict=priorSightings.some(row => row.digest!==payloadDigest||row.state==="conflict");
  const alreadyObserved=Boolean(existing)||priorSightings.length>0;
  let same=false;
  if(existing) {
    try {
      same=nativeSignature(JSON.parse(existing.payload))===payloadDigest;
    }
    catch { /* corruption remains a conflict */ }
  }
  else
    same=priorSightings.length>0&&!priorConflict;
  const inserted=alreadyObserved? false:buffer.append(event,[]);
  // Another connection may have enrolled between the first check and append.
  if (!alreadyObserved && !inserted && buffer.eventAdmissionReason(event.observedAt, root?.installationEpochId))
    return false;
  const state=priorConflict? "conflict":alreadyObserved? (same? "duplicate":"conflict"):inserted? "admitted":"conflict";
  database.prepare(`insert into capture_root_observations values(?,?,?,?,?)
    on conflict(root_digest,event_id) do update set state=case when payload_digest<>excluded.payload_digest then 'conflict' else state end`)
    .run(rootDigest,event.id,payloadDigest,event.observedAt,state);
  if(state==="conflict")
    throw new Error("capture_logical_source_conflict");
  return inserted;
}
