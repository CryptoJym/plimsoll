import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { parse as parseToml } from "smol-toml";
import { z } from "zod";
import { accountAssertionContains, accountAssertionV1Schema, type AccountAssertionV1 } from "./account-assertion";
import type { CaptureBaselineFileObservation } from "./capture-baseline";
import { resolveCollectorHome } from "./collector-home";
import { workClassSchema, workComplexityBandSchema } from "../../shared/src/schemas";
const id=z.string().regex(/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/);
export const namespacedWorkItemIdSchema=z.string().max(256).regex(
  /^(?:beads:[A-Za-z0-9][A-Za-z0-9._:-]{0,127}|github:(?:sha256:[a-f0-9]{64}|[A-Za-z0-9._-]+(?:\/[A-Za-z0-9._-]+)?)\/pull\/[1-9][0-9]*|jira:[A-Za-z0-9][A-Za-z0-9._:-]{0,127})$/,
).refine(value => !value.startsWith("github:") ||
  !value.slice(7,value.lastIndexOf("/pull/")).split("/").some(segment => segment === "." || segment === ".."));
const legacyAccountSchema=z.object({
  actorHash: z.string().regex(/^sha256:[a-f0-9]{64}$/),
  validFrom: z.iso.datetime(),validUntil: z.iso.datetime().nullable(),evidenceRef: id
}).strict();
const accountAssertionEpochSchema=z.object({
  actorHash: z.string().regex(/^sha256:[a-f0-9]{64}$/),
  validFrom: z.iso.datetime(),
  evidenceRef: z.string().regex(/^sha256:[a-f0-9]{64}$/),
  installationEpochId: z.string().uuid(),
}).strict();
export const dispatchBindingSchema=z.object({
  sessionId: id,workItemId: z.union([id,namespacedWorkItemIdSchema]),projectKey: z.string().regex(/^sha256:[a-f0-9]{64}$/),
  companyRef: id.nullable(),attemptId: id,parentAttemptId: id.nullable(),acceptedOutcomeId: id.nullable(),
  validFrom: z.iso.datetime(),validUntil: z.iso.datetime().nullable(),evidenceRef: id,
  role: z.enum(["author","reviewer","lead"]).optional(),
  workClass: workClassSchema.optional(),complexityBand: workComplexityBandSchema.optional(),
  techniqueId: id.optional(),techniqueVersion: id.optional(),assignmentId: id.optional(),
  arm: z.enum(["control","treatment"]).optional(),launchedBy: id.optional(),
}).strict();
export const captureRootSchema=z.object({
  rootId: id,profileId: id,installationEpochId: id,
  source: z.enum(["codex","claude_code"]),directory: z.string().min(1),
  /** Explicit enrollment attestation; no search of neighboring auth stores. */
  dispatch: z.array(dispatchBindingSchema).max(1000).optional(),
  /** Legacy account rows remain accepted; new enrollments use the additive V1 contract. */
  account: z.union([legacyAccountSchema,accountAssertionV1Schema]).optional(),
  /** Immutable historical account windows hydrated from the maintenance key. */
  accountAssertions: z.array(accountAssertionV1Schema).max(1024).optional(),
  /** Hash-only mapping from each assertion window to its source-binding epoch. */
  accountAssertionEpochs: z.array(accountAssertionEpochSchema).max(1024).optional(),
}).strict();
export type CaptureRoot=z.infer<typeof captureRootSchema>;
export type DispatchBinding=NonNullable<CaptureRoot["dispatch"]>[number];
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
      const previous = assertions[i - 1];
      if (Date.parse(previous.validFrom) >= Date.parse(assertions[i].validFrom) ||
          previous.validUntil === null ||
          Date.parse(previous.validUntil) > Date.parse(assertions[i].validFrom))
        throw new Error("capture_identity_window_invalid");
    }
    const epochKeys = new Set<string>();
    for (const epoch of root.accountAssertionEpochs ?? []) {
      const key = `${epoch.actorHash}\u0000${epoch.validFrom}\u0000${epoch.evidenceRef}`;
      if (epochKeys.has(key) || !assertions.some(assertion => assertion.actorHash === epoch.actorHash &&
          assertion.validFrom === epoch.validFrom && assertion.evidenceRef === epoch.evidenceRef)) {
        throw new Error("capture_identity_epoch_invalid");
      }
      epochKeys.add(key);
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
type IndexedBinding = {
  root: CaptureRoot;
  binding: DispatchBinding;
  from: number;
  until: number;
  signature: string;
};
export type DispatchBindingSnapshot = {
  roots: readonly CaptureRoot[];
  byRootId: ReadonlyMap<string, CaptureRoot>;
  bySession: ReadonlyMap<string, readonly IndexedBinding[]>;
  rootDigests: ReadonlyMap<string,string>;
  claudeRootDigests: ReadonlySet<string>;
};
const dispatchIndexes = new WeakMap<readonly CaptureRoot[], DispatchBindingSnapshot>();
function dispatchIndex(roots: readonly CaptureRoot[]): DispatchBindingSnapshot {
  const cached = dispatchIndexes.get(roots);
  if (cached) return cached;
  const byRootId = new Map<string, CaptureRoot>();
  const bySession = new Map<string, IndexedBinding[]>();
  const rootDigests = new Map<string,string>();
  const claudeRootDigests = new Set<string>();
  for (const root of roots) {
    byRootId.set(root.rootId, root);
    const digest=captureRootDigest(root);
    rootDigests.set(root.rootId,digest);
    if(root.source==="claude_code") claudeRootDigests.add(digest);
    for (const binding of root.dispatch ?? []) {
      const key = `${root.source}\0${binding.sessionId}`;
      const entries = bySession.get(key) ?? [];
      entries.push({ root, binding, from: Date.parse(binding.validFrom),
        until: binding.validUntil ? Date.parse(binding.validUntil) : Infinity,
        signature: JSON.stringify(binding) });
      bySession.set(key, entries);
    }
  }
  const snapshot = { roots, byRootId, bySession, rootDigests, claudeRootDigests };
  dispatchIndexes.set(roots, snapshot);
  return snapshot;
}

/** The CLI publishes config atomically; the daemon observes its new inode without a restart. */
let dispatchConfigCache: { file: string; stamp: string; roots: CaptureRoot[] } | null = null;
let seenConfigFile: string | null = null;
const seenClaudeSessionRoots = new Map<string, Set<string>>();
export function currentDispatchCaptureRoots(): CaptureRoot[] {
  const file=path.join(resolveCollectorHome().home,"collector.config.json");
  if (seenConfigFile !== file) {
    seenConfigFile = file;
    seenClaudeSessionRoots.clear();
  }
  try {
    const stat=fs.statSync(file);
    const stamp=`${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeMs}:${stat.ctimeMs}`;
    if(dispatchConfigCache?.file===file&&dispatchConfigCache.stamp===stamp)
      return dispatchConfigCache.roots;
    const parsed=JSON.parse(fs.readFileSync(file,"utf8")) as { captureRoots?: unknown };
    const roots=validateCaptureRoots(parsed.captureRoots??[]);
    dispatchIndex(roots);
    dispatchConfigCache={file,stamp,roots};
    return roots;
  } catch {
    dispatchConfigCache=null;
    return [];
  }
}
export function currentDispatchBindingSnapshot(): DispatchBindingSnapshot {
  return dispatchIndex(currentDispatchCaptureRoots());
}
export function currentDispatchRoot(root: CaptureRoot,snapshot=currentDispatchBindingSnapshot()): CaptureRoot {
  const candidate=snapshot.byRootId.get(root.rootId);
  const configured=candidate&&candidate.source===root.source&&candidate.profileId===root.profileId&&
    candidate.installationEpochId===root.installationEpochId&&candidate.directory===root.directory ? candidate : null;
  return configured ? { ...root,dispatch: configured.dispatch }:root;
}
function activeIndexedBindings(snapshot: DispatchBindingSnapshot,source: CaptureRoot["source"],
  sessionId: string,observedAt: string): readonly IndexedBinding[] {
  const at=Date.parse(observedAt);
  return snapshot.bySession.get(`${source}\0${sessionId}`)?.filter(entry => at>=entry.from&&at<entry.until) ?? [];
}
/** Resolve one active attempt, checking every configured copy of that attempt.
 * A retired, disjoint attempt does not disagree with its successor. */
function claudeCandidateBinding(snapshot: DispatchBindingSnapshot,sessionId: string,observedAt: string) {
  const entries=snapshot.bySession.get(`claude_code\0${sessionId}`)??[];
  const at=Date.parse(observedAt);
  let candidate: IndexedBinding|undefined;
  for(const entry of entries) {
    if(at<entry.from||at>=entry.until) continue;
    if(candidate && (entry.binding.attemptId!==candidate.binding.attemptId ||
      entry.signature!==candidate.signature)) return { binding:null,conflict:true,rootDigests:new Set<string>() };
    candidate=entry;
  }
  if(!candidate) return { binding:null,conflict:false,rootDigests:new Set<string>() };
  const roots=new Set<string>();
  const rootDigests=new Set<string>();
  for(const entry of entries) {
    if(entry.binding.attemptId===candidate.binding.attemptId) {
      if(entry.signature!==candidate.signature||roots.has(entry.root.rootId))
        return { binding:null,conflict:true,rootDigests:new Set<string>() };
      roots.add(entry.root.rootId);
      rootDigests.add(snapshot.rootDigests.get(entry.root.rootId)!);
    } else if(entry.from<candidate.until&&candidate.from<entry.until) {
      // Different attempts with overlapping windows cannot own one event.
      return { binding:null,conflict:true,rootDigests:new Set<string>() };
    }
  }
  return { binding:candidate.binding,conflict:false,rootDigests };
}
export function dispatchBindingForSession(source: CaptureRoot["source"],sessionId: string,observedAt: string,
  roots: readonly CaptureRoot[]=currentDispatchCaptureRoots()): DispatchBinding|null {
  const snapshot=dispatchIndex(roots);
  if(source==="claude_code") return claudeCandidateBinding(snapshot,sessionId,observedAt).binding;
  const entries=activeIndexedBindings(snapshot,source,sessionId,observedAt);
  if (entries.length === 0) return null;
  const byRoot=new Set(entries.map(entry => entry.root.rootId));
  const signatures=new Set(entries.map(entry => entry.signature));
  const at=Date.parse(observedAt);
  return byRoot.size===entries.length&&signatures.size===1&&at>=entries[0].from&&at<entries[0].until
    ? entries[0].binding : null;
}
/** A known-root transcript sighting is authoritative; hook/OTLP paths are not. */
export function observeClaudeRootSession(root: CaptureRoot,sessionId: string) {
  if(root.source!=="claude_code") return;
  const roots=seenClaudeSessionRoots.get(sessionId)??new Set<string>();
  roots.add(captureRootDigest(root));
  seenClaudeSessionRoots.set(sessionId,roots);
}
export function claudeSessionRootSightings(sessionId: string): ReadonlySet<string> {
  return seenClaudeSessionRoots.get(sessionId)??new Set<string>();
}
const claudeDispatchSkips={ conflictingBindings:0,otherRootSeen:0,ambiguousRoot:0,
  replayTimeout:0,replayRootUnavailable:0,replayBytesUnvouched:0 };
export function claudeDispatchSkipStatus() {
  return { ...claudeDispatchSkips,total:Object.values(claudeDispatchSkips).reduce((a,b)=>a+b,0) };
}
export function countClaudeReplayTimeout(count=1) {
  claudeDispatchSkips.replayTimeout+=count;
}
export function countClaudeReplayRootUnavailable(count=1) {
  claudeDispatchSkips.replayRootUnavailable+=count;
}
export function countClaudeReplayBytesUnvouched(count=1) {
  claudeDispatchSkips.replayBytesUnvouched+=count;
}
/** Identical fanout copies are one binding; a sighting in an unbound root vetoes it. */
export function claudeBindingForUnrootedEvent(sessionId: string,observedAt: string,
  snapshot=currentDispatchBindingSnapshot(),durableSightings?: ReadonlySet<string>): DispatchBinding|null {
  const candidate=claudeCandidateBinding(snapshot,sessionId,observedAt);
  if(candidate.conflict) {
    claudeDispatchSkips.conflictingBindings++;
    return null;
  }
  if(!candidate.binding) return null;
  if(claudeSessionRootSightings(sessionId).size===0 && !durableSightings?.size)
    return candidate.binding;
  const seen=new Set([...claudeSessionRootSightings(sessionId),...(durableSightings??[])]);
  const configuredSeen=[...seen].filter(digest => snapshot.claudeRootDigests.has(digest));
  if(configuredSeen.some(digest => !candidate.rootDigests.has(digest))) {
    claudeDispatchSkips.otherRootSeen++;
    return null;
  }
  return candidate.binding;
}
export function dispatchBindingMetadata(binding: DispatchBinding): Record<string,unknown> {
  return {
    workItemId: binding.workItemId,dispatchProjectKey: binding.projectKey,
    workEvidenceRef: binding.evidenceRef,attemptId: binding.attemptId,
    ...(binding.parentAttemptId? { parentAttemptId: binding.parentAttemptId }:{}),
    ...(binding.companyRef? { companyRef: binding.companyRef }:{}),
    ...(binding.acceptedOutcomeId? { acceptedOutcomeId: binding.acceptedOutcomeId }:{}),
    ...(binding.role? { role: binding.role }:{}),
    ...(binding.workClass? { workClass: binding.workClass }:{}),
    ...(binding.complexityBand? { complexityBand: binding.complexityBand }:{}),
    ...(binding.techniqueId? { techniqueId: binding.techniqueId }:{}),
    ...(binding.techniqueVersion? { techniqueVersion: binding.techniqueVersion }:{}),
    ...(binding.assignmentId? { assignmentId: binding.assignmentId }:{}),
    ...(binding.arm? { arm: binding.arm }:{}),
    ...(binding.launchedBy? { launchedBy: binding.launchedBy }:{}),
  };
}
export function rootEventMetadata(root: CaptureRoot|undefined,sourceEventId: string,observedAt: string,sessionId?: string,
  accountAttributionEnabled=true,snapshot=currentDispatchBindingSnapshot()): Record<string, unknown> {
  if(!root)
    return {};
  root=currentDispatchRoot(root,snapshot);
  if(root.source==="claude_code"&&sessionId) observeClaudeRootSession(root,sessionId);
  const at=Date.parse(observedAt);
  const accountCandidates = accountAttributionEnabled ? [...new Map(
    [ ...(root.accountAssertions ?? []), ...(root.account ? [root.account] : []) ]
      .map(account => [JSON.stringify([account.actorHash,account.validFrom,account.validUntil,account.evidenceRef]), account] as const),
  ).values()] : [];
  const matchingAccounts = accountCandidates.filter(account => at>=Date.parse(account.validFrom)&&(!account.validUntil||at<Date.parse(account.validUntil)));
  const account = matchingAccounts.length === 1 ? matchingAccounts[0] : null;
  const accountEpochs = account ? (root.accountAssertionEpochs ?? []).filter(epoch =>
    epoch.actorHash===account.actorHash&&epoch.validFrom===account.validFrom&&epoch.evidenceRef===account.evidenceRef) : [];
  const installationEpochId = accountEpochs.length===1 ? accountEpochs[0].installationEpochId : root.installationEpochId;
  const localIndex=snapshot.byRootId.get(root.rootId)?.dispatch===root.dispatch
    ? snapshot : dispatchIndex([root]);
  const localMatches=activeIndexedBindings(localIndex,root.source,sessionId??"",observedAt)
    .filter(entry => entry.root.rootId===root.rootId);
  const localBinding={ binding: localMatches.length===1 ? localMatches[0].binding:null,
    conflict: localMatches.length>1 };
  // The known transcript root must supply its own binding. Other roots can
  // veto on a conflict; they can never lend their binding to this root.
  const sharedBinding=root.source==="claude_code"&&sessionId
    ? dispatchBindingForSession("claude_code",sessionId,observedAt,snapshot.roots) : null;
  const binding=root.source==="claude_code"
    ? localBinding.binding&&sharedBinding&&JSON.stringify(localBinding.binding)===JSON.stringify(sharedBinding)
      ? localBinding.binding : null
    : localBinding.binding;
  const conflict=localBinding.conflict;
  return {
    ...(binding? dispatchBindingMetadata(binding):{}),...(conflict? { workAttributionState: "conflict" }:{}),
    captureRootId: root.rootId,captureProfileId: root.profileId,installationEpochId,
    logicalSourceEventId: sourceEventId,sourceIdentityEvidenceRef: "native_runtime_event_v1",
    ...(account ? { captureAccountHash: account.actorHash,accountEvidenceRef: account.evidenceRef } : {}),
    ...(matchingAccounts.length > 1 || accountEpochs.length > 1 ? { accountAttributionState: "conflict" } : {})
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
const sessionSightingCaches=new WeakMap<object,{version:number;sessions:Map<string,Set<string>>}>();
const captureRootEpochCapabilities=new WeakMap<object,string>();
/** Read-only sidecar used by LocalEventBuffer; JSON cannot mint this capability. */
export function captureRootEventInstallationEpoch(event: object) {
  return captureRootEpochCapabilities.get(event);
}
function ensureRootObservationSchema(database: import("better-sqlite3").Database) {
  if(initializedObservationDatabases.has(database))
    return;
  database.exec(`create table if not exists capture_root_observations (
    root_digest text not null,event_id text not null,payload_digest text not null,
    observed_at text not null,state text not null check(state in ('admitted','duplicate','conflict')),
    primary key(root_digest,event_id));
    create index if not exists idx_capture_root_event on capture_root_observations(event_id);
    create table if not exists capture_root_session_sightings (
      source text not null,session_id text not null,root_digest text not null,
      first_seen_at text not null,primary key(source,session_id,root_digest)
    )`);
  // A rolled-back transaction must not leave a cached schema assertion.
  if(!database.inTransaction)
    initializedObservationDatabases.add(database);
}
/** Prime the schema cache before a history writer slice acquires SQLite's writer. */
export function prepareCaptureRootObservationSchema(database: import("better-sqlite3").Database) {
  ensureRootObservationSchema(database);
}
export function captureRootObservationPayloadDigest(value: Pick<import("../../shared/src/schemas").AiInteractionEvent,
  "source" | "id" | "sessionId" | "observedAt" | "model" | "inputTokens" | "outputTokens" |
  "cacheReadTokens" | "cacheCreationTokens" | "costUsd">) {
  return crypto.createHash("sha256").update(JSON.stringify([
    value.source,value.id,value.sessionId,value.observedAt,value.model,value.inputTokens??null,value.outputTokens??null,
    value.cacheReadTokens??null,value.cacheCreationTokens??null,value.costUsd??null,
  ])).digest("hex");
}
/** Page the pre-sighting observation ledger before retention can erase its raw
 * session ID. The cursor and inserts commit together, so an interrupted page
 * is simply retried. New observations written by an older binary have larger
 * rowids and are visited on the next pass. */
export function promoteLegacyClaudeRootSightings(database: import("better-sqlite3").Database,
  maxRows = 128) {
  ensureRootObservationSchema(database);
  const limit=Math.max(1,Math.min(Math.trunc(maxRows),1024));
  return database.transaction(() => {
    const key="claude_legacy_sighting_promotion_v1";
    const state=database.prepare("select value from maintenance_state where key=?").get(key) as
      {value:string}|undefined;
    const countReason=(reason:string) => database.prepare(`insert into maintenance_state
      (key,value,updated_at) values(?,'1',?) on conflict(key) do update set
      value=cast(value as integer)+1,updated_at=excluded.updated_at`)
      .run(reason,new Date().toISOString());
    let cursor=0n;
    if(state) {
      try {
        if(!/^(0|[1-9][0-9]*)$/.test(state.value)) throw new Error("invalid cursor");
        cursor=BigInt(state.value);
        if(cursor>9_223_372_036_854_775_807n) throw new Error("invalid cursor");
      } catch {
        cursor=0n;
        countReason("claude_legacy_sighting_cursor_reset_count_v1");
      }
    }
    const rows=database.prepare(`select seen.rowid as rowid,seen.root_digest as rootDigest,
        seen.observed_at as observedAt,raw.session_id as sessionId
      from capture_root_observations seen
      left join buffered_events raw on raw.id=seen.event_id and raw.source='claude_code'
      where seen.rowid>? order by seen.rowid limit ?`).safeIntegers().all(cursor,limit) as Array<{
        rowid:bigint;rootDigest:string;observedAt:string;sessionId:string|null;
      }>;
    if(!rows.length) return {visited:0,promoted:0,complete:true};
    const insert=database.prepare(`insert into capture_root_session_sightings
      (source,session_id,root_digest,first_seen_at) values('claude_code',?,?,?)
      on conflict do nothing`);
    let promoted=0;
    for(const row of rows) {
      if(!row.sessionId) {
        countReason("claude_legacy_sighting_row_skipped_count_v1");
        continue;
      }
      try { promoted+=insert.run(row.sessionId,row.rootDigest,row.observedAt).changes; }
      catch { countReason("claude_legacy_sighting_row_skipped_count_v1"); }
    }
    const next=rows.at(-1)?.rowid??cursor;
    database.prepare(`insert into maintenance_state(key,value,updated_at) values(?,?,?)
      on conflict(key) do update set value=excluded.value,updated_at=excluded.updated_at`)
      .run(key,String(next),new Date().toISOString());
    if(promoted) sessionSightingCaches.delete(database);
    return { visited:rows.length,promoted,complete:rows.length<limit };
  }).immediate();
}

/** The final guard belongs in the raw deletion transaction: an older writer
 * may append an observation between the migration page and the prune pass. */
export function promoteClaudeRootSightingsForRaw(database: import("better-sqlite3").Database,
  eventId: string) {
  ensureRootObservationSchema(database);
  const changed=database.prepare(`insert into capture_root_session_sightings
      (source,session_id,root_digest,first_seen_at)
    select 'claude_code',raw.session_id,seen.root_digest,seen.observed_at
    from buffered_events raw join capture_root_observations seen on seen.event_id=raw.id
    where raw.id=? and raw.source='claude_code' and raw.session_id is not null
    on conflict do nothing`).run(eventId).changes;
  if(changed) sessionSightingCaches.delete(database);
}
/** Session-to-root evidence is durable even after the ordinary raw row expires. */
export function durableClaudeRootSessionSightings(database: import("better-sqlite3").Database,
  sessionId: string): ReadonlySet<string> {
  const version=database.pragma("data_version",{simple:true}) as number;
  let cache=sessionSightingCaches.get(database);
  if(!cache||cache.version!==version) {
    cache={version,sessions:new Map()};sessionSightingCaches.set(database,cache);
  }
  const prior=cache.sessions.get(sessionId);
  if(prior) return prior;
  const roots=new Set<string>();
  const hasTable=(name: string) => Boolean(database.prepare(
    "select 1 from sqlite_master where type='table' and name=?").get(name));
  if(hasTable("capture_root_session_sightings")) for(const row of database.prepare(
    "select root_digest as digest from capture_root_session_sightings where source='claude_code' and session_id=?"
  ).all(sessionId) as Array<{digest:string}>) roots.add(row.digest);
  // The existing observation table is the migration source for rows captured
  // before this compact session index existed. The session index bounds it.
  if(hasTable("capture_root_observations")) for(const row of database.prepare(`
    select distinct seen.root_digest as digest from buffered_events as raw
    join capture_root_observations as seen on seen.event_id=raw.id
    where raw.source='claude_code' and raw.session_id=?`).all(sessionId) as Array<{digest:string}>)
    roots.add(row.digest);
  if(cache.sessions.size>=4096) cache.sessions.clear();
  cache.sessions.set(sessionId,roots);
  return roots;
}
/** Persist a known Claude root before its first raw event can be interrupted.
 * A later file or slice of the same root/session only reads the cached sighting. */
export function recordClaudeRootSessionSighting(buffer: import("./buffer").LocalEventBuffer,
  root: CaptureRoot,sessionId: string,observedAt: string): boolean {
  if(root.source!=="claude_code") return false;
  const database=buffer.database;
  ensureRootObservationSchema(database);
  const rootDigest=captureRootDigest(root);
  // The read-only legacy fallback joins raw observations. It is a veto, but
  // cannot substitute for the compact row after raw retention removes it.
  const hasCompactSighting=() => Boolean(database.prepare(`select 1 from capture_root_session_sightings
    where source='claude_code' and session_id=? and root_digest=?`).get(sessionId,rootDigest));
  if(hasCompactSighting()) {
    observeClaudeRootSession(root,sessionId);
    return false;
  }
  // A savepoint inside a raw/cursor transaction would roll back on SIGKILL.
  if(database.inTransaction) throw new Error("claude_sighting_must_precede_raw_transaction");
  const write=() => database.transaction(() => database.prepare(`
    insert into capture_root_session_sightings(source,session_id,root_digest,first_seen_at)
    values('claude_code',?,?,?) on conflict do nothing`).run(sessionId,rootDigest,observedAt)).immediate();
  try { write(); }
  catch(error) {
    // A caught, one-shot failure must still leave the truthful veto durable.
    // If persistence keeps failing, intake stops before any raw row is written.
    try { write(); }
    finally {
      sessionSightingCaches.delete(database);
      if(hasCompactSighting())
        observeClaudeRootSession(root,sessionId);
    }
    throw error;
  }
  sessionSightingCaches.delete(database);
  observeClaudeRootSession(root,sessionId);
  return true;
}
/** Root sightings live beside immutable events; replay/failover never changes the first receipt. */
export function appendRootObservation(buffer: import("./buffer").LocalEventBuffer,event: import("../../shared/src/schemas").AiInteractionEvent,root: CaptureRoot|undefined,historyImportNoLiveSibling=false): boolean {
  const parsedAccount = root?.account ? accountAssertionV1Schema.safeParse(root.account) : null;
  const trustedEpoch = root && parsedAccount?.success && accountAssertionContains(parsedAccount.data, event.observedAt) &&
    event.metadata?.installationEpochId===root.installationEpochId ? root.installationEpochId : undefined;
  if (buffer.eventAdmissionReason(event.observedAt, root?.installationEpochId ?? event.metadata?.installationEpochId, trustedEpoch))
    return false;
  if(!root)
    return buffer.append(event,[]);
  const database=buffer.database;
  ensureRootObservationSchema(database);
  if(root.source==="claude_code"&&event.sessionId)
    recordClaudeRootSessionSighting(buffer,root,event.sessionId,event.observedAt);
  const nativeSignature=captureRootObservationPayloadDigest;
  const payloadDigest=nativeSignature(event),rootDigest=captureRootDigest(root);
  // The sighting is already committed. The raw event and root receipt share
  // one commit; buffer.append uses a nested savepoint inside this transaction.
  const result=database.transaction(() => {
      const existing=database.prepare("select payload_json as payload from buffered_events where id=?").get(event.id) as {
        payload: string;
      }|undefined;
      const priorSightings=database.prepare(`select payload_digest as digest,state
        from capture_root_observations where event_id=?`).all(event.id) as Array<{
        digest: string;
        state: string;
      }>;
      const priorConflict=priorSightings.some(row => row.digest!==payloadDigest||row.state==="conflict");
      const alreadyObserved=Boolean(existing)||priorSightings.length>0;
      let same=false;
      if(existing) {
        try { same=nativeSignature(JSON.parse(existing.payload))===payloadDigest; }
        catch { /* corruption remains a conflict */ }
      }
      else same=priorSightings.length>0&&!priorConflict;
      let inserted=false;
      if(!alreadyObserved) {
        if(trustedEpoch) captureRootEpochCapabilities.set(event,trustedEpoch);
        try { inserted=buffer.append(event,[],{historyImportNoLiveSibling}); }
        finally { captureRootEpochCapabilities.delete(event); }
      }
      // Another connection may have enrolled between the first check and append.
      if(!alreadyObserved&&!inserted&&buffer.eventAdmissionReason(event.observedAt,
        root.installationEpochId,trustedEpoch)) return { inserted:false,state:null };
      const state=priorConflict? "conflict":alreadyObserved? (same? "duplicate":"conflict"):
        inserted? "admitted":"conflict";
      database.prepare(`insert into capture_root_observations values(?,?,?,?,?)
        on conflict(root_digest,event_id) do update set state=case when payload_digest<>excluded.payload_digest then 'conflict' else state end`)
        .run(rootDigest,event.id,payloadDigest,event.observedAt,state);
      return { inserted,state };
    }).immediate();
  if(result.state==="conflict")
    throw new Error("capture_logical_source_conflict");
  return result.inserted;
}

/**
 * Additive root registration (bead eco-6hoxj.53).
 *
 * A host that later gains a native root — a new Claude seat's `projects/`, a
 * new Codex profile's `sessions/`, or the main `~/.claude/projects` an older
 * enrollment never registered — has had no product path to register it:
 * capture roots are minted at enrollment, which lives outside this repository,
 * and the gap has been repaired by host-sealed one-off scripts. The helpers
 * below carry the reviewed semantics of those scripts: the same identity
 * derivation, the same standard shapes, and the same refusal to change
 * anything that already exists.
 */

/** Directory shapes a native root takes under the operator home. */
export const CAPTURE_ROOT_SHAPES = [
  { shape: "claude_home", source: "claude_code" as const, segments: [".claude", "projects"] },
  { shape: "claude_seat", source: "claude_code" as const, parent: ".claude-seats", leaf: "projects" },
  { shape: "codex_home", source: "codex" as const, segments: [".codex", "sessions"] },
  { shape: "codex_profile", source: "codex" as const, parent: ".codex-profiles", leaf: "sessions" },
  // Studio stores Codex rollouts beneath each conductor's own profile. The
  // exact leaf keeps neighboring runtime, cache and other session trees out.
  { shape: "studio_codex_conductor", source: "codex" as const,
    parent: ".clientai/studio/borg/conductors", leaf: "profile/sessions" },
] as const;

export type CaptureRootCandidate = {
  shape: string;
  source: CaptureRoot["source"];
  /** Absolute physical directory. */
  directory: string;
  /** Directory relative to the operator home; a candidate never leaves it. */
  relativeDirectory: string;
  /** Automatic enrollment requires a physical path and source evidence. */
  autoEnroll: boolean;
  reason?: "symlink_component" | "codex_evidence_missing" | "codex_evidence_exhausted" |
    "claude_evidence_missing";
};

export type CaptureRootDiscoveryEntry = {
  source: CaptureRoot["source"];
  state: "registered" | "candidate" | "missing" | "live_covered" | "found_not_recorded";
  /** Relative to the operator home, or null for a configured root outside it. */
  directory: string | null;
  outsideHome: boolean;
  shape: string | null;
  rootId: string | null;
  /** Names of config sections and keys that establish a live path; no values. */
  evidence?: string[];
  reason?: CaptureRootCandidate["reason"];
};

/** Do not follow a link in any component beneath the physical home. */
export function physicalBelowHome(home: string, entry: string): boolean {
  const relative = path.relative(home, entry);
  if (!relative || relative.startsWith("..") || path.isAbsolute(relative)) return false;
  let current = home;
  try {
    for (const segment of relative.split(path.sep)) {
      current = path.join(current, segment);
      const stat = fs.lstatSync(current);
      if (stat.isSymbolicLink()) return false;
    }
    return true;
  } catch { return false; }
}

/** Discovery and the tailer share the same Codex rollout identity rules. */
const CODEX_UUID_EXACT_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const CODEX_ROLLOUT_FILE_RE = /^rollout-(?:.+-)?([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\.jsonl$/i;

export function isCodexUuid(value: unknown): value is string {
  return typeof value === "string" && CODEX_UUID_EXACT_RE.test(value);
}

export function codexRolloutIdFromFilename(file: string): string | undefined {
  const match = CODEX_ROLLOUT_FILE_RE.exec(path.basename(file));
  return match && isCodexUuid(match[1]) ? match[1].toLowerCase() : undefined;
}

export function verifiedCodexSessionMetaId(row: unknown): string | undefined {
  if (!row || typeof row !== "object" || Array.isArray(row)) return undefined;
  const record = row as Record<string, unknown>;
  if (record.type !== "session_meta") return undefined;
  const payload = record.payload;
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) return undefined;
  const id = (payload as Record<string, unknown>).id;
  const timestamp = record.timestamp ?? (payload as Record<string, unknown>).timestamp;
  // Codex rollouts can omit this timestamp; the tailer has always accepted them.
  if (!isCodexUuid(id) ||
      (timestamp !== undefined &&
        (typeof timestamp !== "string" || !Number.isFinite(Date.parse(timestamp))))) return undefined;
  return id.toLowerCase();
}

export function verifiedCodexRolloutSessionId(file: string, row: unknown): string | undefined {
  const filenameId = codexRolloutIdFromFilename(file);
  const metadataId = verifiedCodexSessionMetaId(row);
  return filenameId && filenameId === metadataId ? filenameId : undefined;
}

const CODEX_ORIGINATORS = new Set(["codex", "codex_cli_rs", "codex_exec",
  "codex_app_server", "codex_vscode"]);

function codexMetadataMatches(file: string, row: unknown): boolean {
  if (!verifiedCodexRolloutSessionId(file, row)) return false;
  const payload = (row as { payload: Record<string, unknown> }).payload;
  return typeof payload.originator === "string" && CODEX_ORIGINATORS.has(payload.originator);
}

/** A bounded first-line check, with no rollout body or path exposed in a receipt. */
function codexHomeEvidence(home: string, sessions: string): "verified" | "missing" | "exhausted" {
  const fileLimit = 128;
  const directoryLimit = 4096;
  let examinedFiles = 0;
  let visitedDirectories = 0;
  const datePart = [/^\d{4}$/, /^(0[1-9]|1[0-2])$/, /^(0[1-9]|[12]\d|3[01])$/];
  const scan = (directory: string, depth: number): "verified" | "missing" | "exhausted" => {
    if (++visitedDirectories > directoryLimit) return "exhausted";
    let entries: fs.Dirent[];
    try { entries = fs.readdirSync(directory, { withFileTypes: true }); }
    catch { return "exhausted"; }
    // Descend the newest year/month/day first. Directory entries never consume
    // the rollout-file budget, so a long-lived home reaches its recent files.
    const newestFirst = entries.sort((left, right) => right.name.localeCompare(left.name));
    if (depth < datePart.length) {
      for (const entry of newestFirst) {
        if (!entry.isDirectory() || !datePart[depth]!.test(entry.name)) continue;
        const child = path.join(directory, entry.name);
        if (!physicalBelowHome(home, child)) continue;
        const found = scan(child, depth + 1);
        if (found !== "missing") return found;
      }
    }
    for (const entry of newestFirst) {
      const id = entry.isFile() ? codexRolloutIdFromFilename(entry.name) : undefined;
      if (!id) continue;
      const file = path.join(directory, entry.name);
      if (!physicalBelowHome(home, file)) continue;
      if (examinedFiles >= fileLimit) return "exhausted";
      examinedFiles += 1;
      try {
        const descriptor = fs.openSync(file, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
        try {
          const bytes = Buffer.alloc(16 * 1024);
          const size = fs.readSync(descriptor, bytes, 0, bytes.length, 0);
          // A matching first record cannot certify metadata in an unread tail.
          // Keep this folder available for explicit enrollment instead.
          if (fs.fstatSync(descriptor).size > size) continue;
          const lines = bytes.subarray(0, size).toString("utf8").split("\n");
          let codexMetadataSeen = false;
          let conflict = false;
          for (const line of lines) {
            if (!line.includes('"session_meta"')) continue;
            try {
              const row = JSON.parse(line) as unknown;
              if (!codexMetadataMatches(file, row)) { conflict = true; break; }
              codexMetadataSeen = true;
            } catch { conflict = true; break; }
          }
          if (codexMetadataSeen && !conflict) return "verified";
        } finally { fs.closeSync(descriptor); }
      } catch { /* Another rollout may provide the evidence. */ }
    }
    return "missing";
  };
  return scan(sessions, 0);
}

function claudeHomeEvidence(home: string, projects: string): boolean {
  const marker = path.join(path.dirname(projects), "settings.json");
  if (!physicalBelowHome(home, marker)) return false;
  try {
    const stat = fs.lstatSync(marker);
    if (!stat.isFile() || stat.size === 0 || stat.size > 1_048_576) return false;
    const settings = JSON.parse(fs.readFileSync(marker, "utf8")) as Record<string, unknown>;
    if (!settings || typeof settings !== "object" ||
        !["env", "hooks", "permissions"].some((key) => Object.hasOwn(settings, key))) return false;
    const pending = [{ directory: projects, depth: 0 }];
    let inspected = 0;
    while (pending.length && inspected < 128) {
      const current = pending.shift()!;
      for (const entry of fs.readdirSync(current.directory, { withFileTypes: true })) {
        inspected += 1;
        if (entry.isDirectory() && current.depth < 3) {
          pending.push({ directory: path.join(current.directory, entry.name), depth: current.depth + 1 });
          continue;
        }
        if (!entry.isFile() || !entry.name.endsWith(".jsonl")) continue;
        const file = path.join(current.directory, entry.name);
        if (!physicalBelowHome(home, file)) continue;
        try {
          const descriptor = fs.openSync(file, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
          try {
          const bytes = Buffer.alloc(4096);
          const size = fs.readSync(descriptor, bytes, 0, bytes.length, 0);
          const row = JSON.parse(bytes.subarray(0, size).toString("utf8").split("\n", 1)[0]) as
            { type?: unknown };
          if (["user", "assistant", "system", "summary", "queue-operation"].includes(String(row.type))) return true;
          } finally { fs.closeSync(descriptor); }
        } catch { /* another transcript may provide the evidence */ }
      }
    }
  } catch { return false; }
  return false;
}

function record(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown> : null;
}

/** Read a native-home config only when its physical file stays inside home. */
function nativeConfig(home: string, directory: string, file: string): string | null {
  const target = path.join(path.dirname(directory), file);
  const relative = path.relative(home, target);
  if (relative.startsWith("..") || path.isAbsolute(relative)) return null;
  try {
    const stat = fs.lstatSync(target);
    if (!stat.isFile() || stat.size > 1_048_576) return null;
    return fs.readFileSync(target, "utf8");
  } catch { return null; }
}

function loopbackEndpoint(value: unknown, port: number, suffix: string) {
  return value === `http://127.0.0.1:${port}${suffix}`;
}

/** A read-only diagnostic; only names of matched sections/keys leave here. */
export function captureRootLiveCoverage(
  home: string,
  source: CaptureRoot["source"],
  directory: string,
  port: number,
): string[] {
  const resolvedHome = resolveDiscoveryHome(home);
  if (source === "codex" && path.basename(directory) === "sessions") {
    const sourceText = nativeConfig(resolvedHome, directory, "config.toml");
    if (sourceText === null) return [];
    try {
      const otel = record(record(parseToml(sourceText))?.otel);
      for (const [section, suffix] of [["trace_exporter", "/v1/traces"], ["exporter", "/v1/logs"]] as const) {
        const exporter = record(record(otel?.[section])?.["otlp-http"]);
        const headers = record(exporter?.headers);
        const sourceHeader = headers && Object.entries(headers).find(([name]) => name.toLowerCase() === "x-plimsoll-source");
        if (loopbackEndpoint(exporter?.endpoint, port, suffix) && sourceHeader?.[1] === "codex") {
          return [`otel.${section}.otlp-http.endpoint`,
            `otel.${section}.otlp-http.headers.x-plimsoll-source`];
        }
      }
    } catch { return []; }
    return [];
  }
  if (source !== "claude_code" || path.basename(directory) !== "projects") return [];
  const sourceText = nativeConfig(resolvedHome, directory, "settings.json");
  if (sourceText === null) return [];
  try {
    const settings = record(JSON.parse(sourceText));
    if (!settings) return [];
    const evidence: string[] = [];
    const hooks = record(settings.hooks);
    if (hooks) {
      for (const [event, groups] of Object.entries(hooks)) {
        if (!Array.isArray(groups)) continue;
        const live = groups.some((group) => {
          const handlers = record(group)?.hooks;
          return Array.isArray(handlers) && handlers.some((handler) => {
            const entry = record(handler);
            return entry?.type === "http" &&
              loopbackEndpoint(entry.url, port, "/hooks/claude-code");
          });
        });
        if (live) evidence.push(`hooks.${event}.hooks.type`, `hooks.${event}.hooks.url`);
      }
    }
    const env = record(settings.env);
    const headers = typeof env?.OTEL_EXPORTER_OTLP_HEADERS === "string"
      ? env.OTEL_EXPORTER_OTLP_HEADERS.split(",").some((header) =>
        header.trim().toLowerCase() === "x-plimsoll-source=claude_code") : false;
    if (env?.CLAUDE_CODE_ENABLE_TELEMETRY === "1" && headers) {
      const signals = [["OTEL_LOGS_EXPORTER", "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", "/v1/logs"],
        ["OTEL_METRICS_EXPORTER", "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "/v1/metrics"]] as const;
      for (const [exporter, endpoint, suffix] of signals) {
        const specific = loopbackEndpoint(env[endpoint], port, suffix);
        const common = loopbackEndpoint(env.OTEL_EXPORTER_OTLP_ENDPOINT, port, "");
        if (env[exporter] === "otlp" && (specific || common)) {
          evidence.push("env.CLAUDE_CODE_ENABLE_TELEMETRY", `env.${exporter}`,
            `env.${specific ? endpoint : "OTEL_EXPORTER_OTLP_ENDPOINT"}`,
            "env.OTEL_EXPORTER_OTLP_HEADERS");
          break;
        }
      }
    }
    return [...new Set(evidence)];
  } catch { return []; }
}

/**
 * Python `json.dumps(value, sort_keys=True, separators=(",", ":"),
 * ensure_ascii=True)` for the one shape this derivation uses — an array of
 * strings — so a root added here derives byte-identically to one the sealed
 * helpers appended.
 */
function canonicalAscii(value: readonly string[]): string {
  return JSON.stringify(value).replace(/[^ -~]/g, (character) =>
    `\\u${character.charCodeAt(0).toString(16).padStart(4, "0")}`);
}

/**
 * Root identity as the sealed enrollment helpers derive it:
 * `root-<sha256(canonical([machine, source, directory]))[:24]>`, with the same
 * digest behind `profile-`. The machine label is the fleet label, not a
 * hostname, so callers resolve it from the roots already in the config.
 */
export function deriveCaptureRootIdentity(
  machine: string,
  source: CaptureRoot["source"],
  directory: string,
): { rootId: string; profileId: string } {
  const digest = crypto.createHash("sha256")
    .update(canonicalAscii([machine, source, directory])).digest("hex").slice(0, 24);
  return { rootId: `root-${digest}`, profileId: `profile-${digest}` };
}

/** True when every configured root reproduces its own rootId under `machine`. */
export function captureRootsDeriveFrom(roots: readonly CaptureRoot[], machine: string): boolean {
  return roots.every((root) =>
    deriveCaptureRootIdentity(machine, root.source, root.directory).rootId === root.rootId);
}

/**
 * The machine label the config was enrolled under. It is stored nowhere — only
 * its digests are — so it is recovered by checking candidate labels against the
 * roots that already exist. A host with no roots yet has nothing to check
 * against, and the caller must state the label explicitly.
 */
export function resolveCaptureRootMachineLabel(
  roots: readonly CaptureRoot[],
  candidates: readonly string[],
): string | null {
  if (!roots.length) return null;
  return candidates.find((candidate) => candidate.length > 0 && captureRootsDeriveFrom(roots, candidate)) ?? null;
}

/**
 * The home a discovery runs against, resolved the way a physical root is.
 * `add` resolves the home through this too: a directory `discover` reports as
 * a candidate must never be refused `path_outside_home` merely because a
 * component of `$HOME` is a symlink.
 */
export function resolveDiscoveryHome(home: string): string {
  try { return fs.realpathSync(path.resolve(home)); }
  catch { return path.resolve(home); }
}

/** The physical directory a configured root captures from. */
export function configuredCaptureRootDirectory(root: Pick<CaptureRoot, "directory">): string {
  return physicalCaptureRootDirectory(root.directory) ?? path.resolve(root.directory);
}

/** The physical directory an entry names, following a link; undefined when it is neither. */
export function physicalCaptureRootDirectory(entry: string): string | undefined {
  try {
    const resolved = fs.realpathSync(entry);
    return fs.statSync(resolved).isDirectory() ? resolved : undefined;
  } catch {
    // A dangling link or an unreadable entry is skipped, never reported.
    return undefined;
  }
}

/**
 * Candidate directories in the standard shapes under `home`. Read-only: it
 * stats directories, never opens a file, and never leaves the home.
 */
export function discoverCaptureRootCandidates(home: string): CaptureRootCandidate[] {
  const resolvedHome = resolveDiscoveryHome(home);
  const found: CaptureRootCandidate[] = [];
  const add = (shape: string, source: CaptureRoot["source"], entry: string) => {
    const relativeDirectory = path.relative(resolvedHome, entry);
    // A seat relocated to shared storage and symlinked in resolves outside the
    // home; that is the seat tooling's root to register, not a home candidate.
    if (relativeDirectory.startsWith("..") || path.isAbsolute(relativeDirectory)) return;
    const physical = physicalBelowHome(resolvedHome, entry);
    if (!physical && !fs.existsSync(entry)) return;
    if (physical && !fs.statSync(entry).isDirectory()) return;
    const codexEvidence = physical && source === "codex"
      ? codexHomeEvidence(resolvedHome, entry) : null;
    const evidence = source === "codex" ? codexEvidence === "verified" :
      physical && claudeHomeEvidence(resolvedHome, entry);
    found.push({ shape, source, directory: entry, relativeDirectory,
      autoEnroll: evidence, ...(!physical ? { reason: "symlink_component" as const } :
        !evidence ? { reason: source === "codex" ?
          codexEvidence === "exhausted" ? "codex_evidence_exhausted" as const : "codex_evidence_missing" as const :
          "claude_evidence_missing" as const } : {}) });
  };
  for (const shape of CAPTURE_ROOT_SHAPES) {
    if ("segments" in shape) {
      add(shape.shape, shape.source, path.join(resolvedHome, ...shape.segments));
      continue;
    }
    const parent = path.join(resolvedHome, shape.parent);
    if (!physicalBelowHome(resolvedHome, parent)) continue;
    let entries: fs.Dirent[];
    try { entries = fs.readdirSync(parent, { withFileTypes: true }); }
    catch { continue; }
    for (const entry of [...entries].sort((left, right) => left.name < right.name ? -1 : left.name > right.name ? 1 : 0)) {
      if (entry.name.startsWith(".")) continue;
      add(shape.shape, shape.source, path.join(parent, entry.name, shape.leaf));
    }
  }
  return found;
}

/**
 * Every configured root plus every unregistered candidate, as one list. A
 * configured root outside the home keeps its identity but not its path, and is
 * never stat-ed: discovery stays inside the home.
 */
export function discoverCaptureRoots(
  home: string,
  roots: readonly CaptureRoot[],
  port: number,
): CaptureRootDiscoveryEntry[] {
  const resolvedHome = resolveDiscoveryHome(home);
  const candidates = discoverCaptureRootCandidates(resolvedHome);
  const shapes = new Map(candidates.map((candidate) => [candidate.directory, candidate.shape]));
  const configured = new Set(roots.map((root) => configuredCaptureRootDirectory(root)));
  const entries: CaptureRootDiscoveryEntry[] = roots.map((root) => {
    const directory = configuredCaptureRootDirectory(root);
    const relative = path.relative(resolvedHome, directory);
    const outsideHome = relative.startsWith("..") || path.isAbsolute(relative);
    return {
      source: root.source,
      state: outsideHome || physicalCaptureRootDirectory(root.directory) !== undefined
        ? "registered" as const
        : "missing" as const,
      directory: outsideHome ? null : relative,
      outsideHome,
      shape: outsideHome ? null : shapes.get(directory) ?? null,
      rootId: root.rootId,
    };
  });
  for (const candidate of candidates) {
    if (configured.has(candidate.directory)) continue;
    const evidence = candidate.autoEnroll
      ? captureRootLiveCoverage(resolvedHome, candidate.source, candidate.directory, port) : [];
    entries.push({
      source: candidate.source,
      state: !candidate.autoEnroll ? "found_not_recorded" : evidence.length ? "live_covered" : "candidate",
      directory: candidate.relativeDirectory,
      outsideHome: false,
      shape: candidate.shape,
      rootId: null,
      ...(candidate.reason ? { reason: candidate.reason } : {}),
      ...(evidence.length ? { evidence } : {}),
    });
  }
  return entries;
}

/**
 * One entry the baseline walk could not resolve unambiguously. The count was
 * always reported; the entries are listed as well (bead eco-6hoxj.55, review
 * N5) so `--allow-scan-errors` can name exactly what it is leaving unfenced.
 */
export type CaptureRootScanError = {
  path: string;
  reason: "directory_unreadable" | "not_a_regular_file" | "stat_failed";
};

/** Every file the `source` tailer would discover under a capture root.
 *
 * Mirrors `IncrementalJsonlDiscovery`'s predicates so the set fenced at
 * registration is the set that would otherwise be replayed: a recursive walk
 * matching `*.jsonl` for Claude transcripts and `rollout-*.jsonl` for Codex
 * rollouts, never following a symlinked candidate. Anything the walk cannot
 * resolve is counted, never guessed: the caller refuses rather than publish a
 * fence it cannot prove exhaustive. */
export function captureRootBaselineFiles(
  source: CaptureRoot["source"],
  directory: string,
): { files: string[]; errors: number; errorEntries: CaptureRootScanError[] } {
  const matches = source === "codex"
    ? (name: string) => name.startsWith("rollout-") && name.endsWith(".jsonl")
    : (name: string) => name.endsWith(".jsonl");
  const files: string[] = [];
  const errorEntries: CaptureRootScanError[] = [];
  const walk = (current: string) => {
    let entries: fs.Dirent[];
    try { entries = fs.readdirSync(current, { withFileTypes: true }); }
    catch { errorEntries.push({ path: current, reason: "directory_unreadable" }); return; }
    for (const entry of entries) {
      const candidate = path.join(current, entry.name);
      if (entry.isDirectory()) { walk(candidate); continue; }
      if (!matches(entry.name)) continue;
      // A symlinked or non-regular candidate is the discovery's own error
      // class: one physical generation must never be fenced under an alias.
      if (entry.isSymbolicLink() || !entry.isFile()) {
        errorEntries.push({ path: candidate, reason: "not_a_regular_file" });
        continue;
      }
      files.push(candidate);
    }
  };
  walk(directory);
  return { files: files.sort(), errors: errorEntries.length, errorEntries };
}

/** One stat-only observation per file, as the tailers build theirs. */
export function captureRootBaselineObservations(
  files: readonly string[],
): { observations: CaptureBaselineFileObservation[]; errors: number; errorEntries: CaptureRootScanError[] } {
  const observations: CaptureBaselineFileObservation[] = [];
  const errorEntries: CaptureRootScanError[] = [];
  for (const file of files) {
    try {
      const identity = fs.lstatSync(file, { bigint: true });
      if (identity.isSymbolicLink() || !identity.isFile()) {
        errorEntries.push({ path: file, reason: "not_a_regular_file" });
        continue;
      }
      observations.push({
        path: file,
        device: identity.dev,
        inode: identity.ino,
        size: identity.size,
        birthtimeNs: identity.birthtimeNs,
      });
    } catch { errorEntries.push({ path: file, reason: "stat_failed" }); }
  }
  return { observations, errors: errorEntries.length, errorEntries };
}
