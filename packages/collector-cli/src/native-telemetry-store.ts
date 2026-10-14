import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import Database from "better-sqlite3";
import { remoteLinkageHash, normalizeGitRemote } from "../../shared/src/linkage";
import { resolveGitContextUncached } from "./git-context";
import { readNativeLedger, type NativeLedgerEvent, type NativeLedgerReading } from "./native-ledger";
import {
  defaultNativeSubscriptionPaths, ensureNativeSubscriptionSchema, importNativeSubscription,
  inspectNativeSubscriptionSurfaces, type NativeSubscriptionInspection, type NativeSubscriptionPaths, type NativeSubscriptionImportReceipt,
} from "./native-subscription";
import { latestSubscriptionExportSummaries } from "./subscription-exports";
import { capturedAccountHash, nativeAccountLinkResolver, NATIVE_PROVIDER_ACCOUNT_DERIVATION, type NativeAccountLinkEvidence, type NativeAccountDerivation } from "./native-account-links";
import { nativeProjectLink, type NativeProjectEvidence } from "./native-project-links";

function hash(value: unknown): string {
  return crypto.createHash("sha256").update(JSON.stringify(value)).digest("hex");
}

export function ensureNativeTelemetryStore(db: Database.Database): void {
  ensureNativeSubscriptionSchema(db);
  db.exec(`create table if not exists native_capture_cursors (
    source_key text primary key,generation text not null,through_rowid integer not null,
    baseline_at text not null,updated_at text not null,history_state text not null
  );
  create table if not exists native_thread_events (
    event_key text primary key,source text not null,event_type text,observed_at text,
    thread_id text,model text,input_tokens integer,output_tokens integer,
    cache_read_tokens integer,cache_write_tokens integer,reference_cost_usd real,
    cost_kind text,project_key text,account_hash text,usage_authority text not null,
    authority_accepted integer,updated_at text not null
  );
  create index if not exists native_thread_events_thread on native_thread_events(source,thread_id,observed_at);
  create table if not exists native_thread_projects (
    source text not null,thread_id text not null,project_key text,project_label text,
    state text not null,evidence text not null,updated_at text not null,
    primary key(source,thread_id)
  );
  create table if not exists native_quota_history (
    snapshot_key text not null,source_digest text not null,first_seen_at text,last_seen_at text,
    observation_json text not null,primary key(snapshot_key,source_digest)
  );
  create table if not exists native_subscription_accounts (
    provider text not null,account_ref text not null,observation_json text not null,
    captured_at text not null,primary key(provider,account_ref)
  );
  create table if not exists native_account_link_evidence (
    link_key text primary key,source text not null,account_ref text not null,
    account_hash text,state text not null,reason text,observation_json text not null,
    first_seen_at text not null,last_seen_at text not null
  );
  create table if not exists native_project_link_evidence (
    source text not null,thread_id text not null,project_key text not null,
    project_label text,state text not null,evidence text not null,
    first_seen_at text not null,last_seen_at text not null,
    primary key(source,thread_id,project_key,state,evidence)
  );
  create table if not exists native_event_project_evidence (
    event_key text not null,project_key text not null,state text not null,evidence text not null,
    primary key(event_key,project_key,state,evidence)
  );`);
  const columns = new Set((db.prepare("pragma table_info(native_thread_events)").all() as Array<{ name: string }>).map(row => row.name));
  for (const [column, definition] of [
    ["account_hash_conflict", "integer not null default 0"],
    ["provider_account_hash", "text"], ["provider_account_derivation", "text"], ["provider_account_evidence", "text"],
    ["provider_account_hash_conflict", "integer not null default 0"],
    ["project_evidence", "text"], ["repository_key", "text"],
  ]) if (!columns.has(column)) db.exec(`alter table native_thread_events add column ${column} ${definition}`);
  // Retain existing local candidates, including their evidence state. Old event
  // project_key values lack provenance and stay UNKNOWN until a bounded replay.
  db.exec(`insert or ignore into native_project_link_evidence
    select source,thread_id,project_key,project_label,state,evidence,updated_at,updated_at
    from native_thread_projects where project_key is not null;
    insert or ignore into native_event_project_evidence
    select event_key,project_key,'UNKNOWN','legacy_unclassified' from native_thread_events
    where project_key is not null and project_evidence is null`);
}

function saveProjectCandidate(db: Database.Database, source: string, threadId: string,
  projectKey: string, projectLabel: string | null, state: string, evidence: string, now: string) {
  db.prepare(`insert into native_project_link_evidence values (?,?,?,?,?,?,?,?)
    on conflict(source,thread_id,project_key,state,evidence) do update set
    project_label=coalesce(excluded.project_label,native_project_link_evidence.project_label),last_seen_at=excluded.last_seen_at`)
    .run(source,threadId,projectKey,projectLabel,state,evidence,now,now);
}

type ProjectThreadReading = {events:Array<{source:string;threadId:string|null}>};

function inferCodexProjects(db: Database.Database, reading: ProjectThreadReading, now: string, stateFile: string) {
  const ids = [...new Set(reading.events.filter((event) => event.source === "codex" && event.threadId).map((event) => event.threadId!))].slice(0, 128);
  if (!ids.length || !fs.existsSync(stateFile)) return;
  let state: Database.Database | undefined;
  try {
    state = new Database(stateFile, { readonly: true, fileMustExist: true, timeout: 500 });
    const rows = state.prepare(`select id,cwd,git_origin_url from threads where id in (${ids.map(() => "?").join(",")})`).all(...ids) as Array<{ id: string; cwd: string; git_origin_url: string | null }>;
    const save = db.prepare(`insert into native_thread_projects values (?,?,?,?,?,?,?)
      on conflict(source,thread_id) do update set project_key=excluded.project_key,
      project_label=excluded.project_label,state=excluded.state,evidence=excluded.evidence,updated_at=excluded.updated_at
      where native_thread_projects.state <> 'observed'`);
    for (const row of rows) {
      const context = row.git_origin_url ? undefined : resolveGitContextUncached(row.cwd);
      const projectKey = remoteLinkageHash(row.git_origin_url) ?? context?.remoteUrlHash ?? null;
      const projectLabel = normalizeGitRemote(row.git_origin_url) ?? context?.remoteLabel ?? null;
      if (projectKey) {
        saveProjectCandidate(db,"codex",row.id,projectKey,projectLabel,"inferred","native_thread_repository_metadata",now);
        save.run("codex", row.id, projectKey, projectLabel, "inferred", "native_thread_repository_metadata", now);
      }
    }
  } catch { /* A missing native project binding stays UNKNOWN. */ }
  finally { state?.close(); }
}

function inferClaudeProjects(db: Database.Database, reading: ProjectThreadReading, now: string, projectsRoot: string,
  deadline=Date.now()+750, contexts=new Map<string,ReturnType<typeof resolveGitContextUncached>>()) {
  const ids=[...new Set(reading.events.filter((event) => event.source === "claude_code" && event.threadId)
    .map((event) => event.threadId!).filter((id) => /^[a-zA-Z0-9_-]{1,128}$/.test(id)))];
  const pending = new Set(ids.slice(0,128));
  if (!pending.size) return;
  try {
    const rootStat=fs.lstatSync(projectsRoot);
    if (!rootStat.isDirectory() || rootStat.isSymbolicLink()) return;
    const directories=fs.readdirSync(projectsRoot,{withFileTypes:true}).filter((entry)=>entry.isDirectory()&&!entry.isSymbolicLink()).slice(0,4096);
    const save=db.prepare(`insert into native_thread_projects values ('claude_code',?,?,?,?,?,?)
      on conflict(source,thread_id) do nothing`);
    for (const directory of directories) {
      if (Date.now()>deadline || !pending.size) break;
      for (const id of pending) {
        if (Date.now()>deadline) break;
        const file=path.join(projectsRoot,directory.name,`${id}.jsonl`);
        let fd:number|undefined;
        try {
          const stat=fs.lstatSync(file);
          if (!stat.isFile() || stat.isSymbolicLink()) continue;
          fd=fs.openSync(file,fs.constants.O_RDONLY|fs.constants.O_NOFOLLOW);
          const bytes=Buffer.alloc(Math.min(64*1024,stat.size));
          const length=fs.readSync(fd,bytes,0,bytes.length,0);
          const lines=bytes.subarray(0,length).toString("utf8").split("\n");
          if (length<stat.size) lines.pop();
          const seenCwds=new Set<string>();
          for (const line of lines.slice(0,64)) {
            if (Date.now()>deadline) break;
            if (!line.includes('"cwd"')) continue;
            const row=JSON.parse(line) as {cwd?:unknown;sessionId?:unknown};
            if (row.sessionId && row.sessionId!==id) continue;
            if (typeof row.cwd!=="string" || !path.isAbsolute(row.cwd)) continue;
            if (seenCwds.has(row.cwd)) continue;
            seenCwds.add(row.cwd);
            if (!contexts.has(row.cwd)) contexts.set(row.cwd,resolveGitContextUncached(row.cwd));
            const context=contexts.get(row.cwd);
            if (context?.remoteUrlHash) {
              saveProjectCandidate(db,"claude_code",id,context.remoteUrlHash,context.remoteLabel??null,"inferred","native_transcript_repository_metadata",now);
              save.run(id,context.remoteUrlHash,context.remoteLabel??null,"inferred","native_transcript_repository_metadata",now);
            }
          }
        } catch { /* No complete metadata line means UNKNOWN, never a guessed path. */ }
        finally { if (fd!==undefined) fs.closeSync(fd); }
      }
    }
  } catch { /* Missing native project metadata remains UNKNOWN. */ }
}

function capturedProviderAccount(event: Pick<NativeLedgerEvent,"source" | "authorityAccepted" | "providerAccountHash" | "providerAccountDerivation" | "providerAccountEvidence">) {
  const hash = capturedAccountHash(event.providerAccountHash);
  const expected = event.source === "codex" ? "native047_metadata_user_account_id" :
    event.source === "claude_code" ? "native047_metadata_user_account_uuid" : null;
  return hash?.length === 23 && event.authorityAccepted !== false && expected &&
    event.providerAccountDerivation === NATIVE_PROVIDER_ACCOUNT_DERIVATION && event.providerAccountEvidence === expected ?
    { hash, derivation: NATIVE_PROVIDER_ACCOUNT_DERIVATION, evidence: expected } : null;
}

export function persistNativeTelemetry(
  db: Database.Database,
  inspection: NativeSubscriptionInspection,
  options: { sourceKey?: string; now?: string; codexStateFile?: string; claudeProjectsRoot?: string; replayEvidenceOnly?: boolean } = {},
) {
  const now = options.now ?? inspection.observedAt;
  const sourceKey = options.sourceKey ?? "local_collector";
  ensureNativeTelemetryStore(db);
  const native: NativeSubscriptionImportReceipt = options.replayEvidenceOnly ? {
    schema: inspection.schema, quotaRecords: 0, quotaInserted: 0, quotaReplayed: 0, quotaRevised: 0,
    threadRecords: 0, threadInserted: 0, threadReplayed: 0, threadRevised: 0,
  } : importNativeSubscription(db, inspection, now);
  const reading = inspection.ledgerReading;
  const evidenceUpdates = `
    project_key=coalesce(excluded.project_key,native_thread_events.project_key),
    project_evidence=case when excluded.project_key is not null then excluded.project_evidence else native_thread_events.project_evidence end,
    repository_key=coalesce(excluded.repository_key,native_thread_events.repository_key),
    account_hash_conflict=case when native_thread_events.account_hash is not null and excluded.account_hash is not null
      and length(replace(lower(native_thread_events.account_hash),'sha256:',''))=length(replace(lower(excluded.account_hash),'sha256:',''))
      and replace(lower(native_thread_events.account_hash),'sha256:','') <> replace(lower(excluded.account_hash),'sha256:','')
      then 1 else native_thread_events.account_hash_conflict end,
    account_hash=coalesce(native_thread_events.account_hash,excluded.account_hash),
    provider_account_hash_conflict=case when native_thread_events.provider_account_hash is not null and excluded.provider_account_hash is not null
      and native_thread_events.provider_account_derivation=excluded.provider_account_derivation
      and native_thread_events.provider_account_hash <> excluded.provider_account_hash
      then 1 else native_thread_events.provider_account_hash_conflict end,
    provider_account_hash=coalesce(native_thread_events.provider_account_hash,excluded.provider_account_hash),
    provider_account_derivation=coalesce(native_thread_events.provider_account_derivation,excluded.provider_account_derivation),
    provider_account_evidence=coalesce(native_thread_events.provider_account_evidence,excluded.provider_account_evidence),
    updated_at=excluded.updated_at`;
  const insert = db.prepare(`insert into native_thread_events
    (event_key,source,event_type,observed_at,thread_id,model,input_tokens,output_tokens,
    cache_read_tokens,cache_write_tokens,reference_cost_usd,cost_kind,project_key,account_hash,
    usage_authority,authority_accepted,updated_at,project_evidence,repository_key,
    provider_account_hash,provider_account_derivation,provider_account_evidence)
    values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    on conflict(event_key) do update set
    ${evidenceUpdates},
    usage_authority=excluded.usage_authority,authority_accepted=excluded.authority_accepted,
    input_tokens=excluded.input_tokens,output_tokens=excluded.output_tokens,
    cache_read_tokens=excluded.cache_read_tokens,cache_write_tokens=excluded.cache_write_tokens,
    reference_cost_usd=excluded.reference_cost_usd,cost_kind=excluded.cost_kind`);
  // The replay uses an UPDATE, never inserts a missing token event and never
  // revises its counters, authority, price or source timestamp.
  const replay = db.prepare(`update native_thread_events set ${evidenceUpdates.replaceAll("excluded.","@")} where event_key=@event_key`);
  let ledgerInserted = 0, ledgerReplayed = 0, ledgerSkippedUnstored = 0;
  db.transaction(() => {
    for (const account of inspection.accounts) {
      if (account.accountRef) db.prepare(`insert into native_subscription_accounts values (?,?,?,?)
        on conflict(provider,account_ref) do update set observation_json=excluded.observation_json,captured_at=excluded.captured_at`)
        .run(account.provider, account.accountRef, JSON.stringify(account), now);
      const claims = account.accountLinks ?? (account.accountLink ? [account.accountLink] : []);
      for (const claim of claims) db.prepare(`insert into native_account_link_evidence values (?,?,?,?,?,?,?,?,?)
        on conflict(link_key) do update set observation_json=excluded.observation_json,last_seen_at=excluded.last_seen_at`)
        .run(hash([claim.source,claim.accountRef,claim.accountHash,claim.state,claim.reason,claim.derivation]),
          claim.source,claim.accountRef,claim.accountHash,claim.state,claim.reason,JSON.stringify(claim),now,now);
    }
    for (const snapshot of inspection.quotaSnapshots) {
      if (options.replayEvidenceOnly) continue;
      const key = hash([snapshot.provider,snapshot.accountRef,snapshot.windowSlot,snapshot.windowLabel]);
      db.prepare(`insert into native_quota_history values (?,?,?,?,?)
        on conflict(snapshot_key,source_digest) do update set last_seen_at=excluded.last_seen_at`)
        .run(key,snapshot.sourceDigest,snapshot.observedAt,snapshot.observedAt,JSON.stringify(snapshot));
    }
    for (const event of reading.events) {
      if (!options.replayEvidenceOnly && [event.inputTokens,event.outputTokens,event.cacheReadTokens,event.cacheWriteTokens,event.referenceCostUsd].every((value) => value === null)) continue;
      const key = hash([sourceKey,event.source,event.id]);
      const existed = db.prepare("select 1 from native_thread_events where event_key=?").get(key);
      if (options.replayEvidenceOnly && !existed) { ledgerSkippedUnstored += 1; continue; }
      const providerEvidence = capturedProviderAccount(event);
      if (options.replayEvidenceOnly) replay.run({
        event_key: key, project_key: event.projectKey, project_evidence: event.projectEvidence ?? null,
        repository_key: event.repositoryKey ?? null, account_hash: capturedAccountHash(event.accountHash) ? event.accountHash : null,
        provider_account_hash: providerEvidence?.hash ?? null, provider_account_derivation: providerEvidence?.derivation ?? null,
        provider_account_evidence: providerEvidence?.evidence ?? null, updated_at: now,
      });
      else insert.run(key,event.source,event.eventType,event.observedAt,event.threadId,event.model,
        event.inputTokens,event.outputTokens,event.cacheReadTokens,event.cacheWriteTokens,event.referenceCostUsd,
        event.costKind,event.projectKey,capturedAccountHash(event.accountHash) ? event.accountHash : null,event.usageAuthority,
        event.authorityAccepted === null ? null : event.authorityAccepted ? 1 : 0,now,
        event.projectEvidence ?? null,event.repositoryKey ?? null,
        providerEvidence?.hash ?? null,providerEvidence?.derivation ?? null,providerEvidence?.evidence ?? null);
      const saveProject = db.prepare("insert or ignore into native_event_project_evidence values (?,?,?,?)");
      if (event.projectKey) saveProject.run(key,event.projectKey,
        event.projectEvidence === "explicit_project_key" ? "observed" : event.projectEvidence === "repository_hash" ? "inferred" : "UNKNOWN",
        event.projectEvidence ?? "legacy_unclassified");
      if (event.repositoryKey) saveProject.run(key,event.repositoryKey,"inferred","repository_hash");
      if (existed) ledgerReplayed += 1; else ledgerInserted += 1;
    }
    if (!options.replayEvidenceOnly && reading.state === "observed" && reading.generation && reading.throughRowid !== null) {
      db.prepare(`insert into native_capture_cursors values (?,?,?,?,?,?) on conflict(source_key) do update set
        through_rowid=case when native_capture_cursors.generation=excluded.generation
          then max(native_capture_cursors.through_rowid,excluded.through_rowid) else excluded.through_rowid end,
        generation=excluded.generation,updated_at=excluded.updated_at`)
        .run(sourceKey,reading.generation,reading.throughRowid,now,now,"UNKNOWN_before_baseline");
    }
  })();
  if (!options.replayEvidenceOnly) {
    inferCodexProjects(db,reading,now,options.codexStateFile ?? path.join(os.homedir(),".codex/state_5.sqlite"));
    inferClaudeProjects(db,reading,now,options.claudeProjectsRoot ?? path.join(os.homedir(),".claude/projects"));
  }
  return { ...native, ledgerInserted, ledgerReplayed, ledgerSkippedUnstored, ledgerReadState: reading.state,
    ledgerCoverage: reading.coverage, hasMore: reading.hasMore, earlierHistory: "UNKNOWN" };
}

export function collectNativeTelemetry(
  db: Database.Database,
  options: { paths?: Partial<NativeSubscriptionPaths>; now?: string; codexStateFile?: string; claudeProjectsRoot?: string } = {},
) {
  ensureNativeTelemetryStore(db);
  const paths = { ...defaultNativeSubscriptionPaths(), ...options.paths };
  const cursor = db.prepare("select generation,through_rowid from native_capture_cursors where source_key='local_collector'").get() as { generation: string; through_rowid: number } | undefined;
  let generation: string | null = null;
  try { const stat = fs.statSync(paths.collectorDb); generation = `${stat.dev}:${stat.ino}:${stat.birthtimeMs}`; } catch { /* UNKNOWN */ }
  const reading = readNativeLedger(paths.collectorDb, { afterRowid: cursor && cursor.generation === generation ? cursor.through_rowid : null, limit: 10_000 });
  const inspection = inspectNativeSubscriptionSurfaces({ paths, now: options.now, ledgerReading: reading });
  const receipt = persistNativeTelemetry(db, inspection, { now: options.now, codexStateFile: options.codexStateFile,claudeProjectsRoot:options.claudeProjectsRoot });
  return { inspection, receipt };
}

/** Recheck bounded native repository metadata for stored chat IDs. This writes
 * project candidates only; it cannot alter tokens, identity evidence or cursors. */
export function enrichNativeProjectMetadata(db: Database.Database, options: {
  now?:string;codexStateFile?:string;claudeProjectsRoots?:string[];
} = {}) {
  ensureNativeTelemetryStore(db);
  const threads=db.prepare(`select source,thread_id as threadId from native_thread_events
    where thread_id is not null group by source,thread_id order by max(observed_at) desc limit 256`).all() as Array<{source:string;threadId:string}>;
  const reading={events:threads}, now=options.now ?? new Date().toISOString();
  inferCodexProjects(db,reading,now,options.codexStateFile ?? path.join(os.homedir(),".codex/state_5.sqlite"));
  const deadline=Date.now()+2000;
  const roots=[...new Set(options.claudeProjectsRoots ?? [path.join(os.homedir(),".claude/projects")])].slice(0,32);
  // A large first folder must not starve every registered profile forever.
  // Rotate the starting root; retain all candidates, including cross-folder conflicts.
  const minute=Math.floor(Date.parse(now)/60_000);
  const offset=roots.length && Number.isFinite(minute) ? minute%roots.length : 0;
  const ordered=[...roots.slice(offset),...roots.slice(0,offset)];
  let rootsChecked=0;
  const contexts=new Map<string,ReturnType<typeof resolveGitContextUncached>>();
  for (const root of ordered) {
    if (Date.now()>deadline) break;
    inferClaudeProjects(db,reading,now,root,deadline,contexts);rootsChecked++;
  }
  return {threadGroupsSelected:threads.length,projectRootsChecked:rootsChecked,rootRotationOffset:offset,
    remainingRootCoverage:rootsChecked<roots.length ? "UNKNOWN" : "bounded_observation",historyCompleteness:"UNKNOWN"};
}

/** Explicit one-time enrichment of existing sidecar events from the last raw
 * row window. No cursor writes, source scans, new token events, counters or
 * transcript/project-directory reads. Repeat invocation is idempotent. */
export function replayNativeTelemetryEvidence(
  db: Database.Database,
  options: { paths?: Partial<NativeSubscriptionPaths>; now?: string; sourceKey?: string; limit?: number; timeoutMs?: number } = {},
) {
  ensureNativeTelemetryStore(db);
  const paths = { ...defaultNativeSubscriptionPaths(), ...options.paths };
  const reading = readNativeLedger(paths.collectorDb,{ limit: options.limit ?? 50_000, timeoutMs: options.timeoutMs });
  const inspection = inspectNativeSubscriptionSurfaces({ paths, now: options.now, ledgerReading: reading, identityEvidenceOnly: true });
  const receipt = persistNativeTelemetry(db,inspection,{ sourceKey: options.sourceKey, now: options.now, replayEvidenceOnly: true });
  return { inspection, receipt: { ...receipt, mode: "identity_project_evidence_replay" as const,
    rawRowsSelected: reading.rawRowCount ?? null, maximumRowid: reading.maximumRowid,
    throughRowid: reading.throughRowid, cursorUpdated: false as const, tokenCountersUpdated: false as const } };
}

export function nativeTelemetryReport(db: Database.Database, limit = 100) {
  ensureNativeTelemetryStore(db);
  const accountEvidence = (db.prepare("select observation_json from native_account_link_evidence").all() as Array<{ observation_json: string }>)
    .map(row => JSON.parse(row.observation_json) as NativeAccountLinkEvidence);
  const resolveAccount = nativeAccountLinkResolver(accountEvidence);
  const activitySql = `select source,account_hash as capturedHash,account_hash_conflict as identityConflict,
    provider_account_hash as providerAccountHash,provider_account_derivation as providerAccountDerivation,
    provider_account_evidence as providerAccountEvidence,provider_account_hash_conflict as providerIdentityConflict,
    count(*) as observedEvents,sum(input_tokens) as observedInputTokens,sum(output_tokens) as observedOutputTokens,
    sum(cache_read_tokens) as observedCacheReadTokens,sum(cache_write_tokens) as observedCacheWriteTokens,
    sum(reference_cost_usd) as referenceCostUsd,
    sum(case when authority_accepted is null then 1 else 0 end) as unprovedAuthorityEvents
    from native_thread_events where (authority_accepted=1 or authority_accepted is null)`;
  type Activity = { source: string; capturedHash: string | null; identityConflict: number;
    providerAccountHash: string | null; providerAccountDerivation: NativeAccountDerivation | null;
    providerAccountEvidence: NativeLedgerEvent["providerAccountEvidence"]; providerIdentityConflict: number;
    observedEvents: number; observedInputTokens: number | null; observedOutputTokens: number | null;
    observedCacheReadTokens: number | null; observedCacheWriteTokens: number | null;
    referenceCostUsd: number | null; unprovedAuthorityEvents: number };
  const mapActivity = (rows: Activity[]) => rows.map(({ capturedHash, identityConflict, providerIdentityConflict, ...activity }) => {
    const providerEvidence = capturedProviderAccount({ ...activity, authorityAccepted: null });
    const actor = resolveAccount(activity.source,capturedHash,identityConflict === 1);
    const fullAssertion = capturedAccountHash(capturedHash)?.length === 71;
    let link = providerEvidence ? resolveAccount(activity.source,providerEvidence.hash,
      providerIdentityConflict === 1 || fullAssertion && identityConflict === 1,providerEvidence.derivation) : actor;
    if (providerEvidence && fullAssertion && actor.accountRef && link.accountRef && actor.accountRef !== link.accountRef) {
      link = resolveAccount(activity.source,providerEvidence.hash,true,providerEvidence.derivation);
    }
    return { ...activity, ...link, actorHash: capturedHash, actorHashConflict: identityConflict === 1,
      providerAccountHashConflict: providerIdentityConflict === 1,
      actualSubscriptionSpendUsd: null, cashState: "UNKNOWN" as const };
  });
  const coverage = (activity: ReturnType<typeof mapActivity>) => {
    let totalEvents = 0, capturedHashEvents = 0, linkedEvents = 0;
    const unknownReasons: Record<string, number> = {};
    for (const group of activity) {
      totalEvents += group.observedEvents;
      if (group.accountHash) capturedHashEvents += group.observedEvents;
      if (group.accountRef) linkedEvents += group.observedEvents;
      else unknownReasons[group.unknownReason ?? "no_proven_native_identity"] =
        (unknownReasons[group.unknownReason ?? "no_proven_native_identity"] ?? 0) + group.observedEvents;
    }
    return { totalEvents, capturedHashEvents, linkedEvents, unknownEvents: totalEvents - linkedEvents,
      linkedEventFraction: totalEvents > 0 ? linkedEvents / totalEvents : null,
      state: linkedEvents === 0 ? "UNKNOWN" : linkedEvents < totalEvents ? "partial" : "observed",
      scope: "captured_sidecar_events", completeness: "UNKNOWN", unknownReasons };
  };
  const accountGroup = "source,account_hash,account_hash_conflict,provider_account_hash,provider_account_derivation,provider_account_evidence,provider_account_hash_conflict";
  const accountActivity = mapActivity(db.prepare(`${activitySql}
    group by ${accountGroup} order by source,account_hash,provider_account_hash`).all() as Activity[]);
  const threadActivity = db.prepare(`${activitySql} and source=? and thread_id=?
    group by ${accountGroup} order by account_hash,provider_account_hash`);
  const projectCandidates = db.prepare(`select project_key as projectKey,project_label as projectLabel,
    state,evidence,null as observedEvents from native_project_link_evidence where source=? and thread_id=?
    union all select p.project_key as projectKey,null as projectLabel,p.state,p.evidence,count(*) as observedEvents
    from native_event_project_evidence p join native_thread_events e on e.event_key=p.event_key
    where e.source=? and e.thread_id=? and (e.authority_accepted=1 or e.authority_accepted is null)
    group by p.project_key,p.state,p.evidence order by state,evidence,projectKey`);
  const threads = db.prepare(`select source,thread_id as threadId,count(*) as observedEvents,
    sum(input_tokens) as observedInputTokens,sum(output_tokens) as observedOutputTokens,
    sum(cache_read_tokens) as observedCacheReadTokens,sum(cache_write_tokens) as observedCacheWriteTokens,
    sum(reference_cost_usd) as referenceCostUsd,
    sum(case when authority_accepted is null then 1 else 0 end) as unprovedAuthorityEvents,
    count(distinct case when project_evidence='explicit_project_key' then project_key end) as projectCount,
    max(case when project_evidence='explicit_project_key' then project_key end) as observedProjectKey,
    max(observed_at) as lastObservedAt
    from native_thread_events where thread_id is not null and (authority_accepted=1 or authority_accepted is null)
    group by source,thread_id order by lastObservedAt desc limit ?`).all(limit) as Array<{
      source:string;threadId:string;observedEvents:number;observedInputTokens:number|null;
      observedOutputTokens:number|null;observedCacheReadTokens:number|null;observedCacheWriteTokens:number|null;
      referenceCostUsd:number|null;unprovedAuthorityEvents:number;projectCount:number;
      observedProjectKey:string|null;lastObservedAt:string|null;
    }>;
  const mapped = threads.map((thread) => {
    const activity = mapActivity(threadActivity.all(thread.source,thread.threadId) as Activity[]);
    const accountLinkCoverage = coverage(activity);
    const accountRefs = [...new Set(activity.map(group => group.accountRef).filter((ref): ref is string => ref !== null))].sort();
    const projects = projectCandidates.all(thread.source,thread.threadId,thread.source,thread.threadId) as NativeProjectEvidence[];
    const project = nativeProjectLink(projects);
    return { ...thread, ...project,
      projectCount: new Set(projects.filter(candidate => candidate.state === "observed").map(candidate => candidate.projectKey)).size,
      observedProjectKey: project.projectState === "observed" ? project.projectKey : null,
      tokenCompleteness: "UNKNOWN", subscriptionAccountJoin: accountLinkCoverage.state,
      accountRef: accountRefs.length === 1 && accountLinkCoverage.unknownEvents === 0 ? accountRefs[0] : null,
      accountRefs, spansAccounts: accountRefs.length > 1, accountLinkCoverage, accountActivity: activity,
      unknownAccountEvents: accountLinkCoverage.unknownEvents, actualSubscriptionSpendUsd: null, cashState: "UNKNOWN" };
  });
  const eventCount = Number((db.prepare("select count(*) as count from native_thread_events").get() as { count: number }).count);
  const subscriptionAccounts = (db.prepare("select provider,account_ref as accountRef,captured_at as capturedAt from native_subscription_accounts order by provider,account_ref").all() as Array<{ provider: string; accountRef: string; capturedAt: string }>);
  return { schema: "plimsoll.native-telemetry-report.v1", generatedAt: new Date().toISOString(),
    observedTokenEvents: eventCount, earlierHistory: "UNKNOWN", fleetCompleteness: "UNKNOWN",
    threads: mapped, subscriptionAccounts, accountLinkEvidence: accountEvidence,
    accountLinkCoverage: coverage(accountActivity), accountActivity,
    actualSubscriptionSpendUsd: null, cashState: "UNKNOWN",
    dashboardExports: latestSubscriptionExportSummaries(db),
    note: "Account links require captured event hashes and the existing native identity derivation. Identity evidence proves equivalence, not historical activity without a captured hash. Token counters, allowance percentages, reference costs and settled spend are different measures. Project candidates retain their evidence and conflicts; completeness and cash remain UNKNOWN." };
}
