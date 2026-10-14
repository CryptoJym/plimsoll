import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import Database from "better-sqlite3";
import { remoteLinkageHash, normalizeGitRemote } from "../../shared/src/linkage";
import { resolveGitContextUncached } from "./git-context";
import { readNativeLedger, type NativeLedgerReading } from "./native-ledger";
import {
  defaultNativeSubscriptionPaths, ensureNativeSubscriptionSchema, importNativeSubscription,
  inspectNativeSubscriptionSurfaces, type NativeSubscriptionInspection, type NativeSubscriptionPaths,
} from "./native-subscription";
import { latestSubscriptionExportSummaries } from "./subscription-exports";

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
  );`);
}

function inferCodexProjects(db: Database.Database, reading: NativeLedgerReading, now: string, stateFile: string) {
  const ids = [...new Set(reading.events.filter((event) => event.source === "codex" && event.threadId).map((event) => event.threadId!))].slice(0, 128);
  if (!ids.length || !fs.existsSync(stateFile)) return;
  let state: Database.Database | undefined;
  try {
    state = new Database(stateFile, { readonly: true, fileMustExist: true, timeout: 500 });
    const rows = state.prepare(`select id,cwd,git_origin_url from threads where id in (${ids.map(() => "?").join(",")})`).all(...ids) as Array<{ id: string; cwd: string; git_origin_url: string | null }>;
    const save = db.prepare(`insert into native_thread_projects values (?,?,?,?,?,?,?)
      on conflict(source,thread_id) do update set project_key=excluded.project_key,
      project_label=excluded.project_label,state=excluded.state,evidence=excluded.evidence,updated_at=excluded.updated_at`);
    for (const row of rows) {
      const context = row.git_origin_url ? undefined : resolveGitContextUncached(row.cwd);
      const projectKey = remoteLinkageHash(row.git_origin_url) ?? context?.remoteUrlHash ?? null;
      const projectLabel = normalizeGitRemote(row.git_origin_url) ?? context?.remoteLabel ?? null;
      if (projectKey) save.run("codex", row.id, projectKey, projectLabel, "inferred", "native_thread_repository_metadata", now);
    }
  } catch { /* A missing native project binding stays UNKNOWN. */ }
  finally { state?.close(); }
}

function inferClaudeProjects(db: Database.Database, reading: NativeLedgerReading, now: string, projectsRoot: string) {
  const lookup=db.prepare("select 1 from native_thread_projects where source='claude_code' and thread_id=?");
  const ids=[...new Set(reading.events.filter((event) => event.source === "claude_code" && event.threadId && !event.projectKey)
    .map((event) => event.threadId!).filter((id) => /^[a-zA-Z0-9_-]{1,128}$/.test(id)))];
  const pending = new Set(ids.filter((id)=>!lookup.get(id)).slice(0,128));
  if (!pending.size) return;
  const deadline=Date.now()+750;
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
          for (const line of lines.slice(0,64)) {
            if (!line.includes('"cwd"')) continue;
            const row=JSON.parse(line) as {cwd?:unknown;sessionId?:unknown};
            if (row.sessionId && row.sessionId!==id) continue;
            if (typeof row.cwd!=="string" || !path.isAbsolute(row.cwd)) continue;
            const context=resolveGitContextUncached(row.cwd);
            if (context?.remoteUrlHash) save.run(id,context.remoteUrlHash,context.remoteLabel??null,"inferred","native_transcript_repository_metadata",now);
            break;
          }
          pending.delete(id);
        } catch { /* No complete metadata line means UNKNOWN, never a guessed path. */ }
        finally { if (fd!==undefined) fs.closeSync(fd); }
      }
    }
  } catch { /* Missing native project metadata remains UNKNOWN. */ }
}

export function persistNativeTelemetry(
  db: Database.Database,
  inspection: NativeSubscriptionInspection,
  options: { sourceKey?: string; now?: string; codexStateFile?: string; claudeProjectsRoot?: string } = {},
) {
  const now = options.now ?? inspection.observedAt;
  const sourceKey = options.sourceKey ?? "local_collector";
  ensureNativeTelemetryStore(db);
  const native = importNativeSubscription(db, inspection, now);
  const reading = inspection.ledgerReading;
  const insert = db.prepare(`insert into native_thread_events values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    on conflict(event_key) do update set project_key=coalesce(excluded.project_key,native_thread_events.project_key),
    usage_authority=excluded.usage_authority,authority_accepted=excluded.authority_accepted,
    input_tokens=excluded.input_tokens,output_tokens=excluded.output_tokens,
    cache_read_tokens=excluded.cache_read_tokens,cache_write_tokens=excluded.cache_write_tokens,
    reference_cost_usd=excluded.reference_cost_usd,cost_kind=excluded.cost_kind,updated_at=excluded.updated_at`);
  let ledgerInserted = 0, ledgerReplayed = 0;
  db.transaction(() => {
    for (const account of inspection.accounts) {
      if (account.accountRef) db.prepare(`insert into native_subscription_accounts values (?,?,?,?)
        on conflict(provider,account_ref) do update set observation_json=excluded.observation_json,captured_at=excluded.captured_at`)
        .run(account.provider, account.accountRef, JSON.stringify(account), now);
    }
    for (const snapshot of inspection.quotaSnapshots) {
      const key = hash([snapshot.provider,snapshot.accountRef,snapshot.windowSlot,snapshot.windowLabel]);
      db.prepare(`insert into native_quota_history values (?,?,?,?,?)
        on conflict(snapshot_key,source_digest) do update set last_seen_at=excluded.last_seen_at`)
        .run(key,snapshot.sourceDigest,snapshot.observedAt,snapshot.observedAt,JSON.stringify(snapshot));
    }
    for (const event of reading.events) {
      if ([event.inputTokens,event.outputTokens,event.cacheReadTokens,event.cacheWriteTokens,event.referenceCostUsd].every((value) => value === null)) continue;
      const key = hash([sourceKey,event.source,event.id]);
      const existed = db.prepare("select 1 from native_thread_events where event_key=?").get(key);
      insert.run(key,event.source,event.eventType,event.observedAt,event.threadId,event.model,
        event.inputTokens,event.outputTokens,event.cacheReadTokens,event.cacheWriteTokens,event.referenceCostUsd,
        event.costKind,event.projectKey,event.accountHash,event.usageAuthority,
        event.authorityAccepted === null ? null : event.authorityAccepted ? 1 : 0,now);
      if (existed) ledgerReplayed += 1; else ledgerInserted += 1;
    }
    if (reading.state === "observed" && reading.generation && reading.throughRowid !== null) {
      db.prepare(`insert into native_capture_cursors values (?,?,?,?,?,?) on conflict(source_key) do update set
        generation=excluded.generation,through_rowid=excluded.through_rowid,updated_at=excluded.updated_at`)
        .run(sourceKey,reading.generation,reading.throughRowid,now,now,"UNKNOWN_before_baseline");
    }
  })();
  inferCodexProjects(db,reading,now,options.codexStateFile ?? path.join(os.homedir(),".codex/state_5.sqlite"));
  inferClaudeProjects(db,reading,now,options.claudeProjectsRoot ?? path.join(os.homedir(),".claude/projects"));
  return { ...native, ledgerInserted, ledgerReplayed, ledgerReadState: reading.state,
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

export function nativeTelemetryReport(db: Database.Database, limit = 100) {
  ensureNativeTelemetryStore(db);
  const threads = db.prepare(`select source,thread_id as threadId,count(*) as observedEvents,
    sum(input_tokens) as observedInputTokens,sum(output_tokens) as observedOutputTokens,
    sum(cache_read_tokens) as observedCacheReadTokens,sum(cache_write_tokens) as observedCacheWriteTokens,
    sum(reference_cost_usd) as referenceCostUsd,
    sum(case when authority_accepted is null then 1 else 0 end) as unprovedAuthorityEvents,
    count(distinct project_key) as projectCount,max(project_key) as observedProjectKey,
    max(observed_at) as lastObservedAt
    from native_thread_events where thread_id is not null and (authority_accepted=1 or authority_accepted is null)
    group by source,thread_id order by lastObservedAt desc limit ?`).all(limit) as Array<{
      source:string;threadId:string;observedEvents:number;observedInputTokens:number|null;
      observedOutputTokens:number|null;observedCacheReadTokens:number|null;observedCacheWriteTokens:number|null;
      referenceCostUsd:number|null;unprovedAuthorityEvents:number;projectCount:number;
      observedProjectKey:string|null;lastObservedAt:string|null;
    }>;
  const mapped = threads.map((thread) => {
    const candidate = db.prepare("select project_key,project_label,state from native_thread_projects where source=? and thread_id=?").get(thread.source,thread.threadId) as { project_key: string; project_label: string; state: string } | undefined;
    const projectCount = Number(thread.projectCount);
    return { ...thread, projectKey: projectCount === 1 ? thread.observedProjectKey : projectCount === 0 ? candidate?.project_key ?? null : null,
      projectLabel: projectCount === 0 || projectCount === 1 && candidate?.project_key === thread.observedProjectKey ? candidate?.project_label ?? null : null,
      projectState: projectCount === 1 ? "observed" : projectCount === 0 && candidate ? "inferred" : "UNKNOWN",
      tokenCompleteness: "UNKNOWN", subscriptionAccountJoin: "UNKNOWN", actualSubscriptionSpendUsd: null };
  });
  const eventCount = Number((db.prepare("select count(*) as count from native_thread_events").get() as { count: number }).count);
  return { schema: "plimsoll.native-telemetry-report.v1", generatedAt: new Date().toISOString(),
    observedTokenEvents: eventCount, earlierHistory: "UNKNOWN", fleetCompleteness: "UNKNOWN",
    threads: mapped, dashboardExports: latestSubscriptionExportSummaries(db),
    note: "Token counters, provider allowance percentages, reference costs, and settled bank spend are different measures. Inferred projects are reviewable; unproved account/thread joins remain UNKNOWN." };
}
