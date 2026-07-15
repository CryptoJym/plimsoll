#!/usr/bin/env node

import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { gunzipSync } from "node:zlib";

import Database from "better-sqlite3";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import {
  DASHBOARD_WINDOWS,
  DashboardProjectionStore,
} from "../packages/collector-cli/src/dashboard-projection";
import {
  dashboardAccounts,
  dashboardReposWithTail,
  dashboardSessions,
  dashboardSummary,
  type SubscriptionConfig,
} from "../packages/collector-cli/src/dashboard-api";
import { createCollectorServer } from "../packages/collector-cli/src/server";
import { aiInteractionEventSchema } from "../packages/shared/src/index";

const DAY_MS = 24 * 60 * 60 * 1000;
const NOW = new Date("2026-07-15T12:00:00.000Z");
const MACHINE_SENTINEL = os.hostname();
const RAW_SENTINEL = "PROJECTION_RAW_CONTENT_SENTINEL";
const EMAIL_SENTINEL = "projection-proof@example.test";
const URL_SENTINEL = "https://github.com/projection-proof/private-repo";
const API_KEY_SENTINEL = "sk-projection-秘密-🔐-never-persist";
const PATH_SENTINEL = "/Users/秘密/clients/🚫/private-worktree";
const originalDateNow = Date.now;
Date.now = () => NOW.getTime();

const checks: Array<{ name: string; detail: Record<string, unknown> }> = [];

function check(name: string, condition: unknown, detail: Record<string, unknown> = {}) {
  assert.ok(condition, `${name}: ${JSON.stringify(detail)}`);
  checks.push({ name, detail });
}

function hash(fill: string) {
  return `sha256:${fill.repeat(64).slice(0, 64)}`;
}

function uuid(n: number) {
  return `00000000-0000-4000-8000-${String(n).padStart(12, "0")}`;
}

let eventSequence = 1;
function event(input: {
  observedAt?: string;
  sessionId?: string;
  source?: "claude_code" | "codex";
  eventType?: "assistant_response" | "tool_use" | "tool_result" | "usage_rollout";
  actionClass?: string;
  model?: string;
  inputTokens?: number;
  outputTokens?: number;
  cacheReadTokens?: number;
  cacheCreationTokens?: number;
  costUsd?: number;
  repoHash?: string;
  branchHash?: string;
  actorId?: string;
  metadata?: Record<string, unknown>;
}) {
  const git = input.repoHash || input.branchHash
    ? { remoteUrlHash: input.repoHash, branchHash: input.branchHash }
    : undefined;
  return aiInteractionEventSchema.parse({
    id: uuid(eventSequence++),
    tenantId: "local",
    source: input.source ?? "codex",
    dataMode: "metadata",
    eventType: input.eventType ?? "assistant_response",
    observedAt: input.observedAt ?? new Date(NOW.getTime() - DAY_MS).toISOString(),
    sessionId: input.sessionId,
    actionClass: input.actionClass ?? "other",
    model: input.model,
    inputTokens: input.inputTokens,
    outputTokens: input.outputTokens,
    cacheReadTokens: input.cacheReadTokens,
    cacheCreationTokens: input.cacheCreationTokens,
    costUsd: input.costUsd,
    actorId: input.actorId,
    metadata: { ...(input.metadata ?? {}), ...(git ? { git } : {}) },
  });
}

function settle(buffer: LocalEventBuffer, now = NOW, maxSlices = 100) {
  const receipts = [];
  for (let slice = 0; slice < maxSlices; slice += 1) {
    const state = buffer.projection.status();
    if (
      state.ready &&
      !state.dirty &&
      state.backfill.complete &&
      state.backfill.parityComplete &&
      Object.values(state.backlog).every((value) => value === 0)
    ) {
      return receipts;
    }
    receipts.push(buffer.projection.runMaintenance(now));
  }
  throw new Error(`projection did not settle: ${JSON.stringify(buffer.projection.status())}`);
}

function readySnapshot(
  buffer: LocalEventBuffer,
  days: number,
  subscriptions: SubscriptionConfig[] = [],
) {
  const read = buffer.projection.readSnapshot(days, subscriptions);
  assert.equal(read.kind, "ready", JSON.stringify(read));
  return read.kind === "ready" ? read.snapshot : assert.fail("snapshot not ready");
}

function near(actual: number, expected: number, tolerance: number, name: string) {
  assert.ok(
    Math.abs(actual - expected) <= tolerance,
    `${name}: actual=${actual} expected=${expected} tolerance=${tolerance}`,
  );
}

function byKey(rows: Array<Record<string, unknown>>, key: string) {
  return new Map(rows.map((row) => [String(row[key]), row]));
}

async function listen(server: ReturnType<typeof createCollectorServer>) {
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => resolve());
  });
  const address = server.address();
  assert.ok(address && typeof address !== "string");
  return address.port;
}

async function closeServer(server: ReturnType<typeof createCollectorServer>) {
  await new Promise<void>((resolve) => server.close(() => resolve()));
}

function p95(values: number[]) {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.ceil(sorted.length * 0.95) - 1] ?? 0;
}

function projectionTableText(db: Database.Database, table: string) {
  const rows = db.prepare(`select * from ${table}`).all();
  return JSON.stringify(rows);
}

type ProofCompactItem = {
  rawRowid: number;
  observedAt: string;
  source: string;
  eventType: string;
  actionClass: string | null;
  windowMask: number;
};

function compactItems(db: Database.Database) {
  const segments = db.prepare(
    `select payload_gzip as payload from dashboard_compact_segments order by segment_id`,
  ).all() as Array<{ payload: Buffer }>;
  return segments.flatMap((segment) =>
    JSON.parse(gunzipSync(segment.payload).toString("utf8")) as ProofCompactItem[]);
}

function dropProjectionState(db:Database.Database){
  const objects=db.prepare(
    `select type,name from sqlite_master where name like 'dashboard_%'
      or name like 'trg_dashboard_%' or name='capture_activity_state'`,
  ).all() as Array<{type:"table"|"trigger"|"index";name:string}>;
  for(const object of objects.filter((row)=>row.type==="trigger"))db.exec(`drop trigger if exists ${object.name}`);
  for(const object of objects.filter((row)=>row.type==="table"))db.exec(`drop table if exists ${object.name}`);
}

function downgradeCompactProjectionToC0(db:Database.Database){
  const triggers=db.prepare(
    `select name from sqlite_master where type='trigger' and name like 'trg_dashboard_%'`,
  ).all() as Array<{name:string}>;
  for(const trigger of triggers)db.exec(`drop trigger if exists ${trigger.name}`);
  db.exec(`
    drop index if exists idx_dashboard_compact_day_segment;
    drop index if exists idx_dashboard_compact_cancellation_day;
    drop table if exists dashboard_compact_day_source;
    drop table if exists dashboard_compact_gc_source_scratch;
    drop table if exists dashboard_compact_gc_days;
  `);
  const cancellationColumns=new Set((db.pragma(
    "table_info(dashboard_compact_cancellations)",
  ) as Array<{name:string}>).map((row)=>row.name));
  if(cancellationColumns.has("bucket_day")){
    db.exec(`alter table dashboard_compact_cancellations drop column bucket_day`);
  }
  const controlColumns=new Set((db.pragma(
    "table_info(dashboard_projection_control)",
  ) as Array<{name:string}>).map((row)=>row.name));
  for(const column of [
    "compact_summary_migration_complete",
    "compact_gc_backlog",
    "compact_gc_schedule",
    "compact_segments_written",
    "compact_gc_items_visited",
    "compact_gc_items_removed",
    "compact_gc_segments_rewritten",
    "compact_gc_segments_deleted",
    "compact_gc_days_completed",
    "compact_gc_restarts",
  ])if(controlColumns.has(column))db.exec(
    `alter table dashboard_projection_control drop column ${column}`,
  );
}

function compactMutationRepairDependencyFixture(root:string,label:string,reopenAfterFirst:boolean){
  const fixturePath=path.join(root,`compact-mutation-repair-${label}.sqlite`);
  let fixture=new LocalEventBuffer(fixturePath);
  for(let index=0;index<4_000;index++)fixture.append(event({
    source:"codex",eventType:"assistant_response",actionClass:"other",
    observedAt:new Date(NOW.getTime()-DAY_MS+index).toISOString(),
  }));
  settle(fixture,NOW,30);
  fixture.database.prepare(`delete from buffered_events where rowid between 3001 and 3250`).run();
  fixture.database.prepare(
    `update dashboard_compact_mutations set queued_at='2026-07-15T00:00:00.000Z'
     where raw_rowid between 3001 and 3250`,
  ).run();
  fixture.database.prepare(
    `update dashboard_projection_repairs set queued_at='2026-07-15T00:00:00.000Z'
     where raw_rowid between 3001 and 3250`,
  ).run();
  const updatedObservedAt=new Date(NOW.getTime()-2*DAY_MS).toISOString();
  fixture.database.prepare(
    `update buffered_events set source='claude_code',event_type='tool_use',
      action_class='edit',observed_at=? where rowid between 501 and 600`,
  ).run(updatedObservedAt);
  fixture.database.prepare(
    `update dashboard_compact_mutations set queued_at='2026-07-15T00:00:01.000Z'
     where raw_rowid between 501 and 600`,
  ).run();
  fixture.database.prepare(
    `update dashboard_projection_repairs set queued_at='2026-07-15T00:00:01.000Z'
     where raw_rowid between 501 and 600`,
  ).run();
  const repairSelectionPlan=(fixture.database.prepare(
    `explain query plan select r.raw_rowid
     from dashboard_projection_repairs r
     left join buffered_events b on b.rowid=r.raw_rowid
     where r.reason!='raw_update' or not exists (
       select 1 from dashboard_compact_mutations m where m.raw_rowid=r.raw_rowid
     )
     order by r.queued_at,r.raw_rowid limit 250`,
  ).all() as Array<{detail:string}>).map((row)=>row.detail);
  const queued=fixture.projection.status().backlog;
  const firstReceipt=fixture.projection.runMaintenance(NOW);
  const afterFirst={
    blockedLowUpdateRepairs:(fixture.database.prepare(
      `select count(*) as n from dashboard_projection_repairs r
       join dashboard_compact_mutations m on m.raw_rowid=r.raw_rowid
       where r.reason='raw_update' and r.raw_rowid between 501 and 600`,
    ).get() as {n:number}).n,
    eligibleHighDeleteRepairs:(fixture.database.prepare(
      `select count(*) as n from dashboard_projection_repairs
       where raw_rowid between 3001 and 3250`,
    ).get() as {n:number}).n,
    mutations:(fixture.database.prepare(
      `select count(*) as n from dashboard_compact_mutations`,
    ).get() as {n:number}).n,
    cancellations:(fixture.database.prepare(
      `select count(*) as n from dashboard_compact_cancellations`,
    ).get() as {n:number}).n,
  };
  if(reopenAfterFirst){
    fixture.close();
    fixture=new LocalEventBuffer(fixturePath);
  }
  const receipts=settle(fixture,NOW,30);
  const payload=compactItems(fixture.database);
  const updatedPayload=payload.filter((item)=>item.rawRowid>=501&&item.rawRowid<=600);
  const sources=fixture.database.prepare(
    `select source,events from dashboard_source_window where days=30 and events!=0 order by source`,
  ).all();
  const finalState={
    raw:(fixture.database.prepare(`select count(*) as n from buffered_events`).get() as {n:number}).n,
    projected:Number((readySnapshot(fixture,30).summary.totals as Record<string,number>).events),
    payloadItems:payload.length,
    repairs:(fixture.database.prepare(`select count(*) as n from dashboard_projection_repairs`).get() as {n:number}).n,
    mutations:(fixture.database.prepare(`select count(*) as n from dashboard_compact_mutations`).get() as {n:number}).n,
    cancellations:(fixture.database.prepare(`select count(*) as n from dashboard_compact_cancellations`).get() as {n:number}).n,
    jobs:(fixture.database.prepare(`select count(*) as n from dashboard_compact_gc_days`).get() as {n:number}).n,
  };
  fixture.close();
  return {queued,repairSelectionPlan,firstReceipt,afterFirst,receipts,updatedObservedAt,
    updatedPayload,sources,finalState};
}

async function main() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-projection-proof-"));
  const dbPath = path.join(root, "ledger.sqlite");
  const buffer = new LocalEventBuffer(dbPath);
  const repoA = hash("a");
  const repoB = hash("b");
  const branchA = hash("c");
  const branchB = hash("d");
  const accountA = hash("e");
  const accountAlias = hash("f");
  const sessionA = "projection-session-alpha";
  const sessionB = "projection-session-beta";
  const subscriptions: SubscriptionConfig[] = [
    { account: "Projection Person", plan: "Max", usdPerMonth: 200, vendor: "anthropic" },
    { account: EMAIL_SENTINEL, plan: "Pro", usdPerMonth: 100, vendor: "openai" },
  ];

  try {
    buffer.recordRepoLabel(repoA, "proof/repo-a");
    buffer.recordRepoLabel(repoB, "proof/repo-b");
    buffer.setPriorityRepo(repoA, URL_SENTINEL);
    buffer.setAccountLabel(accountA, "Projection Person");
    buffer.setAccountEmail(accountA, EMAIL_SENTINEL);
    buffer.setAccountAlias(accountAlias, accountA);

    const multiRepo = [
      event({ sessionId: sessionA, source: "claude_code", model: "claude-proof", inputTokens: 100,
        outputTokens: 10, cacheReadTokens: 20, cacheCreationTokens: 5, costUsd: 1.1,
        repoHash: repoA, branchHash: branchA, actorId: accountAlias,
        metadata:{apiKey:API_KEY_SENTINEL,APIKey:API_KEY_SENTINEL,api_key:API_KEY_SENTINEL,
          workingDirectory:PATH_SENTINEL,work_dir:PATH_SENTINEL,filePath:PATH_SENTINEL} }),
      event({ sessionId: sessionA, source: "claude_code", eventType: "tool_use", actionClass: "edit",
        repoHash: repoA, branchHash: branchA, actorId: accountA }),
      event({ sessionId: sessionA, source: "claude_code", model: "claude-proof", inputTokens: 200,
        outputTokens: 20, costUsd: 2.2, repoHash: repoB, branchHash: branchB, actorId: accountA }),
      // Null repo falls back to event-dominant repo A.
      event({ sessionId: sessionA, source: "claude_code", model: "claude-proof", inputTokens: 50,
        outputTokens: 5, costUsd: 0.5, actorId: accountAlias }),
      event({ sessionId: sessionB, source: "codex", model: "unpriced-proof", inputTokens: 400,
        outputTokens: 40, cacheReadTokens: 80, repoHash: repoB, branchHash: branchB }),
      // Sessionless usage remains in summary but not session/repo/account views.
      event({ source: "codex", model: "gpt-proof", inputTokens: 9, outputTokens: 1, costUsd: 0.09 }),
    ];
    for (const row of multiRepo) buffer.append(row, row.id === multiRepo[0]!.id ? [RAW_SENTINEL] : []);
    check("deterministic_duplicate_is_noop", buffer.append(multiRepo[0]!) === false, {
      facts: (buffer.database.prepare(`select count(*) as n from dashboard_event_facts`).get() as { n: number }).n,
    });

    // Boundary fixtures exercise every supported cutoff. Equality is included;
    // one millisecond before is excluded from that window.
    for (const [index, days] of DASHBOARD_WINDOWS.entries()) {
      buffer.append(event({ sessionId: `boundary-in-${days}`, observedAt: new Date(NOW.getTime() - days * DAY_MS).toISOString(),
        inputTokens: 10 + index, outputTokens: 1, costUsd: 0.01 * (index + 1), repoHash: repoA }));
      buffer.append(event({ sessionId: `boundary-out-${days}`, observedAt: new Date(NOW.getTime() - days * DAY_MS - 1).toISOString(),
        inputTokens: 99, outputTokens: 9, costUsd: 9.99, repoHash: repoB }));
    }

    // More than twelve repositories proves exact 11 + aggregate-tail behavior.
    for (let index = 0; index < 13; index += 1) {
      const repo = `sha256:${(1_000 + index).toString(16).padStart(64, "0")}`;
      buffer.append(event({ sessionId: `tail-session-${index}`, inputTokens: index + 1,
        outputTokens: 1, costUsd: (index + 1) / 100, repoHash: repo, branchHash: hash("1") }));
    }

    const receipts = settle(buffer);
    check("new_capture_settles_without_raw_request_work", receipts.length > 0, {
      slices: receipts.length,
      state: buffer.projection.status(),
    });

    for (const days of DASHBOARD_WINDOWS) {
      const projected = readySnapshot(buffer, days, subscriptions);
      const rawSummary = dashboardSummary(buffer.database, days);
      const rawSessions = dashboardSessions(buffer.database, days);
      const rawRepos = dashboardReposWithTail(buffer.database, days);
      const rawAccounts = dashboardAccounts(buffer.database, subscriptions, days);
      const pt = projected.summary.totals as Record<string, number>;
      const rt = rawSummary.totals as Record<string, number>;
      for (const field of ["events", "tokenEvents", "inputTokens", "outputTokens", "cacheReadTokens",
        "cacheCreationTokens", "sessions", "sessionsWithTokens"] as const) {
        assert.equal(pt[field], rt[field], `${days}d summary.${field}`);
      }
      const nanoTolerance = Math.max(1e-9, Number(rt.events) * 0.5e-9);
      near(Number(pt.costUsd), Number(rt.costUsd), nanoTolerance, `${days}d summary.costUsd`);
      assert.equal((projected.sessions as unknown[]).length, rawSessions.length, `${days}d session rows`);

      const projectedRepos = byKey(projected.repos as Array<Record<string, unknown>>, "repoHash");
      const referenceRepos = byKey(rawRepos as Array<Record<string, unknown>>, "repoHash");
      assert.deepEqual([...projectedRepos.keys()].sort(), [...referenceRepos.keys()].sort(), `${days}d repo keys`);
      for (const [key, reference] of referenceRepos) {
        const actual = projectedRepos.get(key)!;
        for (const field of ["sessions", "branchRefs", "inputTokens", "outputTokens"] as const) {
          assert.equal(actual[field], reference[field], `${days}d repo ${key} ${field}`);
        }
        near(Number(actual.costUsd), Number(reference.costUsd), nanoTolerance, `${days}d repo ${key} cost`);
      }
      const projectedAccounts = projected.accounts as typeof rawAccounts;
      for (const field of ["priorityUsd", "otherUsd", "unlinkedUsd"] as const) {
        near(Number(projectedAccounts.buckets[field]), Number(rawAccounts.buckets[field]), nanoTolerance,
          `${days}d account bucket ${field}`);
      }
      check(`raw_projection_parity_${days}d`, true, {
        events: pt.events,
        sessions: pt.sessions,
        repos: projected.repos.length,
        costToleranceUsd: nanoTolerance,
      });
    }

    const snapshot30 = readySnapshot(buffer, 30, subscriptions);
    const sessionRow = snapshot30.sessions.find((row) => row.repoHash === repoA);
    assert.ok(sessionRow);
    const detail = buffer.projection.sessionDetail(String(sessionRow.sessionId));
    const repoDetail = buffer.projection.repoDetail(repoA, 30);
    check("indexed_fact_details_preserve_receipts", Boolean(
      detail && detail.receipts.linkage.length >= 1 && repoDetail && repoDetail.totals.events >= 1,
    ), { sessionHash: String(sessionRow.sessionId).slice(0, 16), repoEvents: repoDetail?.totals.events });

    buffer.database.prepare(
      `insert into buffered_events
       (id,source,event_type,data_mode,observed_at,payload_json,suppressed_fields_json,created_at,
        session_id,action_class,model,repo_hash,branch_hash,head_sha,machine,account_hash)
       values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
    ).run(uuid(eventSequence++),"codex","assistant_response","metadata",NOW.toISOString(),
      JSON.stringify({apiKey:API_KEY_SENTINEL,workingDirectory:PATH_SENTINEL}),
      JSON.stringify(["apiKey","workingDirectory"]),NOW.toISOString(),PATH_SENTINEL,PATH_SENTINEL,
      API_KEY_SENTINEL,PATH_SENTINEL,PATH_SENTINEL,PATH_SENTINEL,PATH_SENTINEL,API_KEY_SENTINEL);
    settle(buffer);

    const factColumns = (buffer.database.pragma("table_info(dashboard_event_facts)") as Array<{ name: string }>).map((row) => row.name);
    const forbiddenColumns = factColumns.filter((name) => /payload|content|path|url|email|label|event_id|session_id|machine$/.test(name));
    const persistedProjectionText = [
      projectionTableText(buffer.database, "dashboard_event_facts"),
      projectionTableText(buffer.database, "dashboard_snapshots"),
      projectionTableText(buffer.database, "dashboard_source_lifetime"),
      projectionTableText(buffer.database, "dashboard_projection_repairs"),
      projectionTableText(buffer.database, "dashboard_compact_day_source"),
      projectionTableText(buffer.database, "dashboard_compact_gc_days"),
      projectionTableText(buffer.database, "dashboard_compact_gc_source_scratch"),
    ].join("\n");
    check("projection_facts_and_snapshots_are_privacy_safe",
      forbiddenColumns.length === 0 &&
      !persistedProjectionText.includes(MACHINE_SENTINEL) &&
      !persistedProjectionText.includes(RAW_SENTINEL) &&
      !persistedProjectionText.includes(EMAIL_SENTINEL) &&
      !persistedProjectionText.includes(URL_SENTINEL) &&
      !persistedProjectionText.includes(API_KEY_SENTINEL) &&
      !persistedProjectionText.includes(PATH_SENTINEL) &&
      !persistedProjectionText.includes(sessionA),
      { factColumns, forbiddenColumns, persistedBytes: Buffer.byteLength(persistedProjectionText) });

    const generationBeforePresentation = readySnapshot(buffer,30,subscriptions).generation;
    buffer.setAccountLabel(accountA, "Projection Person Renamed");
    const presentation = readySnapshot(buffer, 30, subscriptions);
    check("label_email_subscription_invalidation_is_presentation_only",
      presentation.generation === generationBeforePresentation &&
      presentation.accounts.accounts.some((row) => row.label === "Projection Person Renamed" && row.subscription),
      { generation: presentation.generation });

    const costBefore = Number((readySnapshot(buffer, 30).summary.totals as Record<string, number>).costUsd);
    const correctedId = multiRepo[2]!.id;
    buffer.database.prepare(`update buffered_events set cost_usd=3.7 where id=?`).run(correctedId);
    const stale = buffer.projection.readSnapshot(30);
    check("direct_sql_marks_last_generation_stale_without_raw_fallback",
      stale.kind === "ready" && stale.snapshot.projection.status === "stale",
      { state: stale.kind === "ready" ? stale.snapshot.projection : stale });
    settle(buffer);
    const costAfter = Number((readySnapshot(buffer, 30).summary.totals as Record<string, number>).costUsd);
    near(costAfter - costBefore, 1.5, 1e-9, "corrected event old-minus/new-plus");
    check("corrected_event_applies_old_minus_new_plus", true, { costBefore, costAfter });

    const late = event({ sessionId: "late-arrival", observedAt: new Date(NOW.getTime() - 2 * DAY_MS).toISOString(),
      inputTokens: 77, outputTokens: 7, costUsd: 0.77, repoHash: repoB });
    buffer.append(late);
    settle(buffer);
    const lateSnapshot = readySnapshot(buffer, 30);
    check("late_event_enters_every_eligible_window_once",
      Number((lateSnapshot.summary.totals as Record<string, number>).inputTokens) >= 77 &&
      (buffer.database.prepare(`select count(*) as n from dashboard_event_facts where raw_rowid=(select rowid from buffered_events where id=?)`).get(late.id) as {n:number}).n === 1,
      { generation: lateSnapshot.generation });

    buffer.projection.failNextApplyForProof();
    const failedProjectionEvent = event({ sessionId: "projection-failure", inputTokens: 33, outputTokens: 3, costUsd: 0.33 });
    buffer.append(failedProjectionEvent);
    const repairState = buffer.projection.status();
    check("projection_failure_commits_raw_and_durable_repair",
      (buffer.database.prepare(`select count(*) as n from buffered_events where id=?`).get(failedProjectionEvent.id) as {n:number}).n === 1 &&
      repairState.backlog.repairs === 1 && repairState.degraded,
      { repairState });
    settle(buffer);
    check("projection_repair_replay_is_exact_and_idempotent",
      (buffer.database.prepare(`select count(*) as n from dashboard_event_facts where raw_rowid=(select rowid from buffered_events where id=?)`).get(failedProjectionEvent.id) as {n:number}).n === 1,
      buffer.projection.status());

    const config = collectorConfigSchema.parse({ subscriptions });
    const server = createCollectorServer(config, buffer);
    const port = await listen(server);
    try {
      const base = `http://127.0.0.1:${port}`;
      const first = await fetch(`${base}/api/snapshot?days=30`);
      const firstBody = await first.json() as Record<string, unknown>;
      const etag = first.headers.get("etag");
      const second = await fetch(`${base}/api/snapshot?days=30`, { headers: { "if-none-match": etag! } });
      const generations = await Promise.all(["summary", "sessions", "repos", "accounts"].map(async (surface) => {
        const response = await fetch(`${base}/api/${surface}?days=30`);
        return response.headers.get("x-plimsoll-projection-generation");
      }));
      const unsupported = await fetch(`${base}/api/snapshot?days=31`);
      check("atomic_snapshot_etag_and_compatibility_generation",
        first.status === 200 && typeof firstBody.generation === "number" && second.status === 304 &&
        new Set(generations).size === 1 && generations[0] === String(firstBody.generation) && unsupported.status === 400,
        { generation: firstBody.generation, etag, generations, unsupported: unsupported.status });

      const originalReaddir = fs.readdirSync;
      const originalStat = fs.statSync;
      (fs as unknown as { readdirSync: typeof fs.readdirSync }).readdirSync = (() => {
        throw new Error("filesystem_request_scan_forbidden");
      }) as typeof fs.readdirSync;
      (fs as unknown as { statSync: typeof fs.statSync }).statSync = (() => {
        throw new Error("filesystem_request_scan_forbidden");
      }) as typeof fs.statSync;
      let forcedSnapshot: Response;
      let forcedStatus: Response;
      try {
        [forcedSnapshot, forcedStatus] = await Promise.all([
          fetch(`${base}/api/snapshot?days=30`),
          fetch(`${base}/status`),
        ]);
      } finally {
        (fs as unknown as { readdirSync: typeof fs.readdirSync }).readdirSync = originalReaddir;
        (fs as unknown as { statSync: typeof fs.statSync }).statSync = originalStat;
      }
      check("snapshot_and_status_do_not_touch_filesystem", forcedSnapshot.ok && forcedStatus.ok, {
        snapshot: forcedSnapshot.status,
        status: forcedStatus.status,
      });

      const warmDurations: number[] = [];
      for (let index = 0; index < 25; index += 1) {
        const started = performance.now();
        const response = await fetch(`${base}/api/snapshot?days=30`);
        assert.ok(response.ok);
        await response.arrayBuffer();
        if (index >= 5) warmDurations.push(performance.now() - started);
      }
      const counters = buffer.projection.workCounters();
      check("warm_snapshot_budget_uses_deterministic_zero_scan_counters",
        p95(warmDurations) <= 500 && counters.rawRowsScannedByDashboard === 0 &&
        counters.filesystemEntriesScannedByDashboard === 0,
        { p95Ms: Number(p95(warmDurations).toFixed(3)), counters });

      const generationBeforeScale = Number(firstBody.generation);
      const factsBeforeScale = (buffer.database.prepare(`select count(*) as n from dashboard_event_facts`).get() as {n:number}).n;
      const rawInsert = buffer.database.prepare(
        `insert into buffered_events
         (id,source,event_type,data_mode,observed_at,payload_json,suppressed_fields_json,created_at)
         values (?,?,?,?,?,?,?,?)`,
      );
      buffer.database.transaction(() => {
        for (let index = 0; index < 50_000; index += 1) {
          const id = `scale-raw-${index}`;
          rawInsert.run(id, "unknown", "other", "metadata", "2000-01-01T00:00:00.000Z",
            JSON.stringify({ id }), "[]", "2000-01-01T00:00:00.000Z");
        }
      })();
      const scaledDurations: number[] = [];
      for (let index = 0; index < 20; index += 1) {
        const started = performance.now();
        const response = await fetch(`${base}/api/snapshot?days=30`);
        assert.ok(response.ok);
        const body = await response.json() as { generation: number };
        assert.equal(body.generation, generationBeforeScale);
        scaledDurations.push(performance.now() - started);
      }
      const factsAfterScale = (buffer.database.prepare(`select count(*) as n from dashboard_event_facts`).get() as {n:number}).n;
      check("raw_history_growth_does_not_change_snapshot_work_shape",
        factsBeforeScale === factsAfterScale && p95(scaledDurations) <= 500 &&
        buffer.projection.workCounters().rawRowsScannedByDashboard === 0,
        { rawRowsAdded: 50_000, factCardinality: factsAfterScale,
          p95Ms: Number(p95(scaledDurations).toFixed(3)), generation: generationBeforeScale });
    } finally {
      await closeServer(server);
    }

    const html = fs.readFileSync(path.join(process.cwd(), "packages/collector-cli/src/dashboard.html"), "utf8");
    const inline = html.split("<script>")[1]?.split("</" + "script>")[0] ?? "";
    new Function(inline);
    const serverSource = fs.readFileSync(path.join(process.cwd(), "packages/collector-cli/src/server.ts"), "utf8");
    const readSnapshotSource = DashboardProjectionStore.prototype.readSnapshot.toString();
    check("ui_uses_one_snapshot_request_and_resize_only_redraws_cache",
      html.includes('fetch("/api/snapshot"+q') &&
      !html.includes('fetch("/api/summary"+q') &&
      html.includes('window.addEventListener("resize",()=>refresh(true))'),
      { inlineBytes: Buffer.byteLength(inline) });
    check("production_request_seams_have_no_raw_sql_or_recursive_health",
      !serverSource.includes("dashboardSummary(") &&
      !serverSource.includes("computeCaptureHealth") &&
      !readSnapshotSource.includes("buffered_events") &&
      !readSnapshotSource.includes("readdir")&&
      !readSnapshotSource.includes("decodeCompact")&&
      !readSnapshotSource.includes("gunzip"),
      { serverBytes: Buffer.byteLength(serverSource), readSnapshotBytes: Buffer.byteLength(readSnapshotSource) });

    // Separate expiry/clock fixture keeps the boundary proof independent from
    // the scale-shape repair backlog above.
    const expiryPath = path.join(root, "expiry.sqlite");
    const expiry = new LocalEventBuffer(expiryPath);
    try {
      expiry.append(event({ sessionId: "expiry", observedAt: new Date(NOW.getTime() - 29 * DAY_MS).toISOString(),
        inputTokens: 10, outputTokens: 1, costUsd: 1 }));
      settle(expiry, NOW);
      assert.equal(Number((readySnapshot(expiry, 30).summary.totals as Record<string, number>).events), 1);
      const advanced = new Date(NOW.getTime() + 2 * DAY_MS);
      expiry.projection.runMaintenance(advanced);
      settle(expiry, advanced);
      assert.equal(Number((readySnapshot(expiry, 30).summary.totals as Record<string, number>).events), 0);
      expiry.projection.runMaintenance(new Date(NOW.getTime() - DAY_MS));
      const rollback = expiry.projection.readSnapshot(30);
      check("expiry_boundary_and_clock_rollback_serve_last_coherent_generation",
        rollback.kind === "ready" && rollback.snapshot.projection.status === "stale" &&
        rollback.snapshot.projection.degradedReason === "projection_clock_rollback",
        { rollback: rollback.kind === "ready" ? rollback.snapshot.projection : rollback });
    } finally {
      expiry.close();
    }

    const clockPath=path.join(root,"clock-only.sqlite");
    const clockOnly=new LocalEventBuffer(clockPath);
    const clockBefore=readySnapshot(clockOnly,30);
    const clockAdvanced=new Date(NOW.getTime()+60_000);
    const clockReceipt=clockOnly.projection.runMaintenance(clockAdvanced);
    const clockAfter=readySnapshot(clockOnly,30);
    check("unchanged_clock_advance_updates_cutoff_without_snapshot_rebuild",
      clockReceipt.expiryFacts===0&&clockReceipt.snapshotBuilds===0&&
      clockAfter.generation===clockBefore.generation&&
      clockAfter.window.since===new Date(clockAdvanced.getTime()-30*DAY_MS).toISOString(),
      {generation:clockAfter.generation,since:clockAfter.window.since,receipt:clockReceipt});
    clockOnly.close();

    const compactExpiryPath=path.join(root,"compact-expiry.sqlite");
    const compactExpirySeed=new LocalEventBuffer(compactExpiryPath);
    compactExpirySeed.close();
    const compactExpiryDb=new Database(compactExpiryPath);
    dropProjectionState(compactExpiryDb);
    const compactExpiryInsert=compactExpiryDb.prepare(
      `insert into buffered_events
       (id,source,event_type,data_mode,observed_at,payload_json,suppressed_fields_json,created_at)
       values (?,?,?,?,?,?,?,?)`,
    );
    compactExpiryDb.transaction(()=>{
      for(let index=0;index<2_500;index++)compactExpiryInsert.run(`compact-expiry-${index}`,"codex","otel_span",
        "metadata",new Date(NOW.getTime()-29*DAY_MS).toISOString(),"{}","[]",NOW.toISOString());
    })();
    compactExpiryDb.close();
    const compactExpiry=new LocalEventBuffer(compactExpiryPath);
    settle(compactExpiry,NOW,20);
    const compactExpiryAdvanced=new Date(NOW.getTime()+2*DAY_MS);
    const compactExpiryReceipts=[compactExpiry.projection.runMaintenance(compactExpiryAdvanced)];
    const staleCompactExpiry=compactExpiry.projection.readSnapshot(30);
    compactExpiryReceipts.push(...settle(compactExpiry,compactExpiryAdvanced,20));
    const compactExpiryAfter=readySnapshot(compactExpiry,30);
    check("compact_expiry_is_bounded_and_serves_prior_generation_until_complete",
      compactExpiryReceipts.length>=3&&compactExpiryReceipts.every((receipt)=>receipt.expiryFacts<=1_000)&&
      staleCompactExpiry.kind==="ready"&&staleCompactExpiry.snapshot.projection.status==="stale"&&
      Number((compactExpiryAfter.summary.totals as Record<string,number>).events)===0,
      {receipts:compactExpiryReceipts.map((receipt)=>({expiry:receipt.expiryFacts,backlog:receipt.backlog.expiryWindows})),
        finalGeneration:compactExpiryAfter.generation});
    compactExpiry.close();

    // Generic sessionless zero-value spans dominate the live raw history. They
    // must backfill into compressed segments, not one six-index fact per span.
    const compactPath=path.join(root,"compact.sqlite");
    const compactSeed=new LocalEventBuffer(compactPath);
    compactSeed.close();
    const compactDb=new Database(compactPath);
    dropProjectionState(compactDb);
    const compactInsert=compactDb.prepare(
      `insert into buffered_events
       (id,source,event_type,data_mode,observed_at,payload_json,suppressed_fields_json,created_at,action_class)
       values (?,?,?,?,?,?,?,?,?)`,
    );
    compactDb.transaction(()=>{
      for(let index=0;index<100_000;index++)compactInsert.run(`compact-${index}`,"codex","otel_span",
        "metadata",new Date(NOW.getTime()-(index%10)*DAY_MS).toISOString(),"{}","[]",NOW.toISOString(),
        index%20===0?"read":null);
    })();
    compactDb.pragma("wal_checkpoint(TRUNCATE)");
    compactDb.close();
    const compactBaseBytes=fs.statSync(compactPath).size;
    let compact=new LocalEventBuffer(compactPath);
    const compactReceipts=settle(compact,NOW,240);
    const compactFacts=(compact.database.prepare(`select count(*) as n from dashboard_event_facts`).get() as {n:number}).n;
    const compactSegments=(compact.database.prepare(`select count(*) as n from dashboard_compact_segments`).get() as {n:number}).n;
    const projectionBytes=(compact.database.prepare(
      `select coalesce(sum(pgsize),0) as n from dbstat where name like 'dashboard_%'`,
    ).get() as {n:number}).n;
    compact.database.pragma("wal_checkpoint(TRUNCATE)");
    const compactFileDeltaBytes=fs.statSync(compactPath).size-compactBaseBytes;
    const compactSnapshot=readySnapshot(compact,30);
    check("generic_zero_value_spans_use_bounded_compressed_projection_storage",
      compactFacts===0&&compactSegments<=1_000&&projectionBytes/100_000<=128&&
      compactFileDeltaBytes/100_000<=32&&
      Number((compactSnapshot.summary.totals as Record<string,number>).events)===100_000&&
      compactReceipts.every((receipt)=>receipt.backfillRowsVisited+receipt.parityRowsVisited<=1_000),
      {rawRows:100_000,factRows:compactFacts,segments:compactSegments,projectionBytes,
        sqliteFileDeltaBytes:compactFileDeltaBytes,
        bytesPerRaw:Number((projectionBytes/100_000).toFixed(2)),
        fileDeltaBytesPerRaw:Number((compactFileDeltaBytes/100_000).toFixed(2)),slices:compactReceipts.length});

    const activePath=path.join(root,"active-compact.sqlite");
    const active=new LocalEventBuffer(activePath);
    const activeAppendStarted=performance.now();
    for(let index=0;index<5_000;index++)active.append(event({
      source:"codex",eventType:"assistant_response",actionClass:"other",
      observedAt:new Date(NOW.getTime()-DAY_MS+index).toISOString(),
    }));
    const activeAppendMs=performance.now()-activeAppendStarted;
    const activeBefore=active.projection.status();
    const activeSegmentsBefore=(active.database.prepare(
      `select count(*) as n from dashboard_compact_segments`,
    ).get() as {n:number}).n;
    const activeStale=active.projection.readSnapshot(30);
    check("active_compactable_append_defers_compression_to_durable_repair_batch",
      activeBefore.backlog.repairs===5_000&&activeSegmentsBefore===0&&activeBefore.dirty&&
      activeStale.kind==="ready"&&activeStale.snapshot.projection.status==="stale",
      {appendRows:5_000,appendMs:Number(activeAppendMs.toFixed(2)),segmentsBefore:activeSegmentsBefore,
        backlog:activeBefore.backlog});
    const activeReceipts=settle(active,NOW,40);
    const activeSegments=(active.database.prepare(
      `select count(*) as n from dashboard_compact_segments`,
    ).get() as {n:number}).n;
    const activeProjected=readySnapshot(active,30);
    check("active_5k_compactable_rows_use_repair_day_bounded_segments",
      activeSegments<=25&&activeReceipts.every((receipt)=>receipt.repairRowsVisited<=250&&
        receipt.compactSegmentsWritten<=1&&receipt.compactGcItemsVisited<=1_000)&&
      Number((activeProjected.summary.totals as Record<string,number>).events)===5_000,
      {rows:5_000,segments:activeSegments,slices:activeReceipts.length,
        maxRepairRows:Math.max(0,...activeReceipts.map((receipt)=>receipt.repairRowsVisited)),
        maxSegmentsWritten:Math.max(0,...activeReceipts.map((receipt)=>receipt.compactSegmentsWritten)),
        maxGcItems:Math.max(0,...activeReceipts.map((receipt)=>receipt.compactGcItemsVisited))});
    active.close();

    const mutationDependency=compactMutationRepairDependencyFixture(root,"ordered",false);
    check("compact_mutation_dependency_blocks_low_updates_without_starving_high_deletes",
      mutationDependency.queued.compactMutations===350&&mutationDependency.queued.repairs===350&&
      mutationDependency.firstReceipt.repairRowsVisited===250&&
      mutationDependency.repairSelectionPlan.some((row)=>
        row.includes("idx_dashboard_projection_repairs_queued"))&&
      mutationDependency.repairSelectionPlan.some((row)=>
        row.includes("SEARCH m USING INTEGER PRIMARY KEY"))&&
      mutationDependency.afterFirst.blockedLowUpdateRepairs===100&&
      mutationDependency.afterFirst.eligibleHighDeleteRepairs===0&&
      mutationDependency.afterFirst.mutations===100&&
      mutationDependency.afterFirst.cancellations===250&&
      JSON.stringify(mutationDependency.finalState)===JSON.stringify({raw:3750,projected:3750,
        payloadItems:3750,repairs:0,mutations:0,cancellations:0,jobs:0})&&
      mutationDependency.updatedPayload.length===100&&
      mutationDependency.updatedPayload.every((item)=>item.source==="claude_code"&&
        item.eventType==="tool_use"&&item.actionClass==="edit"&&
        item.observedAt===mutationDependency.updatedObservedAt)&&
      JSON.stringify(mutationDependency.sources)===JSON.stringify([
        {source:"codex",events:3650},{source:"claude_code",events:100},
      ].sort((a,b)=>a.source.localeCompare(b.source)))&&
      mutationDependency.receipts.every((receipt)=>receipt.repairRowsVisited<=250&&
        receipt.compactGcItemsVisited<=1_000),
      mutationDependency);

    const mutationDependencyReopen=compactMutationRepairDependencyFixture(root,"reopen",true);
    check("compact_mutation_dependency_reopen_readds_every_current_update_once",
      mutationDependencyReopen.afterFirst.blockedLowUpdateRepairs===100&&
      mutationDependencyReopen.afterFirst.eligibleHighDeleteRepairs===0&&
      JSON.stringify(mutationDependencyReopen.finalState)===JSON.stringify({raw:3750,projected:3750,
        payloadItems:3750,repairs:0,mutations:0,cancellations:0,jobs:0})&&
      mutationDependencyReopen.updatedPayload.length===100&&
      mutationDependencyReopen.updatedPayload.every((item)=>item.source==="claude_code"&&
        item.eventType==="tool_use"&&item.actionClass==="edit")&&
      mutationDependencyReopen.receipts.every((receipt)=>receipt.repairRowsVisited<=250&&
        receipt.compactGcItemsVisited<=1_000),
      mutationDependencyReopen);

    const pendingPath=path.join(root,"pending-compact-lifecycle.sqlite");
    let pending=new LocalEventBuffer(pendingPath);
    const pendingEvent=event({source:"codex",eventType:"assistant_response",actionClass:"other",
      observedAt:new Date(NOW.getTime()-DAY_MS).toISOString()});
    pending.append(pendingEvent);
    const pendingRawRowid=(pending.database.prepare(
      `select rowid as rawRowid from buffered_events where id=?`,
    ).get(pendingEvent.id) as {rawRowid:number}).rawRowid;
    const pendingInsertReason=(pending.database.prepare(
      `select reason from dashboard_projection_repairs where raw_rowid=?`,
    ).get(pendingRawRowid) as {reason:string}).reason;
    pending.close();
    pending=new LocalEventBuffer(pendingPath);
    pending.database.prepare(
      `update buffered_events set source='claude_code',event_type='tool_use',
        action_class='read',observed_at=? where rowid=?`,
    ).run(new Date(NOW.getTime()-2*DAY_MS).toISOString(),pendingRawRowid);
    pending.close();
    pending=new LocalEventBuffer(pendingPath);
    const pendingFinalObservedAt=new Date(NOW.getTime()-3*DAY_MS).toISOString();
    pending.database.prepare(
      `update buffered_events set action_class='edit',observed_at=? where rowid=?`,
    ).run(pendingFinalObservedAt,pendingRawRowid);
    const pendingReasonBeforeDrain=(pending.database.prepare(
      `select reason from dashboard_projection_repairs where raw_rowid=?`,
    ).get(pendingRawRowid) as {reason:string}).reason;
    const pendingBacklogBeforeDrain=pending.projection.status().backlog;
    pending.close();
    pending=new LocalEventBuffer(pendingPath);
    const pendingReceipts=settle(pending,NOW,20);
    const pendingProjected=readySnapshot(pending,30).summary.totals as Record<string,number>;
    const pendingPayload=compactItems(pending.database);
    const pendingRemaining={
      repairs:(pending.database.prepare(`select count(*) as n from dashboard_projection_repairs`).get() as {n:number}).n,
      mutations:(pending.database.prepare(`select count(*) as n from dashboard_compact_mutations`).get() as {n:number}).n,
      cancellations:(pending.database.prepare(`select count(*) as n from dashboard_compact_cancellations`).get() as {n:number}).n,
    };
    pending.close();
    pending=new LocalEventBuffer(pendingPath);
    const pendingReplay=settle(pending,NOW,5);
    check("pending_compact_insert_updates_preserve_never_projected_receipt_and_apply_latest_once",
      pendingInsertReason==="raw_insert"&&pendingReasonBeforeDrain==="raw_insert"&&
      pendingBacklogBeforeDrain.repairs===1&&pendingBacklogBeforeDrain.compactMutations===0&&
      pendingReceipts.reduce((sum,row)=>sum+row.repairRowsVisited,0)===1&&
      pendingProjected.events===1&&pendingPayload.length===1&&
      pendingPayload[0]?.rawRowid===pendingRawRowid&&
      pendingPayload[0]?.source==="claude_code"&&pendingPayload[0]?.eventType==="tool_use"&&
      pendingPayload[0]?.actionClass==="edit"&&pendingPayload[0]?.observedAt===pendingFinalObservedAt&&
      Object.values(pendingRemaining).every((value)=>value===0)&&pendingReplay.length===0,
      {pendingInsertReason,pendingReasonBeforeDrain,pendingBacklogBeforeDrain,
        receipts:pendingReceipts.length,projectedEvents:pendingProjected.events,
        payload:pendingPayload,remaining:pendingRemaining,replaySlices:pendingReplay.length});
    pending.close();

    const promotePath=path.join(root,"pending-compact-promote.sqlite");
    let promote=new LocalEventBuffer(promotePath);
    const promoteEvent=event({source:"codex",eventType:"assistant_response",actionClass:"other"});
    promote.append(promoteEvent);
    const promoteRawRowid=(promote.database.prepare(
      `select rowid as rawRowid from buffered_events where id=?`,
    ).get(promoteEvent.id) as {rawRowid:number}).rawRowid;
    promote.close();
    promote=new LocalEventBuffer(promotePath);
    promote.database.prepare(
      `update buffered_events set session_id='pending-promote-session',model='promoted-model',
        input_tokens=7,output_tokens=2,cost_usd=0.000009 where rowid=?`,
    ).run(promoteRawRowid);
    const promoteReason=(promote.database.prepare(
      `select reason from dashboard_projection_repairs where raw_rowid=?`,
    ).get(promoteRawRowid) as {reason:string}).reason;
    promote.close();
    promote=new LocalEventBuffer(promotePath);
    settle(promote,NOW,20);
    const promoteTotals=readySnapshot(promote,30).summary.totals as Record<string,number>;
    const promoteState={
      facts:(promote.database.prepare(`select count(*) as n from dashboard_event_facts`).get() as {n:number}).n,
      compactItems:compactItems(promote.database).length,
      repairs:(promote.database.prepare(`select count(*) as n from dashboard_projection_repairs`).get() as {n:number}).n,
      mutations:(promote.database.prepare(`select count(*) as n from dashboard_compact_mutations`).get() as {n:number}).n,
    };
    promote.close();
    promote=new LocalEventBuffer(promotePath);
    const promoteReplay=settle(promote,NOW,5);
    check("pending_compact_insert_promote_applies_one_full_fact_after_reopen",
      promoteReason==="raw_insert"&&promoteState.facts===1&&promoteState.compactItems===0&&
      promoteState.repairs===0&&promoteState.mutations===0&&promoteTotals.events===1&&
      promoteTotals.tokenEvents===1&&promoteTotals.inputTokens===7&&promoteTotals.outputTokens===2&&
      promoteReplay.length===0,
      {promoteReason,promoteState,promoteTotals,replaySlices:promoteReplay.length});
    promote.close();

    const pendingDeletePath=path.join(root,"pending-compact-delete.sqlite");
    let pendingDelete=new LocalEventBuffer(pendingDeletePath);
    const pendingDeleteEvent=event({source:"codex",eventType:"assistant_response",actionClass:"other"});
    pendingDelete.append(pendingDeleteEvent);
    const pendingDeleteRawRowid=(pendingDelete.database.prepare(
      `select rowid as rawRowid from buffered_events where id=?`,
    ).get(pendingDeleteEvent.id) as {rawRowid:number}).rawRowid;
    pendingDelete.close();
    pendingDelete=new LocalEventBuffer(pendingDeletePath);
    pendingDelete.database.prepare(`delete from buffered_events where rowid=?`).run(pendingDeleteRawRowid);
    pendingDelete.close();
    pendingDelete=new LocalEventBuffer(pendingDeletePath);
    settle(pendingDelete,NOW,20);
    const pendingDeleteTotals=readySnapshot(pendingDelete,30).summary.totals as Record<string,number>;
    const pendingDeleteState={
      raw:(pendingDelete.database.prepare(`select count(*) as n from buffered_events`).get() as {n:number}).n,
      facts:(pendingDelete.database.prepare(`select count(*) as n from dashboard_event_facts`).get() as {n:number}).n,
      segments:(pendingDelete.database.prepare(`select count(*) as n from dashboard_compact_segments`).get() as {n:number}).n,
      repairs:(pendingDelete.database.prepare(`select count(*) as n from dashboard_projection_repairs`).get() as {n:number}).n,
      mutations:(pendingDelete.database.prepare(`select count(*) as n from dashboard_compact_mutations`).get() as {n:number}).n,
      cancellations:(pendingDelete.database.prepare(`select count(*) as n from dashboard_compact_cancellations`).get() as {n:number}).n,
    };
    check("pending_compact_insert_delete_settles_to_physical_zero_after_reopen",
      pendingDeleteTotals.events===0&&Object.values(pendingDeleteState).every((value)=>value===0),
      {pendingDeleteTotals,pendingDeleteState});
    pendingDelete.close();

    const failedPendingPath=path.join(root,"failed-pending-compact.sqlite");
    let failedPending=new LocalEventBuffer(failedPendingPath);
    failedPending.projection.failNextApplyForProof();
    const failedPendingEvent=event({source:"codex",eventType:"assistant_response",actionClass:"other"});
    failedPending.append(failedPendingEvent);
    const failedPendingRawRowid=(failedPending.database.prepare(
      `select rowid as rawRowid from buffered_events where id=?`,
    ).get(failedPendingEvent.id) as {rawRowid:number}).rawRowid;
    const failedPendingInitialReason=(failedPending.database.prepare(
      `select reason from dashboard_projection_repairs where raw_rowid=?`,
    ).get(failedPendingRawRowid) as {reason:string}).reason;
    failedPending.close();
    failedPending=new LocalEventBuffer(failedPendingPath);
    failedPending.database.prepare(
      `update buffered_events set source='claude_code',event_type='tool_use',action_class='read' where rowid=?`,
    ).run(failedPendingRawRowid);
    const failedPendingUpdatedReason=(failedPending.database.prepare(
      `select reason from dashboard_projection_repairs where raw_rowid=?`,
    ).get(failedPendingRawRowid) as {reason:string}).reason;
    failedPending.close();
    failedPending=new LocalEventBuffer(failedPendingPath);
    settle(failedPending,NOW,20);
    const failedPendingPayload=compactItems(failedPending.database);
    check("failed_initial_compact_apply_update_preserves_never_projected_state_across_reopen",
      failedPendingInitialReason==="projection_apply_failed"&&
      failedPendingUpdatedReason==="projection_apply_failed"&&failedPendingPayload.length===1&&
      failedPendingPayload[0]?.source==="claude_code"&&failedPendingPayload[0]?.eventType==="tool_use"&&
      failedPendingPayload[0]?.actionClass==="read",
      {failedPendingInitialReason,failedPendingUpdatedReason,payload:failedPendingPayload});

    failedPending.database.prepare(
      `update buffered_events set suppressed_fields_json='["privacy"]' where rowid=?`,
    ).run(failedPendingRawRowid);
    const irrelevantBacklog=failedPending.projection.status().backlog;
    failedPending.close();
    failedPending=new LocalEventBuffer(failedPendingPath);
    settle(failedPending,NOW,20);
    const irrelevantPayload=compactItems(failedPending.database);
    const irrelevantTotals=readySnapshot(failedPending,30).summary.totals as Record<string,number>;
    check("already_projected_compact_irrelevant_update_is_noop_not_duplicate",
      irrelevantBacklog.repairs===1&&irrelevantBacklog.compactMutations===0&&
      irrelevantPayload.length===1&&irrelevantTotals.events===1,
      {irrelevantBacklog,payload:irrelevantPayload});

    const meaningfulObservedAt=new Date(NOW.getTime()-4*DAY_MS).toISOString();
    failedPending.database.prepare(
      `update buffered_events set source='codex',event_type='tool_use',action_class='shell',
        observed_at=? where rowid=?`,
    ).run(meaningfulObservedAt,failedPendingRawRowid);
    const meaningfulBacklog=failedPending.projection.status().backlog;
    failedPending.close();
    failedPending=new LocalEventBuffer(failedPendingPath);
    settle(failedPending,NOW,20);
    const meaningfulPayload=compactItems(failedPending.database);
    const meaningfulRemaining={
      repairs:(failedPending.database.prepare(`select count(*) as n from dashboard_projection_repairs`).get() as {n:number}).n,
      mutations:(failedPending.database.prepare(`select count(*) as n from dashboard_compact_mutations`).get() as {n:number}).n,
      cancellations:(failedPending.database.prepare(`select count(*) as n from dashboard_compact_cancellations`).get() as {n:number}).n,
      gcDays:(failedPending.database.prepare(`select count(*) as n from dashboard_compact_gc_days`).get() as {n:number}).n,
    };
    const meaningfulSources=failedPending.database.prepare(
      `select source,events from dashboard_source_window where days=30 and events!=0 order by source`,
    ).all();
    const meaningfulTotals=readySnapshot(failedPending,30).summary.totals as Record<string,number>;
    check("already_projected_compact_meaningful_update_old_minus_new_plus_is_exact",
      meaningfulBacklog.repairs===1&&meaningfulBacklog.compactMutations===1&&
      meaningfulPayload.length===1&&meaningfulPayload[0]?.source==="codex"&&
      meaningfulPayload[0]?.eventType==="tool_use"&&meaningfulPayload[0]?.actionClass==="shell"&&
      meaningfulPayload[0]?.observedAt===meaningfulObservedAt&&
      Object.values(meaningfulRemaining).every((value)=>value===0)&&
      JSON.stringify(meaningfulSources)===JSON.stringify([{source:"codex",events:1}])&&
      meaningfulTotals.events===1,
      {meaningfulBacklog,payload:meaningfulPayload,remaining:meaningfulRemaining,
        meaningfulSources});

    failedPending.database.prepare(
      `update buffered_events set suppressed_fields_json='["privacy","coalesced"]' where rowid=?`,
    ).run(failedPendingRawRowid);
    const coalescedObservedAt=new Date(NOW.getTime()-5*DAY_MS).toISOString();
    failedPending.database.prepare(
      `update buffered_events set source='claude_code',action_class='edit',observed_at=? where rowid=?`,
    ).run(coalescedObservedAt,failedPendingRawRowid);
    const coalescedBacklog=failedPending.projection.status().backlog;
    failedPending.close();
    failedPending=new LocalEventBuffer(failedPendingPath);
    settle(failedPending,NOW,20);
    const coalescedPayload=compactItems(failedPending.database);
    check("projected_irrelevant_then_meaningful_update_coalesces_one_old_state_mutation",
      coalescedBacklog.repairs===1&&coalescedBacklog.compactMutations===1&&
      coalescedPayload.length===1&&coalescedPayload[0]?.source==="claude_code"&&
      coalescedPayload[0]?.actionClass==="edit"&&
      coalescedPayload[0]?.observedAt===coalescedObservedAt,
      {coalescedBacklog,payload:coalescedPayload});

    failedPending.database.prepare(
      `update buffered_events set suppressed_fields_json='["privacy","delete"]' where rowid=?`,
    ).run(failedPendingRawRowid);
    failedPending.database.prepare(`delete from buffered_events where rowid=?`).run(failedPendingRawRowid);
    const projectedDeleteBacklog=failedPending.projection.status().backlog;
    failedPending.close();
    failedPending=new LocalEventBuffer(failedPendingPath);
    settle(failedPending,NOW,20);
    const projectedDeleteState={
      raw:(failedPending.database.prepare(`select count(*) as n from buffered_events`).get() as {n:number}).n,
      facts:(failedPending.database.prepare(`select count(*) as n from dashboard_event_facts`).get() as {n:number}).n,
      compactItems:compactItems(failedPending.database).length,
      repairs:(failedPending.database.prepare(`select count(*) as n from dashboard_projection_repairs`).get() as {n:number}).n,
      mutations:(failedPending.database.prepare(`select count(*) as n from dashboard_compact_mutations`).get() as {n:number}).n,
      cancellations:(failedPending.database.prepare(`select count(*) as n from dashboard_compact_cancellations`).get() as {n:number}).n,
    };
    check("projected_irrelevant_update_then_delete_still_cancels_old_compact_state",
      projectedDeleteBacklog.repairs===1&&projectedDeleteBacklog.compactMutations===1&&
      Object.values(projectedDeleteState).every((value)=>value===0),
      {projectedDeleteBacklog,projectedDeleteState});
    failedPending.close();

    const c0UpgradePath=path.join(root,"c0-compact-summary-upgrade.sqlite");
    let c0Upgrade=new LocalEventBuffer(c0UpgradePath);
    for(let index=0;index<100;index++)c0Upgrade.append(event({
      source:index<50?"codex":"claude_code",
      eventType:"assistant_response",
      actionClass:"other",
      observedAt:new Date(NOW.getTime()-(index<50?2:1)*DAY_MS+index).toISOString(),
    }));
    settle(c0Upgrade,NOW,20);
    const cancelledRows=c0Upgrade.database.prepare(
      `select rowid as rawRowid from buffered_events where source='codex' order by rowid limit 10`,
    ).all() as Array<{rawRowid:number}>;
    const removeRaw=c0Upgrade.database.prepare(`delete from buffered_events where rowid=?`);
    c0Upgrade.database.transaction(()=>{
      for(const row of cancelledRows)removeRaw.run(row.rawRowid);
      const hold=c0Upgrade.database.prepare(
        `insert into dashboard_projection_repairs (raw_rowid,reason,queued_at) values (?, ?, ?)`,
      );
      for(let index=0;index<300;index++)hold.run(1_000_000+index,"upgrade-proof-hold",NOW.toISOString());
    })();
    const c0DrainReceipt=c0Upgrade.projection.runMaintenance(NOW);
    c0Upgrade.database.prepare(
      `delete from dashboard_projection_repairs where raw_rowid>=1000000`,
    ).run();
    c0Upgrade.database.prepare(`delete from dashboard_compact_gc_days`).run();
    c0Upgrade.database.prepare(`delete from dashboard_compact_gc_source_scratch`).run();
    c0Upgrade.projection.runMaintenance(NOW);
    const c0Ready=readySnapshot(c0Upgrade,30);
    const c0PhysicalState={
      raw:(c0Upgrade.database.prepare(`select count(*) as n from buffered_events`).get() as {n:number}).n,
      segments:(c0Upgrade.database.prepare(`select count(*) as n from dashboard_compact_segments`).get() as {n:number}).n,
      items:(c0Upgrade.database.prepare(
        `select coalesce(sum(event_count),0) as n from dashboard_compact_segments`,
      ).get() as {n:number}).n,
      cancellations:(c0Upgrade.database.prepare(
        `select count(*) as n from dashboard_compact_cancellations`,
      ).get() as {n:number}).n,
      projectedEvents:Number((c0Ready.summary.totals as Record<string,number>).events),
      generation:c0Ready.generation,
    };
    assert.deepEqual(c0PhysicalState,{raw:90,segments:2,items:100,cancellations:10,
      projectedEvents:90,generation:c0Ready.generation});
    c0Upgrade.close();
    const c0Shape=new Database(c0UpgradePath);
    downgradeCompactProjectionToC0(c0Shape);
    c0Shape.close();

    c0Upgrade=new LocalEventBuffer(c0UpgradePath);
    const migrationColumns=new Set((c0Upgrade.database.pragma(
      "table_info(dashboard_projection_control)",
    ) as Array<{name:string}>).map((row)=>row.name));
    const migrationMarker=migrationColumns.has("compact_summary_migration_complete")?
      (c0Upgrade.database.prepare(
        `select compact_summary_migration_complete as complete
         from dashboard_projection_control where singleton=1`,
      ).get() as {complete:number}).complete:-1;
    const migrationJobsBeforeReopen=c0Upgrade.database.prepare(
      `select bucket_day as bucketDay,revision,processing_revision as processingRevision,
        high_water_segment as highWater,cursor_segment as cursorSegment,queued_at as queuedAt
       from dashboard_compact_gc_days order by bucket_day`,
    ).all();
    const migrationNullBuckets=(c0Upgrade.database.prepare(
      `select count(*) as n from dashboard_compact_cancellations where bucket_day is null`,
    ).get() as {n:number}).n;
    const migrationStale=c0Upgrade.projection.readSnapshot(30);
    const migrationStatus=c0Upgrade.projection.status();
    check("c0_compact_summary_upgrade_seeds_every_segment_day_and_marks_prior_snapshot_stale",
      migrationMarker===1&&migrationJobsBeforeReopen.length===2&&migrationNullBuckets===0&&
      migrationStatus.dirty&&migrationStatus.degraded&&migrationStatus.backlog.compactGcDays===2&&
      migrationStale.kind==="ready"&&migrationStale.snapshot.generation===c0PhysicalState.generation&&
      migrationStale.snapshot.projection.status==="stale"&&
      Number((migrationStale.snapshot.summary.totals as Record<string,number>).events)===90,
      {c0DrainReceipt,c0PhysicalState,migrationMarker,migrationJobsBeforeReopen,
        migrationNullBuckets,migrationStatus,migrationStale});
    c0Upgrade.close();
    c0Upgrade=new LocalEventBuffer(c0UpgradePath);
    const migrationJobsAfterReopen=c0Upgrade.database.prepare(
      `select bucket_day as bucketDay,revision,processing_revision as processingRevision,
        high_water_segment as highWater,cursor_segment as cursorSegment,queued_at as queuedAt
       from dashboard_compact_gc_days order by bucket_day`,
    ).all();
    check("c0_compact_summary_upgrade_jobs_and_marker_resume_without_reseed",
      JSON.stringify(migrationJobsAfterReopen)===JSON.stringify(migrationJobsBeforeReopen)&&
      (c0Upgrade.database.prepare(
        `select compact_summary_migration_complete as n from dashboard_projection_control`,
      ).get() as {n:number}).n===1,
      {before:migrationJobsBeforeReopen,after:migrationJobsAfterReopen});
    const c0MigrationReceipts=settle(c0Upgrade,NOW,20);
    const migrationRaw=dashboardSummary(c0Upgrade.database,30).totals as Record<string,number>;
    const migrationProjected=readySnapshot(c0Upgrade,30).summary.totals as Record<string,number>;
    const migrationDaySources=c0Upgrade.database.prepare(
      `select bucket_day as bucketDay,source,event_count as eventCount,
        min_observed_at as minObservedAt,max_observed_at as maxObservedAt
       from dashboard_compact_day_source order by bucket_day,source`,
    ).all();
    const migrationRawBounds=c0Upgrade.database.prepare(
      `select min(observed_at) as oldest,max(observed_at) as newest from buffered_events`,
    ).get() as {oldest:string;newest:string};
    const migrationProjectedBounds=c0Upgrade.database.prepare(
      `select oldest_observed_at as oldest,newest_observed_at as newest
       from dashboard_lifetime_totals where singleton=1`,
    ).get() as {oldest:string;newest:string};
    const migrationRawSources=c0Upgrade.database.prepare(
      `select source,max(observed_at) as lastEventAt from buffered_events group by source order by source`,
    ).all();
    const migrationProjectedSources=c0Upgrade.database.prepare(
      `select source,last_event_at as lastEventAt from dashboard_source_lifetime order by source`,
    ).all();
    const migrationFinalState={
      raw:(c0Upgrade.database.prepare(`select count(*) as n from buffered_events`).get() as {n:number}).n,
      items:compactItems(c0Upgrade.database).length,
      cancellations:(c0Upgrade.database.prepare(`select count(*) as n from dashboard_compact_cancellations`).get() as {n:number}).n,
      jobs:(c0Upgrade.database.prepare(`select count(*) as n from dashboard_compact_gc_days`).get() as {n:number}).n,
      scratch:(c0Upgrade.database.prepare(`select count(*) as n from dashboard_compact_gc_source_scratch`).get() as {n:number}).n,
      marker:(c0Upgrade.database.prepare(
        `select compact_summary_migration_complete as n from dashboard_projection_control`,
      ).get() as {n:number}).n,
    };
    check("c0_compact_summary_upgrade_rebuilds_unaffected_days_and_settles_exactly",
      c0MigrationReceipts.every((receipt)=>receipt.compactGcItemsVisited<=1_000&&
        receipt.repairRowsVisited<=250)&&
      c0MigrationReceipts.reduce((sum,receipt)=>sum+receipt.compactGcSegmentsRewritten,0)===1&&
      c0MigrationReceipts.reduce((sum,receipt)=>sum+receipt.compactGcSegmentsDeleted,0)===0&&
      migrationRaw.events===90&&migrationProjected.events===90&&
      migrationDaySources.length===2&&
      (migrationDaySources as Array<{source:string;eventCount:number}>).some((row)=>
        row.source==="codex"&&row.eventCount===40)&&
      (migrationDaySources as Array<{source:string;eventCount:number}>).some((row)=>
        row.source==="claude_code"&&row.eventCount===50)&&
      JSON.stringify(migrationProjectedBounds)===JSON.stringify(migrationRawBounds)&&
      JSON.stringify(migrationProjectedSources)===JSON.stringify(migrationRawSources)&&
      JSON.stringify(migrationFinalState)===JSON.stringify({raw:90,items:90,cancellations:0,
        jobs:0,scratch:0,marker:1}),
      {receipts:c0MigrationReceipts.length,migrationRaw,migrationProjected,migrationDaySources,
        migrationRawBounds,migrationProjectedBounds,migrationRawSources,
        migrationProjectedSources,migrationFinalState});
    c0Upgrade.close();
    c0Upgrade=new LocalEventBuffer(c0UpgradePath);
    const migrationIdleReceipts=settle(c0Upgrade,NOW,5);
    check("c0_compact_summary_upgrade_marker_prevents_second_reopen_reseed",
      migrationIdleReceipts.length===0&&
      (c0Upgrade.database.prepare(`select count(*) as n from dashboard_compact_gc_days`).get() as {n:number}).n===0&&
      (c0Upgrade.database.prepare(
        `select compact_summary_migration_complete as n from dashboard_projection_control`,
      ).get() as {n:number}).n===1,
      {idleSlices:migrationIdleReceipts.length,status:c0Upgrade.projection.status()});
    c0Upgrade.close();

    const gcPath=path.join(root,"compact-gc.sqlite");
    const gcSeed=new LocalEventBuffer(gcPath);
    gcSeed.close();
    const gcDb=new Database(gcPath);
    dropProjectionState(gcDb);
    const gcInsert=gcDb.prepare(
      `insert into buffered_events
       (id,source,event_type,data_mode,observed_at,payload_json,suppressed_fields_json,created_at,action_class)
       values (?,?,?,?,?,?,?,?,?)`,
    );
    gcDb.transaction(()=>{
      for(let index=0;index<20_000;index++){
        const dayOffset=index<10_000?3:2;
        gcInsert.run(`gc-${index}`,"codex","otel_span","metadata",
          new Date(NOW.getTime()-dayOffset*DAY_MS+(index%10_000)).toISOString(),"{}","[]",
          NOW.toISOString(),index%25===0?"read":null);
      }
    })();
    gcDb.close();
    let gc=new LocalEventBuffer(gcPath);
    settle(gc,NOW,80);
    gc.database.pragma("wal_checkpoint(TRUNCATE)");
    const physicalBefore={
      pages:Number(gc.database.pragma("page_count",{simple:true})),
      freelist:Number(gc.database.pragma("freelist_count",{simple:true})),
      fileBytes:fs.statSync(gcPath).size,
      segments:(gc.database.prepare(`select count(*) as n from dashboard_compact_segments`).get() as {n:number}).n,
    };

    gc.database.prepare(`delete from buffered_events where rowid in (1,10001)`).run();
    const segmentBeforeCrash=(gc.database.prepare(
      `select payload_gzip as payload from dashboard_compact_segments order by segment_id limit 1`,
    ).get() as {payload:Buffer}).payload;
    gc.projection.failNextCompactGcAfterRewriteForProof();
    assert.throws(()=>gc.projection.runMaintenance(NOW),/injected_compact_gc_failure/);
    const segmentAfterCrash=(gc.database.prepare(
      `select payload_gzip as payload from dashboard_compact_segments order by segment_id limit 1`,
    ).get() as {payload:Buffer}).payload;
    check("compact_gc_segment_rewrite_and_cancellation_settlement_are_atomic",
      segmentBeforeCrash.equals(segmentAfterCrash)&&
      (gc.database.prepare(`select count(*) as n from dashboard_compact_cancellations`).get() as {n:number}).n===0&&
      (gc.database.prepare(`select count(*) as n from dashboard_compact_mutations`).get() as {n:number}).n===2,
      {payloadBytes:segmentBeforeCrash.length});

    const firstGc=gc.projection.runMaintenance(NOW);
    const jobsAfterFirst=gc.database.prepare(
      `select bucket_day as bucketDay,cursor_segment as cursorSegment,last_schedule as lastSchedule,
        revision,processing_revision as processingRevision from dashboard_compact_gc_days
       order by bucket_day`,
    ).all() as Array<{bucketDay:string;cursorSegment:number;lastSchedule:number;revision:number;processingRevision:number}>;
    const firstServed=jobsAfterFirst.find((job)=>job.cursorSegment>0);
    assert.ok(firstServed);
    const alreadyScannedRaw=(gc.database.prepare(
      `select rowid as rawRowid from buffered_events where substr(observed_at,1,10)=?
       order by rowid limit 1`,
    ).get(firstServed.bucketDay) as {rawRowid:number}).rawRowid;
    gc.database.prepare(`delete from buffered_events where rowid=?`).run(alreadyScannedRaw);
    const secondGc=gc.projection.runMaintenance(NOW);
    const jobsAfterSecond=gc.database.prepare(
      `select bucket_day as bucketDay,cursor_segment as cursorSegment,last_schedule as lastSchedule,
        revision,processing_revision as processingRevision from dashboard_compact_gc_days
       order by bucket_day`,
    ).all() as Array<{bucketDay:string;cursorSegment:number;lastSchedule:number;revision:number;processingRevision:number}>;
    const otherDay=jobsAfterSecond.find((job)=>job.bucketDay!==firstServed.bucketDay);
    const revisedDay=jobsAfterSecond.find((job)=>job.bucketDay===firstServed.bucketDay);
    check("compact_gc_round_robin_and_frozen_revision_prevent_midpass_loss_or_starvation",
      Boolean(otherDay&&otherDay.cursorSegment>0&&revisedDay&&
        revisedDay.cursorSegment===firstServed.cursorSegment&&
        revisedDay.revision>revisedDay.processingRevision&&
        secondGc.compactGcItemsVisited<=1_000),
      {first:jobsAfterFirst,second:jobsAfterSecond});
    gc.close();
    gc=new LocalEventBuffer(gcPath);
    const reopenedJobs=(gc.database.prepare(
      `select count(*) as n from dashboard_compact_gc_days where cursor_segment>0`,
    ).get() as {n:number}).n;
    gc.database.prepare(`delete from buffered_events`).run();
    const queuedAfterBulkDelete={
      mutations:(gc.database.prepare(`select count(*) as n from dashboard_compact_mutations`).get() as {n:number}).n,
      repairs:(gc.database.prepare(`select count(*) as n from dashboard_projection_repairs`).get() as {n:number}).n,
    };
    const gcReceipts=settle(gc,NOW,260);
    gc.database.pragma("wal_checkpoint(TRUNCATE)");
    const physicalAfter={
      pages:Number(gc.database.pragma("page_count",{simple:true})),
      freelist:Number(gc.database.pragma("freelist_count",{simple:true})),
      fileBytes:fs.statSync(gcPath).size,
      segments:(gc.database.prepare(`select count(*) as n from dashboard_compact_segments`).get() as {n:number}).n,
      payloadItems:(gc.database.prepare(`select coalesce(sum(event_count),0) as n from dashboard_compact_segments`).get() as {n:number}).n,
      cancellations:(gc.database.prepare(`select count(*) as n from dashboard_compact_cancellations`).get() as {n:number}).n,
      gcDays:(gc.database.prepare(`select count(*) as n from dashboard_compact_gc_days`).get() as {n:number}).n,
      daySources:(gc.database.prepare(`select count(*) as n from dashboard_compact_day_source`).get() as {n:number}).n,
    };
    const gcSnapshot=readySnapshot(gc,30);
    const gcBuildsBefore=gc.projection.workCounters().snapshotBuilds;
    for(let request=0;request<25;request++)readySnapshot(gc,30);
    const gcCounters=gc.projection.workCounters();
    check("compact_gc_physically_removes_20k_deleted_history_in_bounded_restart_safe_slices",
      reopenedJobs>0&&physicalAfter.segments===0&&physicalAfter.payloadItems===0&&
      physicalAfter.cancellations===0&&physicalAfter.gcDays===0&&physicalAfter.daySources===0&&
      physicalAfter.freelist>physicalBefore.freelist&&
      Number((gcSnapshot.summary.totals as Record<string,number>).events)===0&&
      gcReceipts.every((receipt)=>receipt.compactGcItemsVisited<=1_000&&
        receipt.repairRowsVisited<=250)&&gcCounters.snapshotBuilds===gcBuildsBefore&&
      gcCounters.rawRowsScannedByDashboard===0&&gcCounters.filesystemEntriesScannedByDashboard===0,
      {rowsDeleted:20_000,before:physicalBefore,after:physicalAfter,slices:gcReceipts.length,
        queuedAfterBulkDelete,
        gc:{itemsVisited:gcReceipts.reduce((sum,row)=>sum+row.compactGcItemsVisited,0),
          itemsRemoved:gcReceipts.reduce((sum,row)=>sum+row.compactGcItemsRemoved,0),
          segmentsRewritten:gcReceipts.reduce((sum,row)=>sum+row.compactGcSegmentsRewritten,0),
          segmentsDeleted:gcReceipts.reduce((sum,row)=>sum+row.compactGcSegmentsDeleted,0),
          restarts:gc.projection.status().counters.compactGcRestarts},
        sqliteFileTruncationPromised:false});
    gc.close();

    compact.database.prepare(
      `update buffered_events set session_id=?,model=?,input_tokens=?,output_tokens=?,cost_usd=? where rowid=1`,
    ).run("compact-promoted-session","promoted-model",5,1,0.000001);
    compact.database.prepare(`delete from buffered_events where rowid=2`).run();
    const mutationBeforeCrash=compact.projection.status();
    compact.close();
    compact=new LocalEventBuffer(compactPath);
    const compactMutationReceipts=settle(compact,NOW,260);
    const compactRaw=dashboardSummary(compact.database,30);
    const compactAfter=readySnapshot(compact,30);
    const compactProjectedTotals=compactAfter.summary.totals as Record<string,number>;
    const compactRawTotals=compactRaw.totals as Record<string,number>;
    check("compact_update_delete_receipts_survive_reopen_and_restore_exact_parity",
      mutationBeforeCrash.backlog.compactMutations===2&&mutationBeforeCrash.backlog.repairs===2&&
      compactProjectedTotals.events===compactRawTotals.events&&
      compactProjectedTotals.tokenEvents===compactRawTotals.tokenEvents&&
      compactProjectedTotals.inputTokens===compactRawTotals.inputTokens&&
      compactProjectedTotals.outputTokens===compactRawTotals.outputTokens&&
      compactProjectedTotals.sessions===compactRawTotals.sessions&&
      Math.abs(compactProjectedTotals.costUsd-compactRawTotals.costUsd)<=1e-9&&
      compactMutationReceipts.every((receipt)=>receipt.repairRowsVisited<=250&&
        receipt.compactGcItemsVisited<=1_000),
      {beforeCrash:mutationBeforeCrash.backlog,events:compactProjectedTotals.events,
        facts:(compact.database.prepare(`select count(*) as n from dashboard_event_facts`).get() as {n:number}).n});
    compact.database.prepare(`delete from buffered_events where rowid=1`).run();
    const factDeleteBeforeCrash=compact.projection.status();
    compact.close();
    compact=new LocalEventBuffer(compactPath);
    settle(compact,NOW,30);
    const factDeleteRaw=dashboardSummary(compact.database,30);
    const factDeleteProjected=readySnapshot(compact,30).summary.totals as Record<string,number>;
    const factDeleteRawTotals=factDeleteRaw.totals as Record<string,number>;
    check("indexed_fact_delete_tombstone_survives_reopen_and_subtracts_exactly",
      factDeleteBeforeCrash.backlog.repairs===1&&factDeleteBeforeCrash.backlog.compactMutations===0&&
      factDeleteProjected.events===factDeleteRawTotals.events&&
      factDeleteProjected.tokenEvents===factDeleteRawTotals.tokenEvents&&
      factDeleteProjected.sessions===factDeleteRawTotals.sessions&&
      (compact.database.prepare(`select count(*) as n from dashboard_event_facts`).get() as {n:number}).n===0,
      {beforeCrash:factDeleteBeforeCrash.backlog,events:factDeleteProjected.events});
    compact.close();

    // Live evidence found a 233,665-row session. The reducer therefore proves
    // a larger real fixture, not a scaled-down timing proxy.
    const giantPath=path.join(root,"giant-session.sqlite");
    let giant=new LocalEventBuffer(giantPath);
    const giantSession=hash("9"),giantRepoA=hash("7"),giantRepoB=hash("8");
    const giantAccountA=hash("5"),giantAccountB=hash("6"),giantMachine=hash("4");
    const giantInsert=giant.database.prepare(
      `insert into dashboard_event_facts
       (projection_id,raw_rowid,source,event_type,observed_at,session_hash,input_tokens,
        output_tokens,cost_nanos,repo_hash,branch_hash,machine_hash,account_hash,suppressed)
       values (?,?,?,?,?,?,?,?,?,?,?,?,?,0)`,
    );
    giant.database.transaction(()=>{
      for(let index=1;index<=250_001;index++){
        const repo=index<=120_000?giantRepoA:index<=220_000?giantRepoB:null;
        const branch=repo===giantRepoA?(index%2?branchA:branchB):repo===giantRepoB?branchB:null;
        const account=index<=130_000?giantAccountA:giantAccountB;
        giantInsert.run(`sha256:${index.toString(16).padStart(64,"0")}`,index,"codex","assistant_response",
          new Date(NOW.getTime()-DAY_MS).toISOString(),giantSession,1,2,1,repo,branch,giantMachine,account);
      }
      giant.database.prepare(
        `insert into dashboard_dirty_sessions
         (days,session_hash,reason,queued_at,revision,restart_revision) values (30,?,?,?,1,1)`,
      ).run(giantSession,"giant_proof",NOW.toISOString());
      giant.database.prepare(
        `update dashboard_projection_control set dirty=1,degraded_reason='projection_repair_backlog' where singleton=1`,
      ).run();
    })();
    const giantFirst=giant.projection.runMaintenance(NOW);
    const staleDuringGiant=giant.projection.readSnapshot(30);
    check("giant_session_first_slice_is_bounded_and_keeps_prior_snapshot",
      giantFirst.sessionRepairRowsVisited===1_000&&giantFirst.backlog.dirtySessions===1&&
      staleDuringGiant.kind==="ready"&&staleDuringGiant.snapshot.projection.status==="stale"&&
      !(giant.database.prepare(
        `select 1 from dashboard_session_root_window where days=30 and session_hash=?`,
      ).get(giantSession)),
      {first:giantFirst,projection:staleDuringGiant.kind==="ready"?staleDuringGiant.snapshot.projection:staleDuringGiant});
    giant.close();
    giant=new LocalEventBuffer(giantPath);
    let giantSlices=1,eventLoopYields=0;
    while(giant.projection.status().backlog.dirtySessions){
      const receipt=giant.projection.runMaintenance(NOW);
      assert.ok(receipt.sessionRepairRowsVisited<=1_000,JSON.stringify(receipt));
      giantSlices++;
      await new Promise<void>((resolve)=>setImmediate(()=>{eventLoopYields++;resolve();}));
      assert.ok(giantSlices<300,"giant session repair failed to converge");
    }
    const giantRoot=giant.database.prepare(
      `select events,input_tokens as inputTokens,output_tokens as outputTokens,cost_nanos as costNanos,
        dominant_repo_hash as dominantRepo,dominant_account_hash as dominantAccount,repo_count as repoCount
       from dashboard_session_root_window where days=30 and session_hash=?`,
    ).get(giantSession) as Record<string,unknown>;
    const giantRepoRow=giant.database.prepare(
      `select input_tokens as inputTokens,output_tokens as outputTokens,cost_nanos as costNanos
       from dashboard_repo_session_window where days=30 and session_hash=? and repo_key=?`,
    ).get(giantSession,giantRepoA) as Record<string,number>;
    check("giant_session_crash_resume_finalizes_exact_dominance_and_totals",
      giantSlices>=251&&eventLoopYields>=250&&Number(giantRoot.events)===250_001&&
      Number(giantRoot.inputTokens)===250_001&&Number(giantRoot.outputTokens)===500_002&&
      Number(giantRoot.costNanos)===250_001&&giantRoot.dominantRepo===giantRepoA&&
      giantRoot.dominantAccount===giantAccountA&&Number(giantRoot.repoCount)===2&&
      giantRepoRow.inputTokens===150_001&&giantRepoRow.outputTokens===300_002&&
      !(giant.database.prepare(`select 1 from dashboard_session_repair_jobs where days=30 and session_hash=?`).get(giantSession)),
      {rows:giantRoot.events,slices:giantSlices,eventLoopYields,dominantRepo:giantRoot.dominantRepo});
    giant.close();

    // Legacy migration proof: remove projection state from a populated ledger,
    // reopen as an upgrade, crash/reopen after one slice, append concurrently,
    // and require bounded fact + independent parity passes before ready.
    const legacyPath = path.join(root, "legacy.sqlite");
    const legacySeed = new LocalEventBuffer(legacyPath);
    legacySeed.close();
    const legacyDb = new Database(legacyPath);
    const legacyInsert = legacyDb.prepare(
      `insert into buffered_events
       (id,source,event_type,data_mode,observed_at,payload_json,suppressed_fields_json,
        created_at,session_id,model,input_tokens,output_tokens,cost_usd)
       values (?,?,?,?,?,?,?,?,?,?,?,?,?)`,
    );
    legacyDb.transaction(() => {
      for (let index = 0; index < 2_300; index += 1) {
        const id = `legacy-${String(index).padStart(6, "0")}`;
        legacyInsert.run(id, "codex", "assistant_response", "metadata",
          new Date(NOW.getTime() - (index % 20) * DAY_MS).toISOString(), JSON.stringify({ id }), "[]",
          NOW.toISOString(), `legacy-session-${index % 5}`, "legacy-model", 1, 1, 0.000000001);
      }
      const metricInsert=legacyDb.prepare(
        `insert into metric_samples
         (id,source,metric_name,observed_at,value,created_at) values (?,?,?,?,?,?)`,
      );
      for(let index=0;index<1_200;index++)metricInsert.run(`legacy-metric-${index}`,"codex","proof.metric",
        NOW.toISOString(),index,NOW.toISOString());
    })();
    dropProjectionState(legacyDb);
    legacyDb.close();

    let legacy = new LocalEventBuffer(legacyPath);
    legacy.database.prepare(`update buffered_events set created_at='2000-01-01T00:00:00.000Z',uploaded_at=? where rowid=1`).run(NOW.toISOString());
    const migrationPrune=legacy.prune(90);
    check("legacy_constructor_does_not_scan_or_materialize_history",
      (legacy.database.prepare(`select count(*) as n from dashboard_event_facts`).get() as {n:number}).n === 0 &&
      legacy.projection.status().backfill.highWater === null&&
      legacy.projection.status().backfill.metricSampleCount===null&&migrationPrune.events===0&&
      (legacy.database.prepare(`select count(*) as n from buffered_events where rowid=1`).get() as {n:number}).n===1,
      legacy.projection.status());
    const firstSlice = legacy.projection.runMaintenance(NOW);
    check("legacy_backfill_slice_is_bounded", firstSlice.backfillRowsVisited === 1_000 && firstSlice.metricRowsVisited===1_000&&firstSlice.parityRowsVisited === 0,
      firstSlice as unknown as Record<string, unknown>);
    legacy.close();
    legacy = new LocalEventBuffer(legacyPath);
    legacy.database.prepare(
      `insert into metric_samples (id,source,metric_name,observed_at,value,created_at) values (?,?,?,?,?,?)`,
    ).run("legacy-metric-concurrent","codex","proof.metric",NOW.toISOString(),1,NOW.toISOString());
    legacy.append(event({ sessionId: "concurrent-after-highwater", inputTokens: 5, outputTokens: 1, costUsd: 0.5 }));
    const migrationReceipts = settle(legacy, NOW, 30);
    check("legacy_backfill_restart_concurrent_append_and_parity_are_bounded",
      migrationReceipts.every((receipt) => receipt.backfillRowsVisited + receipt.parityRowsVisited <= 1_000) &&
      legacy.projection.status().backfill.cursor === 2_300 &&
      legacy.projection.status().backfill.parityComplete,
      { receipts: migrationReceipts.map((receipt) => ({ backfill: receipt.backfillRowsVisited, parity: receipt.parityRowsVisited })),
        state: legacy.projection.status() });
    const legacyRaw = dashboardSummary(legacy.database, 30);
    const legacyProjected = readySnapshot(legacy, 30);
    assert.equal((legacyProjected.summary.totals as Record<string,number>).events,
      (legacyRaw.totals as Record<string,number>).events);
    near(Number((legacyProjected.summary.totals as Record<string,number>).costUsd),
      Number((legacyRaw.totals as Record<string,number>).costUsd), 2_301 * 0.5e-9,
      "legacy cost nano parity");
    check("legacy_projection_ready_only_after_reference_parity", legacy.projection.status().parityReady, {
      generation: legacyProjected.generation,
      events: (legacyProjected.summary.totals as Record<string,number>).events,
    });
    check("legacy_metric_count_is_null_until_bounded_backfill_then_exact",
      (legacyProjected.status.stats as {metricSampleCount:number}).metricSampleCount===1_201&&
      legacy.projection.status().backfill.metricSampleCount===1_201,
      {metric:legacy.projection.status().backfill.metricSampleCount});
    legacy.close();

    console.log(JSON.stringify({
      schema: "plimsoll.dashboard-projection-proof.v1",
      status: "pass",
      checks: checks.length,
      names: checks.map((entry) => entry.name),
      evidence:{
        compactStorage:checks.find((entry)=>entry.name==="generic_zero_value_spans_use_bounded_compressed_projection_storage")?.detail,
        activeBatch:checks.find((entry)=>entry.name==="active_5k_compactable_rows_use_repair_day_bounded_segments")?.detail,
        compactGc:checks.find((entry)=>entry.name==="compact_gc_physically_removes_20k_deleted_history_in_bounded_restart_safe_slices")?.detail,
        giantSession:checks.find((entry)=>entry.name==="giant_session_crash_resume_finalizes_exact_dominance_and_totals")?.detail,
        compactExpiry:checks.find((entry)=>entry.name==="compact_expiry_is_bounded_and_serves_prior_generation_until_complete")?.detail,
      },
      liveStateTouched: false,
      providerNetworkCalled: false,
    }, null, 2));
  } finally {
    buffer.close();
    fs.rmSync(root, { recursive: true, force: true });
    Date.now = originalDateNow;
  }
}

main().catch((error) => {
  Date.now = originalDateNow;
  console.error(error instanceof Error ? error.stack : error);
  process.exitCode = 1;
});
