import type Database from "better-sqlite3";
import crypto from "node:crypto";

export const REPO_CONTEXT_REPLAY_PARSER_VERSION = "repo-context-replay:v1";

export type RepoContextReplayReason =
  | "source_unavailable"
  | "boundary_unavailable"
  | "resolution_failed"
  | "worker_crash"
  | "replay_exhausted";

export type RepoContextDrainReceipt = {
  schema: "repo-context-drain-receipt/v1";
  runId: string;
  status: "disabled" | "ran" | "yielded_fresh" | "no_sources" | "inventory_incomplete";
  replayPassId: string | null;
  sourceCursor: {
    sourceKey: string;
    sourceDigest: string;
    position: string;
  } | null;
  rowsInspected: number;
  candidateContexts: number;
  distinctCwdGroups: number;
  successfulContexts: number;
  oversizedSourceRecords: number;
  unavailableSourceRoots: number;
  unavailableSourceEntries: number;
  sourceGenerationsChanged: number;
  unresolvedContexts: Record<RepoContextReplayReason, number>;
  expiredLinks: number;
  reResolvedExpiredLinks: number;
  elapsedMs: number;
  scanBudgetExhausted: boolean;
  lookupBudgetExhausted: boolean;
  contextBudgetExhausted: boolean;
  cwdBudgetExhausted: boolean;
  sliceCutShort: boolean;
};

const zeroReasons = (): Record<RepoContextReplayReason, number> => ({
  source_unavailable: 0,
  boundary_unavailable: 0,
  resolution_failed: 0,
  worker_crash: 0,
  replay_exhausted: 0,
});

export function disabledRepoContextDrainReceipt(): RepoContextDrainReceipt {
  return {
    schema: "repo-context-drain-receipt/v1",
    runId: "disabled",
    status: "disabled",
    replayPassId: null,
    sourceCursor: null,
    rowsInspected: 0,
    candidateContexts: 0,
    distinctCwdGroups: 0,
    successfulContexts: 0,
    oversizedSourceRecords: 0,
    unavailableSourceRoots: 0,
    unavailableSourceEntries: 0,
    sourceGenerationsChanged: 0,
    unresolvedContexts: zeroReasons(),
    expiredLinks: 0,
    reResolvedExpiredLinks: 0,
    elapsedMs: 0,
    scanBudgetExhausted: false,
    lookupBudgetExhausted: false,
    contextBudgetExhausted: false,
    cwdBudgetExhausted: false,
    sliceCutShort: false,
  };
}

export function ensureRepoContextReplaySchema(database: Database.Database) {
  database.exec(`
    create table if not exists repo_context_replay_control (
      singleton integer primary key check (singleton = 1),
      pass_id text not null,
      pass_number integer not null check (pass_number >= 1),
      inventory_digest text not null,
      last_source_key text,
      status text not null check (status in ('active', 'complete')),
      started_at text not null,
      completed_at text
    );
    create table if not exists repo_context_replay_passes (
      pass_id text primary key,
      pass_number integer not null check (pass_number >= 1),
      inventory_digest text not null,
      status text not null check (status in ('active', 'complete', 'abandoned')),
      started_at text not null,
      completed_at text
    ) without rowid;
    create table if not exists repo_context_replay_cursors (
      source_key text not null,
      source_digest text not null,
      source text not null check (source in ('codex', 'claude_code')),
      position text not null,
      parser_version text not null,
      pass_id text not null,
      status text not null check (status in ('active', 'complete', 'unavailable')),
      updated_at text not null,
      primary key (source_key, source_digest, parser_version)
    ) without rowid;
    create table if not exists repo_context_replay_attempts (
      source_key text not null,
      source_digest text not null,
      position text not null,
      context_id text not null,
      pass_id text not null,
      outcome text not null check (outcome in (
        'started', 'not_pending', 'success', 'boundary_unavailable',
        'resolution_failed', 'worker_crash', 'replay_exhausted', 'record_too_large'
      )),
      attempted_at text not null,
      primary key (source_key, source_digest, position, context_id, pass_id)
    ) without rowid;
    create table if not exists repo_context_drain_runs (
      run_id text primary key,
      receipt_json text not null,
      completed_at text not null
    ) without rowid;
    create index if not exists idx_repo_context_replay_attempts_started
      on repo_context_replay_attempts (outcome, attempted_at, context_id);
    create index if not exists idx_repo_context_replay_passes_inventory
      on repo_context_replay_passes (inventory_digest, status, pass_number);
    create index if not exists idx_repo_context_drain_runs_completed
      on repo_context_drain_runs (completed_at, run_id);
  `);
  const attemptsSql = database.prepare(
    `select sql from sqlite_master where type = 'table' and name = 'repo_context_replay_attempts'`,
  ).pluck().get() as string | undefined;
  if (attemptsSql && !attemptsSql.includes("'record_too_large'")) {
    database.transaction(() => {
      database.exec(`
        alter table repo_context_replay_attempts rename to repo_context_replay_attempts_legacy;
        create table repo_context_replay_attempts (
          source_key text not null,
          source_digest text not null,
          position text not null,
          context_id text not null,
          pass_id text not null,
          outcome text not null check (outcome in (
            'started', 'not_pending', 'success', 'boundary_unavailable',
            'resolution_failed', 'worker_crash', 'replay_exhausted', 'record_too_large'
          )),
          attempted_at text not null,
          primary key (source_key, source_digest, position, context_id, pass_id)
        ) without rowid;
        insert into repo_context_replay_attempts
          (source_key, source_digest, position, context_id, pass_id, outcome, attempted_at)
        select source_key, source_digest, position, context_id, pass_id, outcome, attempted_at
        from repo_context_replay_attempts_legacy;
        drop table repo_context_replay_attempts_legacy;
      `);
    }).immediate();
    database.exec(`
      create index if not exists idx_repo_context_replay_attempts_started
        on repo_context_replay_attempts (outcome, attempted_at, context_id);
    `);
  }
}

export function writeRepoContextDrainReceipt(
  database: Database.Database,
  receipt: RepoContextDrainReceipt,
) {
  ensureRepoContextReplaySchema(database);
  database.prepare(
    `insert into repo_context_drain_runs (run_id, receipt_json, completed_at)
     values (?, ?, ?)
     on conflict(run_id) do update set
       receipt_json = excluded.receipt_json,
       completed_at = excluded.completed_at`,
  ).run(receipt.runId, JSON.stringify(receipt), new Date().toISOString());
}

export function readLatestRepoContextDrainReceipt(
  database: Database.Database,
): RepoContextDrainReceipt | null {
  ensureRepoContextReplaySchema(database);
  const row = database.prepare(
    `select receipt_json as receiptJson from repo_context_drain_runs
     order by completed_at desc, run_id desc limit 1`,
  ).get() as { receiptJson: string } | undefined;
  if (!row) return null;
  try {
    const parsed = JSON.parse(row.receiptJson) as RepoContextDrainReceipt;
    return parsed.schema === "repo-context-drain-receipt/v1" ? parsed : null;
  } catch {
    return null;
  }
}

export function emptyRepoContextReplayReasons() {
  return zeroReasons();
}

export type RepoContextReplayPass = {
  passId: string;
  passNumber: number;
  inventoryDigest: string;
  status: "active" | "complete";
};

export function beginRepoContextReplayPass(
  database: Database.Database,
  inventoryDigest: string,
  at = new Date().toISOString(),
): RepoContextReplayPass {
  ensureRepoContextReplaySchema(database);
  const control = database.prepare(
    `select pass_id as passId, pass_number as passNumber,
       inventory_digest as inventoryDigest, status
     from repo_context_replay_control where singleton = 1`,
  ).get() as RepoContextReplayPass | undefined;
  if (control?.status === "active" && control.inventoryDigest === inventoryDigest) return control;
  const passNumber = (control?.passNumber ?? 0) + 1;
  const passId = crypto.randomUUID();
  database.transaction(() => {
    if (control?.status === "active") {
      database.prepare(
        `update repo_context_replay_passes set status = 'abandoned', completed_at = ?
         where pass_id = ? and status = 'active'`,
      ).run(at, control.passId);
    }
    database.prepare(
      `insert into repo_context_replay_passes
         (pass_id, pass_number, inventory_digest, status, started_at, completed_at)
       values (?, ?, ?, 'active', ?, null)`,
    ).run(passId, passNumber, inventoryDigest, at);
    database.prepare(
      `insert into repo_context_replay_control
         (singleton, pass_id, pass_number, inventory_digest, last_source_key,
          status, started_at, completed_at)
       values (1, ?, ?, ?, null, 'active', ?, null)
       on conflict(singleton) do update set
         pass_id = excluded.pass_id,
         pass_number = excluded.pass_number,
         inventory_digest = excluded.inventory_digest,
         last_source_key = null,
         status = 'active',
         started_at = excluded.started_at,
         completed_at = null`,
    ).run(passId, passNumber, inventoryDigest, at);
  }).immediate();
  return { passId, passNumber, inventoryDigest, status: "active" };
}

export function repoContextReplayCursor(
  database: Database.Database,
  input: { sourceKey: string; sourceDigest: string; passId: string },
) {
  ensureRepoContextReplaySchema(database);
  const row = database.prepare(
    `select position, status, pass_id as passId
     from repo_context_replay_cursors
     where source_key = ? and source_digest = ? and parser_version = ?`,
  ).get(input.sourceKey, input.sourceDigest, REPO_CONTEXT_REPLAY_PARSER_VERSION) as {
    position: string;
    status: "active" | "complete" | "unavailable";
    passId: string;
  } | undefined;
  return row?.passId === input.passId ? row : null;
}

export function advanceRepoContextReplayCursor(
  database: Database.Database,
  input: {
    sourceKey: string;
    sourceDigest: string;
    source: "codex" | "claude_code";
    position: string;
    passId: string;
    status: "active" | "complete" | "unavailable";
    at?: string;
  },
) {
  ensureRepoContextReplaySchema(database);
  database.prepare(
    `insert into repo_context_replay_cursors
       (source_key, source_digest, source, position, parser_version, pass_id, status, updated_at)
     values (@sourceKey, @sourceDigest, @source, @position, @parserVersion, @passId, @status, @at)
     on conflict(source_key, source_digest, parser_version) do update set
       source = excluded.source,
       position = excluded.position,
       pass_id = excluded.pass_id,
       status = excluded.status,
       updated_at = excluded.updated_at`,
  ).run({ ...input, parserVersion: REPO_CONTEXT_REPLAY_PARSER_VERSION,
    at: input.at ?? new Date().toISOString() });
  database.prepare(
    `update repo_context_replay_control set last_source_key = ?
     where singleton = 1 and pass_id = ?`,
  ).run(input.sourceKey, input.passId);
}

export function recordRepoContextReplayAttempt(
  database: Database.Database,
  input: {
    sourceKey: string;
    sourceDigest: string;
    position: string;
    contextId: string;
    passId: string;
    outcome: "started" | "not_pending" | "success" | "boundary_unavailable" |
      "resolution_failed" | "worker_crash" | "replay_exhausted" | "record_too_large";
    at?: string;
  },
) {
  ensureRepoContextReplaySchema(database);
  database.prepare(
    `insert into repo_context_replay_attempts
       (source_key, source_digest, position, context_id, pass_id, outcome, attempted_at)
     values (@sourceKey, @sourceDigest, @position, @contextId, @passId, @outcome, @at)
     on conflict(source_key, source_digest, position, context_id, pass_id) do update set
       outcome = case
         when repo_context_replay_attempts.outcome = 'success'
           then repo_context_replay_attempts.outcome
         else excluded.outcome
       end,
       attempted_at = excluded.attempted_at`,
  ).run({ ...input, at: input.at ?? new Date().toISOString() });
}

export function completeRepoContextReplayPass(
  database: Database.Database,
  passId: string,
  at = new Date().toISOString(),
) {
  ensureRepoContextReplaySchema(database);
  return database.transaction(() => {
    const changed = database.prepare(
      `update repo_context_replay_passes
       set status = 'complete', completed_at = ?
       where pass_id = ? and status = 'active'`,
    ).run(at, passId).changes;
    database.prepare(
      `update repo_context_replay_control
       set status = 'complete', completed_at = ?
       where singleton = 1 and pass_id = ?`,
    ).run(at, passId);
    return changed > 0;
  }).immediate();
}

export function completedRepoContextReplayPasses(
  database: Database.Database,
  inventoryDigest?: string,
) {
  ensureRepoContextReplaySchema(database);
  const row = inventoryDigest
    ? database.prepare(
        `select count(*) as n from repo_context_replay_passes
         where status = 'complete' and inventory_digest = ?`,
      ).get(inventoryDigest)
    : database.prepare(
        `select count(*) as n from repo_context_replay_passes where status = 'complete'`,
      ).get();
  return (row as { n: number }).n;
}
