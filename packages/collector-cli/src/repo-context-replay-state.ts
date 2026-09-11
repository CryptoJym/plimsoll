import type Database from "better-sqlite3";

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
  status: "disabled" | "ran" | "yielded_fresh" | "no_sources";
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
        'resolution_failed', 'worker_crash', 'replay_exhausted'
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
    create index if not exists idx_repo_context_drain_runs_completed
      on repo_context_drain_runs (completed_at, run_id);
  `);
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
