/** Durable join restart and capture-root fence recovery. All paths are inside the selected collector home. */
import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import Database from "better-sqlite3";

import { unsealCaptureBaselineGenerations, type CaptureBaselineSealResult } from "./capture-baseline";
import { collectorConfigSchema, withCollectorConfigMutationLock } from "./config";

const OBLIGATION_SCHEMA = "plimsoll.join-restart-obligation/v2";
const ROOT_JOURNAL_TABLE = "join_root_registration_journal";
const MAX_OBLIGATION_BYTES = 2 * 1024 * 1024;

export type JoinRestartObligation = {
  schema: typeof OBLIGATION_SCHEMA;
  operationId: string;
  port: number;
  configPath: string;
  configBeforeRoots: string;
  configBeforeSha256: string;
  priorManifestDigest: string | null;
  priorContent: string | null;
  priorOwnedTemplateContent: string | null;
  replacementManifestDigest: string;
  createdAt: string;
};

export type JoinRootJournal = {
  operationId: string;
  beforeConfigSha256: string;
  afterConfigSha256: string;
  state: "pending" | "committed";
  seals: Array<{ source: "codex" | "claude_code"; runId: string; keys: string[] }>;
};

export const joinRestartObligationPath = (home: string) => path.join(home, "join.restart-obligation.json");
const sha256 = (bytes: string | Buffer) => createHash("sha256").update(bytes).digest("hex");

function fsyncDirectory(directory: string) {
  const descriptor = fs.openSync(directory, "r");
  try { fs.fsyncSync(descriptor); } finally { fs.closeSync(descriptor); }
}

export function writeJoinRestartObligation(home: string, input: Omit<JoinRestartObligation, "schema" | "operationId" | "configBeforeSha256" | "createdAt">) {
  const obligation: JoinRestartObligation = { ...input, schema: OBLIGATION_SCHEMA,
    operationId: randomUUID(), configBeforeSha256: sha256(input.configBeforeRoots),
    createdAt: new Date().toISOString() };
  const file = joinRestartObligationPath(home);
  const descriptor = fs.openSync(file, "wx", 0o600);
  try {
    fs.writeFileSync(descriptor, `${JSON.stringify(obligation)}\n`);
    fs.fsyncSync(descriptor);
  } finally { fs.closeSync(descriptor); }
  fsyncDirectory(home);
  return obligation;
}

export function readJoinRestartObligation(home: string): JoinRestartObligation | null {
  const file = joinRestartObligationPath(home);
  let stat: fs.Stats;
  try { stat = fs.lstatSync(file); }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return null;
    throw error;
  }
  if (!stat.isFile() || stat.isSymbolicLink() || stat.nlink !== 1 ||
      (stat.mode & 0o777) !== 0o600 || stat.size > MAX_OBLIGATION_BYTES)
    throw new Error("Join restart obligation is not an owned private file.");
  const descriptor = fs.openSync(file, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
  try {
    const bound = fs.fstatSync(descriptor);
    if (bound.dev !== stat.dev || bound.ino !== stat.ino || bound.nlink !== 1)
      throw new Error("Join restart obligation changed during inspection.");
    const value = JSON.parse(fs.readFileSync(descriptor, "utf8")) as Partial<JoinRestartObligation>;
    if (value.schema !== OBLIGATION_SCHEMA || typeof value.operationId !== "string" ||
        !/^[0-9a-f-]{36}$/i.test(value.operationId) ||
        !Number.isSafeInteger(value.port) || value.port! < 1 || value.port! > 65535 ||
        value.configPath !== path.join(home, "collector.config.json") ||
        typeof value.configBeforeRoots !== "string" ||
        sha256(value.configBeforeRoots) !== value.configBeforeSha256 ||
        !/^sha256:[0-9a-f]{64}$/.test(value.replacementManifestDigest ?? "") ||
        (value.priorContent !== null && typeof value.priorContent !== "string") ||
        (value.priorManifestDigest !== null && typeof value.priorManifestDigest !== "string") ||
        (value.priorOwnedTemplateContent !== null && typeof value.priorOwnedTemplateContent !== "string") ||
        (value.priorContent === null) !== (value.priorManifestDigest === null) ||
        (value.priorContent !== null && `sha256:${sha256(value.priorContent!)}` !== value.priorManifestDigest))
      throw new Error("Join restart obligation is invalid.");
    collectorConfigSchema.parse(JSON.parse(value.configBeforeRoots));
    return value as JoinRestartObligation;
  } finally { fs.closeSync(descriptor); }
}

export function clearJoinRestartObligation(home: string) {
  fs.rmSync(joinRestartObligationPath(home));
  fsyncDirectory(home);
}

function ensureRootJournal(database: Database.Database) {
  database.exec(`create table if not exists ${ROOT_JOURNAL_TABLE} (
    operation_id text primary key,
    before_config_sha256 text not null,
    after_config_sha256 text not null,
    state text not null check(state in ('pending', 'committed')),
    seals_json text not null
  )`);
}

export function journalJoinedRootSeals(
  database: Database.Database,
  operationId: string,
  beforeConfigSha256: string,
  afterConfigSha256: string,
  seal: () => CaptureBaselineSealResult[],
): CaptureBaselineSealResult[] {
  ensureRootJournal(database);
  return database.transaction(() => {
    const seals = seal();
    const records = seals.filter((entry) => entry.runId !== null && entry.sealedGenerationKeys.length)
      .map((entry) => ({ source: entry.source, runId: entry.runId!, keys: entry.sealedGenerationKeys }));
    database.prepare(`insert into ${ROOT_JOURNAL_TABLE} (
      operation_id, before_config_sha256, after_config_sha256, state, seals_json
    ) values (?, ?, ?, 'pending', ?)`).run(operationId, beforeConfigSha256, afterConfigSha256,
      JSON.stringify(records));
    return seals;
  }).immediate();
}

export function readJoinedRootJournal(database: Database.Database, operationId: string): JoinRootJournal | null {
  ensureRootJournal(database);
  const row = database.prepare(`select operation_id as operationId,
    before_config_sha256 as beforeConfigSha256, after_config_sha256 as afterConfigSha256,
    state, seals_json as sealsJson from ${ROOT_JOURNAL_TABLE} where operation_id = ?`)
    .get(operationId) as (Omit<JoinRootJournal, "seals"> & { sealsJson: string }) | undefined;
  if (!row) return null;
  const seals = JSON.parse(row.sealsJson) as JoinRootJournal["seals"];
  if (!Array.isArray(seals) || !seals.every((entry) =>
    ["codex", "claude_code"].includes(entry.source) && typeof entry.runId === "string" &&
    Array.isArray(entry.keys) && entry.keys.every((key) => typeof key === "string")))
    throw new Error("Join root journal has invalid seal keys.");
  return { operationId: row.operationId, beforeConfigSha256: row.beforeConfigSha256,
    afterConfigSha256: row.afterConfigSha256, state: row.state, seals };
}

export function markJoinedRootJournalCommitted(database: Database.Database, operationId: string) {
  ensureRootJournal(database);
  database.prepare(`update ${ROOT_JOURNAL_TABLE} set state = 'committed' where operation_id = ?`)
    .run(operationId);
}

export function clearJoinedRootJournal(database: Database.Database, operationId: string) {
  ensureRootJournal(database);
  database.prepare(`delete from ${ROOT_JOURNAL_TABLE} where operation_id = ?`).run(operationId);
}

export function rollbackJoinedRootJournal(database: Database.Database, journal: JoinRootJournal) {
  if (journal.state !== "pending") return;
  database.transaction(() => {
    for (const entry of journal.seals) {
      const result = unsealCaptureBaselineGenerations(database, entry.source, entry.runId,
        entry.keys, new Date().toISOString());
      // A prior interrupted recovery may already have removed some rows.
      if (result.removed > entry.keys.length) throw new Error("Join root journal removed unexpected rows.");
    }
    clearJoinedRootJournal(database, journal.operationId);
  }).immediate();
}

/** Restore the exact pre-add bytes, while refusing a concurrent config edit. */
export function restoreJoinConfigBytes(obligation: JoinRestartObligation, afterConfigSha256: string | null) {
  withCollectorConfigMutationLock(obligation.configPath, () => {
    const current = fs.readFileSync(obligation.configPath);
    const actual = sha256(current);
    if (actual === obligation.configBeforeSha256) return;
    if (actual !== afterConfigSha256) throw new Error("Collector config changed during join recovery.");
    const temporary = path.join(path.dirname(obligation.configPath),
      `.join-root-rollback-${process.pid}-${randomUUID()}.tmp`);
    const descriptor = fs.openSync(temporary, "wx", 0o600);
    try {
      fs.writeFileSync(descriptor, obligation.configBeforeRoots);
      fs.fsyncSync(descriptor);
    } finally { fs.closeSync(descriptor); }
    try {
      fs.renameSync(temporary, obligation.configPath);
      fsyncDirectory(path.dirname(obligation.configPath));
    } finally { fs.rmSync(temporary, { force: true }); }
  });
}

export function withJoinRootJournal<T>(ledgerPath: string, action: (database: Database.Database) => T): T | null {
  if (!fs.existsSync(ledgerPath)) return null;
  const database = new Database(ledgerPath, { fileMustExist: true, timeout: 5_000 });
  try { return action(database); } finally { database.close(); }
}
