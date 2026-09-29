/** Durable join restart and capture-root fence recovery. All paths are inside the selected collector home. */
import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import Database from "better-sqlite3";

import { unsealCaptureBaselineGenerations, type CaptureBaselineSealResult } from "./capture-baseline";
import { collectorConfigSchema, withCollectorConfigMutationLock } from "./config";

const OBLIGATION_SCHEMA = "plimsoll.join-restart-obligation/v2";
const PREPARED_JOURNAL_SCHEMA = "plimsoll.join-prepared-obligation/v1";
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
  priorOwnedTemplateIdentityContent: string | null;
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
const preparedJournalPath = (file: string, operationId: string) =>
  `${file}.prepared-journal-${operationId}`;
const sha256 = (bytes: string | Buffer) => createHash("sha256").update(bytes).digest("hex");

type PreparedJournal = {
  schema: typeof PREPARED_JOURNAL_SCHEMA;
  operationId: string;
  preparedName: string;
  device: string;
  inode: string;
};

function matchesInode(file: string, device: string, inode: string) {
  const stat = fs.lstatSync(file, { bigint: true });
  return stat.isFile() && !stat.isSymbolicLink() &&
    String(stat.dev) === device && String(stat.ino) === inode;
}

function unlinkOwned(file: string, device: string, inode: string) {
  if (!pathExistsNoFollow(file)) return;
  if (!matchesInode(file, device, inode))
    throw new Error("Join prepared path changed ownership; leaving it in place.");
  fs.unlinkSync(file);
}

function readPreparedJournal(file: string, operationId: string, device: string, inode: string) {
  const journalFile = preparedJournalPath(file, operationId);
  const stat = fs.lstatSync(journalFile);
  if (!stat.isFile() || stat.isSymbolicLink() || stat.nlink !== 1 ||
      (stat.mode & 0o777) !== 0o600 || stat.size > 4096)
    throw new Error("Join prepared journal is not an owned private file.");
  const descriptor = fs.openSync(journalFile, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
  try {
    const bound = fs.fstatSync(descriptor);
    if (bound.dev !== stat.dev || bound.ino !== stat.ino || bound.size !== stat.size)
      throw new Error("Join prepared journal changed during inspection.");
    const journal = JSON.parse(fs.readFileSync(descriptor, "utf8")) as Partial<PreparedJournal>;
    const prefix = `${path.basename(file)}.prepared-${operationId}-`;
    if (journal.schema !== PREPARED_JOURNAL_SCHEMA || journal.operationId !== operationId ||
        typeof journal.preparedName !== "string" ||
        !journal.preparedName.startsWith(prefix) ||
        !/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i.test(
          journal.preparedName.slice(prefix.length)) ||
        journal.device !== device || journal.inode !== inode)
      throw new Error("Join prepared journal does not prove ownership of the prepared link.");
    return { journalFile, journal: journal as PreparedJournal,
      device: String(fs.fstatSync(descriptor, { bigint: true }).dev),
      inode: String(fs.fstatSync(descriptor, { bigint: true }).ino) };
  } finally { fs.closeSync(descriptor); }
}

function pathExistsNoFollow(file: string) {
  try { fs.lstatSync(file); return true; }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return false;
    throw error;
  }
}

function fsyncDirectory(directory: string) {
  const descriptor = fs.openSync(directory, "r");
  try { fs.fsyncSync(descriptor); } finally { fs.closeSync(descriptor); }
}

export function writeJoinRestartObligation(home: string, input: Omit<JoinRestartObligation, "schema" | "operationId" | "configBeforeSha256" | "createdAt">) {
  const obligation: JoinRestartObligation = { ...input, schema: OBLIGATION_SCHEMA,
    operationId: randomUUID(), configBeforeSha256: sha256(input.configBeforeRoots),
    createdAt: new Date().toISOString() };
  const file = joinRestartObligationPath(home);
  if (pathExistsNoFollow(file)) throw new Error("A previous collector restart obligation remains unresolved.");
  // The random prepared name is recorded with its inode in a private journal
  // before publication. A later hard link at the same name has no journal.
  const prepared = `${file}.prepared-${obligation.operationId}-${randomUUID()}`;
  const journalFile = preparedJournalPath(file, obligation.operationId);
  const descriptor = fs.openSync(prepared, "wx", 0o600);
  const created = fs.fstatSync(descriptor, { bigint: true });
  const device = String(created.dev), inode = String(created.ino);
  let journalIdentity: { device: string; inode: string } | null = null;
  let published = false;
  try {
    try {
      fs.writeFileSync(descriptor, `${JSON.stringify(obligation)}\n`);
      fs.fsyncSync(descriptor);
    } finally { fs.closeSync(descriptor); }
    const journal: PreparedJournal = { schema: PREPARED_JOURNAL_SCHEMA,
      operationId: obligation.operationId, preparedName: path.basename(prepared), device, inode };
    const journalDescriptor = fs.openSync(journalFile, "wx", 0o600);
    const journalStat = fs.fstatSync(journalDescriptor, { bigint: true });
    journalIdentity = { device: String(journalStat.dev), inode: String(journalStat.ino) };
    try {
      fs.writeFileSync(journalDescriptor, `${JSON.stringify(journal)}\n`);
      fs.fsyncSync(journalDescriptor);
    } finally { fs.closeSync(journalDescriptor); }
    fsyncDirectory(home);
    try { fs.linkSync(prepared, file); }
    catch (error) {
      if ((error as NodeJS.ErrnoException).code === "EEXIST")
        throw new Error("A previous collector restart obligation remains unresolved.");
      throw error;
    }
    published = true;
    fsyncDirectory(home);
    unlinkOwned(prepared, device, inode);
    unlinkOwned(journalFile, journalIdentity.device, journalIdentity.inode);
    fsyncDirectory(home);
  } finally {
    if (!published) {
      if (journalIdentity) unlinkOwned(journalFile, journalIdentity.device, journalIdentity.inode);
      unlinkOwned(prepared, device, inode);
    }
  }
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
  if (!stat.isFile() || stat.isSymbolicLink() || (stat.nlink !== 1 && stat.nlink !== 2) ||
      (stat.mode & 0o777) !== 0o600 || stat.size > MAX_OBLIGATION_BYTES)
    throw new Error("Join restart obligation is not an owned private file.");
  const descriptor = fs.openSync(file, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
  try {
    const bound = fs.fstatSync(descriptor);
    if (bound.dev !== stat.dev || bound.ino !== stat.ino || bound.nlink !== stat.nlink)
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
        (value.priorOwnedTemplateIdentityContent !== undefined &&
          value.priorOwnedTemplateIdentityContent !== null &&
          typeof value.priorOwnedTemplateIdentityContent !== "string") ||
        (value.priorContent === null) !== (value.priorManifestDigest === null) ||
        (value.priorContent !== null && `sha256:${sha256(value.priorContent!)}` !== value.priorManifestDigest))
      throw new Error("Join restart obligation is invalid.");
    collectorConfigSchema.parse(JSON.parse(value.configBeforeRoots));
    const journalFile = preparedJournalPath(file, value.operationId);
    if (stat.nlink === 2 || pathExistsNoFollow(journalFile)) {
      if (!pathExistsNoFollow(journalFile))
        throw new Error("Join restart obligation has an unowned hard link; leaving it in place.");
      const finalIdentity = fs.fstatSync(descriptor, { bigint: true });
      const prepared = readPreparedJournal(file, value.operationId,
        String(finalIdentity.dev), String(finalIdentity.ino));
      const sibling = path.join(home, prepared.journal.preparedName);
      if (stat.nlink === 2) {
        if (!pathExistsNoFollow(sibling) ||
            !matchesInode(sibling, prepared.journal.device, prepared.journal.inode) ||
            fs.lstatSync(sibling).nlink !== 2)
          throw new Error("Join restart obligation has an unowned hard link; leaving it in place.");
        unlinkOwned(sibling, prepared.journal.device, prepared.journal.inode);
      } else if (pathExistsNoFollow(sibling)) {
        throw new Error("Join prepared path exists without an owned hard link; leaving it in place.");
      }
      unlinkOwned(prepared.journalFile, prepared.device, prepared.inode);
      fsyncDirectory(home);
    }
    return { ...value, priorOwnedTemplateIdentityContent:
      value.priorOwnedTemplateIdentityContent ?? null } as JoinRestartObligation;
  } finally { fs.closeSync(descriptor); }
}

export function clearJoinRestartObligation(home: string) {
  fs.rmSync(joinRestartObligationPath(home));
  fsyncDirectory(home);
}

/** Called only after the caller proved the unchanged prior agent is serving. */
export function setAsideUnreadableJoinRestartObligation(home: string, reason: string) {
  const file = joinRestartObligationPath(home);
  const stat = fs.lstatSync(file);
  if (!stat.isFile() || stat.isSymbolicLink() || stat.nlink !== 1 ||
      (stat.mode & 0o777) !== 0o600 || stat.size > MAX_OBLIGATION_BYTES)
    throw new Error("The unreadable join restart obligation is not an owned private file.");
  const suffix = randomUUID();
  const aside = `${file}.unreadable-${suffix}`;
  fs.renameSync(file, aside);
  const note = path.join(home, `join.restart-obligation.recovery-${suffix}.json`);
  const descriptor = fs.openSync(note, "wx", 0o600);
  try {
    fs.writeFileSync(descriptor, `${JSON.stringify({ schema: "plimsoll.join-recovery-note/v1",
      reason, aside: path.basename(aside), createdAt: new Date().toISOString() })}\n`);
    fs.fsyncSync(descriptor);
  } finally { fs.closeSync(descriptor); }
  fsyncDirectory(home);
  return path.basename(note);
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

export function rollbackJoinedRootJournal(database: Database.Database, journal: JoinRootJournal,
  includeCommitted = false) {
  if (journal.state !== "pending" && !includeCommitted) return;
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
