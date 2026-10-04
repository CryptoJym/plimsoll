import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { performance } from "node:perf_hooks";
import Database from "better-sqlite3";
import { z } from "zod";
import { resolveCollectorHome } from "./collector-home";
import { assertPrivateStateDirectory, fsyncStateDirectory, readPrivateStateFile } from "./collector-state-io";
import { dispatchTerminalProofSchema, type DispatchTerminalProof } from "./dispatch-binding-lifecycle";
import { assertDispatchHistoryBuildPairQualified, assertDispatchHistoryWriterDeadline, dispatchHistoryAdoptionRequired,
  dispatchHistoryPublicationQualified, hasDispatchHistoryAdoption, qualifyDispatchHistoryPublication,
  type DispatchRollbackRootInventory } from "./dispatch-history-adoption";

export const DISPATCH_HISTORY_LIMITS = Object.freeze({
  hotRowsPerRoot: 1_000, historyRowsPerRoot: 4_096, historyRows: 65_536,
  fileBytes: 32 * 1024 * 1024, rawBytes: 16 * 1024 * 1024, directoryBytes: 256 * 1024 * 1024,
  files: 8, bindingBytes: 4_096, terminalRows: 4_096, terminalBytes: 32_768, queryRows: 1_024, queryBytes: 4 * 1024 * 1024,
  readDeadlineMs: 2_000, queryDeadlineMs: 100, writerDeadlineMs: 5_000, cachedGenerations: 2,
});
export const dispatchHistoryRefSchema = z.object({
  schema: z.literal("dispatch-history/v1"), generation: z.string().uuid(),
  sha256: z.string().regex(/^[a-f0-9]{64}$/), rootDigest: z.string().regex(/^[a-f0-9]{64}$/),
  totalRows: z.number().int().min(1).max(DISPATCH_HISTORY_LIMITS.historyRows),
  rootRows: z.number().int().min(1).max(DISPATCH_HISTORY_LIMITS.historyRowsPerRoot),
  terminalRows: z.number().int().min(0).max(DISPATCH_HISTORY_LIMITS.terminalRows),
}).strict();
export type DispatchHistoryRef = z.infer<typeof dispatchHistoryRefSchema>;
type Binding = { sessionId: string; attemptId: string; validFrom: string; validUntil: string | null };
export type DispatchHistoryRoot<B extends Binding = Binding> = {
  source: "codex" | "claude_code"; rootId: string; profileId: string;
  installationEpochId: string; directory: string; dispatch?: B[]; dispatchHistory?: DispatchHistoryRef;
};
type RecordRow = { rootDigest: string; custody: string; source: string; sessionId: string;
  attemptId: string; fromMs: number; untilMs: number; binding: string };
type Archive = { database: Database.Database; records: RecordRow[]; rootCounts: Map<string, number>;
  terminals: DispatchTerminalProof[]; ref: DispatchHistoryRef; file: string; stamp: string };
const caches = new Map<string, Archive>();
let historyFailure: string | null = null;
const hash = (bytes: string | Buffer) => crypto.createHash("sha256").update(bytes).digest("hex");
const custody = (root: DispatchHistoryRoot) => JSON.stringify([
  root.source, root.rootId, root.profileId, root.installationEpochId, root.directory,
]);
export const dispatchHistoryRootDigest = (root: DispatchHistoryRoot) => hash(custody(root));
const recordKey = (row: RecordRow) => JSON.stringify([row.rootDigest, row.sessionId, row.attemptId, row.fromMs]);
const directoryPath = () => path.join(resolveCollectorHome().home, "dispatch-binding-history");
const filePath = (ref: DispatchHistoryRef) => path.join(directoryPath(), `${ref.sha256}.sqlite`);
function deadline(start: number, maximum: number, kind: string) {
  if (performance.now() - start > maximum) throw new Error(`dispatch_history_${kind}_deadline_exceeded`);
}
const SCHEMA = `create table history_meta (generation text not null, archived_at text not null);
  create table history_bindings (
    root_digest text not null, custody text not null, source text not null, session_id text not null,
    attempt_id text not null, from_ms integer not null, until_ms integer not null, binding text not null,
    primary key (root_digest,session_id,attempt_id,from_ms)
  ) without rowid;
  create index history_session on history_bindings(source,session_id);
  create index history_attempt on history_bindings(attempt_id);
  create table history_terminals (proof_id text primary key, proof text not null) without rowid;`;
const COLUMNS = `root_digest as rootDigest,custody,source,session_id as sessionId,
  attempt_id as attemptId,from_ms as fromMs,until_ms as untilMs,binding`;
const expectedObjects = ["history_attempt", "history_bindings", "history_meta", "history_session", "history_terminals"];

function archiveStamp(file: string) {
  assertPrivateStateDirectory(path.dirname(file));
  const stat = fs.lstatSync(file);
  // Validate even a cache hit: a missing/replaced/symlinked file is never a hit.
  if (!stat.isFile() || stat.isSymbolicLink() || stat.nlink !== 1 || (stat.mode & 0o7077) !== 0 ||
      (process.getuid && stat.uid !== process.getuid()) || stat.size > DISPATCH_HISTORY_LIMITS.fileBytes)
    throw new Error("dispatch_history_file_unsafe");
  return `${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeMs}:${stat.ctimeMs}`;
}

function loadArchive(refInput: DispatchHistoryRef, validate: (input: unknown) => Binding): Archive {
  const started = performance.now(), ref = dispatchHistoryRefSchema.parse(refInput);
  const file = filePath(ref), stamp = archiveStamp(file);
  const key = JSON.stringify([file, ref.generation, ref.sha256]);
  const cached = caches.get(key);
  if (cached && cached.stamp === stamp) {
    if (cached.records.length !== ref.totalRows || cached.terminals.length !== ref.terminalRows)
      throw new Error("dispatch_history_generation_mismatch");
    deadline(started, DISPATCH_HISTORY_LIMITS.readDeadlineMs, "read");
    return cached;
  }
  if (cached) { cached.database.close(); caches.delete(key); }
  const bytes = readPrivateStateFile(file, DISPATCH_HISTORY_LIMITS.fileBytes);
  if (hash(bytes) !== ref.sha256) throw new Error("dispatch_history_digest_mismatch");
  // Deserialization of pinned, bounded bytes avoids SQLite pathname following,
  // WAL/journal side effects and a second open of a replaceable source file.
  const database = new Database(bytes, { readonly: true });
  try {
    database.pragma("query_only = ON");
    const objects = database.prepare("select name,type,sql from sqlite_master order by name limit 6").all() as { name: string; type: string; sql: string }[];
    if (JSON.stringify(objects.map(row => row.name)) !== JSON.stringify(expectedObjects))
      throw new Error("dispatch_history_schema_invalid");
    const normalize = (sql: string) => sql.toLowerCase().replace(/\s+/g, " ").trim();
    const statements = SCHEMA.split(";").map(sql => sql.trim()).filter(Boolean).map(normalize);
    if (objects.some(row => !["table", "index"].includes(row.type) || !statements.includes(normalize(row.sql))))
      throw new Error("dispatch_history_schema_invalid");
    const meta = database.prepare("select generation,archived_at as archivedAt from history_meta limit 2").all() as
      Array<{ generation: string; archivedAt: string }>;
    if (meta.length !== 1 || meta[0].generation !== ref.generation ||
        !z.iso.datetime().safeParse(meta[0].archivedAt).success || !Number.isFinite(Date.parse(meta[0].archivedAt)))
      throw new Error("dispatch_history_generation_mismatch");
    const records = database.prepare(`select ${COLUMNS} from history_bindings
      order by root_digest,session_id,attempt_id,from_ms limit ?`).all(DISPATCH_HISTORY_LIMITS.historyRows + 1) as RecordRow[];
    if (records.length !== ref.totalRows || records.length > DISPATCH_HISTORY_LIMITS.historyRows)
      throw new Error("dispatch_history_row_bound_exceeded");
    const rootCounts = new Map<string, number>(), keys = new Set<string>(), terminalKeys = new Set<string>();
    let rawBytes = 0;
    for (const row of records) {
      deadline(started, DISPATCH_HISTORY_LIMITS.readDeadlineMs, "read");
      if (typeof row.binding !== "string" || Buffer.byteLength(row.binding) > DISPATCH_HISTORY_LIMITS.bindingBytes ||
          typeof row.custody !== "string" || Buffer.byteLength(row.custody) > 8_192)
        throw new Error("dispatch_history_byte_bound_exceeded");
      rawBytes += Buffer.byteLength(row.binding) + Buffer.byteLength(row.custody);
      if (rawBytes > DISPATCH_HISTORY_LIMITS.rawBytes) throw new Error("dispatch_history_byte_bound_exceeded");
      const identity = JSON.parse(row.custody);
      if (!Array.isArray(identity) || identity.length !== 5 || identity.some(value => typeof value !== "string") ||
          !["codex", "claude_code"].includes(identity[0]) || !path.isAbsolute(identity[4]) ||
          path.resolve(identity[4]) !== identity[4] || hash(row.custody) !== row.rootDigest || row.source !== identity[0])
        throw new Error("dispatch_history_custody_mismatch");
      const binding = validate(JSON.parse(row.binding));
      terminalKeys.add(JSON.stringify([row.rootDigest,binding.sessionId,binding.attemptId,row.untilMs,
        (binding as Binding & {workItemId:string}).workItemId]));
      if (JSON.stringify(binding) !== row.binding || row.sessionId !== binding.sessionId ||
          row.attemptId !== binding.attemptId || row.fromMs !== Date.parse(binding.validFrom) ||
          binding.validUntil === null || row.untilMs !== Date.parse(binding.validUntil) ||
          !Number.isSafeInteger(row.fromMs) || !Number.isSafeInteger(row.untilMs) || row.fromMs >= row.untilMs ||
          row.untilMs > Date.parse(meta[0].archivedAt)) throw new Error("dispatch_history_binding_invalid");
      const key = recordKey(row);
      if (keys.has(key)) throw new Error("dispatch_history_collision");
      keys.add(key);
      const count = (rootCounts.get(row.rootDigest) ?? 0) + 1;
      if (count > DISPATCH_HISTORY_LIMITS.historyRowsPerRoot) throw new Error("dispatch_history_row_bound_exceeded");
      rootCounts.set(row.rootDigest, count);
    }
    const terminalRows = database.prepare("select proof_id as proofId,proof from history_terminals limit ?")
      .all(DISPATCH_HISTORY_LIMITS.terminalRows+1) as Array<{proofId:string;proof:string}>;
    if (terminalRows.length !== ref.terminalRows || terminalRows.length > DISPATCH_HISTORY_LIMITS.terminalRows)
      throw new Error("dispatch_history_terminal_bound_exceeded");
    const terminals = terminalRows.map(row => {
      deadline(started, DISPATCH_HISTORY_LIMITS.readDeadlineMs, "read");
      if (typeof row.proof !== "string" || Buffer.byteLength(row.proof) > DISPATCH_HISTORY_LIMITS.terminalBytes)
        throw new Error("dispatch_history_byte_bound_exceeded");
      rawBytes+=Buffer.byteLength(row.proof);
      if(rawBytes>DISPATCH_HISTORY_LIMITS.rawBytes)throw new Error("dispatch_history_byte_bound_exceeded");
      const proof=dispatchTerminalProofSchema.parse(JSON.parse(row.proof));
      if (proof.proofId !== row.proofId || JSON.stringify(proof)!==row.proof ||
          Date.parse(proof.terminalAt)>Date.parse(proof.issuedAt) || Date.parse(proof.issuedAt)>Date.parse(meta[0].archivedAt) ||
          !proof.bindings.every(target=>terminalKeys.has(JSON.stringify([target.rootDigest,proof.sessionId,
            proof.attemptId,Date.parse(proof.terminalAt),proof.workItemId]))))
        throw new Error("dispatch_history_terminal_invalid");
      return proof;
    });
    deadline(started, DISPATCH_HISTORY_LIMITS.readDeadlineMs, "read");
    const archive = { database, records, rootCounts, terminals, ref, file, stamp };
    while (caches.size >= DISPATCH_HISTORY_LIMITS.cachedGenerations) {
      const first = caches.keys().next().value!;
      caches.get(first)!.database.close(); caches.delete(first);
    }
    caches.set(key, archive);
    historyFailure = null;
    return archive;
  } catch (error) { database.close(); throw error; }
}

function inventoryArchive(roots: readonly DispatchHistoryRoot[], validate: (input: unknown) => Binding): Archive | null {
  const refs = roots.filter(root => root.dispatchHistory).map(root => ({ root, ref: root.dispatchHistory! }));
  if (!refs.length) return null;
  const first = refs[0].ref;
  for (const { root, ref } of refs) {
    dispatchHistoryRefSchema.parse(ref);
    if (ref.rootDigest !== dispatchHistoryRootDigest(root) || ref.generation !== first.generation ||
        ref.sha256 !== first.sha256 || ref.totalRows !== first.totalRows || ref.terminalRows !== first.terminalRows)
      throw new Error("dispatch_history_snapshot_mismatch");
  }
  const archive = loadArchive(first, validate);
  const presentDigests = new Set(roots.map(dispatchHistoryRootDigest));
  for (const digest of archive.rootCounts.keys()) if (!presentDigests.has(digest))
    throw new Error("dispatch_history_partial_snapshot");
  for (const { ref } of refs) if (archive.rootCounts.get(ref.rootDigest) !== ref.rootRows)
    throw new Error("dispatch_history_generation_mismatch");
  // A participating root cannot silently omit its historical membership.
  for (const root of roots) if (!root.dispatchHistory && archive.rootCounts.has(dispatchHistoryRootDigest(root)))
    throw new Error("dispatch_history_partial_snapshot");
  return archive;
}

export function historicalDispatchBindings<B extends Binding>(roots: readonly DispatchHistoryRoot<B>[],
  selector: { source: "codex" | "claude_code"; sessionId: string } | { attemptId: string },
  validate: (input: unknown) => B): Array<{ root: DispatchHistoryRoot<B>; binding: B }> {
  const started = performance.now();
  try {
    const archive = inventoryArchive(roots, validate);
    if (!archive) return [];
    const queryStarted = performance.now();
    const byDigest = new Map(roots.map(root => [dispatchHistoryRootDigest(root), root]));
    const [where, args] = "attemptId" in selector
      ? ["attempt_id=?", [selector.attemptId]] : ["source=? and session_id=?", [selector.source, selector.sessionId]];
    const rows = archive.database.prepare(`select ${COLUMNS} from history_bindings indexed by
      ${"attemptId" in selector ? "history_attempt" : "history_session"} where ${where} limit ?`)
      .all(...args, DISPATCH_HISTORY_LIMITS.queryRows + 1) as RecordRow[];
    if (rows.length > DISPATCH_HISTORY_LIMITS.queryRows) throw new Error("dispatch_history_query_bound_exceeded");
    let rawBytes = 0;
    const result: Array<{ root: DispatchHistoryRoot<B>; binding: B }> = [];
    for (const row of rows) {
      rawBytes += Buffer.byteLength(row.binding);
      if (rawBytes > DISPATCH_HISTORY_LIMITS.queryBytes) throw new Error("dispatch_history_query_byte_bound_exceeded");
      const root = byDigest.get(row.rootDigest);
      if (root) result.push({ root, binding: validate(JSON.parse(row.binding)) });
    }
    deadline(queryStarted, DISPATCH_HISTORY_LIMITS.queryDeadlineMs, "query");
    deadline(started, DISPATCH_HISTORY_LIMITS.readDeadlineMs + DISPATCH_HISTORY_LIMITS.queryDeadlineMs, "query");
    historyFailure = null;
    return result;
  } catch (error) {
    historyFailure = error instanceof Error ? error.message : "dispatch_history_unavailable";
    throw error;
  }
}

function buildArchive(records: RecordRow[], now: Date, terminals: DispatchTerminalProof[]) {
  const started = performance.now();
  const generation = crypto.randomUUID(), database = new Database(":memory:");
  let image: Buffer;
  try {
    database.exec(SCHEMA);
    database.prepare("insert into history_meta values (?,?)").run(generation, now.toISOString());
    const insert = database.prepare("insert into history_bindings values (?,?,?,?,?,?,?,?)");
    database.transaction(() => {
      for (const row of records) {
        deadline(started, DISPATCH_HISTORY_LIMITS.writerDeadlineMs, "writer");
        insert.run(row.rootDigest, row.custody, row.source, row.sessionId, row.attemptId, row.fromMs, row.untilMs, row.binding);
      }
      const terminal=database.prepare("insert into history_terminals values (?,?)");
      for(const proof of terminals) {
        deadline(started, DISPATCH_HISTORY_LIMITS.writerDeadlineMs, "writer");
        const body=JSON.stringify(proof);
        if(Buffer.byteLength(body)>DISPATCH_HISTORY_LIMITS.terminalBytes)throw new Error("dispatch_history_byte_bound_exceeded");
        terminal.run(proof.proofId,body);
      }
    })();
    image = database.serialize();
  } finally { database.close(); }
  if (image.length > DISPATCH_HISTORY_LIMITS.fileBytes)
    throw new Error("dispatch_history_storage_pressure");
  assertDispatchHistoryWriterDeadline();
  return { generation, sha256: hash(image), totalRows: records.length, image };
}

function publishArchive(generation: { generation: string; sha256: string; totalRows: number; image: Buffer }) {
  const started = performance.now(), directory = directoryPath(), { image } = generation;
  assertDispatchHistoryWriterDeadline();
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  assertPrivateStateDirectory(directory);
  const names: string[]=[];
  const handle=fs.opendirSync(directory);
  try { while(names.length<=DISPATCH_HISTORY_LIMITS.files) {
    const entry=handle.readSync();if(!entry)break;names.push(entry.name);
  } } finally {handle.closeSync();}
  if (names.length >= DISPATCH_HISTORY_LIMITS.files) throw new Error("dispatch_history_storage_pressure");
  let bytes = 0;
  for (const name of names) {
    if (!/^(?:[a-f0-9]{64}\.sqlite|\.generation-[a-f0-9-]+\.tmp)$/.test(name))
      throw new Error("dispatch_history_directory_unsafe");
    const file = path.join(directory, name);
    archiveStamp(file); bytes += fs.lstatSync(file).size;
  }
  if (bytes + image.length > DISPATCH_HISTORY_LIMITS.directoryBytes)
    throw new Error("dispatch_history_storage_pressure");
  const target = path.join(directory, `${generation.sha256}.sqlite`);
  let descriptor: number | undefined;
  try {
    deadline(started, DISPATCH_HISTORY_LIMITS.writerDeadlineMs, "writer");
    // Exclusive creation never replaces an existing digest. The profile does
    // not reference this image until every byte and the directory are synced.
    // A crash during creation leaves one unreferenced, pressure-counted file;
    // no hard-link/unlink interval can strand a generation with two links.
    descriptor = fs.openSync(target, fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_NOFOLLOW, 0o600);
    fs.writeFileSync(descriptor, image); fs.fsyncSync(descriptor); fs.closeSync(descriptor); descriptor = undefined;
    fsyncStateDirectory(directory);
    deadline(started, DISPATCH_HISTORY_LIMITS.writerDeadlineMs, "writer");
    assertDispatchHistoryWriterDeadline();
  } finally {
    if (descriptor !== undefined) fs.closeSync(descriptor);
  }
}

function rollbackInventory<B extends Binding>(roots: readonly DispatchHistoryRoot<B>[],
  records: readonly RecordRow[], terminals: readonly DispatchTerminalProof[]) {
  const byRoot = new Map<string, string[]>();
  for (const row of records) {
    const values = byRoot.get(row.rootDigest) ?? []; values.push(row.binding); byRoot.set(row.rootDigest, values);
  }
  const inventory: DispatchRollbackRootInventory[] = roots.map(root => {
    const { dispatch, dispatchHistory: ignored, ...metadata } = root;
    const rootDigest = dispatchHistoryRootDigest(root), historical = byRoot.get(rootDigest) ?? [];
    const bindings = [...(dispatch ?? []).map(binding => JSON.stringify(binding)), ...historical].sort();
    return { rootDigest, rootMetadataSha256: hash(JSON.stringify(metadata)), inventorySha256: hash(JSON.stringify(bindings)),
      hot: dispatch?.length ?? 0, historical: historical.length, total: bindings.length };
  }).sort((a,b) => a.rootDigest.localeCompare(b.rootDigest));
  return { roots: inventory, terminalSha256: hash(JSON.stringify(terminals.map(proof => JSON.stringify(proof)).sort())) };
}

/**
 * Stage a complete immutable history BEFORE the profile rename. That rename
 * alone selects the generation. The SQLite mutation-lock COMMIT stores no
 * binding data and cannot undo publication. Unreferenced generations remain
 * bounded recovery/rollback evidence; they are never automatically pruned.
 */
export function updateDispatchHistory<B extends Binding, R extends DispatchHistoryRoot<B>>(roots: readonly R[], now: Date,
  transform: (root: R, bindings: readonly B[]) => B[], validate: (input: unknown) => B, terminalProof?: DispatchTerminalProof) {
  const started = performance.now();
  if (!Number.isFinite(now.getTime())) throw new Error("dispatch_clock_invalid");
  const previous = inventoryArchive(roots, validate);
  const buildingHistory = hasDispatchHistoryAdoption() || Boolean(previous) || Boolean(terminalProof);
  const terminals=[...(previous?.terminals??[])];
  if(terminalProof) {
    const prior=terminals.find(proof=>proof.proofId===terminalProof.proofId);
    if(prior&&JSON.stringify(prior)!==JSON.stringify(terminalProof))throw new Error("dispatch_history_terminal_collision");
    if(!prior)terminals.push(dispatchTerminalProofSchema.parse(terminalProof));
    if(terminals.length>DISPATCH_HISTORY_LIMITS.terminalRows)throw new Error("dispatch_history_terminal_bound_exceeded");
  }
  const byRoot = new Map<string, RecordRow[]>();
  for (const row of previous?.records ?? []) {
    const rows = byRoot.get(row.rootDigest) ?? []; rows.push(row); byRoot.set(row.rootDigest, rows);
  }
  const nextRecords = new Map((previous?.records ?? []).map(row => [recordKey(row), row]));
  const legacyRoots: R[] = [];
  let archived = 0;
  const nextRoots = roots.map(root => {
    deadline(started, DISPATCH_HISTORY_LIMITS.writerDeadlineMs, "writer");
    const rootDigest = dispatchHistoryRootDigest(root), oldRows = byRoot.get(rootDigest) ?? [];
    const old = oldRows.map(row => validate(JSON.parse(row.binding)));
    const combined = [...(root.dispatch ?? []), ...old];
    const seen = new Set<string>();
    for (const binding of combined) {
      const key = JSON.stringify([binding.sessionId, binding.attemptId, binding.validFrom]);
      if (seen.has(key)) throw new Error("dispatch_history_collision");
      seen.add(key);
    }
    const updated = transform(root, combined).map(binding => validate(binding));
    const hot: B[] = [], historical: Array<{ binding: B; serialized: string }> = [];
    for (const binding of updated) {
      const from = Date.parse(binding.validFrom), until = binding.validUntil === null ? Infinity : Date.parse(binding.validUntil);
      if (!Number.isFinite(from) || Number.isNaN(until) || from >= until) throw new Error("capture_dispatch_window_invalid");
      if (Number.isFinite(until) && until <= now.getTime()) historical.push({ binding, serialized: JSON.stringify(binding) });
      else hot.push(binding);
    }
    if (hot.length > DISPATCH_HISTORY_LIMITS.hotRowsPerRoot) throw new Error("dispatch_binding_capacity_exceeded");
    if (historical.length > DISPATCH_HISTORY_LIMITS.historyRowsPerRoot) throw new Error("dispatch_history_row_bound_exceeded");
    // Previously archived evidence is immutable, including a closed interval
    // of an attempt whose native thread later continues under another interval.
    if (buildingHistory) {
      const historicalBytes = new Set(historical.map(entry => entry.serialized));
      for (const row of oldRows) if (!historicalBytes.has(row.binding))
        throw new Error("dispatch_historical_binding_immutable");
      const rootCustody = custody(root);
      for (const { binding, serialized } of historical) {
        const row: RecordRow = { rootDigest, custody: rootCustody, source: root.source,
          sessionId: binding.sessionId, attemptId: binding.attemptId, fromMs: Date.parse(binding.validFrom),
          untilMs: Date.parse(binding.validUntil!), binding: serialized };
        if (Buffer.byteLength(row.binding) > DISPATCH_HISTORY_LIMITS.bindingBytes || Buffer.byteLength(row.custody) > 8_192)
          throw new Error("dispatch_history_byte_bound_exceeded");
        const key = recordKey(row), existing = nextRecords.get(key);
        if (existing && JSON.stringify(existing) !== JSON.stringify(row)) throw new Error("dispatch_history_collision");
        if (!existing) { nextRecords.set(key, row); archived++; }
      }
    }
    const { dispatchHistory: ignored, ...legacy } = root;
    legacyRoots.push({ ...legacy, dispatch: updated } as R);
    hot.sort((a, b) => Date.parse(b.validFrom) - Date.parse(a.validFrom) || b.attemptId.localeCompare(a.attemptId) || b.sessionId.localeCompare(a.sessionId));
    return { ...root, dispatch: hot, dispatchHistory: undefined };
  });
  // Ordinary binds that fit the legacy representation keep that representation.
  // Over-cap admission and every history/terminal publication require explicit
  // qualification before even creating an archive directory or generation file.
  if (!hasDispatchHistoryAdoption()) {
    if (!previous && !terminalProof && legacyRoots.every(root => (root.dispatch?.length ?? 0) <= DISPATCH_HISTORY_LIMITS.hotRowsPerRoot))
      return { roots: legacyRoots, archived: 0, pruned: 0 as const };
    dispatchHistoryAdoptionRequired(roots);
  }
  assertDispatchHistoryBuildPairQualified(roots);
  if (nextRecords.size > DISPATCH_HISTORY_LIMITS.historyRows) throw new Error("dispatch_history_row_bound_exceeded");
  const records = [...nextRecords.values()];
  let rawBytes=0;
  for(const row of records) {
    rawBytes+=Buffer.byteLength(row.binding)+Buffer.byteLength(row.custody);
    if(rawBytes>DISPATCH_HISTORY_LIMITS.rawBytes)throw new Error("dispatch_history_byte_bound_exceeded");
  }
  for(const proof of terminals) {
    rawBytes+=Buffer.byteLength(JSON.stringify(proof));
    if(rawBytes>DISPATCH_HISTORY_LIMITS.rawBytes)throw new Error("dispatch_history_byte_bound_exceeded");
  }
  const counts = new Map<string, number>();
  for (const row of records) counts.set(row.rootDigest, (counts.get(row.rootDigest) ?? 0) + 1);
  deadline(started, DISPATCH_HISTORY_LIMITS.writerDeadlineMs, "writer");
  const staged = archived || terminals.length!==(previous?.terminals.length??0) ? buildArchive(records, now, terminals) : null;
  const generation = staged ?? previous?.ref;
  const result = nextRoots.map(root => {
    const rootDigest = dispatchHistoryRootDigest(root), rootRows = counts.get(rootDigest) ?? 0;
    const { dispatchHistory: ignored, ...withoutHistory } = root;
    return { ...withoutHistory, ...(rootRows && generation ? { dispatchHistory: {
      schema: "dispatch-history/v1" as const, generation: generation.generation, sha256: generation.sha256,
      totalRows: records.length, rootDigest, rootRows,terminalRows:terminals.length,
    } } : {}) } as R;
  });
  if (generation) {
    const image = staged?.image ?? readPrivateStateFile(previous!.file, DISPATCH_HISTORY_LIMITS.fileBytes);
    if (hash(image) !== generation.sha256) throw new Error("dispatch_history_digest_mismatch");
    qualifyDispatchHistoryPublication(result, image, { generation: generation.generation,
      ...rollbackInventory(result, records, terminals) }, now);
    if (!dispatchHistoryPublicationQualified(result)) throw new Error("dispatch_history_adoption_reader_inventory_mismatch");
    if (staged) publishArchive(staged);
  }
  return { roots: result, archived, pruned: 0 as const };
}

/** All config writers, not only bind/close, must enforce the same adoption gate. */
export function qualifyStoredDispatchHistoryForPublication<B extends Binding>(roots: readonly DispatchHistoryRoot<B>[],
  validate: (input: unknown) => B, now = new Date(), actualProfile?: Buffer) {
  if (!roots.some(root => root.dispatchHistory)) return;
  if (dispatchHistoryPublicationQualified(roots, actualProfile)) { assertDispatchHistoryWriterDeadline(); return; }
  if (!hasDispatchHistoryAdoption()) dispatchHistoryAdoptionRequired(roots);
  const archive = inventoryArchive(roots, validate);
  if (!archive) throw new Error("dispatch_history_adoption_history_missing");
  const image = readPrivateStateFile(archive.file, DISPATCH_HISTORY_LIMITS.fileBytes);
  if (hash(image) !== archive.ref.sha256) throw new Error("dispatch_history_digest_mismatch");
  qualifyDispatchHistoryPublication(roots, image, { generation: archive.ref.generation,
    ...rollbackInventory(roots, archive.records, archive.terminals) }, now, actualProfile);
}

/** Lossless downgrade only when the legacy finite profile can represent every current binding. */
export function materializeDispatchHistoryForRollback<B extends Binding, R extends DispatchHistoryRoot<B>>(
  roots: readonly R[], validate: (input: unknown) => B): R[] {
  const archive = inventoryArchive(roots, validate);
  if(archive?.terminals.length) throw new Error("dispatch_history_rollback_terminal_markers_unsupported");
  return roots.map(root => {
    const digest = dispatchHistoryRootDigest(root);
    const history = archive?.records.filter(row => row.rootDigest === digest).map(row => validate(JSON.parse(row.binding))) ?? [];
    const dispatch = [...(root.dispatch ?? []), ...history];
    if (dispatch.length > DISPATCH_HISTORY_LIMITS.hotRowsPerRoot) throw new Error("dispatch_history_rollback_capacity_exceeded");
    const { dispatchHistory: ignored, ...legacy } = root;
    return { ...legacy, dispatch } as R;
  });
}

/** Every writer must carry previously published historical evidence forward. */
export function validateDispatchHistoryTransition<B extends Binding>(before: readonly DispatchHistoryRoot<B>[],
  after: readonly DispatchHistoryRoot<B>[], validate: (input: unknown) => B) {
  const previous = inventoryArchive(before, validate), next = inventoryArchive(after, validate);
  if (!previous) return;
  const retained = new Set(after.map(dispatchHistoryRootDigest));
  const indexed = new Map((next?.records ?? []).map(row => [recordKey(row), row.binding]));
  for (const row of previous.records) {
    if (retained.has(row.rootDigest) && indexed.get(recordKey(row)) !== row.binding)
      throw new Error("dispatch_history_generation_loss");
  }
  for (const proof of previous.terminals) if (!next?.terminals.some(candidate=>JSON.stringify(candidate)===JSON.stringify(proof)))
    throw new Error("dispatch_history_terminal_loss");
}

export function assertNoTerminalDispatchContinuation<B extends Binding>(roots: readonly DispatchHistoryRoot<B>[],
  binding:B,validate:(input:unknown)=>B) {
  const archive=inventoryArchive(roots,validate),digests=new Set(roots.map(dispatchHistoryRootDigest));
  for(const proof of archive?.terminals??[]) {
    if(proof.sessionId!==binding.sessionId||!proof.bindings.some(target=>digests.has(target.rootDigest)))continue;
    if(proof.terminalScope==="attempt_irrevocably_retired"&&proof.attemptId!==binding.attemptId)continue;
    if(binding.validUntil===null||Date.parse(binding.validUntil)>Date.parse(proof.terminalAt))
      throw new Error("dispatch_terminal_identity_cannot_continue");
  }
}

export function dispatchHistoryPressure<B extends Binding>(roots: readonly DispatchHistoryRoot<B>[], validate: (input: unknown) => B) {
  try {
    inventoryArchive(roots, validate);
    return { state: "known" as const, limits: DISPATCH_HISTORY_LIMITS, roots: roots.map(root => ({
      rootId: root.rootId, hot: root.dispatch?.length ?? 0,
      openOrUnknown: root.dispatch?.filter(binding => binding.validUntil === null).length ?? 0,
      historical: root.dispatchHistory?.rootRows ?? 0,
      availableHot: DISPATCH_HISTORY_LIMITS.hotRowsPerRoot - (root.dispatch?.length ?? 0),
    })) };
  } catch (error) {
    return { state: "unknown" as const, limits: DISPATCH_HISTORY_LIMITS, availableHot: null,
      reason: error instanceof Error ? error.message : "dispatch_history_unavailable" };
  }
}
export function dispatchHistoryLookupStatus() { return { state: historyFailure ? "unknown" : "ready", reason: historyFailure }; }
