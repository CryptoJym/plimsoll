/** Explicit import of fenced, pre-enrollment JSONL history. */
import crypto from "node:crypto";
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { performance } from "node:perf_hooks";
import type Database from "better-sqlite3";
import { aiInteractionEventSchema, estimateCostUsd, type AiInteractionEvent } from "../../shared/src/index";
import { priceForModel } from "../../shared/src/pricing";
import type { LocalEventBuffer } from "./buffer";
import { captureBaselineExcludedReceipt, captureBaselineStatus } from "./capture-baseline";
import { appendRootObservation, captureRootDigest, captureRootObservationPayloadDigest, inspectCaptureRoots,
  prepareCaptureRootObservationSchema, recordClaudeRootSessionSighting,
  rootEventMetadata, validateCaptureRoots, type CaptureRoot } from "./capture-root-inventory";
import { deterministicEventId } from "./normalizer";
import { ensureJsonlScanState, jsonlScanStateKey, rememberJsonlScanCursor,
  type JsonlTailRead } from "./jsonl-byte-tailer";
import { rootCursorKey } from "./capture-root-inventory";

const UUID_AT_END = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const MAX_LINE_BYTES = 16 * 1024 * 1024;
const READ_BYTES = 128 * 1024;
const WRITER_INITIAL_ROWS = 4;
// Leave room for a slow individual row and commit while targeting 120 ms.
// The 64-row cap produced a 264.8 ms slice on a 32-core host at 0.42 load/core.
const WRITER_MAX_ROWS = 40;
const WRITER_TARGET_MS = 120;
const WRITER_HARD_MS = 250;
const WAL_LIMIT_BYTES = 256 * 1024 * 1024;
const WAL_STALL_MS = 60_000;
type DB = Database.Database;
type Amounts = { input: number; cacheRead: number; cacheCreation: number; output: number };
type CodexState = { sessionId: string; previous: Amounts; index: number; observedBaseline: boolean;
  reasoningOutput: number; contextOccurrenceIndex: number; model?: string; sessionStartedAt?: string;
  originator?: string; cliVersion?: string; planType?: string };
type ClaudeRevision = { sessionId: string; messageId: string; messageKey: string; current: Amounts };
type RecordBytes = { recordIndex: number; byteOffset: number; digest: string };
type SessionBytes = { length: number; digest: string; records?: RecordBytes[] };
type PrefixCheckpoint = RecordBytes & { current: Amounts; messageKey?: string; codexState?: CodexState };
type ConfinedParent = { path: string; stamp: string };
type File = { file: string; fileKey: string; limit: number; stamp: string; fencedAt: string;
  parents: ConfinedParent[];
  sessionId?: string; records?: RecordBytes[]; lineOffset?: number; lineDigest?: () => string;
  shorterImportedCopy?: boolean; writtenRecordIndex?: number; importedByteLength?: number;
  recordRefusals?: Array<{ reason: string; byteOffset: number; usage?: boolean }>;
  prefixHash?: string; savedPrefixHash?: string; baselineDefined?: boolean;
  initialCodex?: CodexState; finalCodex?: CodexState;
  initialClaude?: Map<string, Amounts>; finalClaude?: Map<string, ClaudeRevision>;
  cursor?: { observedSize: number; committedOffset: number; fileIdentity: string;
    headHash: string | null; headBytes: number; continuityHash: string | null;
    continuityBytes: number; mtimeMs: number; ctimeMs: number } };
type Candidate = { event: AiInteractionEvent; sourceId: string; claudeRevision?: ClaudeRevision;
  prefixCheckpoint?: PrefixCheckpoint };
export type CaptureHistoryPlan = {
  refusals: Array<{ fileKey: string; reason: string; missingRows: 0 }>;
  status: "capture_roots_history_plan"; rootId: string; source: CaptureRoot["source"];
  dryRun: true; files: number; sessions: number; skippedLiveSessions: number;
  existingRows: number; missingRows: number; firstObservedAt: string | null;
  lastObservedAt: string | null; tokens: Amounts; fencedBytes: number; since: string | null;
};
export type CaptureHistoryApplyReceipt = Omit<CaptureHistoryPlan, "status" | "dryRun"> & {
  status: "capture_roots_history_imported"; importedRows: number; importedTokens: Amounts;
  totalImportedRows: number; maxWriterSliceMs: number; overBudgetSlices: number;
  maxWriterWorkMs: number; maxWriterRowMs: number; writerSliceHistogram: Record<string, number>;
  writerSlices: number; timeBudgetStops: number; runId: string; attemptId: string;
  maxWalBytes: number; walPauseMs: number;
};
type Options = { since?: string; stopAfterSlices?: number; attemptId?: string;
  /** Synthetic proof override; the CLI never accepts these controls. */
  walLimitBytes?: number; walStallMs?: number };

function refusal(reason: string): never { throw new Error(`capture_history_refused:${reason}`); }
function validIso(value: string) {
  return Number.isFinite(Date.parse(value)) && new Date(value).toISOString() === value;
}
function table(db: DB, name: string) {
  return Boolean(db.prepare(`select 1 from sqlite_master where type='table' and name=?`).get(name));
}
function stamp(stat: fs.BigIntStats) {
  // A live provider may append after the fence; size/mtime are not generation
  // identity. The fenced bytes themselves are hashed across the two scans.
  return `${stat.dev}:${stat.ino}:${stat.birthtimeNs}`;
}
function beforeEnrollment(stat: fs.BigIntStats, enrolledAt: string) {
  const at = BigInt(Date.parse(enrolledAt)) * 1_000_000n;
  return stat.birthtimeNs <= at || stat.mtimeNs <= at;
}
function confinedDirectory(directory: string, expected?: ConfinedParent) {
  let stat: fs.BigIntStats;
  try { stat = fs.lstatSync(directory, { bigint: true }); }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") refusal("root_parent_changed");
    throw error;
  }
  if (!stat.isDirectory() || (expected && stamp(stat) !== expected.stamp))
    refusal("root_parent_changed");
  return { path: directory, stamp: stamp(stat) };
}
function verifyParents(file: File) {
  for (const parent of file.parents) confinedDirectory(parent.path, parent);
}
function candidateFiles(db: DB, root: CaptureRoot): File[] {
  validateCaptureRoots([root]);
  if (inspectCaptureRoots([root])[0]?.state !== "ready") refusal("root_not_physical_and_ready");
  const source = captureBaselineStatus(db).sources.find(row => row.source === root.source);
  if (!source || source.status !== "complete" || source.unresolvedObservationErrors !== 0)
    refusal("baseline_incomplete_or_ambiguous");
  const found: Array<File | { file: string; stat: fs.BigIntStats }> = [];
  const rootParent = confinedDirectory(root.directory);
  const dirs = [{ directory: root.directory, parents: [rootParent] }];
  let entries = 0;
  while (dirs.length) {
    const { directory, parents } = dirs.pop()!;
    for (const parent of parents) confinedDirectory(parent.path, parent);
    const handle = fs.opendirSync(directory, { bufferSize: 32 });
    try {
      // opendir can follow an exchanged parent. Check again after it returns,
      // then bind every descendant to the physical chain seen at discovery.
      for (const parent of parents) confinedDirectory(parent.path, parent);
      let entry: fs.Dirent | null;
      while ((entry = handle.readSync())) {
        entries += 1;
        if (entries > 2_000_000) refusal("root_entry_limit");
        const file = path.join(directory, entry.name);
        for (const parent of parents) confinedDirectory(parent.path, parent);
        const stat = fs.lstatSync(file, { bigint: true });
        if (entry.isSymbolicLink() || stat.isSymbolicLink()) refusal("root_symlink_entry");
        if (entry.isDirectory()) {
          if (!stat.isDirectory()) refusal("root_parent_changed");
          dirs.push({ directory: file, parents: [...parents,
            { path: file, stamp: stamp(stat) }] });
          continue;
        }
        if (!entry.isFile() || !stat.isFile()) refusal("root_nonregular_entry");
        if (!entry.name.endsWith(".jsonl") ||
            (root.source === "codex" && !entry.name.startsWith("rollout-"))) continue;
        for (const parent of parents) confinedDirectory(parent.path, parent);
        const receipt = captureBaselineExcludedReceipt(db, root.source, {
          path: file, device: stat.dev, inode: stat.ino, size: stat.size, birthtimeNs: stat.birthtimeNs,
        });
        if (receipt) {
          if (BigInt(receipt.baselineSize) > stat.size) refusal("fenced_prefix_truncated");
          found.push({ file, fileKey: crypto.createHash("sha256").update(`${root.rootId}\0${file}`).digest("hex"),
            limit: receipt.baselineSize, stamp: stamp(stat), fencedAt: receipt.baselinedAt,
            parents });
        } else found.push({ file, stat });
      }
    } finally { handle.closeSync(); }
    for (const parent of parents) confinedDirectory(parent.path, parent);
  }
  const fenced = found.filter((file): file is File => "limit" in file);
  if (!fenced.length) refusal("no_fenced_generation_evidence");
  const fenceTimes = new Set(fenced.map(file => file.fencedAt));
  if (fenceTimes.size !== 1) refusal("mixed_root_fence_instants");
  const enrolledAt = fenced[0]!.fencedAt;
  for (const file of found) {
    if ("stat" in file && beforeEnrollment(file.stat, enrolledAt)) refusal("unfenced_pre_enrollment_file");
  }
  return fenced.sort((a, b) => a.file.localeCompare(b.file));
}

function openFencedFile(file: File): number {
  // The path can be exchanged after directory discovery. O_NOFOLLOW closes
  // the check/open race; the descriptor and path must still name the fence.
  verifyParents(file);
  const before = fs.lstatSync(file.file, { bigint: true });
  if (!before.isFile() || stamp(before) !== file.stamp || before.size < BigInt(file.limit))
    refusal("file_changed_before_read");
  let fd: number;
  try { fd = fs.openSync(file.file, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW); }
  catch (error) {
    if (["ELOOP", "EMLINK", "ENOENT"].includes((error as NodeJS.ErrnoException).code ?? ""))
      refusal("file_changed_before_read");
    throw error;
  }
  try {
    const opened = fs.fstatSync(fd, { bigint: true });
    verifyParents(file);
    const after = fs.lstatSync(file.file, { bigint: true });
    if (!opened.isFile() || !after.isFile() || stamp(opened) !== file.stamp ||
        stamp(after) !== file.stamp || opened.size < BigInt(file.limit))
      refusal("file_changed_before_read");
    return fd;
  } catch (error) { fs.closeSync(fd); throw error; }
}

/** Reads only complete lines inside the exact fenced prefix; source bytes do
 * not enter a receipt or the ledger. */
function* lines(file: File): Generator<string> {
  const fd = openFencedFile(file);
  const decoder = new TextDecoder("utf-8", { fatal: true });
  const hash = crypto.createHash("sha256");
  const recordHash = crypto.createHash("sha256");
  let offset = 0;
  let pending = Buffer.alloc(0);
  try {
    const before = fs.fstatSync(fd, { bigint: true });
    if (stamp(before) !== file.stamp || before.size < BigInt(file.limit))
      refusal("file_changed_before_read");
    while (offset < file.limit) {
      const take = Math.min(READ_BYTES, file.limit - offset);
      const part = Buffer.allocUnsafe(take);
      const read = fs.readSync(fd, part, 0, take, offset);
      if (read !== take) refusal("fenced_prefix_short_read");
      hash.update(part);
      offset += read;
      const bytes = pending.length ? Buffer.concat([pending, part]) : part;
      let start = 0;
      for (let index = 0; index < bytes.length; index += 1) {
        if (bytes[index] !== 10) continue;
        const line = bytes.subarray(start, index);
        if (line.length > MAX_LINE_BYTES) refusal("record_exceeds_byte_budget");
        recordHash.update(bytes.subarray(start, index + 1));
        file.lineOffset = offset - bytes.length + index + 1;
        file.lineDigest = () => recordHash.copy().digest("hex");
        let text: string;
        try { text = decoder.decode(line); }
        catch {
          (file.recordRefusals ??= []).push({ reason: "source_utf8_invalid", byteOffset: file.lineOffset });
          text = line.toString("utf8");
        }
        yield text;
        start = index + 1;
      }
      pending = Buffer.from(bytes.subarray(start));
      if (pending.length > MAX_LINE_BYTES) refusal("record_exceeds_byte_budget");
    }
    if (pending.length) refusal("fenced_partial_record");
    const after = fs.fstatSync(fd, { bigint: true });
    verifyParents(file);
    const currentPath = fs.lstatSync(file.file, { bigint: true });
    if (!currentPath.isFile() || stamp(currentPath) !== file.stamp ||
        stamp(after) !== file.stamp || after.size < BigInt(file.limit))
      refusal("file_changed_during_read");
    const digest = hash.digest("hex");
    if (file.prefixHash && file.prefixHash !== digest) refusal("fenced_prefix_changed");
    if (file.savedPrefixHash && file.savedPrefixHash !== digest)
      refusal("fenced_history_changed_since_import");
    file.prefixHash = digest;
    const headBytes = Math.min(512, file.limit);
    const continuityBytes = Math.min(512, file.limit);
    const head = Buffer.alloc(headBytes);
    const continuity = Buffer.alloc(continuityBytes);
    if (headBytes && fs.readSync(fd, head, 0, headBytes, 0) !== headBytes) refusal("fenced_prefix_short_read");
    if (continuityBytes && fs.readSync(fd, continuity, 0, continuityBytes,
      file.limit - continuityBytes) !== continuityBytes) refusal("fenced_prefix_short_read");
    file.cursor = { observedSize: Number(after.size), committedOffset: file.limit,
      fileIdentity: stamp(after), headHash: headBytes ? crypto.createHash("sha256").update(head).digest("hex") : null,
      headBytes, continuityHash: continuityBytes ? crypto.createHash("sha256").update(continuity).digest("hex") : null,
      continuityBytes, mtimeMs: Number(after.mtimeNs) / 1_000_000,
      ctimeMs: Number(after.ctimeNs) / 1_000_000 };
  } finally { fs.closeSync(fd); }
}

function verifyFencedPrefix(file: File) {
  if (!file.prefixHash) refusal("prefix_hash_missing");
  const fd = openFencedFile(file);
  const hash = crypto.createHash("sha256");
  try {
    const before = fs.fstatSync(fd, { bigint: true });
    if (stamp(before) !== file.stamp || before.size < BigInt(file.limit))
      refusal("file_changed_before_read");
    for (let offset = 0; offset < file.limit;) {
      const take = Math.min(1024 * 1024, file.limit - offset);
      const chunk = Buffer.allocUnsafe(take);
      if (fs.readSync(fd, chunk, 0, take, offset) !== take) refusal("fenced_prefix_short_read");
      hash.update(chunk);
      offset += take;
    }
    const after = fs.fstatSync(fd, { bigint: true });
    verifyParents(file);
    if (stamp(after) !== file.stamp || after.size < BigInt(file.limit) ||
        hash.digest("hex") !== file.prefixHash) refusal("fenced_prefix_changed");
  } finally { fs.closeSync(fd); }
}

function amount(value: unknown) {
  if (value === undefined) return 0;
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0)
    refusal("invalid_token_counter");
  return value;
}
function totals(value: unknown): Amounts {
  if (!value || typeof value !== "object" || Array.isArray(value)) refusal("missing_token_totals");
  const row = value as Record<string, unknown>;
  return { input: amount(row.input_tokens),
    cacheRead: amount(row.cached_input_tokens ?? row.cache_read_input_tokens),
    cacheCreation: amount(row.cache_creation_input_tokens), output: amount(row.output_tokens) };
}
function positiveDelta(now: Amounts, prior: Amounts): Amounts {
  if (now.input < prior.input || now.cacheRead < prior.cacheRead ||
      now.cacheCreation < prior.cacheCreation || now.output < prior.output)
    refusal("counter_regression");
  return { input: now.input - prior.input, cacheRead: now.cacheRead - prior.cacheRead,
    cacheCreation: now.cacheCreation - prior.cacheCreation, output: now.output - prior.output };
}
function zero(value: Amounts) {
  return value.input === 0 && value.output === 0 && value.cacheRead === 0 && value.cacheCreation === 0;
}
function sameAmounts(a: Amounts, b: Amounts) {
  return a.input === b.input && a.cacheRead === b.cacheRead &&
    a.cacheCreation === b.cacheCreation && a.output === b.output;
}
function belowAmounts(a: Amounts, b: Amounts) {
  return a.input < b.input || a.cacheRead < b.cacheRead ||
    a.cacheCreation < b.cacheCreation || a.output < b.output;
}
const ZERO_AMOUNTS: Amounts = { input: 0, cacheRead: 0, cacheCreation: 0, output: 0 };
function initialCodexState(sessionId: string): CodexState {
  return { sessionId, previous: { ...ZERO_AMOUNTS }, index: -1,
    observedBaseline: false, reasoningOutput: 0, contextOccurrenceIndex: -1 };
}
function decodeCodexState(raw: string, sessionId: string): CodexState {
  let state: CodexState;
  try { state = JSON.parse(raw) as CodexState; }
  catch { refusal("import_counter_state_invalid"); }
  if (state.sessionId !== sessionId || !Number.isSafeInteger(state.index) || state.index < -1 ||
      !Number.isSafeInteger(state.contextOccurrenceIndex) || state.contextOccurrenceIndex < -1 ||
      typeof state.observedBaseline !== "boolean" || !state.previous ||
      !Object.values(state.previous).every(value => Number.isSafeInteger(value) && value >= 0))
    refusal("import_counter_state_invalid");
  return state;
}
function loadFileBaseline(db: DB, root: CaptureRoot, file: File) {
  if (!table(db, "capture_history_file_state")) return;
  const saved = db.prepare(`select root_id as rootId,source,prefix_hash as prefixHash,
    baseline_json as baselineJson,handoff_ready as ready,published_rows as publishedRows
    from capture_history_file_state where file_key=?`)
    .get(file.fileKey) as { rootId: string; source: string; prefixHash: string;
      baselineJson: string; ready: number; publishedRows: number } | undefined;
  if (!saved) return;
  if (saved.rootId !== root.rootId || saved.source !== root.source) refusal("import_file_state_conflict");
  // Publication belongs to this file, not to the entire root. An earlier
  // file may have committed while this one's prefix was still unverified.
  if (saved.ready !== 1 && saved.publishedRows === 0) return;
  let baseline: { codex?: string; claude?: Array<[string, Amounts]> };
  try { baseline = JSON.parse(saved.baselineJson) as typeof baseline; }
  catch { refusal("import_file_state_invalid"); }
  file.savedPrefixHash = saved.prefixHash;
  file.baselineDefined = true;
  if (baseline.codex) {
    const session = path.basename(file.file, ".jsonl").match(UUID_AT_END)?.[0]?.toLowerCase();
    if (!session) refusal("codex_file_session_missing");
    file.initialCodex = decodeCodexState(baseline.codex, session);
  }
  file.initialClaude = new Map(baseline.claude ?? []);
}
function durableCodexState(db: DB, sessionId: string): CodexState | undefined {
  if (!table(db, "capture_history_session_counters")) return undefined;
  const row = db.prepare(`select state_json as state from capture_history_session_counters
    where source='codex' and session_id=?`).get(sessionId) as { state: string } | undefined;
  return row ? decodeCodexState(row.state, sessionId) : undefined;
}
function rememberPrefixCheckpoint(db: DB, source: CaptureRoot["source"], sessionId: string,
  checkpoint: PrefixCheckpoint) {
  db.prepare(`insert or ignore into capture_history_import_prefixes
    (source,session_id,prefix_digest,input_tokens,cache_read_tokens,
      cache_creation_tokens,output_tokens,message_key,parser_state_json)
    values (?,?,?,?,?,?,?,?,?)`).run(source, sessionId, checkpoint.digest,
      checkpoint.current.input, checkpoint.current.cacheRead,
      checkpoint.current.cacheCreation, checkpoint.current.output,
      checkpoint.messageKey ?? null, checkpoint.codexState ? JSON.stringify(checkpoint.codexState) : null);
}

function importedSessionBytes(db: DB, source: CaptureRoot["source"], sessionId: string) {
  if (!table(db, "capture_history_session_bytes")) return undefined;
  return db.prepare(`select imported_length as length,prefix_digest as digest
    from capture_history_session_bytes where source=? and session_id=?`)
    .get(source, sessionId) as SessionBytes | undefined;
}
function importedSessionWithoutBytes(db: DB, source: CaptureRoot["source"], sessionId: string) {
  return (table(db, "capture_history_import_prefixes") && Boolean(db.prepare(`select 1
    from capture_history_import_prefixes where source=? and session_id=? limit 1`).get(source, sessionId))) ||
    (table(db, "capture_history_session_counters") && Boolean(db.prepare(`select 1
      from capture_history_session_counters where source=? and session_id=?`).get(source, sessionId)));
}
function fingerprintLength(file: File, length: number) {
  if (length === file.limit) return file.prefixHash!;
  const fd = openFencedFile(file);
  const hash = crypto.createHash("sha256");
  try {
    for (let offset = 0; offset < length;) {
      const take = Math.min(READ_BYTES, length - offset);
      const bytes = Buffer.allocUnsafe(take);
      if (fs.readSync(fd, bytes, 0, take, offset) !== take) refusal("fenced_prefix_short_read");
      hash.update(bytes); offset += take;
    }
    verifyParents(file);
    return hash.digest("hex");
  } finally { fs.closeSync(fd); }
}
function verifyImportedBytes(db: DB, root: CaptureRoot, file: File, verified: Map<string, SessionBytes>) {
  if (!file.sessionId) return;
  const stored = importedSessionBytes(db, root.source, file.sessionId);
  const prior = verified.get(file.sessionId) ?? stored;
  if (!prior && importedSessionWithoutBytes(db, root.source, file.sessionId))
    refusal("imported_prefix_fingerprint_missing");
  if (prior) {
    if (!Number.isSafeInteger(prior.length) || prior.length < 0 || !/^[0-9a-f]{64}$/.test(prior.digest))
      refusal("imported_prefix_fingerprint_missing");
    if (file.limit >= prior.length) {
      if (fingerprintLength(file, prior.length) !== prior.digest)
        // Keep the existing refusal label for unmodified reviewer callers.
        refusal("counter_regression:copied_prefix_bytes_differ");
    } else {
      const last = file.records?.at(-1);
      if (!last) refusal("copied_prefix_usage_record_missing");
      const saved = prior.records ? prior.records.find(record => record.recordIndex === last.recordIndex &&
        record.byteOffset === last.byteOffset) : table(db, "capture_history_record_bytes")
        ? db.prepare(`select prefix_digest as digest from capture_history_record_bytes
          where source=? and session_id=? and record_index=? and byte_offset=?`)
          .get(root.source, file.sessionId, last.recordIndex, last.byteOffset) as { digest: string } | undefined
        : undefined;
      if (!saved || !/^[0-9a-f]{64}$/.test(saved.digest)) refusal("copied_prefix_record_fingerprint_missing");
      if (saved.digest !== last.digest) refusal("counter_regression:copied_prefix_bytes_differ");
      file.shorterImportedCopy = true;
    }
    file.importedByteLength = stored?.length;
  }
  if (!prior || file.limit > prior.length)
    return { length: file.limit, digest: file.prefixHash!, records: file.records };
}
function rememberRecordBytes(db: DB, source: CaptureRoot["source"], sessionId: string, record: RecordBytes) {
  db.prepare(`insert into capture_history_record_bytes
    (source,session_id,record_index,byte_offset,prefix_digest) values (?,?,?,?,?)
    on conflict(source,session_id,record_index,byte_offset) do update set prefix_digest=excluded.prefix_digest
    where excluded.byte_offset>coalesce((select imported_length from capture_history_session_bytes
      where source=excluded.source and session_id=excluded.session_id),0)`)
    .run(source, sessionId, record.recordIndex, record.byteOffset, record.digest);
}
function rememberSessionBytes(db: DB, source: CaptureRoot["source"], sessionId: string, length: number, digest: string) {
  db.prepare(`insert into capture_history_session_bytes (source,session_id,imported_length,prefix_digest)
    values (?,?,?,?) on conflict(source,session_id) do update set
      imported_length=excluded.imported_length,prefix_digest=excluded.prefix_digest
    where excluded.imported_length>capture_history_session_bytes.imported_length`)
    .run(source, sessionId, length, digest);
}
function isLiveSession(db: DB, source: CaptureRoot["source"], sessionId: string) {
  return (db.prepare(`select authority from session_usage_authority where source=? and session_id=?`)
    .get(source, sessionId) as { authority: string } | undefined)?.authority === "live";
}
function lastStoredRecordIndex(db: DB, root: CaptureRoot, file: File) {
  const length = Math.min(importedSessionBytes(db, root.source, file.sessionId!)?.length ?? 0, file.limit);
  const saved = db.prepare(`select max(record_index) as n from capture_history_record_bytes
    where source=? and session_id=? and byte_offset<=?`).get(root.source, file.sessionId, length) as { n: number | null };
  return saved.n ?? -1;
}
async function prepareRecordBytes(db: DB, root: CaptureRoot, file: File, through: number,
  afterSlice: () => Promise<void>) {
  let written = file.writtenRecordIndex ?? lastStoredRecordIndex(db, root, file);
  file.writtenRecordIndex = written;
  // Zero-token records can separate two counted rows by an arbitrary amount.
  // Save the excess in small transactions before the counted-row writer;
  // that writer saves at most 40 remaining fingerprints with its rows.
  while (through - written > WRITER_MAX_ROWS) {
    let last = written;
    let live = false;
    db.transaction(() => {
      verifyParents(file);
      if (isLiveSession(db, root.source, file.sessionId!)) { live = true; return; }
      const started = performance.now();
      let rows = 0;
      do { rememberRecordBytes(db, root.source, file.sessionId!, file.records![++last]!); rows++; }
      while (last < through - WRITER_MAX_ROWS && rows < WRITER_MAX_ROWS &&
        performance.now() - started < WRITER_TARGET_MS);
    }).immediate();
    if (live) return;
    written = last;
    file.writtenRecordIndex = last;
    await afterSlice();
    await new Promise<void>(resolve => setTimeout(resolve, 250));
  }
}
async function rememberRemainingRecordBytes(db: DB, root: CaptureRoot, file: File) {
  if (!file.sessionId || isLiveSession(db, root.source, file.sessionId)) return;
  const saved = lastStoredRecordIndex(db, root, file);
  const remaining = (file.records ?? []).filter(record => record.recordIndex > saved);
  for (let index = 0; index < remaining.length;) {
    db.transaction(() => {
      verifyParents(file);
      if (isLiveSession(db, root.source, file.sessionId!)) { index = remaining.length; return; }
      const started = performance.now();
      let rows = 0;
      do { rememberRecordBytes(db, root.source, file.sessionId!, remaining[index++]!); rows++; }
      while (index < remaining.length && rows < WRITER_MAX_ROWS && performance.now() - started < WRITER_TARGET_MS);
    }).immediate();
    if (index < remaining.length) await new Promise<void>(resolve => setTimeout(resolve, 250));
  }
}
function event(root: CaptureRoot, sourceId: string, sessionId: string, observedAt: string,
  model: string | undefined, delta: Amounts, extra: Record<string, unknown> = {},
  unvalidated = false): AiInteractionEvent {
  if (!validIso(observedAt)) refusal("invalid_observed_time");
  const priced = unvalidated ? null : estimateCostUsd({ model, inputTokens: delta.input, outputTokens: delta.output,
    cacheReadTokens: delta.cacheRead, cacheCreationTokens: delta.cacheCreation });
  const metadata: Record<string, unknown> = {
    ...rootEventMetadata(root, sourceId, observedAt, sessionId),
    usageSource: root.source === "codex" ? "rollout" : "transcript",
    historyImport: "pre_enrollment_fenced_prefix_v1", ...extra,
  };
  if (priced) {
    metadata.costEstimated = true;
    metadata.costKind = "estimated";
    const rate = priceForModel(model);
    if (rate) { metadata.rateVersion = `catalog_${rate.asOf}`; metadata.rateObservedAt = `${rate.asOf}T00:00:00.000Z`; }
  }
  return aiInteractionEventSchema.parse({
    id: sourceId, tenantId: "local", source: root.source, dataMode: "metadata",
    eventType: root.source === "codex" ? "usage_rollout" : "usage_transcript",
    observedAt, sessionId, model, actionClass: "other", inputTokens: delta.input,
    outputTokens: delta.output, cacheReadTokens: delta.cacheRead,
    ...(delta.cacheCreation ? { cacheCreationTokens: delta.cacheCreation } : {}),
    ...(priced ? { costUsd: priced.costUsd, costKind: "estimated" as const } : {}),
    ...(typeof metadata.captureAccountHash === "string" ? { actorId: metadata.captureAccountHash } : {}),
    metadata,
  });
}

function rememberRecordRefusal(file: File, error: unknown, usage = false) {
  const reason = String(error).match(/capture_history_refused:(.+)$/)?.[1];
  if (!reason) throw error;
  (file.recordRefusals ??= []).push({ reason, byteOffset: file.lineOffset!, usage });
}

function* codexEvents(root: CaptureRoot, file: File): Generator<Candidate> {
  const sessionId = path.basename(file.file, ".jsonl").match(UUID_AT_END)?.[0]?.toLowerCase();
  if (!sessionId) refusal("codex_file_session_missing");
  file.sessionId = sessionId;
  // A file is parsed from its original baseline. Cross-folder recognition is
  // a separate byte comparison after EOF, independent of these counters.
  const state = structuredClone(file.initialCodex ?? initialCodexState(sessionId));
  file.initialCodex ??= structuredClone(state);
  file.records = [];
  file.recordRefusals = [];
  for (const line of lines(file)) {
    let usage = false;
    try {
      if (!line.includes('"session_meta"') && !line.includes('"turn_context"') &&
          !line.includes('"token_count"')) continue;
      let parsed: Record<string, any>;
      try { parsed = JSON.parse(line) as Record<string, any>; }
      catch { usage = /"type"\s*:\s*"token_count"/.test(line); refusal("codex_relevant_json_invalid"); }
      if (parsed.type === "session_meta") {
        state.contextOccurrenceIndex += 1;
        const id = parsed.payload?.id;
        if (typeof id !== "string" || id.toLowerCase() !== sessionId) refusal("codex_session_mismatch");
        if (typeof parsed.timestamp === "string") state.sessionStartedAt = parsed.timestamp;
        else if (typeof parsed.payload?.timestamp === "string") state.sessionStartedAt = parsed.payload.timestamp;
        if (typeof parsed.payload?.originator === "string") state.originator = parsed.payload.originator;
        if (typeof parsed.payload?.cli_version === "string") state.cliVersion = parsed.payload.cli_version;
      } else if (parsed.type === "turn_context") {
        state.contextOccurrenceIndex += 1;
        if (typeof parsed.payload?.model === "string") state.model = parsed.payload.model;
      } else if (parsed.type === "event_msg" && parsed.payload?.type === "token_count") {
        usage = true;
        const reported = parsed.payload?.info?.total_token_usage;
        if (!reported) { state.index += 1; continue; }
        const record: RecordBytes = { recordIndex: file.records.length,
          byteOffset: file.lineOffset!, digest: file.lineDigest!() };
        file.records.push(record);
        const current = totals(reported);
        state.index += 1;
        const delta = positiveDelta(current, state.previous);
        const firstUnknown = !state.observedBaseline && !zero(current);
        state.previous = current;
        state.observedBaseline = true;
        state.reasoningOutput = amount(reported.reasoning_output_tokens);
        if (typeof parsed.payload?.rate_limits?.plan_type === "string")
          state.planType = parsed.payload.rate_limits.plan_type;
        if (zero(delta)) continue;
        const id = deterministicEventId(["codex-rollout", sessionId, String(state.index)]);
        const observedAt = parsed.timestamp;
        if (typeof observedAt !== "string") refusal("codex_timestamp_missing");
        const marginal = firstUnknown ? { input: 0, cacheRead: 0, cacheCreation: 0, output: 0 } : delta;
        yield { sourceId: id, event: event(root, id, sessionId, observedAt, state.model, marginal,
          { turnIndex: state.index, ...(firstUnknown ? { counterLineage: "unknown_nonzero_first",
            sourceCumulativeInput: current.input, sourceCumulativeCachedInput: current.cacheRead,
            sourceCumulativeOutput: current.output } : {}) }, firstUnknown),
          prefixCheckpoint: { ...record, current, codexState: structuredClone(state) } };
      }
    } catch (error) { rememberRecordRefusal(file, error, usage); }
  }
  file.finalCodex = structuredClone(state);
}

function* claudeEvents(root: CaptureRoot, file: File): Generator<Candidate> {
  let sessionId = path.basename(file.file, ".jsonl").match(UUID_AT_END)?.[0]?.toLowerCase();
  const byMessage = new Map<string, Amounts>();
  file.initialClaude ??= new Map();
  file.finalClaude = new Map();
  file.records = [];
  file.recordRefusals = [];
  for (const line of lines(file)) {
    let usage = false;
    try {
      if (!line.includes('"assistant"') || !line.includes('"usage"')) continue;
      let parsed: Record<string, any>;
      try { parsed = JSON.parse(line) as Record<string, any>; }
      catch { usage = /"type"\s*:\s*"assistant"/.test(line); refusal("claude_relevant_json_invalid"); }
      if (parsed.type !== "assistant") continue;
      usage = true;
      const claimed = typeof parsed.sessionId === "string"
        ? parsed.sessionId.match(UUID_AT_END)?.[0]?.toLowerCase() : undefined;
      sessionId ??= claimed;
      if (!sessionId || (claimed && claimed !== sessionId)) refusal("claude_session_mismatch");
      file.sessionId = sessionId;
      const record: RecordBytes = { recordIndex: file.records.length,
        byteOffset: file.lineOffset!, digest: file.lineDigest!() };
      file.records.push(record);
      const messageId = parsed.message?.id;
      if (typeof messageId !== "string" || !messageId) refusal("claude_message_id_missing");
      const messageKey = crypto.createHash("sha256").update(messageId).digest("hex");
      const stateKey = `${sessionId}\0${messageKey}`;
      const current = totals(parsed.message?.usage);
      const prior = byMessage.get(messageId) ?? file.initialClaude.get(stateKey);
      const delta = positiveDelta(current, prior ?? ZERO_AMOUNTS);
      byMessage.set(messageId, current);
      const revision = { sessionId, messageId, messageKey, current };
      file.finalClaude.set(stateKey, revision);
      if (zero(delta)) continue;
      const id = prior
        ? deterministicEventId(["claude-transcript-revision", sessionId, messageId,
            String(current.input), String(current.cacheRead), String(current.cacheCreation), String(current.output)])
        : deterministicEventId(["claude-transcript", sessionId, messageId]);
      if (typeof parsed.timestamp !== "string") refusal("claude_timestamp_missing");
      const model = typeof parsed.message?.model === "string" ? parsed.message.model : undefined;
      yield { sourceId: id, event: event(root, id, sessionId, parsed.timestamp, model, delta),
        claudeRevision: revision, prefixCheckpoint: { ...record, current, messageKey } };
    } catch (error) { rememberRecordRefusal(file, error, usage); }
  }
  file.sessionId ??= sessionId;
}

type ScanResult = { plan: CaptureHistoryPlan; files: File[] };
function sourceDigest(files: File[], since: string | undefined) {
  if (files.some(file => !file.prefixHash)) refusal("prefix_hash_missing");
  return crypto.createHash("sha256").update(JSON.stringify([since ?? null,
    files.map(file => [file.file, file.limit, file.stamp, file.fencedAt, file.prefixHash])
  ])).digest("hex");
}
type ResumePoint = { index: number; digest: string | null };
function candidateDigest(candidate: Candidate, index: number) {
  return crypto.createHash("sha256").update(JSON.stringify([
    index, candidate.sourceId, captureRootObservationPayloadDigest(candidate.event),
  ])).digest("hex");
}
async function scan(db: DB, root: CaptureRoot, options: Options,
  onMissing?: (candidate: Candidate, index: number, digest: string, file: File) => Promise<void>,
  expectedFiles?: File[], resume?: ResumePoint,
  onFileReady?: (file: File) => Promise<void>,
  onFilePublished?: (file: File) => Promise<void>): Promise<ScanResult> {
  if (options.since && !validIso(options.since)) refusal("since_invalid_iso");
  const files = candidateFiles(db, root);
  if (expectedFiles && (files.length !== expectedFiles.length || files.some((file, index) =>
    file.file !== expectedFiles[index]?.file || file.stamp !== expectedFiles[index]?.stamp ||
    file.limit !== expectedFiles[index]?.limit || file.fencedAt !== expectedFiles[index]?.fencedAt)))
    refusal("source_changed_after_plan");
  if (expectedFiles) files.forEach((file, index) => {
    const expected = expectedFiles[index]!;
    file.prefixHash = expected.prefixHash;
    file.savedPrefixHash = expected.savedPrefixHash;
    file.baselineDefined = true;
    file.initialCodex = expected.initialCodex ? structuredClone(expected.initialCodex) : undefined;
    file.initialClaude = new Map(expected.initialClaude ?? []);
  });
  else files.forEach(file => loadFileBaseline(db, root, file));
  // A root observation identifies a pruned tailer event by ID even if the
  // retention receipt itself has no source. Only receipts with neither that
  // durable provenance nor a candidate ID remain globally ambiguous.
  const receiptCount = table(db, "raw_retention_receipts")
    ? (db.prepare(table(db, "capture_root_observations")
        ? `select count(*) as n from raw_retention_receipts r where not exists
            (select 1 from capture_root_observations o where o.event_id=r.event_id
              and o.state in ('admitted','duplicate'))
            or exists (select 1 from capture_root_observations o where o.event_id=r.event_id
              and o.state='conflict')`
        : `select count(*) as n from raw_retention_receipts`).get() as { n: number }).n : 0;
  const prior = db.prepare(`select source,session_id as sessionId,event_type as eventType,
    observed_at as observedAt,model,input_tokens as inputTokens,output_tokens as outputTokens,
    cache_read_tokens as cacheReadTokens,cache_creation_tokens as cacheCreationTokens
    from buffered_events where id=? limit 1`);
  const retained = table(db, "raw_retention_receipts")
    ? db.prepare(`select 1 from raw_retention_receipts where event_id=? limit 1`) : null;
  const observed = table(db, "capture_root_observations")
    ? db.prepare(`select payload_digest as digest,state
        from capture_root_observations where event_id=?`) : null;
  // Imported source IDs are UUID delivery IDs. The delivery_id primary key is
  // indexed; an OR against raw_id would full-scan a large outbox per event.
  const outbox = table(db, "upload_outbox")
    ? db.prepare(`select 1 from upload_outbox where delivery_id=? limit 1`) : null;
  const uploaded = table(db, "upload_receipts")
    ? db.prepare(`select 1 from upload_receipts where delivery_id=? limit 1`) : null;
  const authority = db.prepare(`select authority from session_usage_authority where source=? and session_id=?`);
  const liveRaw = db.prepare(`select 1 from buffered_events where source=? and session_id=?
    and event_type not in ('usage_rollout','usage_transcript')
    and (input_tokens is not null or output_tokens is not null or cache_read_tokens is not null
      or cache_creation_tokens is not null or cost_usd is not null) limit 1`);
  const sessions = new Set<string>();
  const skippedLive = new Set<string>();
  const seen = new Map<string, string>();
  const matchedReceipts = new Set<string>();
  let candidateIndex = 0;
  let resumeVerified = !resume?.index;
  const plan: CaptureHistoryPlan = { status: "capture_roots_history_plan", rootId: root.rootId,
    source: root.source, dryRun: true, refusals: [], files: files.length, sessions: 0, skippedLiveSessions: 0,
    existingRows: 0, missingRows: 0, firstObservedAt: null, lastObservedAt: null,
    tokens: { input: 0, cacheRead: 0, cacheCreation: 0, output: 0 },
    fencedBytes: files.reduce((sum, file) => sum + file.limit, 0), since: options.since ?? null };
  const verifiedSessions = new Map<string, SessionBytes>();
  for (const file of files) {
    // Parse into memory; neither planning counts nor publication start until
    // all bytes have been compared with the imported session's fingerprint.
    let events: Candidate[];
    try {
      events = [...(root.source === "codex" ? codexEvents(root, file) : claudeEvents(root, file))];
      const newBytes = verifyImportedBytes(db, root, file, verifiedSessions);
      const lastUsageOffset = file.records?.at(-1)?.byteOffset ?? 0;
      const brokenRecord = file.recordRefusals?.find(row =>
        row.usage || !file.shorterImportedCopy || row.byteOffset <= lastUsageOffset);
      if (brokenRecord) refusal(brokenRecord.reason);
      if (newBytes && file.sessionId) verifiedSessions.set(file.sessionId, newBytes);
    } catch (error) {
      const reason = String(error).match(/capture_history_refused:(.+)$/)?.[1];
      if (!reason || expectedFiles) throw error;
      plan.refusals.push({ fileKey: file.fileKey, reason, missingRows: 0 });
      continue;
    }
    const verifiedCandidates: Array<{ candidate: Candidate; index: number; digest: string }> = [];
    for (const candidate of events) {
      candidateIndex += 1;
      if (resume && candidateIndex === resume.index) {
        if (candidateDigest(candidate, candidateIndex) !== resume.digest)
          refusal("resume_cursor_digest_changed");
        resumeVerified = true;
      }
      const e = candidate.event;
      const session = e.sessionId!;
      if (file.shorterImportedCopy) continue;
      if (options.since && e.observedAt < options.since) continue;
      if (e.observedAt >= file.fencedAt) continue;
      sessions.add(session);
      if (!plan.firstObservedAt || e.observedAt < plan.firstObservedAt) plan.firstObservedAt = e.observedAt;
      if (!plan.lastObservedAt || e.observedAt > plan.lastObservedAt) plan.lastObservedAt = e.observedAt;
      const live = (authority.get(root.source, session) as { authority: string } | undefined)?.authority === "live" ||
        Boolean(liveRaw.get(root.source, session));
      if (live) { skippedLive.add(session); continue; }
      const digest = crypto.createHash("sha256").update(JSON.stringify([
        e.observedAt, e.model, e.inputTokens, e.outputTokens, e.cacheReadTokens, e.cacheCreationTokens,
      ])).digest("hex");
      const previous = seen.get(candidate.sourceId);
      if (previous && previous !== digest) refusal("source_event_conflict");
      if (previous) continue;
      seen.set(candidate.sourceId, digest);
      const sightings = (observed?.all(candidate.sourceId) ?? []) as Array<{ digest: string; state: string }>;
      if (sightings.some(row => row.state === "conflict" ||
          row.digest !== captureRootObservationPayloadDigest(e)))
        refusal("prior_root_observation_conflict");
      if (retained?.get(candidate.sourceId) && sightings.length === 0)
        matchedReceipts.add(candidate.sourceId);
      const existing = prior.get(candidate.sourceId) as {
        source: string; sessionId: string | null; eventType: string; observedAt: string;
        model: string | null; inputTokens: number | null; outputTokens: number | null;
        cacheReadTokens: number | null; cacheCreationTokens: number | null;
      } | undefined;
      if (existing && (existing.source !== e.source || existing.sessionId !== session ||
          existing.eventType !== e.eventType || existing.observedAt !== e.observedAt ||
          existing.model !== (e.model ?? null) || (existing.inputTokens ?? 0) !== (e.inputTokens ?? 0) ||
          (existing.outputTokens ?? 0) !== (e.outputTokens ?? 0) ||
          (existing.cacheReadTokens ?? 0) !== (e.cacheReadTokens ?? 0) ||
          (existing.cacheCreationTokens ?? 0) !== (e.cacheCreationTokens ?? 0)))
        refusal("existing_event_conflict");
      if (existing || retained?.get(candidate.sourceId) || sightings.length > 0 ||
          outbox?.get(candidate.sourceId) || uploaded?.get(candidate.sourceId)) {
        plan.existingRows += 1;
        continue;
      }
      // Byte-proven imported records stay old even when a prior time filter
      // left no counted row or delivery ID for them. Only bytes after that
      // imported length can supply new rows from this copy.
      if (file.importedByteLength !== undefined &&
          candidate.prefixCheckpoint!.byteOffset <= file.importedByteLength) continue;
      plan.missingRows += 1;
      plan.tokens.input += e.inputTokens ?? 0;
      plan.tokens.output += e.outputTokens ?? 0;
      plan.tokens.cacheRead += e.cacheReadTokens ?? 0;
      plan.tokens.cacheCreation += e.cacheCreationTokens ?? 0;
      if (onMissing) {
        if (resume && candidateIndex <= resume.index) refusal("resume_cursor_evidence_lost");
        verifiedCandidates.push({ candidate, index: candidateIndex,
          digest: candidateDigest(candidate, candidateIndex) });
      }
      if (plan.missingRows % 4096 === 0) await new Promise<void>(resolve => setImmediate(resolve));
    }
    // The generator has reached EOF and compared all fenced bytes with the
    // preflight digest. Only this file's verified candidates may be published.
    verifyParents(file);
    if (onFileReady) await onFileReady(file);
    for (const item of verifiedCandidates) {
      await onMissing!(item.candidate, item.index, item.digest, file);
    }
    if (onFilePublished) {
      verifyParents(file);
      await onFilePublished(file);
    }
  }
  plan.sessions = sessions.size;
  if (!resumeVerified) refusal("resume_cursor_missing");
  plan.skippedLiveSessions = skippedLive.size;
  // A legacy pruned row's receipt has no session or source. It could be an
  // earlier OTLP/hook capture of any still-missing session, so refuse.
  if (plan.missingRows > 0 && receiptCount !== matchedReceipts.size) refusal("unattributed_pruned_rows");
  if (plan.missingRows > 0 && plan.firstObservedAt && plan.lastObservedAt) {
    const unkeyed = db.prepare(`select 1 from buffered_events where source=? and session_id is null
      and observed_at between ? and ? and event_type not in ('usage_rollout','usage_transcript')
      and (input_tokens is not null or output_tokens is not null or cache_read_tokens is not null
        or cache_creation_tokens is not null or cost_usd is not null) limit 1`)
      .get(root.source, plan.firstObservedAt, plan.lastObservedAt);
    if (unkeyed) refusal("unkeyed_live_usage_overlap");
  }
  return { plan, files };
}

export async function planCaptureHistory(db: DB, root: CaptureRoot, options: Pick<Options, "since"> = {}) {
  maintenanceIdle(db);
  if (active.size) refusal("another_import_in_process");
  if (table(db, "capture_history_import_lock")) {
    const held = db.prepare(`select owner_pid as pid,owner_start as started
      from capture_history_import_lock where singleton=1`).get() as
        { pid: number; started: string } | undefined;
    if (held) {
      const holder = processStart(held.pid);
      if (holder === "unknown") refusal("import_holder_identity_unknown");
      if (holder === held.started) refusal("another_import_holds_ledger");
    }
  }
  return (await scan(db, root, options)).plan;
}

function processStart(pid: number): string | "unknown" | null {
  if (!Number.isSafeInteger(pid) || pid <= 0) return "unknown";
  try { process.kill(pid, 0); }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ESRCH") return null;
    return "unknown";
  }
  try {
    const value = execFileSync("/bin/ps", ["-p", String(pid), "-o", "lstart="],
      { encoding: "utf8", timeout: 2_000, stdio: ["ignore", "pipe", "ignore"],
        env: { ...process.env, TZ: "UTC", LC_ALL: "C" } }).trim();
    return value || "unknown";
  } catch { return "unknown"; }
}
function maintenanceIdle(db: DB) {
  const row = db.prepare(`select value from maintenance_state where key='session_summary_legacy_rebuild_v1'`)
    .get() as { value: string } | undefined;
  if (!row) return;
  try { if ((JSON.parse(row.value) as { phase?: string }).phase === "done") return; }
  catch { /* Fail closed. */ }
  refusal("maintenance_rebuild_active_or_unknown");
}
function ensureImportLockSchema(db: DB) {
  db.exec(`create table if not exists capture_history_import_lock (
    singleton integer primary key check(singleton=1), root_id text not null,
    owner_pid integer not null, owner_start text not null, owner_attempt_id text);`);
  const lockColumns = db.pragma("table_info(capture_history_import_lock)") as Array<{ name: string }>;
  if (!lockColumns.some(column => column.name === "owner_attempt_id"))
    db.exec("alter table capture_history_import_lock add column owner_attempt_id text");
}
function ensureImportSchema(db: DB) {
  db.exec(`create table if not exists capture_history_import_runs (
    root_id text primary key, root_digest text not null, source_digest text not null,
    since_window text, source text not null,
    run_id text not null, imported_rows integer not null default 0,
    input_tokens integer not null default 0, cache_read_tokens integer not null default 0,
    cache_creation_tokens integer not null default 0, output_tokens integer not null default 0,
    started_at text not null, updated_at text not null, completed_at text,
    resume_candidate_index integer not null default 0, resume_candidate_digest text);
    create table if not exists capture_history_file_state (
      file_key text primary key,root_id text not null,source text not null,
      prefix_hash text not null,baseline_json text not null,
      handoff_ready integer not null default 0 check(handoff_ready in (0,1)),
      published_rows integer not null default 0 check(published_rows>=0));
    create table if not exists capture_history_session_counters (
      source text not null,session_id text not null,last_index integer not null,
      state_json text not null,primary key(source,session_id));
    create table if not exists capture_history_session_bytes (
      source text not null,session_id text not null,imported_length integer not null,
      prefix_digest text not null,primary key(source,session_id));
    create table if not exists capture_history_record_bytes (
      source text not null,session_id text not null,record_index integer not null,
      byte_offset integer not null,prefix_digest text not null,
      primary key(source,session_id,record_index,byte_offset));
    create table if not exists capture_history_import_prefixes (
      source text not null,session_id text not null,prefix_digest text not null,
      input_tokens integer not null,cache_read_tokens integer not null,
      cache_creation_tokens integer not null,output_tokens integer not null,
      message_key text,parser_state_json text,
      primary key(source,session_id,prefix_digest));
    create table if not exists transcript_usage_revision_state (
      source text not null check(source='claude_code'),session_id text not null,
      message_key text not null,input_tokens integer not null,cache_read_tokens integer not null,
      cache_creation_tokens integer not null,output_tokens integer not null,
      repo_context_id text,context_conflict integer not null default 0,
      updated_at text not null,primary key(source,session_id,message_key));
    create trigger if not exists capture_history_import_block_prune
      before insert on raw_retention_receipts
      when exists (select 1 from capture_history_import_lock where singleton=1)
      begin select raise(abort, 'capture_history_import_active'); end;`);
  const columns = db.pragma("table_info(capture_history_import_runs)") as Array<{ name: string }>;
  if (!columns.some(column => column.name === "source_digest"))
    db.exec("alter table capture_history_import_runs add column source_digest text");
  if (!columns.some(column => column.name === "since_window"))
    db.exec("alter table capture_history_import_runs add column since_window text");
  if (!columns.some(column => column.name === "resume_candidate_index"))
    db.exec("alter table capture_history_import_runs add column resume_candidate_index integer not null default 0");
  if (!columns.some(column => column.name === "resume_candidate_digest"))
    db.exec("alter table capture_history_import_runs add column resume_candidate_digest text");
  const fileColumns = db.pragma("table_info(capture_history_file_state)") as Array<{ name: string }>;
  if (!fileColumns.some(column => column.name === "handoff_ready"))
    db.exec("alter table capture_history_file_state add column handoff_ready integer not null default 0");
  if (!fileColumns.some(column => column.name === "published_rows")) {
    db.exec("alter table capture_history_file_state add column published_rows integer not null default 0");
    // A pre-upgrade interrupted file may have published a partial slice.
    // Its per-file count is unknown, so keep its original digest binding.
    db.exec(`update capture_history_file_state set published_rows=1
      where handoff_ready=0 and exists (select 1 from capture_history_import_runs r
        where r.root_id=capture_history_file_state.root_id and r.imported_rows>0)`);
  }
  const revisionColumns = db.pragma("table_info(transcript_usage_revision_state)") as Array<{ name: string }>;
  if (!revisionColumns.some(column => column.name === "repo_context_id"))
    db.exec("alter table transcript_usage_revision_state add column repo_context_id text");
  if (!revisionColumns.some(column => column.name === "context_conflict"))
    db.exec("alter table transcript_usage_revision_state add column context_conflict integer not null default 0");
  if (!table(db, "rollout_scan_state") || !(db.pragma("table_info(rollout_scan_state)") as Array<{ name: string }>)
    .some(column => column.name === "parser_state_json")) ensureJsonlScanState(db);
}

function rememberFileBaseline(db: DB, root: CaptureRoot, file: File) {
  if (!file.prefixHash) refusal("prefix_hash_missing");
  const baseline = JSON.stringify({
    ...(file.initialCodex ? { codex: JSON.stringify(file.initialCodex) } : {}),
    claude: [...(file.initialClaude ?? new Map()).entries()],
  });
  // Refresh only this file's unpublished baseline after its entire prefix
  // verifies. A file with committed rows keeps its original digest binding.
  db.prepare(`delete from capture_history_file_state where file_key=?
    and handoff_ready=0 and published_rows=0`).run(file.fileKey);
  db.prepare(`insert or ignore into capture_history_file_state
    (file_key,root_id,source,prefix_hash,baseline_json,handoff_ready) values (?,?,?,?,?,0)`)
    .run(file.fileKey, root.rootId, root.source, file.prefixHash, baseline);
  const saved = db.prepare(`select prefix_hash as hash from capture_history_file_state where file_key=?`)
    .get(file.fileKey) as { hash: string };
  if (saved.hash !== file.prefixHash) refusal("fenced_history_changed_since_import");
}

function rememberClaudeRevision(db: DB, revision: ClaudeRevision) {
  const { sessionId, messageKey, current } = revision;
  db.prepare(`insert into transcript_usage_revision_state
    (source,session_id,message_key,input_tokens,cache_read_tokens,cache_creation_tokens,
      output_tokens,repo_context_id,context_conflict,updated_at)
    values ('claude_code',?,?,?,?,?,?,null,0,?)
    on conflict(source,session_id,message_key) do update set
      input_tokens=excluded.input_tokens,cache_read_tokens=excluded.cache_read_tokens,
      cache_creation_tokens=excluded.cache_creation_tokens,output_tokens=excluded.output_tokens,
      updated_at=excluded.updated_at
    where excluded.input_tokens>=transcript_usage_revision_state.input_tokens
      and excluded.cache_read_tokens>=transcript_usage_revision_state.cache_read_tokens
      and excluded.cache_creation_tokens>=transcript_usage_revision_state.cache_creation_tokens
      and excluded.output_tokens>=transcript_usage_revision_state.output_tokens`)
    .run(sessionId, messageKey, current.input, current.cacheRead,
      current.cacheCreation, current.output, new Date().toISOString());
}

function rememberFileCursor(db: DB, root: CaptureRoot, file: File) {
  if (!file.cursor) refusal("file_cursor_missing");
  const key = rootCursorKey([root], file.file);
  const existing = db.prepare(`select committed_offset as offset from rollout_scan_state where file=?`)
    .get(jsonlScanStateKey(key)) as { offset: number | null } | undefined;
  if (existing?.offset !== null && existing?.offset !== undefined && existing.offset > file.limit) return;
  const state = root.source === "codex" ? (() => {
    const final = file.finalCodex;
    if (!final) refusal("codex_parser_state_missing");
    return { parserKind: "codex-rollout-v2", checkpointVersion: 2,
      conversationId: final.sessionId, previous: { input: final.previous.input,
        cachedInput: final.previous.cacheRead, output: final.previous.output,
        reasoningOutput: final.reasoningOutput }, tokenCountIndex: final.index,
      contextOccurrenceIndex: final.contextOccurrenceIndex,
      ...(final.model ? { model: final.model } : {}),
      ...(final.sessionStartedAt ? { sessionStartedAt: final.sessionStartedAt } : {}),
      ...(final.originator ? { originator: final.originator } : {}),
      ...(final.cliVersion ? { cliVersion: final.cliVersion } : {}),
      ...(final.planType ? { planType: final.planType } : {}) };
  })() : (() => {
    const revisions = [...(file.finalClaude?.values() ?? [])];
    const session = revisions[0]?.sessionId ??
      path.basename(file.file, ".jsonl").match(UUID_AT_END)?.[0]?.toLowerCase();
    return { parserKind: "claude-transcript-v3", checkpointVersion: 3,
      ...(session ? { sessionId: session } : {}),
      usageRevisions: revisions.slice(-64).map(revision => ({
        messageId: revision.messageId, input: revision.current.input,
        cacheRead: revision.current.cacheRead, cacheCreation: revision.current.cacheCreation,
        output: revision.current.output,
      })) };
  })();
  const c = file.cursor;
  // The fenced file was read and compared before publication. Its fingerprint
  // covers exactly this committed offset, including every non-usage line.
  const read = { ...c, committedPrefixHash: file.prefixHash,
    deferredBytes: c.observedSize - c.committedOffset,
    workRemaining: c.observedSize > c.committedOffset, unresolvedRecord: null } as JsonlTailRead;
  rememberJsonlScanCursor(db, key, state.parserKind, state.checkpointVersion, read, state);
}
const active = new Set<string>();
export async function applyCaptureHistory(buffer: LocalEventBuffer, root: CaptureRoot,
  options: Options = {}): Promise<CaptureHistoryApplyReceipt> {
  if (active.size) refusal("import_in_progress");
  active.add(root.rootId);
  const attemptId = options.attemptId ?? crypto.randomUUID();
  const db = buffer.database;
  let lockOwned = false;
  try {
  maintenanceIdle(db);
  ensureImportLockSchema(db);
  const ownerStart = processStart(process.pid);
  if (!ownerStart || ownerStart === "unknown") refusal("process_identity_unavailable");
  const observedLock = db.prepare(`select root_id as rootId,owner_pid as pid,
    owner_start as started,owner_attempt_id as attemptId
    from capture_history_import_lock where singleton=1`).get() as
      { rootId: string; pid: number; started: string; attemptId: string | null } | undefined;
  if (observedLock) {
    const holder = processStart(observedLock.pid);
    if (holder === "unknown") refusal("import_holder_identity_unknown");
    if (holder === observedLock.started) refusal("import_in_progress");
  }
  // Reserve the singleton before any preflight await. The active check and
  // insertion share one IMMEDIATE transaction, including for another process.
  db.transaction(() => {
    maintenanceIdle(db);
    const held = db.prepare(`select root_id as rootId,owner_pid as pid,
      owner_start as started,owner_attempt_id as attemptId
      from capture_history_import_lock where singleton=1`).get() as
        { rootId: string; pid: number; started: string; attemptId: string | null } | undefined;
    if (held) {
      // A live or newly changed holder always wins. Only the exact stale row
      // whose process identity was checked outside the write lock is removed.
      if (!observedLock || held.rootId !== observedLock.rootId ||
          held.pid !== observedLock.pid || held.started !== observedLock.started ||
          held.attemptId !== observedLock.attemptId) refusal("import_in_progress");
      db.prepare(`delete from capture_history_import_lock where singleton=1`).run();
    }
    db.prepare(`insert into capture_history_import_lock
      (singleton,root_id,owner_pid,owner_start,owner_attempt_id) values (1,?,?,?,?)`)
      .run(root.rootId, process.pid, ownerStart, attemptId);
  }).immediate();
  lockOwned = true;
  ensureImportSchema(db);
  // appendRootObservation normally initializes this lazily. Inside a writer
  // transaction that cache deliberately stays cold after rollback, so every
  // imported row would otherwise repeat CREATE TABLE/INDEX work.
  prepareCaptureRootObservationSchema(db);
  const first = await scan(db, root, options);
  if (first.plan.refusals.length) refusal(first.plan.refusals[0]!.reason);
  const fencedSourceDigest = sourceDigest(first.files, options.since);
  let runId = "";
  let resume: ResumePoint = { index: 0, digest: null };
  db.transaction(() => {
    maintenanceIdle(db);
    const held = db.prepare(`select root_id as rootId, owner_pid as pid, owner_start as started,
      owner_attempt_id as attemptId
      from capture_history_import_lock where singleton=1`).get() as
        { rootId: string; pid: number; started: string; attemptId: string | null } | undefined;
    if (!held || held.rootId !== root.rootId || held.pid !== process.pid ||
        held.started !== ownerStart || held.attemptId !== attemptId) refusal("import_lock_lost");
    const prior = db.prepare(`select root_digest as digest,source_digest as sourceDigest,
      since_window as sinceWindow,run_id as runId,
      resume_candidate_index as resumeIndex,resume_candidate_digest as resumeDigest,
      imported_rows as importedRows,completed_at as completedAt
      from capture_history_import_runs where root_id=?`)
      .get(root.rootId) as { digest: string; sourceDigest: string | null;
        sinceWindow: string | null; runId: string;
        resumeIndex: number; resumeDigest: string | null; importedRows: number;
        completedAt: string | null } | undefined;
    const digest = captureRootDigest(root);
    if (prior && prior.digest !== digest) refusal("root_identity_changed_since_import");
    if (prior && prior.sourceDigest !== fencedSourceDigest) {
      // The source digest also binds the since window. Only file bytes may
      // be re-preflighted; a changed or legacy-unknown window still refuses.
      if (prior.sinceWindow !== JSON.stringify(options.since ?? null))
        refusal("fenced_history_changed_since_import");
      // Preserve the digest of every file that published a row or parser
      // handoff. A different, still-unpublished file may be re-preflighted
      // even when earlier files in this root have committed.
      const boundFiles = db.prepare(`select file_key as fileKey,prefix_hash as prefixHash
        from capture_history_file_state where root_id=?
          and (handoff_ready=1 or published_rows>0)`).all(root.rootId) as
        Array<{ fileKey: string; prefixHash: string }>;
      const currentHashes = new Map(first.files.map(file => [file.fileKey, file.prefixHash]));
      if (prior.completedAt !== null || (prior.importedRows > 0 && boundFiles.length === 0) ||
          boundFiles.some(file => currentHashes.get(file.fileKey) !== file.prefixHash))
        refusal("fenced_history_changed_since_import");
      db.prepare(`update capture_history_import_runs set source_digest=?,
        resume_candidate_index=0,resume_candidate_digest=null where root_id=?`)
        .run(fencedSourceDigest, root.rootId);
      // Re-scan bound files from their stored parser baselines. Existing IDs
      // dedupe; resetting the candidate cursor also discards progress made
      // through an uncommitted file whose bytes may now differ.
      prior.resumeIndex = 0;
      prior.resumeDigest = null;
    }
    if (prior && (!Number.isSafeInteger(prior.resumeIndex) || prior.resumeIndex < 0 ||
        (prior.resumeIndex === 0) !== (prior.resumeDigest === null)))
      refusal("resume_cursor_invalid");
    runId = prior?.runId ?? crypto.randomUUID();
    resume = { index: prior?.resumeIndex ?? 0, digest: prior?.resumeDigest ?? null };
    if (!prior) db.prepare(`insert into capture_history_import_runs
      (root_id,root_digest,source_digest,since_window,source,run_id,started_at,updated_at)
      values (?,?,?,?,?,?,?,?)`)
      .run(root.rootId, digest, fencedSourceDigest, JSON.stringify(options.since ?? null), root.source, runId,
        new Date().toISOString(), new Date().toISOString());
  }).immediate();
  const priorAutoCheckpoint = db.pragma("wal_autocheckpoint", { simple: true }) as number;
  // SQLite's default auto-checkpoint runs synchronously at COMMIT and can
  // turn a 16-row writer into an unbounded checkpoint. Keep checkpoints
  // outside the writer slice; the daemon's worker may also checkpoint.
  db.pragma("wal_autocheckpoint = 0");
  let importedRows = 0;
  let maxWriterSliceMs = 0;
  let overBudgetSlices = 0;
  let maxWriterWorkMs = 0;
  let maxWriterRowMs = 0;
  let slices = 0;
  let nextRows = WRITER_INITIAL_ROWS;
  let fastSliceStreak = 0;
  let timeBudgetStops = 0;
  let maxWalBytes = 0;
  let walPauseMs = 0;
  const walLimit = options.walLimitBytes ?? WAL_LIMIT_BYTES;
  const walStall = options.walStallMs ?? WAL_STALL_MS;
  if (!Number.isSafeInteger(walLimit) || walLimit < 1 ||
      !Number.isSafeInteger(walStall) || walStall < 1) refusal("wal_budget_invalid");
  const walPath = `${db.name}-wal`;
  const walSize = () => {
    try { return fs.statSync(walPath).size; }
    catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") return 0;
      throw error;
    }
  };
  const pauseForWal = async () => {
    let size = walSize();
    maxWalBytes = Math.max(maxWalBytes, size);
    if (size <= walLimit) return;
    const pausedAt = performance.now();
    let lastProgress = pausedAt;
    let bestBacklog = Number.POSITIVE_INFINITY;
    while (size > walLimit) {
      const status = db.pragma("wal_checkpoint(PASSIVE)") as Array<{
        busy: number; log: number; checkpointed: number }>;
      const frames = status[0];
      const backlog = frames ? Math.max(0, frames.log - frames.checkpointed) : Number.POSITIVE_INFINITY;
      if (backlog < bestBacklog) { bestBacklog = backlog; lastProgress = performance.now(); }
      // PASSIVE moves committed frames out of the WAL. Once possible, a
      // no-wait truncate releases its physical space without holding a writer
      // while waiting for a pinned reader or competing intake connection.
      const previousBusyTimeout = db.pragma("busy_timeout", { simple: true }) as number;
      try {
        db.pragma("busy_timeout = 0");
        try { db.pragma("wal_checkpoint(TRUNCATE)"); }
        catch (error) {
          if (!(error as Error).message.includes("SQLITE_BUSY")) throw error;
        }
      } finally { db.pragma(`busy_timeout = ${previousBusyTimeout}`); }
      const next = walSize();
      if (next < size) lastProgress = performance.now();
      size = next;
      maxWalBytes = Math.max(maxWalBytes, size);
      if (size <= walLimit) break;
      if (performance.now() - lastProgress >= walStall) refusal("wal_checkpoint_stalled");
      await new Promise<void>(resolve => setTimeout(resolve, 250));
    }
    walPauseMs += performance.now() - pausedAt;
  };
  const writerSliceHistogram: Record<string, number> = {
    "under25ms": 0, "25to50ms": 0, "50to100ms": 0, "100to250ms": 0, "250to750ms": 0, "750msOrMore": 0,
  };
  const importedTokens: Amounts = { input: 0, cacheRead: 0, cacheCreation: 0, output: 0 };
  let pending: Array<{ candidate: Candidate; index: number; digest: string; fileKey: string }> = [];
  const flush = async () => {
    if (!pending.length) return;
    const batch = pending.slice(0, nextRows);
    const fileKey = batch[0]!.fileKey;
    if (batch.some(item => item.fileKey !== fileKey)) refusal("mixed_file_slice");
    const sourceFile = first.files.find(file => file.fileKey === fileKey);
    if (!sourceFile) refusal("source_file_missing");
    verifyParents(sourceFile);
    const throughRecord = batch.at(-1)!.candidate.prefixCheckpoint?.recordIndex;
    if (throughRecord !== undefined) {
      let recordSlices = 0;
      await prepareRecordBytes(db, root, sourceFile, throughRecord, async () => {
        if (++recordSlices % 8 === 0) db.pragma("wal_checkpoint(PASSIVE)");
        await pauseForWal();
      });
    }
    pending = pending.slice(batch.length);
    maintenanceIdle(db);
    let writerStarted = 0;
    let writerWorkEnded = 0;
    let writerRowMs = 0;
    let stoppedForTime = false;
    let processed = 0;
    const waitingStarted = performance.now();
    const receipt = buffer.withHistoryImportAdmission(root.installationEpochId, () =>
      buffer.transactionWithRepoContextHandoffs(() => {
        // The IMMEDIATE transaction acquired the writer before this callback.
        // Contention waiting for another writer is not our held writer slice.
        writerStarted = performance.now();
        verifyParents(sourceFile);
        const lock = db.prepare(`select root_id as rootId, owner_pid as pid, owner_start as started,
          owner_attempt_id as attemptId
          from capture_history_import_lock where singleton=1`).get() as
            { rootId: string; pid: number; started: string; attemptId: string | null } | undefined;
        if (!lock || lock.rootId !== root.rootId || lock.pid !== process.pid ||
            lock.started !== ownerStart || lock.attemptId !== attemptId)
          refusal("import_lock_lost");
        maintenanceIdle(db);
        const counts: Amounts = { input: 0, cacheRead: 0, cacheCreation: 0, output: 0 };
        let rows = 0;
        for (const item of batch) {
          const rowStarted = performance.now();
          const e = item.candidate.event;
          // A concurrent live writer may have won the session while files
          // were read; session authority is checked again under the writer.
          const authority = db.prepare(`select authority from session_usage_authority where source=? and session_id=?`)
            .get(root.source, e.sessionId) as { authority: string } | undefined;
          if (authority?.authority !== "live" && appendRootObservation(buffer, e, root, true)) {
            rows += 1;
            counts.input += e.inputTokens ?? 0;
            counts.output += e.outputTokens ?? 0;
            counts.cacheRead += e.cacheReadTokens ?? 0;
            counts.cacheCreation += e.cacheCreationTokens ?? 0;
            if (item.candidate.claudeRevision) rememberClaudeRevision(db, item.candidate.claudeRevision);
            if (item.candidate.prefixCheckpoint)
              rememberPrefixCheckpoint(db, root.source, e.sessionId!, item.candidate.prefixCheckpoint);
            const recordIndex = item.candidate.prefixCheckpoint?.recordIndex;
            if (recordIndex !== undefined) {
              for (let index = (sourceFile.writtenRecordIndex ?? -1) + 1; index <= recordIndex; index++)
                rememberRecordBytes(db, root.source, e.sessionId!, sourceFile.records![index]!);
              sourceFile.writtenRecordIndex = recordIndex;
              const record = sourceFile.records![recordIndex]!;
              rememberSessionBytes(db, root.source, e.sessionId!, record.byteOffset, record.digest);
            }
          }
          processed += 1;
          writerRowMs = Math.max(writerRowMs, performance.now() - rowStarted);
          if (performance.now() - writerStarted >= WRITER_TARGET_MS && processed < batch.length) {
            stoppedForTime = true;
            break;
          }
        }
        db.prepare(`update capture_history_import_runs set imported_rows=imported_rows+?,
          input_tokens=input_tokens+?,cache_read_tokens=cache_read_tokens+?,
          cache_creation_tokens=cache_creation_tokens+?,output_tokens=output_tokens+?,updated_at=?,
          resume_candidate_index=?,resume_candidate_digest=?
          where root_id=?`).run(rows, counts.input, counts.cacheRead, counts.cacheCreation,
            counts.output, new Date().toISOString(), batch[processed - 1]!.index,
            batch[processed - 1]!.digest, root.rootId);
        if (rows) db.prepare(`update capture_history_file_state
          set published_rows=published_rows+? where file_key=?`).run(rows, fileKey);
        verifyParents(sourceFile);
        writerWorkEnded = performance.now();
        return { rows, counts };
      }));
    if (processed < batch.length) pending = batch.slice(processed).concat(pending);
    resume = { index: batch[processed - 1]!.index, digest: batch[processed - 1]!.digest };
    const elapsed = performance.now() - writerStarted;
    maxWriterSliceMs = Math.max(maxWriterSliceMs, elapsed);
    maxWriterWorkMs = Math.max(maxWriterWorkMs, writerWorkEnded - writerStarted);
    maxWriterRowMs = Math.max(maxWriterRowMs, writerRowMs);
    if (elapsed >= WRITER_HARD_MS) overBudgetSlices += 1;
    const histogramBucket = elapsed < 25 ? "under25ms" : elapsed < 50 ? "25to50ms" :
      elapsed < 100 ? "50to100ms" : elapsed < 250 ? "100to250ms" :
      elapsed < 750 ? "250to750ms" : "750msOrMore";
    writerSliceHistogram[histogramBucket]! += 1;
    if (stoppedForTime) timeBudgetStops += 1;
    // Use the committed cost, including commit, to size the next row cap.
    // A slow row or commit pushes the next slice toward one row; sustained
    // headroom is required to grow again.
    const targetRows = Math.max(1, Math.floor(processed * WRITER_TARGET_MS / Math.max(elapsed, 1)));
    if (elapsed > 200) {
      nextRows = Math.max(1, Math.min(Math.floor(nextRows / 2), targetRows));
      fastSliceStreak = 0;
    } else if (elapsed < 80 && !stoppedForTime) {
      // Grow only after sustained headroom; a single fast commit is not a
      // reliable estimate of the next commit on a shared host.
      fastSliceStreak += 1;
      if (fastSliceStreak >= 3) {
        nextRows = Math.min(WRITER_MAX_ROWS,
          nextRows + Math.max(1, Math.ceil(nextRows / 12)));
        fastSliceStreak = 0;
      }
    } else {
      fastSliceStreak = 0;
      if (elapsed > WRITER_TARGET_MS)
        nextRows = Math.max(1, Math.min(nextRows - 1, targetRows));
    }
    importedRows += receipt.rows;
    importedTokens.input += receipt.counts.input;
    importedTokens.output += receipt.counts.output;
    importedTokens.cacheRead += receipt.counts.cacheRead;
    importedTokens.cacheCreation += receipt.counts.cacheCreation;
    slices += 1;
    if (options.stopAfterSlices === slices) throw new Error("capture_history_injected_crash");
    // Checkpoint outside the writer transaction before this root's WAL grows
    // large. A root may finish before 512 slices, so that old cadence never
    // ran during the scale import.
    if (slices % 8 === 0) db.pragma("wal_checkpoint(PASSIVE)");
    await pauseForWal();
    // SQLite's busy handler can miss a narrow unlock window and repeatedly
    // lose to the next import slice. Leave a full writer handoff interval for
    // hook, OTLP and tailer writers; observed import contention gets longer.
    const writerWaitMs = writerStarted - waitingStarted;
    await new Promise<void>(resolve => setTimeout(resolve, writerWaitMs > 5 ? 500 : 250));
  };
  try {
    await pauseForWal();
    // Verify every prefix before the first imported row. Suffix growth is
    // allowed, but a rewrite of any fenced byte aborts the entire preflight.
    for (const file of first.files) verifyFencedPrefix(file);
    await scan(db, root, options, async (candidate, index, digest, file) => {
      pending.push({ candidate, index, digest, fileKey: file.fileKey });
      while (pending.length >= nextRows) await flush();
    }, first.files, resume, async file => {
      // This small row preserves the exact initial parser baseline before a
      // crash can leave only some verified candidates committed.
      db.transaction(() => rememberFileBaseline(db, root, file)).immediate();
      // Main keeps a Claude folder sighting durable before counted rows can
      // fail or roll back. This callback runs after byte verification and
      // before any counted-row writer transaction for this file.
      if (root.source === "claude_code" && file.sessionId && !isLiveSession(db, root.source, file.sessionId))
        recordClaudeRootSessionSighting(buffer, root, file.sessionId, file.fencedAt);
    }, async file => {
      while (pending.length) await flush();
      await rememberRemainingRecordBytes(db, root, file);
      // The normal tailer defers this file until its verified cumulative
      // state and all of its historical rows have reached the ledger.
      if (root.source === "claude_code") {
        const revisions = [...(file.finalClaude?.values() ?? [])];
        for (let index = 0; index < revisions.length; index += 16) {
          const batch = revisions.slice(index, index + 16);
          db.transaction(() => {
            verifyParents(file);
            for (const revision of batch) {
              const authority = db.prepare(`select authority from session_usage_authority
                where source='claude_code' and session_id=?`).get(revision.sessionId) as
                  { authority: string } | undefined;
              if (authority?.authority !== "live") rememberClaudeRevision(db, revision);
            }
          }).immediate();
          if (index + 16 < revisions.length)
            await new Promise<void>(resolve => setTimeout(resolve, 250));
        }
      }
      db.transaction(() => {
        verifyParents(file);
        if (file.finalCodex && !isLiveSession(db, root.source, file.finalCodex.sessionId)) {
          const final = file.finalCodex;
          const prior = durableCodexState(db, final.sessionId);
          // A shorter verified copy may have a different token-count index.
          // The global baseline follows admitted cumulative usage, never a
          // larger index whose counter would move backward.
          if (!prior || (!belowAmounts(final.previous, prior.previous) &&
              (!sameAmounts(final.previous, prior.previous) || final.index > prior.index))) {
            db.prepare(`insert into capture_history_session_counters
              (source,session_id,last_index,state_json) values ('codex',?,?,?)
              on conflict(source,session_id) do update set
                last_index=excluded.last_index,state_json=excluded.state_json`)
              .run(final.sessionId, final.index, JSON.stringify(final));
          }
        }
        if (file.sessionId && !isLiveSession(db, root.source, file.sessionId))
          rememberSessionBytes(db, root.source, file.sessionId, file.limit, file.prefixHash!);
        rememberFileCursor(db, root, file);
        db.prepare(`update capture_history_file_state set handoff_ready=1 where file_key=?`)
          .run(file.fileKey);
      }).immediate();
    });
    while (pending.length) await flush();
    db.transaction(() => {
      const lock = db.prepare(`select root_id as rootId,owner_pid as pid,owner_start as started,
        owner_attempt_id as attemptId
        from capture_history_import_lock where singleton=1`).get() as
          { rootId: string; pid: number; started: string; attemptId: string | null } | undefined;
      if (!lock || lock.rootId !== root.rootId || lock.pid !== process.pid ||
          lock.started !== ownerStart || lock.attemptId !== attemptId)
        refusal("import_lock_lost");
      db.prepare(`update capture_history_import_runs set completed_at=?,updated_at=? where root_id=?`)
        .run(new Date().toISOString(), new Date().toISOString(), root.rootId);
      db.prepare(`delete from capture_history_import_lock where singleton=1 and owner_attempt_id=?`)
        .run(attemptId);
      lockOwned = false;
    }).immediate();
  } finally {
    db.pragma(`wal_autocheckpoint = ${priorAutoCheckpoint}`);
  }
  const total = db.prepare(`select imported_rows as rows from capture_history_import_runs where root_id=?`)
    .get(root.rootId) as { rows: number };
  const { status: _status, dryRun: _dryRun, ...plan } = first.plan;
  return { ...plan, status: "capture_roots_history_imported", importedRows,
    importedTokens, totalImportedRows: total.rows, maxWriterSliceMs,
    overBudgetSlices, maxWriterWorkMs, maxWriterRowMs, writerSliceHistogram,
    writerSlices: slices, timeBudgetStops, runId, attemptId, maxWalBytes, walPauseMs };
  } finally {
    try {
      if (lockOwned) db.prepare(`delete from capture_history_import_lock
        where singleton=1 and owner_attempt_id=?`).run(attemptId);
    } finally { active.delete(root.rootId); }
  }
}
