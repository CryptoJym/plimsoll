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
type File = { file: string; fileKey: string; limit: number; stamp: string; fencedAt: string;
  prefixHash?: string; savedPrefixHash?: string; baselineDefined?: boolean;
  initialCodex?: CodexState; finalCodex?: CodexState;
  initialClaude?: Map<string, Amounts>; finalClaude?: Map<string, ClaudeRevision>;
  cursor?: { observedSize: number; committedOffset: number; fileIdentity: string;
    headHash: string | null; headBytes: number; continuityHash: string | null;
    continuityBytes: number; mtimeMs: number; ctimeMs: number } };
type Candidate = { event: AiInteractionEvent; sourceId: string; claudeRevision?: ClaudeRevision };
export type CaptureHistoryPlan = {
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
function candidateFiles(db: DB, root: CaptureRoot): File[] {
  validateCaptureRoots([root]);
  if (inspectCaptureRoots([root])[0]?.state !== "ready") refusal("root_not_physical_and_ready");
  const source = captureBaselineStatus(db).sources.find(row => row.source === root.source);
  if (!source || source.status !== "complete" || source.unresolvedObservationErrors !== 0)
    refusal("baseline_incomplete_or_ambiguous");
  const found: Array<File | { file: string; stat: fs.BigIntStats }> = [];
  const dirs = [root.directory];
  let entries = 0;
  while (dirs.length) {
    const directory = dirs.pop()!;
    const handle = fs.opendirSync(directory, { bufferSize: 32 });
    try {
      let entry: fs.Dirent | null;
      while ((entry = handle.readSync())) {
        entries += 1;
        if (entries > 2_000_000) refusal("root_entry_limit");
        const file = path.join(directory, entry.name);
        if (entry.isSymbolicLink()) refusal("root_symlink_entry");
        if (entry.isDirectory()) { dirs.push(file); continue; }
        if (!entry.isFile()) refusal("root_nonregular_entry");
        if (!entry.name.endsWith(".jsonl") ||
            (root.source === "codex" && !entry.name.startsWith("rollout-"))) continue;
        const stat = fs.lstatSync(file, { bigint: true });
        if (!stat.isFile()) refusal("file_not_regular");
        const receipt = captureBaselineExcludedReceipt(db, root.source, {
          path: file, device: stat.dev, inode: stat.ino, size: stat.size, birthtimeNs: stat.birthtimeNs,
        });
        if (receipt) {
          if (BigInt(receipt.baselineSize) > stat.size) refusal("fenced_prefix_truncated");
          found.push({ file, fileKey: crypto.createHash("sha256").update(`${root.rootId}\0${file}`).digest("hex"),
            limit: receipt.baselineSize, stamp: stamp(stat), fencedAt: receipt.baselinedAt });
        } else found.push({ file, stat });
      }
    } finally { handle.closeSync(); }
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
        yield decoder.decode(line);
        start = index + 1;
      }
      pending = Buffer.from(bytes.subarray(start));
      if (pending.length > MAX_LINE_BYTES) refusal("record_exceeds_byte_budget");
    }
    if (pending.length) refusal("fenced_partial_record");
    const after = fs.fstatSync(fd, { bigint: true });
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
    baseline_json as baselineJson,handoff_ready as ready
    from capture_history_file_state where file_key=?`)
    .get(file.fileKey) as { rootId: string; source: string; prefixHash: string;
      baselineJson: string; ready: number } | undefined;
  if (!saved) return;
  if (saved.rootId !== root.rootId || saved.source !== root.source) refusal("import_file_state_conflict");
  if (saved.ready !== 1) {
    const run = db.prepare(`select imported_rows as rows,completed_at as completedAt
      from capture_history_import_runs where root_id=?`).get(root.rootId) as
        { rows: number; completedAt: string | null } | undefined;
    // An attempt that died before its first publication may re-preflight a
    // changed prefix; no parser handoff or counted row escaped that attempt.
    if (run?.rows === 0 && run.completedAt === null) return;
  }
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
function durableClaudeRevision(db: DB, sessionId: string, messageKey: string): Amounts | undefined {
  if (!table(db, "transcript_usage_revision_state")) return undefined;
  const row = db.prepare(`select input_tokens as input,cache_read_tokens as cacheRead,
    cache_creation_tokens as cacheCreation,output_tokens as output
    from transcript_usage_revision_state where source='claude_code' and session_id=? and message_key=?`)
    .get(sessionId, messageKey) as Amounts | undefined;
  return row;
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

function* codexEvents(root: CaptureRoot, file: File, db: DB,
  sessions: Map<string, CodexState>, seenHashes: Map<string, Set<string>>): Generator<Candidate> {
  const sessionId = path.basename(file.file, ".jsonl").match(UUID_AT_END)?.[0]?.toLowerCase();
  if (!sessionId) refusal("codex_file_session_missing");
  const state = structuredClone(file.initialCodex ?? sessions.get(sessionId) ??
    (file.baselineDefined ? undefined : durableCodexState(db, sessionId)) ?? initialCodexState(sessionId));
  file.initialCodex ??= structuredClone(state);
  let tokenObservations = 0;
  let duplicateRestart = false;
  for (const line of lines(file)) {
    if (!line.includes('"session_meta"') && !line.includes('"turn_context"') &&
        !line.includes('"token_count"')) continue;
    let parsed: Record<string, any>;
    try { parsed = JSON.parse(line) as Record<string, any>; }
    catch { refusal("codex_relevant_json_invalid"); }
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
      state.index += 1;
      const reported = parsed.payload?.info?.total_token_usage;
      if (!reported) continue;
      const current = totals(reported);
      if (tokenObservations === 0 &&
          (current.input < state.previous.input || current.cacheRead < state.previous.cacheRead ||
            current.cacheCreation < state.previous.cacheCreation || current.output < state.previous.output)) {
        // A second physical copy of the same session prefix starts its
        // counters again. Parse it from zero, but accept that reset only if
        // its whole fenced digest exactly matches a prior file's digest.
        duplicateRestart = true;
        state.previous = { ...ZERO_AMOUNTS };
        state.index = 0;
        state.observedBaseline = false;
        state.reasoningOutput = 0;
        file.initialCodex = initialCodexState(sessionId);
      }
      const delta = positiveDelta(current, state.previous);
      tokenObservations += 1;
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
          sourceCumulativeOutput: current.output } : {}) }, firstUnknown) };
    }
  }
  if (duplicateRestart && !seenHashes.get(sessionId)?.has(file.prefixHash ?? ""))
    refusal("counter_regression");
  file.finalCodex = structuredClone(state);
  sessions.set(sessionId, structuredClone(state));
  const hashes = seenHashes.get(sessionId) ?? new Set<string>();
  hashes.add(file.prefixHash!);
  seenHashes.set(sessionId, hashes);
}

function* claudeEvents(root: CaptureRoot, file: File,
  revisions: Map<string, Map<string, Amounts>>, db: DB): Generator<Candidate> {
  let sessionId = path.basename(file.file, ".jsonl").match(UUID_AT_END)?.[0]?.toLowerCase();
  const firstInFile = new Set<string>();
  file.initialClaude ??= new Map();
  file.finalClaude = new Map();
  for (const line of lines(file)) {
    if (!line.includes('"assistant"') || !line.includes('"usage"')) continue;
    let parsed: Record<string, any>;
    try { parsed = JSON.parse(line) as Record<string, any>; }
    catch { refusal("claude_relevant_json_invalid"); }
    if (parsed.type !== "assistant") continue;
    const claimed = typeof parsed.sessionId === "string"
      ? parsed.sessionId.match(UUID_AT_END)?.[0]?.toLowerCase() : undefined;
    sessionId ??= claimed;
    if (!sessionId || (claimed && claimed !== sessionId))
      refusal("claude_session_mismatch");
    const byMessage = revisions.get(sessionId) ?? new Map<string, Amounts>();
    revisions.set(sessionId, byMessage);
    const messageId = parsed.message?.id;
    if (typeof messageId !== "string" || !messageId) refusal("claude_message_id_missing");
    const messageKey = crypto.createHash("sha256").update(messageId).digest("hex");
    const stateKey = `${sessionId}\0${messageKey}`;
    if (!firstInFile.has(stateKey)) {
      firstInFile.add(stateKey);
      if (file.baselineDefined) {
        const initial = file.initialClaude.get(stateKey);
        if (initial) byMessage.set(messageId, initial);
        else byMessage.delete(messageId);
      } else {
        const initial = byMessage.get(messageId) ?? durableClaudeRevision(db, sessionId, messageKey);
        if (initial) {
          byMessage.set(messageId, initial);
          file.initialClaude.set(stateKey, initial);
        }
      }
    }
    const current = totals(parsed.message?.usage);
    const prior = byMessage.get(messageId);
    const delta = positiveDelta(current, prior ?? { input: 0, cacheRead: 0, cacheCreation: 0, output: 0 });
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
      claudeRevision: revision };
  }
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
  onMissing?: (candidate: Candidate, index: number, digest: string) => Promise<void>,
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
  const revisions = new Map<string, Map<string, Amounts>>();
  const codexStates = new Map<string, CodexState>();
  const codexHashes = new Map<string, Set<string>>();
  let candidateIndex = 0;
  let resumeVerified = !resume?.index;
  const plan: CaptureHistoryPlan = { status: "capture_roots_history_plan", rootId: root.rootId,
    source: root.source, dryRun: true, files: files.length, sessions: 0, skippedLiveSessions: 0,
    existingRows: 0, missingRows: 0, firstObservedAt: null, lastObservedAt: null,
    tokens: { input: 0, cacheRead: 0, cacheCreation: 0, output: 0 },
    fencedBytes: files.reduce((sum, file) => sum + file.limit, 0), since: options.since ?? null };
  for (const file of files) {
    // A generator checks its prefix digest only when it reaches EOF. Keep
    // candidates outside the ledger until that check has succeeded, so a
    // changed file cannot expose a partial import to upload or projection.
    const verifiedCandidates: Array<{ candidate: Candidate; index: number; digest: string }> = [];
    const events = root.source === "codex" ? codexEvents(root, file, db, codexStates, codexHashes)
      : claudeEvents(root, file, revisions, db);
    for (const candidate of events) {
      candidateIndex += 1;
      if (resume && candidateIndex === resume.index) {
        if (candidateDigest(candidate, candidateIndex) !== resume.digest)
          refusal("resume_cursor_digest_changed");
        resumeVerified = true;
      }
      const e = candidate.event;
      const session = e.sessionId!;
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
    if (onFileReady) await onFileReady(file);
    for (const item of verifiedCandidates) {
      await onMissing!(item.candidate, item.index, item.digest);
    }
    if (onFilePublished) await onFilePublished(file);
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
    root_id text primary key, root_digest text not null, source_digest text not null, source text not null,
    run_id text not null, imported_rows integer not null default 0,
    input_tokens integer not null default 0, cache_read_tokens integer not null default 0,
    cache_creation_tokens integer not null default 0, output_tokens integer not null default 0,
    started_at text not null, updated_at text not null, completed_at text,
    resume_candidate_index integer not null default 0, resume_candidate_digest text);
    create table if not exists capture_history_file_state (
      file_key text primary key,root_id text not null,source text not null,
      prefix_hash text not null,baseline_json text not null,
      handoff_ready integer not null default 0 check(handoff_ready in (0,1)));
    create table if not exists capture_history_session_counters (
      source text not null,session_id text not null,last_index integer not null,
      state_json text not null,primary key(source,session_id));
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
  if (!columns.some(column => column.name === "resume_candidate_index"))
    db.exec("alter table capture_history_import_runs add column resume_candidate_index integer not null default 0");
  if (!columns.some(column => column.name === "resume_candidate_digest"))
    db.exec("alter table capture_history_import_runs add column resume_candidate_digest text");
  const fileColumns = db.pragma("table_info(capture_history_file_state)") as Array<{ name: string }>;
  if (!fileColumns.some(column => column.name === "handoff_ready"))
    db.exec("alter table capture_history_file_state add column handoff_ready integer not null default 0");
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
  const read = { ...c, deferredBytes: c.observedSize - c.committedOffset,
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
  const first = await scan(db, root, options);
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
    const prior = db.prepare(`select root_digest as digest,source_digest as sourceDigest,run_id as runId,
      resume_candidate_index as resumeIndex,resume_candidate_digest as resumeDigest,
      imported_rows as importedRows,completed_at as completedAt
      from capture_history_import_runs where root_id=?`)
      .get(root.rootId) as { digest: string; sourceDigest: string | null; runId: string;
        resumeIndex: number; resumeDigest: string | null; importedRows: number;
        completedAt: string | null } | undefined;
    const digest = captureRootDigest(root);
    if (prior && prior.digest !== digest) refusal("root_identity_changed_since_import");
    if (prior && prior.sourceDigest !== fencedSourceDigest) {
      // An interrupted attempt that published nothing may re-preflight a
      // changed prefix from byte zero. Published history stays immutable.
      if (prior.importedRows !== 0 || prior.completedAt !== null)
        refusal("fenced_history_changed_since_import");
      db.prepare(`update capture_history_import_runs set source_digest=?,
        resume_candidate_index=0,resume_candidate_digest=null where root_id=?`)
        .run(fencedSourceDigest, root.rootId);
      db.prepare(`delete from capture_history_file_state where root_id=? and handoff_ready=0`)
        .run(root.rootId);
      prior.resumeIndex = 0;
      prior.resumeDigest = null;
    }
    if (prior && (!Number.isSafeInteger(prior.resumeIndex) || prior.resumeIndex < 0 ||
        (prior.resumeIndex === 0) !== (prior.resumeDigest === null)))
      refusal("resume_cursor_invalid");
    runId = prior?.runId ?? crypto.randomUUID();
    resume = { index: prior?.resumeIndex ?? 0, digest: prior?.resumeDigest ?? null };
    if (!prior) db.prepare(`insert into capture_history_import_runs
      (root_id,root_digest,source_digest,source,run_id,started_at,updated_at) values (?,?,?,?,?,?,?)`)
      .run(root.rootId, digest, fencedSourceDigest, root.source, runId,
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
  let pending: Array<{ candidate: Candidate; index: number; digest: string }> = [];
  const flush = async () => {
    if (!pending.length) return;
    const batch = pending.slice(0, nextRows);
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
    // Use the committed cost to size the next row cap. A single expensive row
    // keeps the next slice at one row until the ledger becomes responsive.
    if (elapsed > 200) {
      nextRows = Math.max(1, Math.floor(nextRows / 2));
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
      if (elapsed > WRITER_TARGET_MS) nextRows = Math.max(1, nextRows - 1);
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
    await scan(db, root, options, async (candidate, index, digest) => {
      pending.push({ candidate, index, digest });
      while (pending.length >= nextRows) await flush();
    }, first.files, resume, async file => {
      // This small row preserves the exact initial parser baseline before a
      // crash can leave only some verified candidates committed.
      db.transaction(() => rememberFileBaseline(db, root, file)).immediate();
    }, async file => {
      while (pending.length) await flush();
      // The normal tailer defers this file until its verified cumulative
      // state and all of its historical rows have reached the ledger.
      if (root.source === "claude_code") {
        const revisions = [...(file.finalClaude?.values() ?? [])];
        for (let index = 0; index < revisions.length; index += 16) {
          const batch = revisions.slice(index, index + 16);
          db.transaction(() => {
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
        if (file.finalCodex) {
          db.prepare(`insert into capture_history_session_counters
            (source,session_id,last_index,state_json) values ('codex',?,?,?)
            on conflict(source,session_id) do update set
              last_index=excluded.last_index,state_json=excluded.state_json
            where excluded.last_index>capture_history_session_counters.last_index`)
            .run(file.finalCodex.sessionId, file.finalCodex.index, JSON.stringify(file.finalCodex));
        }
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
