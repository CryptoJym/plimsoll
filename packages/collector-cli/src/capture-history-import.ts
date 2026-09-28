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

const UUID_AT_END = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const MAX_LINE_BYTES = 16 * 1024 * 1024;
const READ_BYTES = 128 * 1024;
const WRITER_ROWS = 8;
const WRITER_BUDGET_MS = 750;
type DB = Database.Database;
type File = { file: string; limit: number; stamp: string; fencedAt: string; prefixHash?: string };
type Amounts = { input: number; cacheRead: number; cacheCreation: number; output: number };
type Candidate = { event: AiInteractionEvent; sourceId: string };
export type CaptureHistoryPlan = {
  status: "capture_roots_history_plan"; rootId: string; source: CaptureRoot["source"];
  dryRun: true; files: number; sessions: number; skippedLiveSessions: number;
  existingRows: number; missingRows: number; firstObservedAt: string | null;
  lastObservedAt: string | null; tokens: Amounts; fencedBytes: number; since: string | null;
};
export type CaptureHistoryApplyReceipt = Omit<CaptureHistoryPlan, "status" | "dryRun"> & {
  status: "capture_roots_history_imported"; importedRows: number; importedTokens: Amounts;
  totalImportedRows: number; maxWriterSliceMs: number; overBudgetSlices: number;
  maxWriterWorkMs: number; maxWriterRowMs: number; runId: string;
};
type Options = { since?: string; stopAfterSlices?: number };

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
        const stat = fs.statSync(file, { bigint: true });
        if (!stat.isFile()) refusal("file_not_regular");
        const receipt = captureBaselineExcludedReceipt(db, root.source, {
          path: file, device: stat.dev, inode: stat.ino, size: stat.size, birthtimeNs: stat.birthtimeNs,
        });
        if (receipt) {
          if (BigInt(receipt.baselineSize) > stat.size) refusal("fenced_prefix_truncated");
          found.push({ file, limit: receipt.baselineSize, stamp: stamp(stat), fencedAt: receipt.baselinedAt });
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

/** Reads only complete lines inside the exact fenced prefix; source bytes do
 * not enter a receipt or the ledger. */
function* lines(file: File): Generator<string> {
  const fd = fs.openSync(file.file, "r");
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
    const after = fs.fstatSync(fd, { bigint: true });
    if (stamp(after) !== file.stamp || after.size < BigInt(file.limit))
      refusal("file_changed_during_read");
    const digest = hash.digest("hex");
    if (file.prefixHash && file.prefixHash !== digest) refusal("fenced_prefix_changed");
    file.prefixHash = digest;
  } finally { fs.closeSync(fd); }
}

function verifyFencedPrefix(file: File) {
  if (!file.prefixHash) refusal("prefix_hash_missing");
  const fd = fs.openSync(file.file, "r");
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

function* codexEvents(root: CaptureRoot, file: File): Generator<Candidate> {
  const sessionId = path.basename(file.file, ".jsonl").match(UUID_AT_END)?.[0]?.toLowerCase();
  if (!sessionId) refusal("codex_file_session_missing");
  let model: string | undefined;
  let previous: Amounts = { input: 0, cacheRead: 0, cacheCreation: 0, output: 0 };
  let observedBaseline = false;
  let index = -1;
  for (const line of lines(file)) {
    if (!line.includes('"session_meta"') && !line.includes('"turn_context"') &&
        !line.includes('"token_count"')) continue;
    let parsed: Record<string, any>;
    try { parsed = JSON.parse(line) as Record<string, any>; }
    catch { refusal("codex_relevant_json_invalid"); }
    if (parsed.type === "session_meta") {
      const id = parsed.payload?.id;
      if (typeof id !== "string" || id.toLowerCase() !== sessionId) refusal("codex_session_mismatch");
    } else if (parsed.type === "turn_context") {
      if (typeof parsed.payload?.model === "string") model = parsed.payload.model;
    } else if (parsed.type === "event_msg" && parsed.payload?.type === "token_count") {
      index += 1;
      const reported = parsed.payload?.info?.total_token_usage;
      if (!reported) continue;
      const current = totals(reported);
      const delta = positiveDelta(current, previous);
      const firstUnknown = !observedBaseline && !zero(current);
      previous = current;
      observedBaseline = true;
      if (zero(delta)) continue;
      const id = deterministicEventId(["codex-rollout", sessionId, String(index)]);
      const observedAt = parsed.timestamp;
      if (typeof observedAt !== "string") refusal("codex_timestamp_missing");
      const marginal = firstUnknown ? { input: 0, cacheRead: 0, cacheCreation: 0, output: 0 } : delta;
      yield { sourceId: id, event: event(root, id, sessionId, observedAt, model, marginal,
        { turnIndex: index, ...(firstUnknown ? { counterLineage: "unknown_nonzero_first",
          sourceCumulativeInput: current.input, sourceCumulativeCachedInput: current.cacheRead,
          sourceCumulativeOutput: current.output } : {}) }, firstUnknown) };
    }
  }
}

function* claudeEvents(root: CaptureRoot, file: File,
  revisions: Map<string, Map<string, Amounts>>): Generator<Candidate> {
  let sessionId = path.basename(file.file, ".jsonl").match(UUID_AT_END)?.[0]?.toLowerCase();
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
    const current = totals(parsed.message?.usage);
    const prior = byMessage.get(messageId);
    const delta = positiveDelta(current, prior ?? { input: 0, cacheRead: 0, cacheCreation: 0, output: 0 });
    byMessage.set(messageId, current);
    if (zero(delta)) continue;
    const id = prior
      ? deterministicEventId(["claude-transcript-revision", sessionId, messageId,
          String(current.input), String(current.cacheRead), String(current.cacheCreation), String(current.output)])
      : deterministicEventId(["claude-transcript", sessionId, messageId]);
    if (typeof parsed.timestamp !== "string") refusal("claude_timestamp_missing");
    const model = typeof parsed.message?.model === "string" ? parsed.message.model : undefined;
    yield { sourceId: id, event: event(root, id, sessionId, parsed.timestamp, model, delta) };
  }
}

type ScanResult = { plan: CaptureHistoryPlan; files: File[] };
function sourceDigest(files: File[], since: string | undefined) {
  if (files.some(file => !file.prefixHash)) refusal("prefix_hash_missing");
  return crypto.createHash("sha256").update(JSON.stringify([since ?? null,
    files.map(file => [file.file, file.limit, file.stamp, file.fencedAt, file.prefixHash])
  ])).digest("hex");
}
async function scan(db: DB, root: CaptureRoot, options: Options,
  onMissing?: (candidate: Candidate) => Promise<void>, expectedFiles?: File[]): Promise<ScanResult> {
  if (options.since && !validIso(options.since)) refusal("since_invalid_iso");
  const files = candidateFiles(db, root);
  if (expectedFiles && (files.length !== expectedFiles.length || files.some((file, index) =>
    file.file !== expectedFiles[index]?.file || file.stamp !== expectedFiles[index]?.stamp ||
    file.limit !== expectedFiles[index]?.limit || file.fencedAt !== expectedFiles[index]?.fencedAt)))
    refusal("source_changed_after_plan");
  if (expectedFiles) files.forEach((file, index) => { file.prefixHash = expectedFiles[index]!.prefixHash; });
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
  const plan: CaptureHistoryPlan = { status: "capture_roots_history_plan", rootId: root.rootId,
    source: root.source, dryRun: true, files: files.length, sessions: 0, skippedLiveSessions: 0,
    existingRows: 0, missingRows: 0, firstObservedAt: null, lastObservedAt: null,
    tokens: { input: 0, cacheRead: 0, cacheCreation: 0, output: 0 },
    fencedBytes: files.reduce((sum, file) => sum + file.limit, 0), since: options.since ?? null };
  for (const file of files) {
    const events = root.source === "codex" ? codexEvents(root, file) : claudeEvents(root, file, revisions);
    for (const candidate of events) {
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
      if (onMissing) await onMissing(candidate);
      if (plan.missingRows % 4096 === 0) await new Promise<void>(resolve => setImmediate(resolve));
    }
  }
  plan.sessions = sessions.size;
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
function ensureImportSchema(db: DB) {
  db.exec(`create table if not exists capture_history_import_lock (
    singleton integer primary key check(singleton=1), root_id text not null,
    owner_pid integer not null, owner_start text not null);
    create table if not exists capture_history_import_runs (
    root_id text primary key, root_digest text not null, source_digest text not null, source text not null,
    run_id text not null, imported_rows integer not null default 0,
    input_tokens integer not null default 0, cache_read_tokens integer not null default 0,
    cache_creation_tokens integer not null default 0, output_tokens integer not null default 0,
    started_at text not null, updated_at text not null, completed_at text);
    create trigger if not exists capture_history_import_block_prune
      before insert on raw_retention_receipts
      when exists (select 1 from capture_history_import_lock where singleton=1)
      begin select raise(abort, 'capture_history_import_active'); end;`);
  const columns = db.pragma("table_info(capture_history_import_runs)") as Array<{ name: string }>;
  if (!columns.some(column => column.name === "source_digest"))
    db.exec("alter table capture_history_import_runs add column source_digest text");
}
const active = new Set<string>();
export async function applyCaptureHistory(buffer: LocalEventBuffer, root: CaptureRoot,
  options: Options = {}): Promise<CaptureHistoryApplyReceipt> {
  if (active.size) refusal("another_import_in_process");
  const db = buffer.database;
  const first = await scan(db, root, options); // all refusal evidence before writing
  const fencedSourceDigest = sourceDigest(first.files, options.since);
  maintenanceIdle(db);
  ensureImportSchema(db);
  const ownerStart = processStart(process.pid);
  if (!ownerStart || ownerStart === "unknown") refusal("process_identity_unavailable");
  let runId = "";
  db.transaction(() => {
    maintenanceIdle(db);
    const held = db.prepare(`select root_id as rootId, owner_pid as pid, owner_start as started
      from capture_history_import_lock where singleton=1`).get() as
        { rootId: string; pid: number; started: string } | undefined;
    if (held) {
      const holder = processStart(held.pid);
      if (holder === "unknown") refusal("import_holder_identity_unknown");
      if ((held.pid !== process.pid || held.started !== ownerStart || held.rootId !== root.rootId) &&
          holder === held.started) refusal("another_import_holds_ledger");
    }
    if (held) db.prepare(`delete from capture_history_import_lock where singleton=1`).run();
    db.prepare(`insert into capture_history_import_lock values (1,?,?,?)`)
      .run(root.rootId, process.pid, ownerStart);
    const prior = db.prepare(`select root_digest as digest,source_digest as sourceDigest,run_id as runId
      from capture_history_import_runs where root_id=?`)
      .get(root.rootId) as { digest: string; sourceDigest: string | null; runId: string } | undefined;
    const digest = captureRootDigest(root);
    if (prior && prior.digest !== digest) refusal("root_identity_changed_since_import");
    if (prior && prior.sourceDigest !== fencedSourceDigest)
      refusal("fenced_history_changed_since_import");
    runId = prior?.runId ?? crypto.randomUUID();
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
  active.add(root.rootId);
  let importedRows = 0;
  let maxWriterSliceMs = 0;
  let overBudgetSlices = 0;
  let maxWriterWorkMs = 0;
  let maxWriterRowMs = 0;
  let slices = 0;
  const importedTokens: Amounts = { input: 0, cacheRead: 0, cacheCreation: 0, output: 0 };
  let pending: Candidate[] = [];
  const flush = async () => {
    if (!pending.length) return;
    const batch = pending;
    pending = [];
    maintenanceIdle(db);
    let writerStarted = 0;
    let writerWorkEnded = 0;
    let writerRowMs = 0;
    const receipt = buffer.withHistoryImportAdmission(root.installationEpochId, () =>
      buffer.transactionWithRepoContextHandoffs(() => {
        // The IMMEDIATE transaction acquired the writer before this callback.
        // Contention waiting for another writer is not our held writer slice.
        writerStarted = performance.now();
        const lock = db.prepare(`select root_id as rootId, owner_pid as pid, owner_start as started
          from capture_history_import_lock where singleton=1`).get() as
            { rootId: string; pid: number; started: string } | undefined;
        if (!lock || lock.rootId !== root.rootId || lock.pid !== process.pid || lock.started !== ownerStart)
          refusal("import_lock_lost");
        maintenanceIdle(db);
        const counts: Amounts = { input: 0, cacheRead: 0, cacheCreation: 0, output: 0 };
        let rows = 0;
        for (const candidate of batch) {
          const rowStarted = performance.now();
          const e = candidate.event;
          // A concurrent live writer may have won the session while files
          // were read; session authority is checked again under the writer.
          const authority = db.prepare(`select authority from session_usage_authority where source=? and session_id=?`)
            .get(root.source, e.sessionId) as { authority: string } | undefined;
          if (authority?.authority === "live") continue;
          if (db.prepare(`select 1 from raw_retention_receipts where event_id=?`).get(e.id)) continue;
          if (!appendRootObservation(buffer, e, root)) continue;
          rows += 1;
          counts.input += e.inputTokens ?? 0;
          counts.output += e.outputTokens ?? 0;
          counts.cacheRead += e.cacheReadTokens ?? 0;
          counts.cacheCreation += e.cacheCreationTokens ?? 0;
          writerRowMs = Math.max(writerRowMs, performance.now() - rowStarted);
        }
        db.prepare(`update capture_history_import_runs set imported_rows=imported_rows+?,
          input_tokens=input_tokens+?,cache_read_tokens=cache_read_tokens+?,
          cache_creation_tokens=cache_creation_tokens+?,output_tokens=output_tokens+?,updated_at=?
          where root_id=?`).run(rows, counts.input, counts.cacheRead, counts.cacheCreation,
            counts.output, new Date().toISOString(), root.rootId);
        writerWorkEnded = performance.now();
        return { rows, counts };
      }));
    const elapsed = performance.now() - writerStarted;
    maxWriterSliceMs = Math.max(maxWriterSliceMs, elapsed);
    maxWriterWorkMs = Math.max(maxWriterWorkMs, writerWorkEnded - writerStarted);
    maxWriterRowMs = Math.max(maxWriterRowMs, writerRowMs);
    if (elapsed >= WRITER_BUDGET_MS) overBudgetSlices += 1;
    importedRows += receipt.rows;
    importedTokens.input += receipt.counts.input;
    importedTokens.output += receipt.counts.output;
    importedTokens.cacheRead += receipt.counts.cacheRead;
    importedTokens.cacheCreation += receipt.counts.cacheCreation;
    slices += 1;
    if (options.stopAfterSlices === slices) throw new Error("capture_history_injected_crash");
    if (slices % 512 === 0) db.pragma("wal_checkpoint(PASSIVE)");
    await new Promise<void>(resolve => setImmediate(resolve));
  };
  try {
    // Verify every prefix before the first ledger mutation. Suffix growth is
    // allowed, but a rewrite of any fenced byte aborts the entire preflight.
    for (const file of first.files) verifyFencedPrefix(file);
    await scan(db, root, options, async candidate => {
      pending.push(candidate);
      if (pending.length >= WRITER_ROWS) await flush();
    }, first.files);
    await flush();
    db.transaction(() => {
      const lock = db.prepare(`select root_id as rootId,owner_pid as pid,owner_start as started
        from capture_history_import_lock where singleton=1`).get() as
          { rootId: string; pid: number; started: string } | undefined;
      if (!lock || lock.rootId !== root.rootId || lock.pid !== process.pid || lock.started !== ownerStart)
        refusal("import_lock_lost");
      db.prepare(`update capture_history_import_runs set completed_at=?,updated_at=? where root_id=?`)
        .run(new Date().toISOString(), new Date().toISOString(), root.rootId);
      db.prepare(`delete from capture_history_import_lock where singleton=1`).run();
    }).immediate();
  } finally {
    try { db.pragma(`wal_autocheckpoint = ${priorAutoCheckpoint}`); }
    finally { active.delete(root.rootId); }
  }
  const total = db.prepare(`select imported_rows as rows from capture_history_import_runs where root_id=?`)
    .get(root.rootId) as { rows: number };
  const { status: _status, dryRun: _dryRun, ...plan } = first.plan;
  return { ...plan, status: "capture_roots_history_imported", importedRows,
    importedTokens, totalImportedRows: total.rows, maxWriterSliceMs,
    overBudgetSlices, maxWriterWorkMs, maxWriterRowMs, runId };
}
