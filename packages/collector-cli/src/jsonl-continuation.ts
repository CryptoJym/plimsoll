import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import type Database from "better-sqlite3";
import { captureRootDigest, type CaptureRoot } from "./capture-root-inventory";
import { jsonlScanStateKey, type JsonlScanCursor, type JsonlTailRead, type JsonlTailReadLimits, type JsonlTailerIo } from "./jsonl-byte-tailer";
import { start, feed, checkpoint, restore, project, LIMITS } from "./oversized-extractor.mjs";
import { fingerprint, extend, equal, resumePolicy, sameSnapshot, type Fingerprint, type Snapshot } from "./oversized-continuity.mjs";

// This table is deliberately separate from every legacy cursor column. Old
// binaries ignore it: downgrade therefore REQUIRES the stopped/root-disabled
// procedure in the implementation receipt, particularly after a rewrite.
export const CONTINUATION_VERSION = 1;
export const MAX_ENVELOPE_BYTES = 48 * 1024;
const sha = (value: string | Buffer) => crypto.createHash("sha256").update(value).digest("hex");
const REFUSALS = new Set(["incomplete_record", "generation_changed", "rewrite_ambiguous", "generation_rewrite_ambiguous", "prior_cursor_invalid", "prefix_changed", "prior_cursor_changed", "source_changed", "malformed_json", "invalid_utf8", "max_depth", "allowed_scalar_too_large", "allowed_number_too_large", "allowed_projection_too_large", "offset_overflow", "newline_before_end", "parser_refused"]);
const safe = (n: unknown): n is number => Number.isSafeInteger(n) && (n as number) >= 0;
const digest = (value: unknown) => sha(JSON.stringify(value));
const exact = (value: unknown, keys: string[]): value is Record<string, any> => !!value && typeof value === "object" && !Array.isArray(value) && Object.keys(value).sort().join() === keys.sort().join();
type Provider = "codex" | "claude";
type Binding = { provider: Provider; cursorKey: string; fileKey: string; root: string; profile: string; epoch: string; enrollment: string; baseline: string; inventory: string };
type Envelope = { sha256?: string; version: 1; rollbackVersion: "0.7.4"; binding: Binding; priorCursor: string; ancestors: string; snapshot: Snapshot;
 parser: string; prefix: Fingerprint; verification: { snapshot: Snapshot; prefix: Fingerprint } | null;
 reason: string | null };
export type ContinuationProposal = {
 action: "checkpoint" | "complete" | "park"; reason: string | null; requiredMinimumBytes: number | null;
 scanBytesAdvanced: number; prefixBytesRead: number;
 /** Called ONLY inside the existing cursor/event transaction. No filesystem I/O. */
 applyCheckpoint(): void;
 assertCurrent(): void;
 remove(): void;
};
export type ContinuationOptions = {
 database: Database.Database; provider: Provider; cursorKey: string; root?: CaptureRoot;
 directory: string; deadline: number;
 /** Metadata-only eligibility check. Called before every body/probe/scalar read. */
 eligible(): boolean;
};

export function ensureJsonlContinuationStore(db: Database.Database) {
 db.exec(`create table if not exists jsonl_continuations (
   provider text not null check(provider in ('codex','claude')),
   file_key text not null check(length(file_key)=64),
   envelope_json text not null check(length(cast(envelope_json as blob))<=49152),
   primary key(provider,file_key)
 ) without rowid;
 create index if not exists jsonl_continuations_file on jsonl_continuations(file_key)`);
}
export function jsonlCursorDigest(db: Database.Database, key: string): string {
 const row = db.prepare("select * from rollout_scan_state where file=?").get(jsonlScanStateKey(key)) as Record<string, unknown> | undefined;
 return digest(row ? Object.fromEntries(Object.keys(row).sort().map(k => [k, row[k]])) : null);
}
function optionalRow(db: Database.Database, table: string, sql: string, arg?: string) {
 if (!db.prepare("select 1 from sqlite_master where type='table' and name=?").get(table)) return null;
 return (arg === undefined ? db.prepare(sql).get() : db.prepare(sql).get(arg)) ?? null;
}
function binding(options: ContinuationOptions, file: string): Binding {
 const db = options.database, source = options.provider === "codex" ? "codex" : "claude_code";
 return { provider: options.provider, cursorKey: jsonlScanStateKey(options.cursorKey), fileKey: sha(file),
   root: options.root ? captureRootDigest(options.root) : sha(options.directory),
   profile: digest(options.root?.profileId ?? null), epoch: digest(options.root?.installationEpochId ?? null),
   enrollment: digest(optionalRow(db, "collector_workspace_binding", "select current_workspace_id,current_device_id,current_installation_epoch_id,current_installation_epoch_started_at from collector_workspace_binding where singleton=1")),
   baseline: digest(optionalRow(db, "automatic_capture_baseline_state", "select schema_version,run_id,status,started_at,completed_at from automatic_capture_baseline_state where source=?", source)),
   inventory: digest(optionalRow(db, "capture_root_inventory_bindings", "select inventory_digest from capture_root_inventory_bindings where source=?", source)) };
}
function validSnapshot(s: unknown): s is Snapshot {
 return exact(s, ["identity", "size", "mtimeNs", "ctimeNs"]) && typeof s.identity === "string" && /^\d+:\d+:\d+$/.test(s.identity) && safe(s.size) &&
   typeof s.mtimeNs === "string" && /^\d{1,22}$/.test(s.mtimeNs) && typeof s.ctimeNs === "string" && /^\d{1,22}$/.test(s.ctimeNs);
}
function decode(raw: string): Envelope {
 if (Buffer.byteLength(raw) > MAX_ENVELOPE_BYTES) throw new Error("envelope_limit");
 const e = JSON.parse(raw);
 if (!validSeal(e)) throw new Error("corrupt_envelope");
 if (!exact(e, ["sha256", "version", "rollbackVersion", "binding", "priorCursor", "ancestors", "snapshot", "parser", "prefix", "verification", "reason"]) || e.version !== 1 || e.rollbackVersion !== "0.7.4" ||
   !exact(e.binding, ["provider", "cursorKey", "fileKey", "root", "profile", "epoch", "enrollment", "baseline", "inventory"]) ||
   !["codex", "claude"].includes(e.binding.provider) || !Object.entries(e.binding).every(([k, v]) => k === "provider" || typeof v === "string" && /^[a-f0-9]{64}$/.test(v)) ||
   typeof e.priorCursor !== "string" || !/^[a-f0-9]{64}$/.test(e.priorCursor) || !validSnapshot(e.snapshot) || typeof e.ancestors !== "string" || !/^[a-f0-9]{64}$/.test(e.ancestors) ||
   !(e.reason === null || typeof e.reason === "string" && /^[a-z_]{1,64}$/.test(e.reason))) throw new Error("invalid_envelope");
 const p = restore(e.parser);
 equal(e.prefix, e.prefix);
 if (p.provider !== e.binding.provider || e.prefix.start !== p.recordStart || e.prefix.end !== p.scanOffset || p.scanOffset > e.snapshot.size) throw new Error("parser_fingerprint_mismatch");
 if (e.verification !== null) {
   if (!exact(e.verification, ["snapshot", "prefix"]) || !validSnapshot(e.verification.snapshot)) throw new Error("invalid_verification");
   equal(e.verification.prefix, e.verification.prefix);
   if (e.verification.prefix.start !== p.recordStart || e.verification.prefix.end > p.scanOffset ||
     e.verification.snapshot.identity !== e.snapshot.identity || e.verification.snapshot.size < e.snapshot.size) throw new Error("invalid_verification");
 }
 if (e.reason !== null && !REFUSALS.has(e.reason)) throw new Error("invalid_refusal");
 return e as Envelope;
}
function sealed(body:Record<string, unknown>) {
 const {sha256: _ignored, ...value} = body;
 return JSON.stringify({...value,sha256:digest(value)});
}
function validSeal(value:unknown) {
 if (!value || typeof value !== "object" || Array.isArray(value)) return false;
 const {sha256: sum, ...body} = value as Record<string, unknown>;
 return typeof sum === "string" && /^[a-f0-9]{64}$/.test(sum) && digest(body) === sum;
}
function encode(e: Envelope) { const raw = sealed(e); decode(raw); return raw; }
function snapshot(stat: fs.BigIntStats): Snapshot {
 const size = Number(stat.size);
 if (!stat.isFile() || !safe(size)) throw new Error("unsafe_generation");
 return { identity: `${stat.dev}:${stat.ino}:${stat.birthtimeNs}`, size, mtimeNs: String(stat.mtimeNs), ctimeNs: String(stat.ctimeNs) };
}

/** Wrap the generic reader at its unresolved-record boundary. Existing complete
 * slices retain their provider behavior. A continuation always intercepts BEFORE
 * generic rewrite handling, even when its binding or serialization is corrupt. */
export function readJsonlContinuation<T>(file: string, stat: fs.Stats, cursor: JsonlScanCursor<T> | undefined,
 limits: JsonlTailReadLimits, io: JsonlTailerIo, options: ContinuationOptions): JsonlTailRead | undefined {
 const db = options.database, b = binding(options, file), priorCursor = jsonlCursorDigest(db, options.cursorKey);
 const previousRaw = (db.prepare("select envelope_json from jsonl_continuations where provider=? and file_key=?").get(options.provider, b.fileKey) as { envelope_json: string } | undefined)?.envelope_json;
 const maxBytes = Math.min(LIMITS.sliceBytes, Math.max(0, Math.floor(limits.maxBytes ?? LIMITS.sliceBytes)));
 let bytesRead = 0, prefixBytesRead = 0, fd: number | undefined, current: Snapshot | undefined;
 let envelope: Envelope | undefined, initialOffset = 0, admittedAncestors: string | undefined;
 let legacy: JsonlTailRead | undefined;
 let reason: string | null = null, requiredMinimumBytes: number | null = null, pendingFence: string | null = null;
 const close = () => { if (fd !== undefined) { fs.closeSync(fd); fd = undefined; } legacy?.close(); };
 const assertCurrent = () => {
   const row = db.prepare("select envelope_json from jsonl_continuations where provider=? and file_key=?").get(options.provider, b.fileKey) as {envelope_json:string}|undefined;
   if (db.prepare("select 1 from jsonl_continuations where file_key=? and provider<>? limit 1").get(b.fileKey, options.provider) || row?.envelope_json !== previousRaw || jsonlCursorDigest(db, options.cursorKey) !== priorCursor || digest(binding(options, file)) !== digest(b)) throw new Error("stale_continuation_proposal");
 };
 const result = (action: ContinuationProposal["action"], lines: string[] = [], committedOffset = cursor?.committedOffset ?? 0): JsonlTailRead => {
   const nextRaw = action === "checkpoint" ? pendingFence ?? (envelope ? encode(envelope) : null) : null;
   const snap = current ?? envelope?.snapshot;
   const proposal: ContinuationProposal = { action, reason, requiredMinimumBytes,
     scanBytesAdvanced: action === "checkpoint" && envelope ? Math.max(0, envelope.prefix.end - initialOffset) : 0,
     prefixBytesRead,
     assertCurrent,
     applyCheckpoint() {
       assertCurrent();
       if (nextRaw === null) throw new Error("invalid_checkpoint_proposal");
       db.prepare("insert into jsonl_continuations(provider,file_key,envelope_json) values (?,?,?) on conflict(provider,file_key) do update set envelope_json=excluded.envelope_json").run(options.provider, b.fileKey, nextRaw);
     },
     remove() { assertCurrent(); db.prepare("delete from jsonl_continuations where provider=? and file_key=?").run(options.provider, b.fileKey); },
   };
   return { lines, observedSize: snap?.size ?? stat.size, committedOffset,
     deferredBytes: (snap?.size ?? stat.size) - committedOffset,
     fileIdentity: snap?.identity ?? cursor?.fileIdentity ?? "0:0:0",
     headHash: cursor?.headHash ?? null, headBytes: cursor?.headBytes ?? 0,
     continuityHash: cursor?.continuityHash ?? null, continuityBytes: cursor?.continuityBytes ?? 0,
     mtimeMs: snap ? Number(snap.mtimeNs) / 1e6 : stat.mtimeMs,
     ctimeMs: snap ? Number(snap.ctimeNs) / 1e6 : stat.ctimeMs,
     bytesRead, workRemaining: action === "checkpoint" && !reason || action === "complete" && committedOffset < (snap?.size ?? stat.size),
     unresolvedRecord: null, reset: false, legacyRebuild: false, checkpointRebuild: false,
     continuation: proposal,
     assertStableForCommit() {
       if (current && !sameSnapshot(securePath(), current)) throw new Error("source_changed");
       if (fd !== undefined) check(); else legacy?.assertStableForCommit();
     }, close };
 };
 const protectLegacy = (read:JsonlTailRead) => {
   const validate = read.assertStableForCommit.bind(read);
   read.assertStableForCommit = () => {
     if (!current || !sameSnapshot(securePath(), current)) throw new Error("source_changed");
     validate();
   };
   // CAS also protects the ordinary LF preceding an oversized record. This
   // uses the existing transaction, with no new table row for ordinary work.
   read.continuation = result("complete").continuation;
   return read;
 };
 const park = (why: string) => { reason = why; return result("park"); };
 const refuse = (why: string) => { reason = why; if (envelope) envelope.reason = why; return result(envelope ? "checkpoint" : "park"); };
 // Reject aliases in every ancestor below the admitted root, including changes
 // after open. Root inventory and baseline/epoch changes are checked each time.
 const securePath = () => {
   if (!options.eligible() || digest(binding(options, file)) !== digest(b)) throw new Error("capture_eligibility_changed");
   const root = path.resolve(options.directory);
   if (!file.startsWith(root + path.sep)) throw new Error("capture_path_outside_root");
   const physicalRoot = fs.realpathSync(root);
   const rootStat = fs.lstatSync(root);
   if (!rootStat.isDirectory() || rootStat.isSymbolicLink()) throw new Error("capture_root_alias");
   const ancestry = [`${rootStat.dev}:${rootStat.ino}:${rootStat.birthtimeMs}`];
   let parent = root;
   for (const segment of path.relative(root, path.dirname(file)).split(path.sep).filter(Boolean)) {
     parent = path.join(parent, segment);
     const st = fs.lstatSync(parent);
     if (!st.isDirectory() || st.isSymbolicLink()) throw new Error("capture_ancestor_alias");
     ancestry.push(`${st.dev}:${st.ino}:${st.birthtimeMs}`);
   }
   if (fs.realpathSync(path.dirname(file)) !== path.join(physicalRoot, path.relative(root, path.dirname(file)))) throw new Error("capture_ancestor_changed");
   const ancestors = digest(ancestry);
   if ((envelope?.ancestors ?? admittedAncestors ?? ancestors) !== ancestors) throw new Error("capture_ancestor_changed");
   admittedAncestors = ancestors;
   const st = fs.lstatSync(file, {bigint: true});
   if (st.isSymbolicLink()) throw new Error("capture_file_alias");
   return snapshot(st);
 };
 const check = () => {
   const fresh = securePath();
   if (!current || !sameSnapshot(fresh, current) || fd === undefined || !sameSnapshot(snapshot(fs.fstatSync(fd, {bigint:true})), current)) throw new Error("source_changed");
 };
 const readAt = (at: number, length: number) => {
   if (!safe(at) || !safe(length) || length > LIMITS.sliceBytes || at + length > current!.size || bytesRead + length > maxBytes || performance.now() >= options.deadline) throw new Error("read_budget_exhausted");
   check();
   const bytes = Buffer.alloc(length);
   const n = fs.readSync(fd!, bytes, 0, length, at); bytesRead += n;
   if (n !== length) throw new Error("source_changed");
   return bytes;
 };
 try {
   if (previousRaw === undefined && db.prepare("select 1 from jsonl_continuations where file_key=? limit 1").get(b.fileKey)) return park("binding_mismatch");
   if (previousRaw !== undefined) {
     if (typeof previousRaw !== "string" || Buffer.byteLength(previousRaw) > MAX_ENVELOPE_BYTES) return park("invalid_envelope");
     const retired = retiredReason(previousRaw);
     if (retired) return park(retired);
     try { envelope = decode(previousRaw); } catch { return park("invalid_envelope"); }
     if (digest(envelope.binding) !== digest(b) || envelope.priorCursor !== priorCursor ||
       (cursor && cursor.checkpointStatus !== "valid") || restore(envelope.parser).recordStart !== (cursor?.committedOffset ?? 0)) return park("binding_mismatch");
     initialOffset = envelope.prefix.end;
     if (envelope.reason && envelope.reason !== "incomplete_record") return park(envelope.reason);
     const p = envelope.verification?.prefix ?? envelope.prefix;
     const savedParser = restore(envelope.parser);
     requiredMinimumBytes = savedParser.status === "ready" && !envelope.verification
       ? savedParser.slots.reduce((n, slot) => n + (slot && ["string", "number"].includes(slot.kind) ? slot.end-slot.begin : 0), 0) + Math.min(512, envelope.snapshot.size) + Math.min(512, savedParser.scanOffset)
       : p.end - p.start - p.fullBytes + 1;
     if (maxBytes < requiredMinimumBytes) return park("insufficient_budget");
   } else {
     requiredMinimumBytes = 1025;
     if (maxBytes < requiredMinimumBytes || performance.now() >= options.deadline) return park("insufficient_budget");
     // The old reader performs its bounded head/boundary integrity checks. Its
     // unresolved result is a proposal only; never store its reset/scan fields.
     current = securePath();
     legacy = io.readTail(file, stat, cursor, {...limits, maxBytes, beforeRead: () => {
       if (!current || !sameSnapshot(securePath(), current)) throw new Error("source_changed");
       if (performance.now() >= options.deadline) throw new Error("read_budget_exhausted");
     }, onBytesRead: n => { bytesRead += n; }});
     const incomplete = legacy && !legacy.unresolvedRecord && legacy.lines.length === 0 && legacy.deferredBytes > 0;
     if (!legacy || !legacy.unresolvedRecord && !incomplete) {
       // Keep the established bounded fast path for unescaped records. An
       // escape anywhere in a complete line requires structural projection;
       // raw lexical discriminator tests cannot recognize escaped keys/values.
       // Spans reuse this already charged bounded buffer. Every oversized
       // continuation uses the reviewed parser regardless of escape spelling.
       if (legacy) legacy.lines = legacy.lines.map(line => {
         if (!line.includes("\\")) return line;
         const bytes = Buffer.from(line + "\n");
         const parser = start(options.provider);
         feed(parser, bytes, {deadline: options.deadline});
         if (parser.status !== "ready") throw new Error(parser.reason ?? "read_budget_exhausted");
         return JSON.stringify(project(parser, (at, n) => bytes.subarray(at, at + n)));
       });
       return legacy ? protectLegacy(legacy) : undefined;
     }
     if (legacy.unresolvedRecord && legacy.unresolvedRecord.reason !== "record_exceeds_byte_budget") {
       // Preserve the legacy behavior for ordinary bounded records. Known
       // oversized work (or a source larger than a slice) gets a separate,
       // durable fence before the old rewrite result can reset its cursor.
       if (stat.size <= maxBytes && cursor?.unresolvedRecord?.reason !== "record_exceeds_byte_budget") return protectLegacy(legacy);
       bytesRead = legacy.bytesRead; reason = "generation_rewrite_ambiguous";
       pendingFence = sealed({version:1,rollbackVersion:"0.7.4",retired:true,reason,bindingDigest:digest(b),priorCursor});
       return result("checkpoint");
     }
     bytesRead = legacy.bytesRead;
     if (cursor && (cursor.checkpointStatus !== "valid" || legacy.reset || cursor.committedOffset === null)) return refuse("prior_cursor_invalid");
     current = securePath();
     const offset = cursor?.committedOffset ?? 0;
     initialOffset = offset;
     envelope = { version:1, rollbackVersion:"0.7.4", binding:b, priorCursor, ancestors:admittedAncestors!, snapshot:current,
       parser:checkpoint(start(options.provider, offset)), prefix:fingerprint(offset), verification:null, reason:null };
     return result("checkpoint");
   }
   current = securePath();
   const policy = resumePolicy(envelope.snapshot, current, true);
   if (policy === "generation_changed" || policy === "rewrite_ambiguous") return refuse(policy);
   if (envelope.reason === "incomplete_record" && policy === "resume") return park("incomplete_record");
   envelope.reason = null;
   const probeBytes = (cursor?.headBytes ?? 0) + (cursor?.continuityBytes ?? 0);
   if (policy === "verify_prefix" && maxBytes < probeBytes + requiredMinimumBytes!) {
     requiredMinimumBytes = probeBytes + requiredMinimumBytes!;
     return park("insufficient_budget");
   }
   fd = fs.openSync(file, fs.constants.O_RDONLY | (fs.constants.O_NOFOLLOW ?? 0));
   check();
   if (policy === "verify_prefix") {
     if (cursor?.headHash && sha(readAt(0, cursor.headBytes)) !== cursor.headHash ||
       cursor?.continuityHash && sha(readAt(cursor.committedOffset! - cursor.continuityBytes, cursor.continuityBytes)) !== cursor.continuityHash) return refuse("prior_cursor_changed");
     prefixBytesRead += probeBytes;
     if (!envelope.verification || !sameSnapshot(envelope.verification.snapshot, current)) envelope.verification = {snapshot:current, prefix:fingerprint(envelope.prefix.start)};
     const verification = extend(envelope.verification.prefix, envelope.prefix.end, readAt, {maxBytes: maxBytes - bytesRead, deadline:options.deadline});
     prefixBytesRead += verification.bytesRead;
     if (verification.status === "changed") return refuse("prefix_changed");
     envelope.verification.prefix = verification.fingerprint;
     if (verification.status !== "complete") return result("checkpoint");
     if (!equal(verification.fingerprint, envelope.prefix)) return refuse("prefix_changed");
     envelope.snapshot = current; envelope.verification = null;
   }
   const parser = restore(envelope.parser);
   // One admitted parser read, leaving enough allowance for its old partial
   // fingerprint block. The reviewed fingerprint reuses these in-memory bytes.
   if (parser.status === "scanning" && parser.scanOffset < current.size) {
     const oldPartial = envelope.prefix.end - envelope.prefix.start - envelope.prefix.fullBytes;
     const remaining = maxBytes - bytesRead;
     requiredMinimumBytes = oldPartial + 1;
     if (remaining < requiredMinimumBytes) {
       if (!bytesRead) return park("insufficient_budget");
       return result("checkpoint");
     }
     const offset = parser.scanOffset;
     const bytes = readAt(offset, Math.min(remaining - oldPartial, current.size - offset));
     feed(parser, bytes, {deadline:options.deadline});
     const next = extend(envelope.prefix, parser.scanOffset, (position, n) => {
       if (position >= offset && position + n <= offset + bytes.length) return bytes.subarray(position - offset, position - offset + n);
       const old = readAt(position, offset - position); prefixBytesRead += old.length;
       return Buffer.concat([old, bytes.subarray(0, n - old.length)]);
     }, {maxBytes: LIMITS.sliceBytes, deadline:options.deadline});
     if (next.status === "changed") return refuse("prefix_changed");
     if (next.status !== "complete") return result("checkpoint"); // parser proposal discarded
     envelope.parser = checkpoint(parser); envelope.prefix = next.fingerprint;
   }
   if (parser.status === "refused") return refuse(parser.reason ?? "parser_refused");
   if (parser.status !== "ready") {
     if (parser.scanOffset === current.size) return refuse("incomplete_record");
     return result("checkpoint");
   }
   const scalarBytes = parser.slots.reduce((n, slot) => n + (slot && ["string", "number"].includes(slot.kind) ? slot.end-slot.begin : 0), 0);
   if (scalarBytes > LIMITS.projectionBytes) return refuse("allowed_projection_too_large");
   const headBytes = Math.min(512, current.size), continuityBytes = Math.min(512, parser.scanOffset);
   if (maxBytes - bytesRead < scalarBytes + headBytes + continuityBytes) {
     if (!bytesRead) { requiredMinimumBytes = scalarBytes + headBytes + continuityBytes; return park("insufficient_budget"); }
     return result("checkpoint");
   }
   if (performance.now() >= options.deadline) return bytesRead ? result("checkpoint") : park("deadline_exhausted");
   const record = project(parser, readAt);
   const headHash = sha(readAt(0, headBytes)), continuityHash = sha(readAt(parser.scanOffset - continuityBytes, continuityBytes));
   const completed = result("complete", [JSON.stringify(record)], parser.scanOffset);
   completed.headBytes = headBytes; completed.headHash = headHash;
   completed.continuityBytes = continuityBytes; completed.continuityHash = continuityHash;
   return completed;
 } catch (error) {
   if (error instanceof Error && error.message === "read_budget_exhausted") {
     if (!envelope) reason = "deadline_exhausted";
     return result(envelope ? "checkpoint" : "park");
   }
   // Observed invalidation is separate from the frozen legacy cursor. A failed
   // final snapshot check will reject even this proposal; retry sees the fence.
   if (error instanceof Error && "code" in error && error.code === "ERR_ENCODING_INVALID_ENCODED_DATA") return refuse("invalid_utf8");
   return refuse(error instanceof Error && ["malformed_json", "invalid_utf8", "max_depth", "allowed_scalar_too_large", "allowed_number_too_large", "allowed_projection_too_large", "newline_before_end"].includes(error.message) ? error.message : "source_changed");
 }
}

/** Cadence-count backoff only: no timer and no durable-state write on failed
 * admission. O(64) metadata bounds match the existing pending-candidate cap. */
export class ContinuationAdmission {
 private cadence = 0;
 private readonly held = new Map<string, {until:number; attempts:number}>();
 beginCadence() { this.cadence++; }
 allows(key:string) { return (this.held.get(key)?.until ?? 0) <= this.cadence; }
 park(key:string) {
   const attempts = Math.min(3, (this.held.get(key)?.attempts ?? 0) + 1);
   this.held.delete(key);
   this.held.set(key, {until:this.cadence + Math.min(4, 2 ** attempts), attempts});
   if (this.held.size > 64) this.held.delete(this.held.keys().next().value!);
 }
 progressed(key:string) { this.held.delete(key); }
}
function retiredReason(raw:string): string | null {
 try {
   const r=JSON.parse(raw);
   return validSeal(r) && exact(r,["sha256","version","rollbackVersion","retired","reason","bindingDigest","priorCursor"]) && r.version===1 && r.rollbackVersion==="0.7.4" && r.retired===true && ["retired_binding","generation_rewrite_ambiguous"].includes(r.reason) &&
     typeof r.bindingDigest==="string" && /^[a-f0-9]{64}$/.test(r.bindingDigest) && typeof r.priorCursor==="string" && /^[a-f0-9]{64}$/.test(r.priorCursor) ? r.reason : null;
 } catch { return null; }
}
/** Compact invalidated roots in bounded batches. Keep a path-free fence so
 * returning an old configuration cannot silently restart at zero. The legacy
 * cursor is never deleted/refreshed by continuation retirement. */
export function retireJsonlContinuations(db:Database.Database, provider:Provider, roots:CaptureRoot[]|undefined, deadline:number) {
 if (performance.now() >= deadline) return 0;
 const key=`jsonl_continuation_retirement:${provider}`;
 const after=(db.prepare("select value from maintenance_state where key=?").get(key) as {value:string}|undefined)?.value ?? "";
 const rows=db.prepare("select file_key,envelope_json from jsonl_continuations where provider=? and file_key>? order by file_key limit 16").all(provider,after) as Array<{file_key:string;envelope_json:string}>;
 const rootDigests=roots?.map(captureRootDigest);
 const enrollment=digest(optionalRow(db,"collector_workspace_binding","select current_workspace_id,current_device_id,current_installation_epoch_id,current_installation_epoch_started_at from collector_workspace_binding where singleton=1"));
 let last="",visited=0;
 for(const row of rows) {
   if(performance.now()>=deadline) break;
   last=row.file_key;visited++;
   let e:Envelope;try{e=decode(row.envelope_json);}catch{continue;}
   if(e.binding.enrollment===enrollment && (!rootDigests || rootDigests.includes(e.binding.root))) continue;
   db.prepare("update jsonl_continuations set envelope_json=? where provider=? and file_key=? and envelope_json=?").run(
     sealed({version:1,rollbackVersion:"0.7.4",retired:true,reason:"retired_binding",bindingDigest:digest(e.binding),priorCursor:e.priorCursor}),provider,row.file_key,row.envelope_json);
 }
 if(rows.length<16 && visited===rows.length) last="";
 db.prepare("insert into maintenance_state(key,value,updated_at) values (?,?,?) on conflict(key) do update set value=excluded.value,updated_at=excluded.updated_at").run(key,last,new Date().toISOString());
 return visited;
}
