import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { readLines } from './lines.js';
import { addSdk } from './sdk.js';
import { SdkIds } from './sdk-index.js';
import { narrowPacked } from './seen.js';
import { stateBytes } from './state-json.js';
import { claudeUsage } from './claude-usage.js';
import { HOUR, MAX_STATE, string, type State, type SourceFile, type SdkCursor, type SdkSession } from './types.js';

const SLACK = 5 * 60_000, FILES = 1200, BYTES = 512 * 1024 * 1024, SLICE = 1024 * 1024;
export type SdkCommit = { type: 'event_msg'; seq: number; file: string; cursor: SdkCursor; rows: [string, string, SdkSession][]; ids: Record<string, string> };
function apply(state: State, commit: SdkCommit, ids: SdkIds): void {
  if (!Number.isSafeInteger(commit.seq)) throw new Error('invalid_state');
  if (commit.seq <= (state.sdkCommit ?? 0)) return;
  if (commit.seq !== (state.sdkCommit ?? 0) + 1) throw new Error('invalid_state');
  for (const [day, id, row] of commit.rows) {
    const sessions = (state.sdk ??= {})[day] ??= {}, previous = sessions[id];
    if (!previous) { sessions[id] = row; continue; }
    if (row[1] !== 'unknown') previous[1] = row[1];
    if (row[2]) previous[2] = row[2];
    previous[3] = Math.min(previous[3], row[3]); previous[4] = Math.max(previous[4], row[4]); previous[6] = true;
    for (const [key, counts] of Object.entries(row[5])) {
      const bucket = previous[5][key] ??= [0, 0, 0, 0, 0];
      for (let i = 0; i < counts.length; i++) bucket[i] = Math.min(Number.MAX_SAFE_INTEGER, bucket[i] + counts[i]);
    }
  }
  (state.sdkFiles ??= {})[commit.file] = commit.cursor;
  ids.add(commit.ids); state.sdkCommit = commit.seq;
}

/** An append + fsync commits one file slice. Recovery ignores records already
 * included in the atomic state snapshot, and truncates an interrupted tail.
 * The journal contains only cursors, hashes and the existing compact totals. */
export function sdkJournal(state: State, dir: string) {
  const file = path.join(dir, 'sdk-journal.jsonl');
  const fd = fs.openSync(file, 'a+', 0o600), size = fs.fstatSync(fd).size;
  try {
    if (size > 2 * MAX_STATE) throw new Error('state_capacity');
    narrowPacked(state);
    const ids = new SdkIds(state);
    const result = readLines(file, 0, size, r => apply(state, r as SdkCommit, ids));
    ids.finish();
    if (result.offset !== size) fs.ftruncateSync(fd, result.offset);
  } catch (error) { fs.closeSync(fd); throw error; }
  return {
    append(commit: SdkCommit) { fs.writeFileSync(fd, JSON.stringify(commit) + '\n'); fs.fsyncSync(fd); },
    clear() { fs.ftruncateSync(fd, 0); fs.fsyncSync(fd); },
    close() { fs.closeSync(fd); },
  };
}

/** Newest directory activity under a root: a call file is created per call, so
 * a live brain folder reads first and a dead one is told apart cheaply. */
export function activity(root: string): number {
  let newest = 0;
  try {
    newest = fs.statSync(root).mtimeMs;
    for (const name of fs.readdirSync(root)) { try { newest = Math.max(newest, fs.statSync(path.join(root, name)).mtimeMs); } catch { /* gone */ } }
  } catch { /* unreadable roots are refused by enumeration */ }
  return newest;
}

/** One capture owns the global pass budget across every expanded glob root.
 * Only the current file's small session/hour deltas and ID set are transient. */
export class SdkCapture {
  private ids: SdkIds;
  private files = 0; private bytes = 0; private reading = 0;
  private room: number;
  constructor(private state: State, private now: number, private since: number, private until: number,
    private identity: (session: string, cwd: string) => { account: string; project: string | null },
    private journal?: (commit: SdkCommit) => void, private backfill = false) {
    this.ids = new SdkIds(state);
    // Closed days need no file cursor once their root watermark excludes them.
    const closed = Math.floor(now / (24 * HOUR)) * 24 * HOUR - SLACK;
    for (const [root, start] of Object.entries(state.sdkRoots ?? {})) if (start < since) delete state.sdkRoots![root];
    for (const [file, cursor] of Object.entries(state.sdkFiles ?? {})) if (cursor[4] < since || (cursor[4] < closed && Object.entries(state.sdkRoots ?? {}).some(([root, start]) => file.startsWith(root + path.sep) && cursor[4] < start - SLACK))) delete state.sdkFiles![file];
    this.room = MAX_STATE - SLICE - stateBytes(state, MAX_STATE);
  }
  private full(): boolean { return this.room <= 0 || this.files >= FILES || this.bytes >= BYTES || this.reading >= 12_000; }
  root(root: string, files: (since: number) => Iterable<SourceFile>): { files: number; bytes: number } {
    const beforeFiles = this.files, beforeBytes = this.bytes;
    const watermark = this.backfill ? undefined : this.state.sdkRoots?.[root];
    // A watermarked root whose directories are untouched since holds no newer file.
    if (watermark !== undefined && activity(root) < watermark - SLACK) return { files: 0, bytes: 0 };
    const since = watermark === undefined ? this.since : Math.max(this.since, watermark - SLACK);
    let complete = true;
    for (const source of files(since)) {
      // Skipping costs the budget nothing, so a root of thousands of read files
      // still reaches its unread ones; only reading is charged.
      const file = source.file, stat = source.stat;
      if (stat.mtimeMs < since) continue;
      const cursor = this.state.sdkFiles?.[file];
      const offset = !this.backfill && cursor && cursor[0] === stat.dev && cursor[1] === stat.ino && (stat.size > cursor[2] || (stat.size === cursor[2] && stat.mtimeMs === cursor[4])) ? cursor[3] : 0;
      if (offset === stat.size) continue;
      if (this.full()) { complete = false; break; }
      if (stat.size - offset <= SLICE && stat.size - offset > BYTES - this.bytes) { complete = false; break; }
      const local = new Set<string>(), groups: Record<string, string[]> = {}, delta = { sdk: {} } as State, started = performance.now();
      let result: { offset: number; bytes: number };
      try { result = readLines(file, offset, Math.min(stat.size, offset + BYTES - this.bytes), (r, end) => {
        if (r.type === 'assistant' && r.timestamp && r.message?.usage) {
          const time = Date.parse(r.timestamp);
          if (Number.isFinite(time) && time >= this.since && time < this.until) for (const item of claudeUsage(r)) {
            const key = crypto.createHash('sha256').update('claude_code:' + item.id).digest().subarray(0, 8), hex = key.toString('hex');
            if (!local.has(hex) && !this.ids.has(key)) {
              local.add(hex); const hour = String(Math.floor(time / HOUR)); (groups[hour] ??= []).push(hex);
              const session = string(r.sessionId) || 'unknown', tag = this.identity(session, string(r.cwd, 4096));
              addSdk(delta, session, tag.account, tag.project, item.model, time, item.tokens);
            }
          }
        }
        if (end - offset >= SLICE || local.size >= 256) return false;
      }, false, undefined, started + 12_000 - this.reading); } catch (e) { if ((e as NodeJS.ErrnoException).code === 'ENOENT') continue; throw new Error('source_unreadable'); }
      this.reading += performance.now() - started;
      const packed = Object.fromEntries(Object.entries(groups).map(([hour, keys]) => [hour, Buffer.from(keys.sort().join(''), 'hex').toString('base64')]));
      const rows: SdkCommit['rows'] = [];
      for (const [day, sessions] of Object.entries(delta.sdk!)) for (const [id, row] of Object.entries(sessions)) rows.push([day, id, row]);
      const commit: SdkCommit = { type: 'event_msg', seq: (this.state.sdkCommit ?? 0) + 1, file, cursor: [stat.dev, stat.ino, stat.size, result.offset, stat.mtimeMs], rows, ids: packed };
      // The commit encoding overestimates its added state (it includes existing
      // cursor/row fields too). Reserve a slice for root metadata and the save.
      this.files++; this.bytes += result.bytes;
      const growth = stateBytes(commit, MAX_STATE);
      if (growth > this.room) { this.room = 0; complete = false; break; }
      this.room -= growth;
      // Counts and cursor are durable together, before any subsequent file.
      this.journal?.(commit); apply(this.state, commit, this.ids);
      if (result.offset < stat.size) complete = false;
    }
    // A capped root keeps its old watermark: unread files cannot fall behind it.
    if (complete) (this.state.sdkRoots ??= {})[root] = this.now;
    return { files: this.files - beforeFiles, bytes: this.bytes - beforeBytes };
  }
  finish(): void { this.ids.finish(); }
}
