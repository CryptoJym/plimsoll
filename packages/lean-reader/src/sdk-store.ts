import fs from 'node:fs';
import path from 'node:path';
import type { SdkSession, State } from './types.js';
import { narrowPacked } from './seen.js';

// The JSON checkpoint is a small, readable projection. SQLite atomically owns
// the checkpoint and the retained SDK baseline, including pending flags. The
// existing slice journal still covers a crash before the first checkpoint.
const STORE = Symbol('sdk-store');
type Stored = State & { [STORE]?: SdkStore };
const metadata = ['sdkFiles', 'sdkRoots', 'seenPacked'] as const;
const databasePath = (file: string) => file + '.sdk.sqlite';
function database(file: string, readOnly = false): any {
  const { DatabaseSync } = process.getBuiltinModule('node:sqlite') as any;
  return new DatabaseSync(databasePath(file), { readOnly });
}

export class SdkStore {
  private db: any;
  constructor(readonly file: string) {
    fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
    this.open();
  }
  private open(): any {
    if (!this.db) {
      this.db = database(this.file);
      fs.chmodSync(databasePath(this.file), 0o600);
      this.db.exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=5000;
        CREATE TABLE IF NOT EXISTS checkpoint (id INTEGER PRIMARY KEY CHECK(id=1), value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS sdk (day TEXT NOT NULL, id TEXT NOT NULL, value TEXT NOT NULL,
          pending INTEGER NOT NULL, first REAL NOT NULL, last REAL NOT NULL, PRIMARY KEY(day,id));
        CREATE INDEX IF NOT EXISTS sdk_id ON sdk(id);
        BEGIN;`);
    }
    return this.db;
  }
  get(day: string, id: string): SdkSession | undefined {
    const row = this.open().prepare('SELECT value FROM sdk WHERE day=? AND id=?').get(day, id);
    return row ? JSON.parse(row.value) : undefined;
  }
  put(day: string, id: string, row: SdkSession): void {
    this.open().prepare(`INSERT INTO sdk VALUES(?,?,?,?,?,?) ON CONFLICT(day,id) DO UPDATE SET
      value=excluded.value,pending=excluded.pending,first=excluded.first,last=excluded.last`)
      .run(day, id, JSON.stringify(row), Number(row[6]), row[3], row[4]);
  }
  *rows(pending = false): Generator<[string, string, SdkSession]> {
    // Page by stable primary key so callers may update rows during traversal.
    let day = '', id = '';
    while (true) {
      const page = this.open().prepare(`SELECT day,id,value FROM sdk WHERE (day,id)>(?,?)
        ${pending ? 'AND pending=1' : ''} ORDER BY day,id LIMIT 256`).all(day, id);
      if (!page.length) return;
      for (const row of page) { day = row.day; id = row.id; yield [day, id, JSON.parse(row.value)]; }
    }
  }
  span(id: string): { first: number; last: number } | undefined {
    const row = this.open().prepare('SELECT min(first) AS first,max(last) AS last FROM sdk WHERE id=?').get(id);
    return row.first === null ? undefined : row;
  }
  prune(cutoff: number): void {
    const day = new Date(cutoff - 86400000).toISOString().slice(0, 10);
    this.open().prepare('DELETE FROM sdk WHERE day<=? AND day NOT IN (SELECT day FROM sdk WHERE pending=1)').run(day);
  }
  reset(): void { this.open().exec('DELETE FROM sdk'); }
  checkpoint(state: State): void {
    // Metadata remains compact packed identities/cursors, outside the capped
    // JSON projection. It commits in the SAME transaction as usage and flags.
    const value = { ...state, ...Object.fromEntries(metadata.map(k => [k, state[k]])) };
    this.open().prepare('INSERT INTO checkpoint VALUES(1,?) ON CONFLICT(id) DO UPDATE SET value=excluded.value')
      .run(JSON.stringify(value));
    this.db.exec('COMMIT; BEGIN;');
  }
  close(): void {
    if (this.db) { this.db.exec('ROLLBACK'); this.db.close(); this.db = undefined; }
  }
}
export const sdkStore = (state: State): SdkStore | undefined => (state as Stored)[STORE];
export function attachSdkStore(state: State, file: string): SdkStore {
  const existing = sdkStore(state);
  if (existing) return existing;
  const store = new SdkStore(file);
  try {
    narrowPacked(state);
    if (state.sdk) {
      store.reset();
      for (const [day, sessions] of Object.entries(state.sdk)) for (const [id, row] of Object.entries(sessions)) store.put(day, id, row);
      delete state.sdk;
    }
    Object.defineProperty(state, STORE, { value: store });
    state.version = 4;
    hideSdkMetadata(state);
    return store;
  } catch (error) { store.close(); throw error; }
}
export function hideSdkMetadata(state: State): void {
  for (const key of metadata) if (Object.hasOwn(state, key))
    Object.defineProperty(state, key, { value: state[key], enumerable: false, writable: true, configurable: true });
}
export function storedCheckpoint(file: string): State | undefined {
  if (!fs.existsSync(databasePath(file))) return undefined;
  const db = database(file, true);
  try {
    const table = db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='checkpoint'").get();
    const row = table && db.prepare('SELECT value FROM checkpoint WHERE id=1').get();
    return row ? JSON.parse(row.value) : undefined;
  } finally { db.close(); }
}
export function* sdkRows(state: State, pending = false): Generator<[string, string, SdkSession]> {
  const store = sdkStore(state);
  if (store) yield* store.rows(pending);
  else for (const [day, sessions] of Object.entries(state.sdk ?? {})) for (const [id, row] of Object.entries(sessions))
    if (!pending || row[6]) yield [day, id, row];
}
export const sdkGet = (state: State, day: string, id: string): SdkSession | undefined =>
  sdkStore(state)?.get(day, id) ?? state.sdk?.[day]?.[id];
export function sdkPut(state: State, day: string, id: string, row: SdkSession): void {
  const store = sdkStore(state);
  if (store) store.put(day, id, row); else ((state.sdk ??= {})[day] ??= {})[id] = row;
}
export function pruneSdk(state: State, cutoff: number): void {
  const store = sdkStore(state);
  if (store) store.prune(cutoff);
  else for (const [day, sessions] of Object.entries(state.sdk ?? {}))
    if (Date.parse(day) + 86400000 <= cutoff && Object.values(sessions).every(row => !row[6])) delete state.sdk![day];
}
