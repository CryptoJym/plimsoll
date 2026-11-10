import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { readLines } from './lines.js';
import { Accounts } from './accounts.js';
import { HOUR, fields, type Counts, type Hour, type SdkSession, type Session, type State } from './types.js';

/** Metadata only; neither launch arguments nor transcript content is retained. */
export function sdkLaunches(home: string, since: number): Map<string, { seat: string; project: string | null; at: number }> {
  const result = new Map<string, { seat: string; project: string | null; at: number }>();
  const labels = new Map<string, string>();
  const intern = (value: string) => { if (!labels.has(value)) labels.set(value, value); return labels.get(value)!; };
  const file = path.join(home, '.local/state/fleet-launch/launches-sdk.jsonl');
  try {
    readLines(file, 0, fs.statSync(file).size, r => {
      if (r.kind !== 'sdk' || (r.tool && r.tool !== 'claude_code') || typeof r.session !== 'string' || !/^[A-Za-z0-9_.:-]{1,200}$/.test(r.session)) return;
      const stamp = Date.parse(r.at ?? r.timestamp ?? r.startedAt), at = Number.isFinite(stamp) ? stamp : 0;
      if (at && at < since - 24 * HOUR) return;
      const seat = typeof r.account === 'string' && r.account.length <= 254 ? intern(r.account) : '';
      const project = typeof r.project === 'string' && /^[A-Za-z0-9_.-]{1,160}$/.test(r.project) ? intern(r.project) : null;
      if (at >= (result.get(r.session)?.at ?? -Infinity)) result.set(r.session, { seat, project, at });
      if (result.size > 200_000) throw new Error('state_capacity');
    }, false, ['kind', 'tool', 'session', 'account', 'project', 'at', 'timestamp', 'startedAt']);
  } catch (e) { if ((e as NodeJS.ErrnoException).code !== 'ENOENT') throw e; }
  return result;
}

export function addSdk(state: State, sessionKey: string, account: string, project: string | null, model: string, time: number, tokens: Counts): void {
  const day = new Date(time).toISOString().slice(0, 10), id = crypto.createHash('sha256').update(sessionKey).digest('hex').slice(0, 24);
  const sessions = (state.sdk ??= {})[day] ??= {};
  const row = sessions[id] ??= [sessionKey, account, project, time, time, {}, true];
  if (account !== 'unknown') row[1] = account;
  if (project) row[2] = project;
  row[3] = Math.min(row[3], time); row[4] = Math.max(row[4], time); row[6] = true;
  const key = JSON.stringify([new Date(time).getUTCHours(), model]), bucket = row[5][key] ??= [0, 0, 0, 0, 0];
  for (let i = 0; i < fields.length; i++) bucket[i] = Math.min(Number.MAX_SAFE_INTEGER, bucket[i] + tokens[fields[i]]);
}
export function* sdkHours(day: string, row: SdkSession): Generator<Hour> {
  for (const [key, counts] of Object.entries(row[5])) {
    const [hour, model] = JSON.parse(key);
    yield { hour: new Date(Date.parse(day) + hour * HOUR).toISOString(), tool: 'claude_code', accountKey: row[1], model, sessionKey: row[0],
      projectHint: row[2], projectSource: row[2] ? 'launch_tag' : 'none',
      requests: counts[0], freshInput: counts[1], cacheRead: counts[2], cacheWrite: counts[3], output: counts[4] };
  }
}
export function sdkSession(row: SdkSession, state?: State, id?: string): Session {
  let first = row[3], last = row[4];
  if (state) {
    id ??= crypto.createHash('sha256').update(row[0]).digest('hex').slice(0, 24);
    for (const sessions of Object.values(state.sdk ?? {})) {
      const other = sessions[id];
      if (other) { first = Math.min(first, other[3]); last = Math.max(last, other[4]); }
    }
  }
  return { tool: 'claude_code', sessionKey: row[0], title: null, folderLabel: null, projectHint: row[2], firstSeen: new Date(first).toISOString(), lastSeen: new Date(last).toISOString() };
}
export function* sdkSessions(state: State): Generator<Session> {
  // A session spanning UTC midnight still has one metadata row.
  const previous: Record<string, SdkSession>[] = [];
  for (const sessions of Object.values(state.sdk ?? {})) {
    for (const id in sessions) if (!previous.some(day => Object.hasOwn(day, id))) yield sdkSession(sessions[id], state, id);
    previous.push(sessions);
  }
}
export function refreshSdk(state: State, launches: ReturnType<typeof sdkLaunches>, accounts: Accounts): void {
  for (const [day, sessions] of Object.entries(state.sdk ?? {})) for (const row of Object.values(sessions)) {
    const tag = launches.get(row[0]);
    if (!tag) continue;
    const account = accounts.resolve(tag.seat);
    if (account !== 'unknown' && account !== row[1]) {
      // Replace the previously sent unknown account row, rather than add twice.
      for (const old of sdkHours(day, row)) {
        const id = crypto.createHash('sha256').update(JSON.stringify([old.tool, old.accountKey, old.model, old.sessionKey, Math.floor(Date.parse(old.hour) / HOUR)])).digest('hex').slice(0, 24);
        state.hours[id] = { ...old, requests: 0, freshInput: 0, cacheRead: 0, cacheWrite: 0, output: 0 }; state.pending[id] = true;
      }
      row[1] = account; row[6] = true;
    }
    if (tag.project && tag.project !== row[2]) { row[2] = tag.project; row[6] = true; }
  }
}
