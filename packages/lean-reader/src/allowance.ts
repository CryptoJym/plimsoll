import fs from 'node:fs';
import path from 'node:path';
import { accountKey, directories, expand } from './reader.js';
import { saveJson } from './state-json.js';
import { openUpload, post, readerInfo, uploadError } from './transport.js';
import { HOUR, type AccountStatusRow, type AllowanceRow, type Config, type Outbox, type StatusOutbox } from './types.js';

// The allowance step reads one file: the native usage meter's snapshot of
// every account's allowance windows (on Studio0, rewritten each minute and
// named by config.allowanceFile). A machine without that setting skips it.
// Only hashed account keys leave this module: never an email, provider ID or
// path. Each keyed account also yields one status row: whether it is signed
// in, its banked resets (count, expiry, type and status only) and its credit
// balance; reset and credit IDs, titles and descriptions are dropped here.
const MAX_NATIVE = 16 * 1024 * 1024, MAX_OUTBOX = 3 * 1024 * 1024, MAX_PENDING = 3000, BATCH = 500;
const MAX_STATUS_PENDING = 1000, STATUS_BATCH = 200, MAX_RESETS = 12;
const LAST_INSTANT = Date.UTC(9999, 11, 31, 23, 59, 59, 999);
export type NativeAccount = { tool: AllowanceRow['tool']; accountKey: string | null; state: string | null; rows: AllowanceRow[]; status: AccountStatusRow | null };

const label = (value: unknown, max: number) => typeof value === 'string' && value && !/[\u0000-\u001f\u007f]/.test(value) ? value.slice(0, max) : null;
const percent = (value: unknown) => typeof value === 'number' && Number.isFinite(value) && value >= 0 && value <= 1000 ? value : null;
const instant = (value: unknown) => {
  const time = typeof value === 'string' ? Date.parse(value) : NaN;
  return Number.isFinite(time) && time >= 0 && time <= LAST_INSTANT ? new Date(time).toISOString() : null;
};
const token = (value: unknown) => typeof value === 'string' && /^[A-Za-z][A-Za-z0-9_]{0,39}$/.test(value) ? value : null;
const flag = (value: unknown) => typeof value === 'boolean' ? value : null;
const decimal = (value: unknown) => {
  const text = typeof value === 'number' && Number.isFinite(value) && Math.abs(value) < 1e15 ? String(Math.round(value * 1e6) / 1e6)
    : typeof value === 'string' ? value.trim() : '';
  return /^-?\d{1,24}(?:\.\d{1,12})?$/.test(text) ? text : null;
};

/** Banked resets as counts, expiry instants, types and statuses. The native
 * file carries them as resetCredits or entitlements.rateLimitResetCredits;
 * each credit's ID, title and description stay on this Mac. */
function resetsOf(account: any): AccountStatusRow['resets'] {
  const source = account?.resetCredits ?? account?.entitlements?.rateLimitResetCredits;
  if (!source || typeof source !== 'object') return null;
  const listed = Array.isArray(source.credits) ? source.credits : [];
  const available = Number.isSafeInteger(source.availableCount) && source.availableCount >= 0 && source.availableCount <= 10_000 ? source.availableCount : null;
  return { available, credits: listed.slice(0, MAX_RESETS).map((credit: any) => ({
    expiresAt: instant(credit?.expiresAt), resetType: token(credit?.resetType), status: token(credit?.status) })) };
}
/** The account's credit balance: the first quota bucket that reports one. */
function creditsOf(account: any): AccountStatusRow['credits'] {
  for (const bucket of Array.isArray(account?.quotaBuckets) ? account.quotaBuckets : []) {
    const credits = bucket?.credits;
    if (!credits || typeof credits !== 'object') continue;
    const row = { balance: decimal(credits.balance), hasCredits: flag(credits.hasCredits), unlimited: flag(credits.unlimited) };
    if (row.balance !== null || row.hasCredits !== null || row.unlimited !== null) return row;
  }
  return null;
}
/** Signed in: the provider check's own answer, else the identity's flag. */
function signedInOf(account: any): { signedIn: boolean | null; authState: string | null } {
  const diagnostics = account?.source?.diagnostics ?? {};
  return { signedIn: flag(diagnostics.authLoggedIn) ?? flag(account?.identity?.authenticated), authState: token(diagnostics.authState) };
}
/** When the snapshot was taken: the file's own time, else the account's newest reading. */
function snapshotAt(data: any, account: any): string | null {
  const own = instant(data?.generatedAt);
  if (own) return own;
  const times = [account?.identity?.observedAt, account?.resetCredits?.observedAt,
    ...(Array.isArray(account?.quotaBuckets) ? account.quotaBuckets.flatMap((bucket: any) => [bucket?.observedAt,
      ...(Array.isArray(bucket?.windows) ? bucket.windows.map((window: any) => window?.observedAt) : [])]) : [])]
    .map(instant).filter((value): value is string => value !== null).sort();
  return times.at(-1) ?? null;
}

/** Lowercased email to account key, from the Claude profiles the reader
 * already reads for usage. An email two profiles disagree on is dropped. */
function claudeKeys(home: string, env: Record<string, string | undefined>): Map<string, string> {
  const files = [path.join(home, '.claude.json'),
    ...['.claude-seats', '.claude-accounts'].flatMap(parent => directories(path.join(home, parent)).map(n => path.join(home, parent, n, '.claude.json'))),
    ...(env.CLAUDE_CONFIG_DIR ? [path.join(env.CLAUDE_CONFIG_DIR, '.claude.json')] : [])];
  const keys = new Map<string, string>(), conflicts = new Set<string>();
  for (const file of files) {
    try {
      if (fs.statSync(file).size > 10 * 1024 * 1024) continue;
      const account = JSON.parse(fs.readFileSync(file, 'utf8'))?.oauthAccount;
      const email = typeof account?.emailAddress === 'string' ? account.emailAddress.trim().toLowerCase() : '', key = accountKey(account?.accountUuid);
      if (!email || key === 'unknown') continue;
      if (keys.has(email) && keys.get(email) !== key) conflicts.add(email); else keys.set(email, key);
    } catch { /* A missing or unreadable profile names no account. */ }
  }
  for (const email of conflicts) keys.delete(email);
  return keys;
}

/** The Claude and Codex accounts in the native file, each with one row per
 * allowance window, or null when the file is absent. A Codex account is keyed
 * by its provider account ID and a Claude account by the profile its email
 * matches, the same keys usage hours and seats carry; an account with no key
 * is listed without rows. */
export function nativeAccounts(file: string, home: string, env: Record<string, string | undefined>): NativeAccount[] | null {
  let data: any;
  try {
    if (fs.statSync(file).size > MAX_NATIVE) throw new Error('allowance_unreadable');
    data = JSON.parse(fs.readFileSync(file, 'utf8'));
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return null;
    throw new Error('allowance_unreadable');
  }
  if (!Array.isArray(data?.accounts)) throw new Error('allowance_unreadable');
  let claude: Map<string, string> | undefined;
  const result: NativeAccount[] = [];
  for (const account of data.accounts) {
    const tool = account?.provider === 'claude' ? 'claude_code' : account?.provider === 'codex' ? 'codex' : null;
    if (!tool) continue;
    const identity = account.identity ?? {};
    claude ??= tool === 'claude_code' ? claudeKeys(home, env) : undefined;
    const key = tool === 'codex' ? accountKey(identity.providerAccountId)
      : typeof identity.email === 'string' ? claude?.get(identity.email.trim().toLowerCase()) ?? 'unknown' : 'unknown';
    const state = typeof account.state === 'string' && /^[A-Za-z_]{1,40}$/.test(account.state) ? account.state : null;
    const rows: AllowanceRow[] = [];
    if (key !== 'unknown') for (const bucket of Array.isArray(account.quotaBuckets) ? account.quotaBuckets : []) {
      for (const window of Array.isArray(bucket?.windows) ? bucket.windows : []) {
        const name = label(bucket.limitId, 120), slot = label(window?.slot, 80), observedAt = instant(window?.observedAt ?? bucket.observedAt);
        if (!name || !slot || !observedAt) continue;
        const seconds = window.windowSeconds;
        rows.push({ tool, accountKey: key, bucket: name, slot, plan: label(bucket.planType ?? identity.planType, 80), modelScope: label(bucket.modelScope, 160),
          windowSeconds: Number.isSafeInteger(seconds) && seconds > 0 && seconds <= 31_622_400 ? seconds : null,
          usedPercent: percent(window.usedPercent), remainingPercent: percent(window.remainingPercent), resetsAt: instant(window.resetAt),
          resetPrecision: typeof window.resetPrecision === 'string' && /^[a-z_]{1,24}$/.test(window.resetPrecision) ? window.resetPrecision : null,
          exhausted: typeof window.exhausted === 'boolean' ? window.exhausted : bucket.exhausted === true, accountState: state, observedAt });
      }
    }
    const observedAt = key === 'unknown' ? null : snapshotAt(data, account);
    const status: AccountStatusRow | null = key === 'unknown' || !observedAt ? null
      : { tool, accountKey: key, observedAt, accountState: state, ...signedInOf(account), resets: resetsOf(account), credits: creditsOf(account) };
    result.push({ tool, accountKey: key === 'unknown' ? null : key, state, rows, status });
  }
  return result;
}

const emptyStatus = (): StatusOutbox => ({ pending: {}, seen: {}, lastSend: null, lastError: null, failures: 0, nextSend: 0 });
const emptyOutbox = (): Outbox => ({ version: 1, pending: {}, seen: {}, lastSend: null, lastError: null, readError: null, failures: 0, nextSend: 0, status: emptyStatus() });
export const outboxFile = (stateDir: string) => path.join(stateDir, 'allowance.json');
export function readOutbox(file: string): Outbox {
  try {
    if (fs.statSync(file).size > MAX_OUTBOX) throw new Error('state_capacity');
    const data = JSON.parse(fs.readFileSync(file, 'utf8'));
    if (data?.version === 1 && data.pending && typeof data.pending === 'object' && data.seen && typeof data.seen === 'object') {
      // An outbox saved before account status started its own queue empty.
      const status = data.status?.pending && typeof data.status.pending === 'object' && data.status.seen && typeof data.status.seen === 'object'
        ? { ...emptyStatus(), ...data.status } : emptyStatus();
      return { ...emptyOutbox(), ...data, status };
    }
  } catch { /* A missing or damaged outbox starts empty; it holds no usage. */ }
  return emptyOutbox();
}

/** Queues each window's newest reading, at most one per window per UTC hour.
 * A reading no newer than the last one queued for its window is skipped, so a
 * stale account is sent once, not every pass. */
export function queueAllowance(outbox: Outbox, accounts: NativeAccount[], now: number): void {
  const present = new Set<string>();
  for (const row of accounts.flatMap(account => account.rows)) {
    const observed = Date.parse(row.observedAt), series = JSON.stringify([row.tool, row.accountKey, row.bucket, row.slot]);
    if (observed > now + HOUR) continue;
    present.add(series);
    if (outbox.seen[series] && Date.parse(outbox.seen[series]) >= observed) continue;
    outbox.seen[series] = row.observedAt;
    const key = JSON.stringify([row.tool, row.accountKey, row.bucket, row.slot, Math.floor(observed / HOUR)]);
    if (!outbox.pending[key] || Date.parse(outbox.pending[key].observedAt) < observed) outbox.pending[key] = row;
  }
  for (const series of Object.keys(outbox.seen)) if (!present.has(series)) delete outbox.seen[series];
  const keys = Object.keys(outbox.pending);
  // A long outage keeps the newest readings: about 30 hours of every window.
  if (keys.length > MAX_PENDING) {
    keys.sort((a, b) => Date.parse(outbox.pending[a].observedAt) - Date.parse(outbox.pending[b].observedAt));
    for (const key of keys.slice(0, keys.length - MAX_PENDING)) delete outbox.pending[key];
  }
  queueStatus(outbox.status, accounts, now);
}

/** Queues each account's newest status, at most one per account per UTC
 * hour, so the cloud sees sign-in state hour by hour. */
function queueStatus(queue: StatusOutbox, accounts: NativeAccount[], now: number): void {
  const present = new Set<string>();
  for (const row of accounts.flatMap(account => account.status ? [account.status] : [])) {
    const observed = Date.parse(row.observedAt), series = JSON.stringify([row.tool, row.accountKey]);
    if (observed > now + HOUR) continue;
    present.add(series);
    if (queue.seen[series] && Date.parse(queue.seen[series]) >= observed) continue;
    queue.seen[series] = row.observedAt;
    const key = JSON.stringify([row.tool, row.accountKey, Math.floor(observed / HOUR)]);
    if (!queue.pending[key] || Date.parse(queue.pending[key].observedAt) < observed) queue.pending[key] = row;
  }
  for (const series of Object.keys(queue.seen)) if (!present.has(series)) delete queue.seen[series];
  const keys = Object.keys(queue.pending);
  // A long outage keeps the newest hours: about 30 hours of 33 accounts.
  if (keys.length > MAX_STATUS_PENDING) {
    keys.sort((a, b) => Date.parse(queue.pending[a].observedAt) - Date.parse(queue.pending[b].observedAt));
    for (const key of keys.slice(0, keys.length - MAX_STATUS_PENDING)) delete queue.pending[key];
  }
}

/** Sends the outbox in its own uploads, oldest readings first. A cloud that
 * predates allowance refuses these uploads; usage uploads never wait on them. */
export async function sendAllowance(outbox: Outbox, config: Config, home: string, now: number): Promise<number> {
  const keys = Object.keys(outbox.pending).sort((a, b) => Date.parse(outbox.pending[a].observedAt) - Date.parse(outbox.pending[b].observedAt));
  if (!keys.length || now < outbox.nextSend) return 0;
  let sent = 0;
  try {
    const upload = openUpload(config, home), deadline = Date.now() + 30_000;
    for (let at = 0; at < keys.length; at += BATCH) {
      if (Date.now() >= deadline) throw new Error('upload_deadline');
      const batch = keys.slice(at, at + BATCH);
      await post(upload.url, JSON.stringify({ schema: 'plimsoll.lean.usage.v1', tenantId: upload.tenantId, reader: readerInfo(), sentAt: new Date(now).toISOString(),
        hours: [], sessions: [], seats: [], allowance: batch.map(key => outbox.pending[key]) }), upload.installKey, upload.secret, deadline);
      for (const key of batch) delete outbox.pending[key];
      sent += batch.length; outbox.lastSend = new Date(now).toISOString();
    }
    outbox.lastError = null; outbox.failures = 0; outbox.nextSend = 0;
  } catch (error) {
    outbox.lastError = uploadError(error);
    outbox.failures = Math.min(outbox.failures + 1, 10);
    outbox.nextSend = now + Math.min(3_600_000, 60_000 * 2 ** (outbox.failures - 1));
  }
  return sent;
}

/** Sends queued account status in uploads of its own, after the readings. A
 * cloud that predates account status refuses them; the readings never wait,
 * and the status rows keep their own retry and backoff. */
export async function sendStatus(queue: StatusOutbox, config: Config, home: string, now: number): Promise<number> {
  const keys = Object.keys(queue.pending).sort((a, b) => Date.parse(queue.pending[a].observedAt) - Date.parse(queue.pending[b].observedAt));
  if (!keys.length || now < queue.nextSend) return 0;
  let sent = 0;
  try {
    const upload = openUpload(config, home), deadline = Date.now() + 30_000;
    for (let at = 0; at < keys.length; at += STATUS_BATCH) {
      if (Date.now() >= deadline) throw new Error('upload_deadline');
      const batch = keys.slice(at, at + STATUS_BATCH);
      await post(upload.url, JSON.stringify({ schema: 'plimsoll.lean.usage.v1', tenantId: upload.tenantId, reader: readerInfo(), sentAt: new Date(now).toISOString(),
        hours: [], sessions: [], seats: [], allowanceAccounts: batch.map(key => queue.pending[key]) }), upload.installKey, upload.secret, deadline);
      for (const key of batch) delete queue.pending[key];
      sent += batch.length; queue.lastSend = new Date(now).toISOString();
    }
    queue.lastError = null; queue.failures = 0; queue.nextSend = 0;
  } catch (error) {
    queue.lastError = uploadError(error);
    queue.failures = Math.min(queue.failures + 1, 10);
    queue.nextSend = now + Math.min(3_600_000, 60_000 * 2 ** (queue.failures - 1));
  }
  return sent;
}

/** One allowance step: queue the native file's new readings and account
 * status, save the outbox, send both unless this is a dry run, and save again.
 * Its outcome lives in allowance.json, never in the usage state. */
export async function allowancePass(options: { home: string; env: Record<string, string | undefined>; config: Config; stateDir: string; now: number; dryRun: boolean }): Promise<number> {
  if (!options.config.allowanceFile) return 0;
  const file = outboxFile(options.stateDir), outbox = readOutbox(file);
  try {
    const accounts = nativeAccounts(expand(options.config.allowanceFile, options.home), options.home, options.env);
    if (accounts) queueAllowance(outbox, accounts, options.now);
    outbox.readError = accounts ? null : 'allowance_missing';
  } catch { outbox.readError = 'allowance_unreadable'; }
  saveJson(file, outbox, MAX_OUTBOX);
  if (options.dryRun) return 0;
  const sent = await sendAllowance(outbox, options.config, options.home, options.now);
  const status = await sendStatus(outbox.status, options.config, options.home, options.now);
  saveJson(file, outbox, MAX_OUTBOX);
  return sent + status;
}
