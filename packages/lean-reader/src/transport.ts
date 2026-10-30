import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import type { Config, State, Hour, Session, Seat, Velocity } from './types.js';
import { expand } from './reader.js';
import { branchName, topRepositories } from './evidence.js';

const ROUTE = '/api/work-intelligence/lean/usage';
const MAX_BODY = 1024 * 1024;
// The collector's own config carries its capture roots too: 18.8 MB on one Mac.
const MAX_CREDENTIALS = 64 * 1024 * 1024;
type Body = { schema: string; tenantId: string; reader: { version: string; machineName: string }; sentAt: string; hours: Hour[]; sessions: Session[]; seats: Seat[]; velocity?: Velocity[] };
type Batch = { body: Body; keys: string[]; velocityKeys: string[] };
export type Upload = { url: URL; installKey: string; secret: string; tenantId: string };
export const readerInfo = () => ({ version: '0.1.0', machineName: os.hostname().split('.')[0] });
const wireFields = {
  hours: ['hour', 'tool', 'accountKey', 'model', 'sessionKey', 'projectHint', 'projectSource', 'requests', 'freshInput', 'cacheRead', 'cacheWrite', 'output'],
  sessions: ['tool', 'sessionKey', 'title', 'folderLabel', 'projectHint', 'firstSeen', 'lastSeen'],
  seats: ['tool', 'accountKey', 'plan', 'usedPercent', 'windowMinutes', 'resetsAt', 'creditsBalance', 'observedAt'],
  velocity: ['hour', 'tool', 'accountKey', 'model', 'sessionKey', 'tier', 'effort', 'replies', 'freshInput', 'cacheRead', 'cacheWrite', 'output', 'reasoning', 'timed', 'sOut', 'sDur', 'sOutDur', 'sOut2', 'sDur2', 'hist', 'turns', 'turnMs', 'ttftMs', 'ttftN'],
};

export function makeBatches(state: State, tenantId: string, now: number, config: Config = {}, velocityOnly = false): Batch[] {
  const result: Batch[] = [];
  const includeVelocity = velocityOnly || config.sendVelocity === true;
  const empty = (): Batch => ({ body: { schema: 'plimsoll.lean.usage.v1', tenantId, reader: readerInfo(), sentAt: new Date(now).toISOString(), hours: [], sessions: [], seats: [] }, keys: [], velocityKeys: [] });
  let batch = empty(), bytes = Buffer.byteLength(JSON.stringify(batch.body));
  const append = (kind: 'hours' | 'sessions' | 'seats' | 'velocity', value: any, key?: string) => {
    const original = value;
    value = Object.fromEntries(wireFields[kind].map(field => [field, original[field]]));
    if (kind === 'sessions' && config.sendEvidence === true) {
      value.gitBranch = branchName(original.gitBranch);
      value.reposTouched = topRepositories(Array.isArray(original.reposTouched) ? original.reposTouched : []);
    }
    if (kind === 'sessions') for (const field of ['parentSessionKey', 'entrypoint', 'actions'] as const) if (Object.hasOwn(original, field)) value[field] = original[field];
    if (kind === 'velocity' && !batch.body.velocity) batch.body.velocity = [];
    const n = Buffer.byteLength(JSON.stringify(value)) + 1, limit = kind === 'hours' || kind === 'velocity' ? 2000 : kind === 'sessions' ? 500 : 50;
    const current = kind === 'velocity' ? (batch.body.velocity ?? []) : batch.body[kind];
    if (current.length >= limit || bytes + n > MAX_BODY) { result.push(batch); batch = empty(); bytes = Buffer.byteLength(JSON.stringify(batch.body)); if (kind === 'velocity') batch.body.velocity = []; }
    if (bytes + n > MAX_BODY) throw new Error('row_too_large');
    batch.body[kind]!.push(value); if (key && kind === 'velocity') batch.velocityKeys.push(key); else if (key) batch.keys.push(key); bytes += n;
  };
  if (!velocityOnly) for (const key of Object.keys(state.pending)) if (state.hours[key]) append('hours', state.hours[key], key);
  if (!velocityOnly && state.metadataPending) {
    for (const session of Object.values(state.sessions)) append('sessions', session);
    for (const seat of Object.values(state.seats)) append('seats', seat);
  }
  if (includeVelocity) for (const key of Object.keys(state.velocityPending ?? {})) if (state.velocity[key]) append('velocity', state.velocity[key], key);
  if (batch.body.hours.length || batch.body.sessions.length || batch.body.seats.length || batch.body.velocity?.length) result.push(batch);
  return result;
}

function endpoint(uploadUrl: string, configured?: string): URL {
  let original: URL, target: URL;
  try { original = new URL(uploadUrl); target = configured ? new URL(configured) : new URL(ROUTE, original.origin); } catch { throw new Error('invalid_endpoint'); }
  if (target.username || target.password || target.search || target.hash || original.username || original.password || target.origin !== original.origin) throw new Error('endpoint_origin_mismatch');
  if (target.protocol !== 'https:' && !(target.protocol === 'http:' && ['127.0.0.1', 'localhost', '[::1]'].includes(target.hostname))) throw new Error('insecure_endpoint');
  // An origin override is accepted; the v1 route is always fixed.
  target.pathname = ROUTE; return target;
}

export function post(url: URL, body: string, key: string, secret: string, deadline: number): Promise<void> {
  const timestamp = new Date().toISOString();
  const transport = process.getBuiltinModule(url.protocol === 'https:' ? 'node:https' : 'node:http') as typeof import('node:http');
  return new Promise((resolve, reject) => {
    const request = transport.request(url, { method: 'POST', agent: false, headers: {
      'content-type': 'application/json', 'content-length': Buffer.byteLength(body),
      'x-plimsoll-install-key': key, 'x-plimsoll-upload-timestamp': timestamp,
      'x-plimsoll-upload-signature': 'sha256=' + crypto.createHmac('sha256', secret).update(`${timestamp}.${body}`).digest('hex'),
    } }, response => {
      // node:http never follows redirects. Bound response buffering as well.
      const chunks: Buffer[] = []; let size = 0;
      response.on('data', (chunk: Buffer) => { size += chunk.length; if (size > 64 * 1024) { response.destroy(); request.destroy(new Error('response_too_large')); } else chunks.push(chunk); });
      response.on('error', () => reject(new Error('upload_network_error')));
      response.on('end', () => {
        if (response.statusCode !== 200) { reject(new Error(`upload_http_${response.statusCode ?? 0}`)); return; }
        try { if (JSON.parse(Buffer.concat(chunks).toString()).ok !== true) throw new Error(); resolve(); } catch { reject(new Error('invalid_upload_response')); }
      });
    });
    const timer = setTimeout(() => request.destroy(new Error('upload_deadline')), Math.max(1, deadline - Date.now()));
    request.once('close', () => clearTimeout(timer));
    request.once('error', () => reject(new Error('upload_network_error')));
    request.end(body);
  });
}

/** The collector's upload credentials. Read only inside a send path: dry runs,
 * print-config, status and parser measurements never open this file. */
export function openUpload(config: Config, home: string): Upload {
  const file = expand(config.credentialsFrom ?? path.join(home, 'Library/Application Support/Plimsoll/collector.config.json'), home);
  if (fs.statSync(file).size > MAX_CREDENTIALS) throw new Error('invalid_credentials');
  const credentials = JSON.parse(fs.readFileSync(file, 'utf8'));
  if (!['uploadUrl', 'installKey', 'uploadSigningSecret', 'tenantId'].every(k => typeof credentials[k] === 'string' && credentials[k])) throw new Error('invalid_credentials');
  return { url: endpoint(credentials.uploadUrl, config.endpoint), installKey: credentials.installKey, secret: credentials.uploadSigningSecret, tenantId: credentials.tenantId };
}

/** A send failure as one of a fixed set of codes. Never log filesystem paths,
 * provider data, credentials or response text. */
export function uploadError(error: unknown): string {
  const code = error instanceof Error ? error.message : '';
  return /^(upload_http_\d+|upload_network_error|upload_deadline|invalid_upload_response|invalid_endpoint|endpoint_origin_mismatch|insecure_endpoint|row_too_large|invalid_credentials)$/.test(code) ? code : 'credentials_unavailable';
}

export async function sendPending(state: State, config: Config, options: { home: string; now: number; velocityOnly?: boolean }): Promise<number> {
  const includeVelocity = options.velocityOnly === true || config.sendVelocity === true;
  const hasPending = options.velocityOnly ? Object.keys(state.velocityPending ?? {}).length > 0 : Object.keys(state.pending).length > 0 || state.metadataPending || (includeVelocity && Object.keys(state.velocityPending ?? {}).length > 0);
  if (!hasPending || options.now < state.nextSend) return 0;
  let sent = 0;
  try {
    const upload = openUpload(config, options.home), deadline = Date.now() + 30_000;
    for (const batch of makeBatches(state, upload.tenantId, options.now, config, options.velocityOnly === true)) {
      if (Date.now() >= deadline) throw new Error('upload_deadline');
      await post(upload.url, JSON.stringify(batch.body), upload.installKey, upload.secret, deadline);
      for (const key of batch.keys) delete state.pending[key];
      for (const key of batch.velocityKeys) delete state.velocityPending[key];
      sent += batch.body.hours.length + (batch.body.velocity?.length ?? 0);
      state.lastSend = new Date(options.now).toISOString();
    }
    if (!options.velocityOnly) state.metadataPending = false;
    state.lastError = null; state.failures = 0; state.nextSend = 0;
  } catch (error) {
    state.lastError = uploadError(error);
    state.failures = Math.min(state.failures + 1, 10);
    state.nextSend = options.now + Math.min(3_600_000, 60_000 * 2 ** (state.failures - 1));
  }
  return sent;
}
