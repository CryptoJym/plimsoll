import fs from 'node:fs';
import path from 'node:path';
import { readJsonFields } from './lines.js';
import { protectedPath } from './evidence.js';
import { integer, string, type Counts } from './types.js';

const usageFields = ['inputTokens', 'outputTokens', 'cachedReadTokens', 'cacheCreationTokens', 'modelCalls'];
export const grokFields = [
  'method', 'timestamp', 'params.sessionId', 'params._meta.eventId', 'params.update.sessionUpdate',
  'params.update.usage.modelUsage',
  ...usageFields.flatMap(k => [`params.update.usage.${k}`, `params.update.usage.modelUsage.*.${k}`]),
];

function metadata(file: string, home: string, fields: readonly string[]) {
  if (protectedPath(file, home)) return null;
  try { return readJsonFields(file, fields); } catch { return null; }
}

// Only user_id values reach the caller. Login secrets are skipped by the
// streaming projection, never parsed into a credential object or persisted.
export function grokUser(root: string, home: string): string | null {
  const auth = metadata(path.join(path.dirname(root), 'auth.json'), home, ['*.user_id']);
  const ids = new Set(Object.entries(auth ?? {}).filter(([issuer]) => issuer.startsWith('https://auth.x.ai::'))
    .map(([, entry]) => entry?.user_id).filter((id): id is string => typeof id === 'string' && id.length > 0));
  return ids.size === 1 ? [...ids][0] : null;
}

export type GrokSummary = { cwd: string; model: string; firstSeen: string | null; lastSeen: string | null };
function iso(value: unknown): string | null {
  const time = typeof value === 'number' ? value * 1000 : typeof value === 'string' ? Date.parse(value) : NaN;
  return Number.isFinite(time) && Math.abs(time) < 8.64e15 ? new Date(time).toISOString() : null;
}
export function grokSummary(file: string, home: string): GrokSummary {
  const summary = metadata(path.join(path.dirname(file), 'summary.json'), home, ['info.cwd', 'current_model_id', 'created_at', 'updated_at']);
  return {
    cwd: string(summary?.info?.cwd, 4096), model: string(summary?.current_model_id) || 'unknown',
    firstSeen: iso(summary?.created_at), lastSeen: iso(summary?.updated_at),
  };
}

export function grokFile(file: string, home: string): boolean {
  if (path.basename(file) !== 'updates.jsonl' || protectedPath(file, home)) return false;
  try { return path.basename(fs.realpathSync(file)) === 'updates.jsonl'; } catch { return false; }
}

export function grokTurn(record: Record<string, any>): { id: string; session: string; time: string; models: { model: string; tokens: Counts }[] } | null {
  const p = record.params, usage = p?.update?.usage;
  // Grok Build writes the ACP method with its vendor prefix ("_x.ai/session/update"); accept it with or without one.
  if (typeof record.method !== 'string' || !/^(?:[\w.-]+\/)?session\/update$/.test(record.method) ||
      p?.update?.sessionUpdate !== 'turn_completed' || !usage || typeof usage !== 'object') return null;
  const id = string(p?._meta?.eventId), time = typeof record.timestamp === 'number' ? iso(record.timestamp) : null;
  if (!id || !time) return null;
  const modelUsage = usage.modelUsage;
  if (modelUsage !== undefined && (!modelUsage || typeof modelUsage !== 'object' || Array.isArray(modelUsage))) return null;
  const models = Object.entries(modelUsage ?? { '': usage }).flatMap(([model, value]) => {
    if (!value || typeof value !== 'object' || Array.isArray(value)) return [];
    const u = value as Record<string, unknown>, cached = integer(u.cachedReadTokens);
    return [{ model: string(model).replace(/-build$/, ''), tokens: {
      requests: Object.hasOwn(u, 'modelCalls') ? integer(u.modelCalls) : 1,
      freshInput: Math.max(0, integer(u.inputTokens) - cached), cacheRead: cached,
      cacheWrite: integer(u.cacheCreationTokens), output: integer(u.outputTokens),
    } }];
  });
  return models.length ? { id, session: string(p.sessionId), time, models } : null;
}
