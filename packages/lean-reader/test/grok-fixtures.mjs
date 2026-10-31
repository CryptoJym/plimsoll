import fs from 'node:fs';
import path from 'node:path';

export const since = '2026-10-20T00:00:00Z';
export const until = '2026-10-22T00:00:00Z';
export const sessionId = '00000000-0000-4000-8000-000000000001';
export const options = home => ({ home, env: {}, since, until, now: Date.parse(until), backfill: true });
export const usage = (extra = {}) => ({ inputTokens: 120, cachedReadTokens: 100, cacheCreationTokens: 7, outputTokens: 9, reasoningTokens: 3, modelCalls: 3, ...extra });
export const turn = (id, extra = {}) => ({
  timestamp: Date.parse('2026-10-20T01:02:03Z') / 1000,
  method: '_x.ai/session/update',
  params: { sessionId, update: { sessionUpdate: 'turn_completed', prompt_id: 'synthetic-prompt', usage: usage() }, _meta: { eventId: id } },
  ...extra,
});
export const modelTurn = (id, models, extra = {}) => {
  const record = turn(id, extra);
  record.params.update.usage = usage({ modelUsage: models });
  return record;
};
export function write(home, name, value) {
  const file = path.join(home, name);
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, Array.isArray(value) ? value.map(v => JSON.stringify(v)).join('\n') + '\n' : typeof value === 'string' ? value : JSON.stringify(value));
  return file;
}
export function session(home, records, { root = '.grok', id = sessionId, summary = {} } = {}) {
  const folder = path.join(root, 'sessions', encodeURIComponent('/synthetic/project'), id);
  write(home, path.join(folder, 'summary.json'), { info: { cwd: path.join(home, 'projects/example') }, created_at: since, updated_at: '2026-10-20T03:00:00Z', current_model_id: 'grok-4.7-build', ...summary });
  return write(home, path.join(folder, 'updates.jsonl'), records);
}
export const expectedRow = (extra = {}) => ({ requests: 3, freshInput: 20, cacheRead: 100, cacheWrite: 7, output: 9,
  hour: '2026-10-20T01:00:00.000Z', tool: 'grok', accountKey: 'grok:unknown', model: 'grok-4.7', sessionKey: sessionId, projectHint: null, projectSource: 'none', ...extra });
export const totals = rows => rows.reduce((a, r) => {
  for (const k of ['requests', 'freshInput', 'cacheRead', 'cacheWrite', 'output']) a[k] += r[k];
  return a;
}, { requests: 0, freshInput: 0, cacheRead: 0, cacheWrite: 0, output: 0 });
