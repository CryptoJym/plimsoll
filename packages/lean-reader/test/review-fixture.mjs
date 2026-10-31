import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

export const reader = await import(new URL('../dist/plimsoll-lean.mjs', import.meta.url));
export const window = { since: '2026-10-20T00:00:00Z', until: '2026-10-21T00:00:00Z', now: Date.parse('2026-10-21T00:00:00Z'), env: {}, config: { sendVelocity: true } };
export function fixture(t) {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'lean-review-'));
  t.after(() => fs.rmSync(home, { recursive: true, force: true }));
  return home;
}
export function write(home, relative, rows) {
  const file = path.join(home, relative);
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, rows.map(row => JSON.stringify(row)).join('\n') + '\n');
  return file;
}
export function restart(home, state) {
  const file = path.join(home, 'saved-state.json');
  reader.saveState(file, state);
  return reader.readState(file);
}
export const reply = (id, timestamp = '2026-10-20T00:00:02Z', content = []) => ({ type: 'assistant', timestamp, sessionId: 'synthetic-session', message: { id, model: 'synthetic-model', content, usage: { input_tokens: 10, output_tokens: 100 } } });
export const total = (state, field, group = 'hours') => Object.values(state[group]).reduce((sum, row) => sum + row[field], 0);
