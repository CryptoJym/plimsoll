import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { readJsonFields, readLines } from './lines.js';
import { saveJson } from './state-json.js';

// Exact providerAccountKey derivation; raw UUIDs never enter state or uploads.
export const accountKey = (id: unknown): string => typeof id === 'string' && id ? 'sha256:' + crypto.createHash('sha256').update(JSON.stringify({ stringValue: id })).digest('hex').slice(0, 16) : 'unknown';
const emailOf = (value: unknown) => typeof value === 'string' && /^[^\s@]{1,64}@[^\s@]{1,189}$/.test(value.trim()) ? value.trim().toLowerCase() : '';
const slug = (email: string) => email.replace(/[^a-z0-9]/g, '-');
const LIMIT = 1024 * 1024;
function children(parent: string): string[] {
  try { return fs.readdirSync(parent).sort().filter(name => { try { return fs.statSync(path.join(parent, name)).isDirectory(); } catch { return false; } }); } catch { return []; }
}
export function profileAccount(file: string): { key: string; email: string } {
  try {
    const account = readJsonFields(file, ['oauthAccount.accountUuid', 'oauthAccount.emailAddress'], 10 * LIMIT)?.oauthAccount;
    return { key: accountKey(account?.accountUuid), email: emailOf(account?.emailAddress) };
  } catch { return { key: 'unknown', email: '' }; }
}

/** Shared identity-only cache. Conflicting email or seat aliases stay unknown. */
export class Accounts {
  keys = new Map<string, string>();
  private conflicts = new Set<string>();
  private aliases = new Map<string, string>();
  constructor(private home: string, env: Record<string, string | undefined>) {
    const file = path.join(home, '.local/state/plimsoll-lean/account-keys.json');
    const add = (email: unknown, key: unknown) => {
      const name = emailOf(email);
      if (!name || typeof key !== 'string' || !/^sha256:[0-9a-f]{16}$/.test(key)) return;
      if (this.keys.has(name) && this.keys.get(name) !== key) this.conflicts.add(name);
      else this.keys.set(name, key);
    };
    try {
      if (fs.statSync(file).size <= LIMIT) {
        try {
          const data = JSON.parse(fs.readFileSync(file, 'utf8'));
          if (data?.email && data?.accountKey) add(data.email, data.accountKey);
          else if (data && typeof data === 'object' && !Array.isArray(data)) for (const [email, key] of Object.entries(data)) add(email, key);
        }
        catch { readLines(file, 0, fs.statSync(file).size, r => add(r.email, r.accountKey), false, ['email', 'accountKey']); }
      }
    } catch { /* The fleet cache is optional. */ }
    for (const file of [path.join(home, '.claude.json'),
      ...['.claude-seats', '.claude-accounts'].flatMap(p => children(path.join(home, p)).map(n => path.join(home, p, n, '.claude.json'))),
      ...(env.CLAUDE_CONFIG_DIR ? [path.join(env.CLAUDE_CONFIG_DIR, '.claude.json')] : [])]) {
      const account = profileAccount(file); add(account.email, account.key);
    }
    for (const name of this.conflicts) this.keys.delete(name);
    for (const [email, key] of this.keys) for (const name of [email, slug(email)]) {
      this.aliases.set(name, this.aliases.has(name) && this.aliases.get(name) !== key ? 'unknown' : key);
    }
  }
  resolve(seat: string): string { return this.aliases.get(seat.trim().toLowerCase()) ?? 'unknown'; }
  save(): void {
    if (!this.keys.size && !this.conflicts.size) return;
    const keys = Object.fromEntries([...this.keys].sort(([a], [b]) => a.localeCompare(b)));
    const labels: Record<string, string> = {};
    for (const [email, key] of this.keys) labels[key] = email;
    for (const [file, value] of [
      [path.join(this.home, '.local/state/plimsoll-lean/account-keys.json'), keys],
      [path.join(this.home, 'Library/Application Support/Plimsoll Lean/account-labels.json'), labels],
    ] as const) {
      // Avoid rewriting the shared cache on every five-minute pass.
      try { if (fs.readFileSync(file, 'utf8') === JSON.stringify(value)) continue; } catch { /* First write. */ }
      saveJson(file, value, LIMIT);
    }
  }
}
