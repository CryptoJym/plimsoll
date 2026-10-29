import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import { spawnSync } from 'node:child_process';
import { readLines } from './lines.js';
import { Evidence, genericFolder, launchProject, protectedPath, topRepositories } from './evidence.js';
import { stateBytes } from './state-json.js';
import { Seen } from './seen.js';
import { grokFields, grokFile, grokSummary, grokTurn, grokUser } from './grok.js';
import { actionCounts, counts, fields, HOUR, WINDOW, integer, string, peakMemory, type ActionKind, type Actions, type Config, type Context, type FileState, type Hour, type Options, type State, type Tool, type Counts, type Stats, type Source, type Velocity } from './types.js';

const digest = (x: string) => crypto.createHash('sha256').update(x).digest('hex').slice(0, 24);
// Exact port of shared/src/policy.ts providerAccountKey / hashProtectedValue.
// Do not switch to raw-id hashing: that would break existing cloud seat labels.
export const accountKey = (id: unknown): string => typeof id === 'string' && id ? 'sha256:' + crypto.createHash('sha256').update(JSON.stringify({ stringValue: id })).digest('hex').slice(0, 16) : 'unknown';
export const emptyState = (): State => ({ version: 1, files: {}, hours: {}, velocity: {}, velocityPending: {}, seen: {}, sessions: {}, seats: {}, pending: {}, metadataPending: false, lastSend: null, lastError: null, failures: 0, nextSend: 0, footprint: null });
const context = (account = 'unknown'): Context => ({ model: 'unknown', cwd: '', session: '', account, title: null, records: false, tier: 'standard', effort: 'unknown' });
const velocityCounts = (): Omit<Velocity, 'hour' | 'tool' | 'accountKey' | 'model' | 'sessionKey' | 'tier' | 'effort'> => ({ replies: 0, freshInput: 0, cacheRead: 0, cacheWrite: 0, output: 0, reasoning: 0, timed: 0, sOut: 0, sDur: 0, sOutDur: 0, sOut2: 0, sDur2: 0, hist: Array(16).fill(0), turns: 0, turnMs: 0, ttftMs: 0, ttftN: 0 });
const lower = (value: unknown, fallback = 'unknown') => typeof value === 'string' && value.trim() ? value.trim().toLowerCase().slice(0, 40) : fallback;
const codexTier = (value: unknown) => value === undefined || value === null ? 'standard' : lower(value);
const histBucket = (rate: number) => {
  const bounds = [5, 10, 15, 20, 30, 40, 50, 60, 80, 100, 130, 170, 220, 300, 400];
  for (let i = 0; i < bounds.length; i++) if (rate < bounds[i]) return i;
  return 15;
};
const actionKind = (name: unknown): ActionKind => {
  const n = typeof name === 'string' ? name.toLowerCase() : '';
  if (/apply_patch|^(edit|write|notebookedit)$|\.apply_patch$/.test(n)) return 'edit';
  if (/^(bash|shell|exec_command|functions\.exec|local_shell)|\.exec$/.test(n)) return 'shell';
  if (/^(read|cat|sed|head|tail)$|\.read$/.test(n)) return 'read';
  if (/^(glob|grep|rg|search)$|\.search$/.test(n)) return 'search';
  if (/websearch|webfetch|search_query|open_url/.test(n)) return 'web';
  if (/browser|chrome|playwright/.test(n)) return 'browser';
  if (/spawn_agent|task|agent/.test(n)) return 'agent';
  if (n.startsWith('mcp__') || n.startsWith('mcp.')) return 'mcp';
  return 'other';
};
export const expand = (p: string, home: string) => p === '~' ? home : p.startsWith('~/') ? path.join(home, p.slice(2)) : path.resolve(p);
function directories(parent: string, prefix = ''): string[] {
  try { return fs.readdirSync(parent).sort().filter(n => n.startsWith(prefix)).filter(n => { try { return fs.statSync(path.join(parent, n)).isDirectory(); } catch { return false; } }); } catch { return []; }
}
export function roots(home: string, env: Record<string, string | undefined>, config: Config): { tool: Tool; path: string }[] {
  const result: { tool: Tool; path: string }[] = [
    { tool: 'claude_code', path: path.join(home, '.claude/projects') },
    ...['.claude-seats', '.claude-accounts'].flatMap(parent => directories(path.join(home, parent)).map(n => ({ tool: 'claude_code' as const, path: path.join(home, parent, n, 'projects') }))),
    ...(env.CLAUDE_CONFIG_DIR ? [{ tool: 'claude_code' as const, path: path.join(env.CLAUDE_CONFIG_DIR, 'projects') }] : []),
    { tool: 'codex', path: path.join(home, '.codex/sessions') },
    ...['.codex-profiles', '.codex-accounts', '.codex-lanepool'].flatMap(parent => directories(path.join(home, parent)).map(n => ({ tool: 'codex' as const, path: path.join(home, parent, n, 'sessions') }))),
    ...(env.CODEX_HOME ? [{ tool: 'codex' as const, path: path.join(env.CODEX_HOME, 'sessions') }] : []),
    { tool: 'grok', path: path.join(home, '.grok/sessions') },
    ...directories(home, '.grok-').map(n => ({ tool: 'grok' as const, path: path.join(home, n, 'sessions') })),
    ...(env.GROK_HOME ? [{ tool: 'grok' as const, path: path.join(env.GROK_HOME, 'sessions') }] : []),
  ];
  for (const root of config.roots ?? []) {
    if (typeof root === 'string') result.push({ path: expand(root, home), tool: /grok/.test(root) ? 'grok' : /codex|sessions/.test(root) ? 'codex' : 'claude_code' });
    else if (root && ['codex', 'claude_code', 'grok'].includes(root.tool) && typeof root.path === 'string') result.push({ tool: root.tool, path: expand(root.path, home) });
  }
  const seen = new Set<string>();
  return result.filter(root => { if (root.tool === 'grok' && protectedPath(root.path, home)) return false; try { root.path = fs.realpathSync(root.path); } catch { return false; } const key = root.tool + ':' + root.path; if (seen.has(key)) return false; seen.add(key); return true; });
}
function archivedRollout(name: string, since: number): boolean {
  const match = /^rollout-(\d{4}-\d{2}-\d{2})T/.exec(name);
  return Boolean(match && Date.parse(`${match[1]}T00:00:00Z`) >= since - 24 * HOUR);
}
function* files(root: string, visited: Set<string>, since: number, tool: Tool, home: string): Generator<{ file: string; stat: fs.Stats }> {
  if (tool === 'grok' && protectedPath(root, home)) return;
  const entries: { name: string; directory?: string; file?: string; stat?: fs.Stats }[] = [];
  try {
    const stat = fs.statSync(root), key = `${stat.dev}:${stat.ino}`;
    if (visited.has(key)) return;
    visited.add(key);
    // Some profiles keep years of rollouts in one directory. Filter while
    // streaming, then sort only eligible files and directories, preserving the
    // original traversal order without retaining all historical Dirents.
    const directory = fs.opendirSync(root);
    try {
      let entry;
      while ((entry = directory.readSync())) {
        const file = path.join(root, entry.name);
        if (tool === 'grok' && (protectedPath(file, home) || (!entry.isDirectory() && !entry.isSymbolicLink() && entry.name !== 'updates.jsonl'))) continue;
        if (entry.isDirectory()) entries.push({ name: entry.name, directory: file });
        else if (entry.isSymbolicLink()) {
          try {
            const s = fs.statSync(file);
            if (s.isDirectory()) entries.push({ name: entry.name, directory: file });
            else if (s.isFile() && (entry.name.endsWith('.jsonl') || (tool === 'codex' && entry.name.endsWith('.jsonl.zst') && archivedRollout(entry.name, since))) &&
              (entry.name.endsWith('.jsonl') ? s.mtimeMs >= since : archivedRollout(entry.name, since)) && (tool !== 'grok' || grokFile(file, home))) entries.push({ name: entry.name, file: fs.realpathSync(file), stat: s });
          } catch { /* Optional alias disappeared. */ }
        } else if (entry.isFile() && (entry.name.endsWith('.jsonl') || (tool === 'codex' && entry.name.endsWith('.jsonl.zst') && archivedRollout(entry.name, since)))) {
          let s: fs.Stats;
          try { s = fs.statSync(file); } catch { throw new Error('source_unreadable'); }
          if (entry.name.endsWith('.jsonl') ? s.mtimeMs >= since : archivedRollout(entry.name, since)) entries.push({ name: entry.name, file, stat: s });
        }
      }
    } finally { directory.closeSync(); }
    entries.sort((a, b) => a.name < b.name ? -1 : a.name > b.name ? 1 : 0);
  } catch (e) { if ((e as NodeJS.ErrnoException).code !== 'ENOENT') throw new Error('source_unreadable'); return; }
  for (const entry of entries) {
    if (entry.directory) yield* files(entry.directory, visited, since, tool, home);
    else yield { file: entry.file!, stat: entry.stat! };
  }
}

function archivedSources(sources: Source[]): Source[] {
  const result = [...sources], seen = new Set(sources.map(source => `${source.tool}:${source.path}`));
  for (const source of sources.filter(source => source.tool === 'codex')) {
    const archived = path.join(path.dirname(source.path), 'archived_sessions');
    let real: string;
    try { real = fs.realpathSync(archived); } catch { continue; }
    const key = `codex:${real}`;
    if (!seen.has(key)) { seen.add(key); result.push({ tool: 'codex', path: real }); }
  }
  return result;
}

/** Decode one archived Codex rollout without retaining its uncompressed body in
 * the reader process. The fleet has the small zstd CLI alongside Node 22; the
 * output is mode-600 temporary data and is removed before the pass returns. */
function materializeZstd(file: string): { path: string; size: number; cleanup: () => void } {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'plimsoll-lean-zstd-'));
  const output = path.join(dir, 'rollout.jsonl');
  let fd: number | undefined;
  try {
    fd = fs.openSync(output, 'wx', 0o600);
    const result = spawnSync('zstd', ['-q', '-d', '-c', '--', file], { stdio: ['ignore', fd, 'ignore'] });
    if (result.status !== 0) throw new Error('source_unreadable');
  } catch (error) {
    try { if (fd !== undefined) fs.closeSync(fd); } catch { /* best effort */ }
    fs.rmSync(dir, { recursive: true, force: true });
    throw error instanceof Error && error.message === 'source_unreadable' ? error : new Error('source_unreadable');
  }
  if (fd !== undefined) fs.closeSync(fd);
  const size = fs.statSync(output).size;
  return { path: output, size, cleanup: () => fs.rmSync(dir, { recursive: true, force: true }) };
}

class Projects {
  tags = new Map<string, { project: string; at: number }>();
  cache = new Map<string, string | null>();
  lanes = new Map<string, string | null>();
  folderTags = new Map<string, { project: string; at: number }>();
  constructor(private home: string) {}
  loadTags(active: Set<string>) {
    const file = path.join(this.home, '.local/state/plimsoll-lean/launch-tags.jsonl');
    try {
      // The generic session reader filters record types. Tags have a separate,
      // bounded line reader so arbitrary project names cannot trigger parsing.
      const fd = fs.openSync(file, 'r'), buffer = Buffer.allocUnsafe(64 * 1024);
      const decoder = new TextDecoder();
      let pending = '';
      try { let n; while ((n = fs.readSync(fd, buffer)) > 0) {
        pending += decoder.decode(buffer.subarray(0, n), { stream: true });
        let start = 0, index;
        while ((index = pending.indexOf('\n', start)) !== -1) {
          const line = pending.slice(start, index); start = index + 1;
          if (line.length <= 8192) try { this.tag(JSON.parse(line), active); } catch { /* Invalid tag. */ }
        }
        pending = pending.slice(start);
        if (pending.length > 8192) pending = ''; // Tags are small, metadata only.
      } } finally { fs.closeSync(fd); }
    } catch { /* Launch tags are optional. */ }
  }
  tag(r: Record<string, any>, active: Set<string>) {
    const at = Date.parse(r.at), project = string(r.project), session = string(r.sessionKey);
    if (!['claude_code', 'codex', 'grok'].includes(r.tool) || !session || !project || !Number.isFinite(at)) return;
    const key = r.tool + ':' + session;
    if (!active.has(key)) return;
    if (at >= (this.tags.get(key)?.at ?? -Infinity)) this.tags.set(key, { project, at });
  }
  hint(cwd: string, tool: Tool, session: string): { projectHint: string | null; projectSource: Hour['projectSource']; folderLabel: string | null } {
    const tagged = this.tags.get(tool + ':' + session);
    const lane = launchProject(cwd, this.home, this.lanes);
    const project = tagged?.project ?? lane ?? this.repo(cwd);
    const relative = cwd ? path.relative(this.home, cwd) : '';
    const folderLabel = !project && relative && !relative.startsWith('..') && !path.isAbsolute(relative) ? relative.split(path.sep).slice(0, 3).join('/') : null;
    return { projectHint: project, projectSource: tagged || lane ? 'launch_tag' : project ? 'git_repo' : 'none', folderLabel };
  }
  rememberFolder(cwd: string, tool: Tool, session: string, at: number) {
    const project = launchProject(cwd, this.home, this.lanes), key = tool + ':' + session;
    if (project && at >= (this.folderTags.get(key)?.at ?? -Infinity)) this.folderTags.set(key, { project, at });
  }
  repo(cwd: string): string | null {
    if (!cwd || !path.isAbsolute(cwd)) return null;
    if (this.cache.has(cwd)) return this.cache.get(cwd)!;
    let at = path.resolve(cwd), project: string | null = null;
    while (at !== path.dirname(at)) {
      const marker = path.join(at, '.git');
      try {
        const stat = fs.statSync(marker);
        if (stat.isDirectory() || stat.isFile()) {
          let main = at;
          if (stat.isFile() && stat.size < 8192) {
            // Opening a worktree pointer in a protected folder can block forever (see protectedPath).
            if (protectedPath(marker, this.home)) { project = null; break; }
            const value = fs.readFileSync(marker, 'utf8').match(/^gitdir:\s*(.+)/m)?.[1]?.trim();
            if (value) {
              const gitdir = path.resolve(at, value), split = gitdir.indexOf('/.git/worktrees/');
              if (split >= 0) main = gitdir.slice(0, split);
              else {
                try { const common = fs.readFileSync(path.join(gitdir, 'commondir'), 'utf8').trim(); const dir = path.resolve(gitdir, common); main = path.basename(dir) === '.git' ? path.dirname(dir) : at; } catch { /* Ordinary gitfile. */ }
              }
            }
          }
          project = main === this.home || main === path.parse(main).root || genericFolder(path.basename(main)) ? null : path.basename(main); break;
        }
      } catch { /* Walk toward the nearest repository. */ }
      at = path.dirname(at);
    }
    if (this.cache.size > 4096) this.cache.clear();
    this.cache.set(cwd, project); return project;
  }
}

function claudeAccount(root: string, home: string, env: Record<string, string | undefined>): string {
  const config = path.dirname(root);
  let defaultDirectory = path.join(home, '.claude');
  try { defaultDirectory = fs.realpathSync(defaultDirectory); } catch { /* Optional default profile. */ }
  let explicit = false;
  try { explicit = Boolean(env.CLAUDE_CONFIG_DIR) && fs.realpathSync(env.CLAUDE_CONFIG_DIR!) === config; } catch { /* Optional configured directory. */ }
  const candidates = config === defaultDirectory && !explicit ? [path.join(home, '.claude.json')] : [path.join(config, '.claude.json')];
  for (const file of candidates) {
    try { if (fs.statSync(file).size > 10 * 1024 * 1024) continue; const data = JSON.parse(fs.readFileSync(file, 'utf8')); const key = accountKey(data?.oauthAccount?.accountUuid); if (key !== 'unknown') return key; } catch { /* Missing identity remains unknown. */ }
  }
  return 'unknown';
}

function prune(state: State, cutoff: number) {
  for (const [key, row] of Object.entries(state.hours)) if (Date.parse(row.hour) < cutoff && !state.pending[key]) delete state.hours[key];
  for (const [key, row] of Object.entries(state.velocity ?? {})) if (Date.parse(row.hour) < cutoff && !state.velocityPending?.[key]) delete state.velocity[key];
  for (const key in state.seen) if (state.seen[key] * HOUR < cutoff) delete state.seen[key];
  for (const key in state.evidenceSeen ?? {}) if (state.evidenceSeen![key] * HOUR < cutoff) delete state.evidenceSeen![key];
  for (const [key, evidence] of Object.entries(state.evidence ?? {})) if (evidence.lastSeen < cutoff && !state.metadataPending) delete state.evidence![key];
  for (const [file, tracked] of Object.entries(state.files)) {
    for (const key of Object.keys(tracked.fallback)) if (!state.hours[key]) delete tracked.fallback[key];
    if (tracked.mtime < cutoff && !Object.keys(tracked.fallback).some(k => state.pending[k])) delete state.files[file];
  }
  if (!state.metadataPending) {
    for (const [key, session] of Object.entries(state.sessions)) if (Date.parse(session.lastSeen) < cutoff) delete state.sessions[key];
    for (const [key, seat] of Object.entries(state.seats)) if (Date.parse(seat.observedAt) < cutoff) delete state.seats[key];
  }
}

export function scan(options: Options = {}): { state: State; rows: Hour[]; stats: Stats } {
  const started = performance.now(), home = options.home ?? os.homedir(), now = options.now ?? Date.now();
  const config = options.config ?? {}, state = options.state ?? emptyState();
  // States written by the pre-velocity reader remain valid and gain only the
  // additive velocity outbox when this bundle first opens them.
  state.velocity ??= {};
  state.velocityPending ??= {};
  const cutoff = Math.floor((now - WINDOW) / HOUR) * HOUR;
  const since = options.since ? Date.parse(options.since) : cutoff;
  const until = options.until ? Date.parse(options.until) : now + HOUR;
  if (!Number.isFinite(since) || !Number.isFinite(until) || since >= until) throw new Error('invalid_window');
  const startHour = Math.floor(since / HOUR) * HOUR, endHour = Math.ceil(until / HOUR) * HOUR;
  const inside = (hour: string) => Date.parse(hour) >= startHour && Date.parse(hour) < endHour;
  const velocityOnly = options.velocityOnly === true || options.command === 'velocity-backfill';
  const rebuildVelocity = velocityOnly || options.backfill === true;
  const stats: Stats = { files: 0, bytesRead: 0, rows: 0, sent: 0, errors: 0, milliseconds: 0, peakMemoryBytes: 0, stateBytes: 0 };
  prune(state, cutoff);
  const seen = new Seen(state, 'seen', 'seenPacked', cutoff);
  const evidenceSeen = config.sendEvidence === true ? new Seen(state, 'evidenceSeen', 'evidenceSeenPacked', cutoff) : null;
  // Bound admission while scanning as well as at atomic save. A large source
  // must stop before its ID/metadata maps can exhaust memory. Sampling avoids
  // serializing state for every reply; the writer enforces the exact ceiling.
  // IDs added this pass count at the packed size they will be saved at.
  const checkBudget = () => { stateBytes(state, 9 * 1024 * 1024 - seen.pendingBytes - (evidenceSeen?.pendingBytes ?? 0)); };
  let admissions = 0;
  const budgetTick = () => { if (++admissions % 256 === 0) checkBudget(); };
  if (options.backfill && !velocityOnly) {
    // Zero rows act as replacement tombstones if a corrected source no longer
    // contains a formerly counted hour. Never leave a stale cloud total behind.
    for (const [key, row] of Object.entries(state.hours)) if (inside(row.hour)) { Object.assign(row, counts()); state.pending[key] = true; }
    seen.remove(hour => hour * HOUR >= startHour && hour * HOUR < endHour);
    for (const file of Object.values(state.files)) for (const key of Object.keys(file.fallback)) if (state.hours[key] && inside(state.hours[key].hour)) delete file.fallback[key];
  }
  if (rebuildVelocity) {
    // A velocity-only backfill replaces only the additive rows. Existing hour,
    // session, seat and dedup state is left untouched, while rows that vanish
    // from a corrected transcript are replaced by zero tombstones.
    for (const row of Object.values(state.velocity)) if (inside(row.hour)) Object.assign(row, velocityCounts());
  }
  // A normal/backfill scan must retain the established hour deduplication
  // index. Velocity-only replay gets its own in-pass index because it is
  // deliberately forbidden from changing the hours/sessions/seen data.
  const velocitySeen = velocityOnly ? new Set<string>() : null;
  const projects = new Projects(home), visited = new Set<string>(), visitedFiles = new Set<string>();
  const evidence = evidenceSeen ? new Evidence(home, state, evidenceSeen, startHour, endHour, budgetTick) : null;
  const markAction = (ctx: Context, name: unknown, tool?: Tool) => {
    if (!ctx.actions) ctx.actions = actionCounts();
    const kind = actionKind(name); ctx.actions[kind]++;
    const session = tool && ctx.session ? state.sessions[digest(tool + ':' + ctx.session)] : undefined;
    if (session) { if (!session.actions) session.actions = actionCounts(); session.actions[kind]++; state.metadataPending = true; }
  };
  const addVelocity = (tool: Tool, ctx: Context, stamp: number, tokens: Counts, meta?: { tier?: string; effort?: string; reasoning?: number; dur?: number; turnMs?: number; ttftMs?: number }) => {
    if (!Number.isFinite(stamp) || stamp < startHour || stamp >= endHour || ctx.model === '<synthetic>') return;
    const hour = Math.floor(stamp / HOUR), tier = lower(meta?.tier ?? ctx.tier, tool === 'codex' ? 'standard' : 'unknown'), effort = lower(meta?.effort ?? ctx.effort);
    const sessionKey = ctx.session || 'unknown', key = digest(JSON.stringify([tool, ctx.account, ctx.model || 'unknown', sessionKey, hour, tier, effort]));
    const row = state.velocity[key] ??= { ...velocityCounts(), hour: new Date(hour * HOUR).toISOString(), tool, accountKey: ctx.account, model: ctx.model || 'unknown', sessionKey, tier, effort };
    row.tier = tier; row.effort = effort;
    row.replies = Math.min(Number.MAX_SAFE_INTEGER, row.replies + integer(tokens.requests));
    for (const k of ['freshInput', 'cacheRead', 'cacheWrite', 'output'] as const) row[k] = Math.min(Number.MAX_SAFE_INTEGER, row[k] + integer(tokens[k]));
    row.reasoning = Math.min(Number.MAX_SAFE_INTEGER, row.reasoning + integer(meta?.reasoning));
    if (meta?.dur !== undefined && integer(tokens.requests) === 1 && integer(tokens.output) >= 50 && meta.dur >= 0.3 && meta.dur <= 900) {
      const dur = Math.round(meta.dur * 1000) / 1000, output = integer(tokens.output), rate = output / dur;
      row.timed++; row.sOut += output; row.sDur += dur; row.sOutDur += output * dur; row.sOut2 += output * output; row.sDur2 += dur * dur; row.hist[histBucket(rate)]++;
    }
    if (meta?.turnMs !== undefined) row.turns++; if (meta?.turnMs !== undefined) row.turnMs += integer(meta.turnMs);
    if (meta?.ttftMs !== undefined) { row.ttftN++; row.ttftMs += integer(meta.ttftMs); }
    state.velocityPending[key] = true; budgetTick();
  };
  const addSessionMetadata = (ctx: Context, summary: import('./types.js').Session) => {
    if (ctx.parentSessionKey !== undefined && summary.parentSessionKey !== ctx.parentSessionKey) { summary.parentSessionKey = ctx.parentSessionKey; state.metadataPending = true; }
    if (ctx.entrypoint !== undefined && summary.entrypoint !== ctx.entrypoint) { summary.entrypoint = ctx.entrypoint; state.metadataPending = true; }
    if (ctx.actions && JSON.stringify(summary.actions ?? null) !== JSON.stringify(ctx.actions)) { summary.actions = { ...ctx.actions }; state.metadataPending = true; }
  };
  const add = (tool: Tool, ctx: Context, stamp: unknown, id: string | null, tokens: Counts, file?: FileState, meta?: { tier?: string; effort?: string; reasoning?: number; dur?: number }) => {
    const time = typeof stamp === 'string' ? Date.parse(stamp) : NaN;
    if (!Number.isFinite(time) || time < startHour || time >= endHour) return false;
    const hour = Math.floor(time / HOUR);
    if (id !== null) {
      const seenKey = digest(tool + ':' + id);
      const deduped = velocityOnly ? velocitySeen!.has(seenKey) : seen.has(seenKey);
      if (deduped) return false;
      if (velocityOnly) velocitySeen!.add(seenKey); else seen.add(seenKey, hour);
    }
    budgetTick();
    if (ctx.model === '<synthetic>') return false;
    const sessionKey = ctx.session || 'unknown';
    if (!velocityOnly) {
      const key = digest(JSON.stringify([tool, ctx.account, ctx.model, sessionKey, hour]));
      const hint = projects.hint(ctx.cwd, tool, sessionKey);
      const row = state.hours[key] ??= { ...counts(), hour: new Date(hour * HOUR).toISOString(), tool, accountKey: ctx.account, model: ctx.model, sessionKey, projectHint: hint.projectHint, projectSource: hint.projectSource };
      row.projectHint = hint.projectHint; row.projectSource = hint.projectSource;
      for (const k of fields) row[k] = Math.min(Number.MAX_SAFE_INTEGER, row[k] + tokens[k]);
      state.pending[key] = true;
      if (file) { const own = file.fallback[key] ??= counts(); for (const k of fields) own[k] += tokens[k]; }
      const sessionId = digest(tool + ':' + sessionKey), when = new Date(time).toISOString();
      const summary = state.sessions[sessionId] ??= { tool, sessionKey, title: null, folderLabel: null, projectHint: null, firstSeen: when, lastSeen: when };
      if (when < summary.firstSeen) summary.firstSeen = when;
      if (when >= summary.lastSeen) { summary.lastSeen = when; summary.projectHint = hint.projectHint; summary.folderLabel = config.sendFolderLabels === false ? null : hint.folderLabel; }
      summary.title = config.sendSessionTitles ? ctx.title : null;
      addSessionMetadata(ctx, summary);
      state.metadataPending = true;
    }
    addVelocity(tool, ctx, time, tokens, meta);
    return true;
  };
  const discoveredSources = options.discovered ?? roots(home, options.env ?? process.env, config);
  // Archived Codex rollouts are only needed for the explicit velocity replay;
  // keeping them out of ordinary passes preserves the incremental footprint.
  const sources: Source[] = options.discovered ? discoveredSources : velocityOnly ? archivedSources(discoveredSources) : discoveredSources;
  for (const root of sources) {
    let rootAccount: string | undefined;
    const getAccount = () => {
      if (rootAccount !== undefined) return rootAccount;
      if (root.tool === 'grok') {
        const id = grokUser(root.path, home);
        return rootAccount = id ? accountKey(id) : 'grok:unknown';
      }
      return rootAccount = root.tool === 'claude_code' ? claudeAccount(root.path, home, options.env ?? process.env) : 'unknown';
    };
    for (const { file, stat: discoveredStat } of root.files ?? files(root.path, visited, startHour, root.tool, home)) {
      if (root.tool === 'codex' && !path.basename(file).startsWith('rollout-')) continue;
      if (root.tool === 'grok' && !grokFile(file, home)) continue;
      // Metadata discovery is a snapshot; refresh eligible files immediately
      // before capture so rotations and truncations still reset their context.
      let stat = discoveredStat;
      try { stat = fs.statSync(file); } catch { throw new Error('source_unreadable'); }
      const compressed = file.endsWith('.jsonl.zst');
      const fileIdentity = `${stat.dev}:${stat.ino}`;
      if (visitedFiles.has(fileIdentity) || stat.mtimeMs < startHour) continue;
      visitedFiles.add(fileIdentity);
      let tracked = state.files[file];
      const reset = !tracked || tracked.dev !== stat.dev || tracked.ino !== stat.ino ||
        (compressed ? stat.size !== tracked.size || stat.mtimeMs !== tracked.mtime : stat.size < tracked.size || stat.size < tracked.offset);
      if (!tracked) { tracked = state.files[file] = { dev: stat.dev, ino: stat.ino, size: stat.size, offset: 0, mtime: stat.mtimeMs, context: context(getAccount()), fallback: {} }; budgetTick(); }
      if (reset || (options.backfill && !velocityOnly) || velocityOnly) { const records = tracked.context.records && !options.backfill && !velocityOnly; tracked.offset = 0; tracked.context = context(getAccount()); tracked.context.records = records; }
      const ctx = tracked.context;
      const metadata = root.tool === 'grok' ? grokSummary(file, home) : null;
      if (metadata) { ctx.cwd = metadata.cwd; ctx.model = metadata.model; ctx.account = getAccount(); }
      const updateGrokSummary = () => {
        if (!metadata) return;
        const summary = state.sessions[digest('grok:' + ctx.session)];
        if (!summary) return;
        const before = JSON.stringify(summary), hint = projects.hint(ctx.cwd, 'grok', ctx.session);
        if (metadata.firstSeen && metadata.firstSeen < summary.firstSeen) summary.firstSeen = metadata.firstSeen;
        if (metadata.lastSeen && metadata.lastSeen > summary.lastSeen) summary.lastSeen = metadata.lastSeen;
        summary.projectHint = hint.projectHint; summary.folderLabel = config.sendFolderLabels === false ? null : hint.folderLabel;
        if (before !== JSON.stringify(summary)) state.metadataPending = true;
      };
      updateGrokSummary();
      stats.files++;
      // Upgrading an existing state (or re-enabling evidence) replays evidence
      // alone. Usage cursors, cumulative baselines and replacement totals stay put.
      let evidenceContext = reset || (options.backfill && !velocityOnly) ? { cwd: '', session: '' } : tracked.evidenceContext ?? { cwd: ctx.cwd, session: ctx.session };
      // A lane marker in the session's latest folder also names rows counted
      // before it was seen. Without evidence, the usage context has that folder.
      const remember = (at: number) => evidence
        ? projects.rememberFolder(evidenceContext.cwd, root.tool, evidenceContext.session, evidenceContext.time ?? 0)
        : projects.rememberFolder(ctx.cwd, root.tool, ctx.session, at);
      if (root.tool === 'claude_code') ctx.account = getAccount();
      // Archived files are immutable in ordinary passes; avoid decoding them
      // again when their source metadata has not changed. A velocity replay
      // deliberately decodes each file for its bounded window.
      if (compressed && !velocityOnly && tracked.offset > 0 && !reset && stat.size === tracked.size && stat.mtimeMs === tracked.mtime) { remember(tracked.mtime); continue; }
      let sourcePath = file, sourceSize = stat.size, cleanup = () => {};
      if (compressed) { const decoded = materializeZstd(file); sourcePath = decoded.path; sourceSize = decoded.size; cleanup = decoded.cleanup; }
      try {
        if (evidence && root.tool !== 'grok' && !reset && tracked.evidenceOffset !== tracked.offset) {
          evidenceContext = { cwd: '', session: '' };
          stats.bytesRead += readLines(sourcePath, 0, tracked.offset, r => evidence.observe(root.tool, evidenceContext, r), true).bytes;
          tracked.evidenceOffset = tracked.offset; tracked.evidenceContext = evidenceContext;
        }
        if (tracked.offset === sourceSize && !velocityOnly) { remember(tracked.mtime); continue; }
        const result = readLines(sourcePath, tracked.offset, sourceSize, r => {
          if (root.tool === 'grok') {
            const turn = grokTurn(r);
            if (!turn) return;
            const time = Date.parse(turn.time), seenKey = digest('grok:' + JSON.stringify(turn.id));
            if (time < startHour || time >= endHour || (velocityOnly ? velocitySeen!.has(seenKey) : seen.has(seenKey))) return;
            if (velocityOnly) velocitySeen!.add(seenKey); else seen.add(seenKey, Math.floor(time / HOUR)); budgetTick();
            ctx.session = turn.session || path.basename(path.dirname(file));
            for (const { model, tokens } of turn.models) {
              ctx.model = model || metadata!.model.replace(/-build$/, '');
              add('grok', ctx, turn.time, null, tokens, undefined, { tier: 'unknown', effort: 'unknown', reasoning: 0 });
            }
            updateGrokSummary();
            return;
          }
          evidence?.observe(root.tool, evidenceContext, r);
          if (root.tool === 'claude_code') {
            const eventTime = typeof r.timestamp === 'string' ? Date.parse(r.timestamp) : NaN;
            if (r.type === 'user' && Number.isFinite(eventTime)) ctx.lastInputAt = eventTime;
            if (r.entrypoint !== undefined) ctx.entrypoint = lower(r.entrypoint);
            const parent = r.parentSessionKey ?? r.parentSessionId ?? r.parent_session_id;
            if (parent !== undefined) ctx.parentSessionKey = typeof parent === 'string' && parent ? parent.slice(0, 200) : null;
            if (r.type === 'summary' && config.sendSessionTitles) { ctx.title = string(r.summary); const summary = state.sessions[digest(root.tool + ':' + ctx.session)]; if (summary) { summary.title = ctx.title; state.metadataPending = true; } return; }
            if (r.type !== 'assistant' || !r.timestamp || !r.message?.usage) return;
            const msg = r.message, usage = msg.usage;
            ctx.model = string(msg.model) || 'unknown'; ctx.cwd = string(r.cwd, 4096); ctx.session = string(r.sessionId);
            for (const part of Array.isArray(msg.content) ? msg.content : []) if (part && typeof part === 'object' && part.type === 'tool_use') markAction(ctx, part.name, root.tool);
            const tier = lower(usage.service_tier), effort = lower(r.effort ?? r.perTurnEffort), reasoning = integer(usage.output_tokens_details?.thinking_tokens);
            const time = Date.parse(r.timestamp), base = Math.max(ctx.lastInputAt ?? -Infinity, ctx.lastReplyAt ?? -Infinity), dur = Number.isFinite(base) ? (time - base) / 1000 : NaN;
            const accepted = add(root.tool, ctx, r.timestamp, JSON.stringify(msg.id || r.requestId || r.uuid || null), { requests: 1, freshInput: integer(usage.input_tokens), cacheRead: integer(usage.cache_read_input_tokens), cacheWrite: integer(usage.cache_creation_input_tokens), output: integer(usage.output_tokens) }, undefined, { tier, effort, reasoning, dur });
            if (accepted) ctx.lastReplyAt = time;
            return;
          }
          const p = r.payload ?? {};
          const eventTime = typeof r.timestamp === 'string' ? Date.parse(r.timestamp) : NaN;
          if (r.type === 'event_msg' && ['task_started', 'user_message'].includes(p.type) && Number.isFinite(eventTime)) ctx.lastInputAt = eventTime;
          if (r.type === 'response_item' && (typeof p.type === 'string' && (p.type.endsWith('_call_output') || p.type === 'tool_search_output' || (p.type === 'message' && p.role === 'user'))) && Number.isFinite(eventTime)) ctx.lastInputAt = eventTime;
          if (r.type === 'response_item' && ['function_call', 'custom_tool_call'].includes(p.type)) markAction(ctx, p.name, root.tool);
          if (r.type === 'session_meta') {
            ctx.cwd = string(p.cwd, 4096) || ctx.cwd; ctx.session = string(p.id || p.session_id) || ctx.session;
            if (p.creator_account_id) ctx.account = accountKey(p.creator_account_id);
            ctx.model = string(p.base_instructions?.provenance?.model) || ctx.model;
            if (p.originator !== undefined) ctx.entrypoint = lower(p.originator);
            const parent = p.parentSessionKey ?? p.parent_session_id ?? p.parent_thread_id;
            if (parent !== undefined) ctx.parentSessionKey = typeof parent === 'string' && parent ? parent.slice(0, 200) : null;
          } else if (r.type === 'turn_context') {
            ctx.model = string(p.model) || ctx.model; ctx.cwd = string(p.cwd, 4096) || ctx.cwd;
            if (p.effort !== undefined) ctx.effort = lower(p.effort);
          } else if (r.type === 'event_msg' && p.type === 'thread_settings_applied') {
            const settings = p.thread_settings ?? {};
            ctx.tier = codexTier(settings.service_tier);
            if (settings.reasoning_effort !== undefined) ctx.effort = lower(settings.reasoning_effort);
            if (settings.model) ctx.model = string(settings.model) || ctx.model;
          } else if (r.type === 'event_msg' && p.type === 'task_complete') {
            if (Number.isFinite(eventTime)) addVelocity(root.tool, ctx, eventTime, counts(), { tier: ctx.tier, effort: ctx.effort, turnMs: integer(p.duration_ms), ttftMs: typeof p.time_to_first_token_ms === 'number' ? p.time_to_first_token_ms : undefined });
          }
          if (r.type === 'token_usage_record' || p.type === 'token_usage_record') {
            if (!ctx.records) {
              ctx.records = true;
              if (!velocityOnly) for (const [key, previous] of Object.entries(tracked!.fallback)) {
                const row = state.hours[key];
                if (row) { for (const k of fields) row[k] = Math.max(0, row[k] - previous[k]); state.pending[key] = true; }
              }
              tracked!.fallback = {};
            }
            const u = p.usage ?? {}, cached = integer(u.cached_input_tokens), written = integer(u.cache_write_input_tokens);
            const time = typeof r.timestamp === 'string' ? Date.parse(r.timestamp) : NaN, base = Math.max(ctx.lastInputAt ?? -Infinity, ctx.lastReplyAt ?? -Infinity), dur = Number.isFinite(base) ? (time - base) / 1000 : NaN;
            const accepted = add(root.tool, ctx, r.timestamp, JSON.stringify(p.response_id || [p.turn_id ?? null, r.ordinal ?? null]), { requests: 1, freshInput: Math.max(integer(u.input_tokens) - cached - written, 0), cacheRead: cached, cacheWrite: written, output: integer(u.output_tokens) }, tracked, { tier: ctx.tier, effort: ctx.effort, reasoning: integer(u.reasoning_output_tokens), dur });
            if (accepted) ctx.lastReplyAt = time;
          } else if (p.type === 'token_count' || r.type === 'token_count') {
            const time = typeof r.timestamp === 'string' ? Date.parse(r.timestamp) : NaN;
            if (p.rate_limits && ctx.account !== 'unknown' && Number.isFinite(time)) {
              const rate = p.rate_limits, primary = rate.primary ?? {}, key = root.tool + ':' + ctx.account;
              if (!state.seats[key] || time >= Date.parse(state.seats[key].observedAt)) {
                const resetTime = typeof primary.resets_at === 'number' ? primary.resets_at * 1000 : Date.parse(primary.resets_at);
                state.seats[key] = { tool: root.tool, accountKey: ctx.account, plan: string(rate.plan_type) || null,
                  usedPercent: typeof primary.used_percent === 'number' && Number.isFinite(primary.used_percent) ? primary.used_percent : null,
                  windowMinutes: typeof primary.window_minutes === 'number' ? integer(primary.window_minutes) : null,
                  resetsAt: Number.isFinite(resetTime) && Math.abs(resetTime) < 8.64e15 ? new Date(resetTime).toISOString() : null,
                  creditsBalance: typeof rate.credits?.balance === 'number' || typeof rate.credits?.balance === 'string' ? String(rate.credits.balance).slice(0, 128) : null, observedAt: new Date(time).toISOString() };
                state.metadataPending = true;
                budgetTick();
              }
            }
            const total = p.info?.total_token_usage;
            if (!total || typeof total !== 'object') return;
            const current: Record<string, number> = {};
            for (const k of ['input_tokens', 'cached_input_tokens', 'cache_write_input_tokens', 'output_tokens', 'reasoning_output_tokens', 'total_tokens']) if (Object.hasOwn(total, k)) current[k] = integer(total[k]);
            if (!ctx.records && ctx.previous) {
              let delta: Record<string, number> = {};
              for (const k of Object.keys(current)) delta[k] = current[k] - (ctx.previous[k] ?? 0);
              if (Object.values(delta).some(n => n < 0)) delta = current;
              if (delta.total_tokens > 0) {
                const base = Math.max(ctx.lastInputAt ?? -Infinity, ctx.lastReplyAt ?? -Infinity), dur = Number.isFinite(base) ? (time - base) / 1000 : NaN;
                const accepted = add(root.tool, ctx, r.timestamp, JSON.stringify([file, r.timestamp, current]), { requests: 1, freshInput: Math.max((delta.input_tokens ?? 0) - (delta.cached_input_tokens ?? 0) - (delta.cache_write_input_tokens ?? 0), 0), cacheRead: delta.cached_input_tokens ?? 0, cacheWrite: delta.cache_write_input_tokens ?? 0, output: delta.output_tokens ?? 0 }, tracked, { tier: ctx.tier, effort: ctx.effort, reasoning: delta.reasoning_output_tokens ?? 0, dur });
                if (accepted) ctx.lastReplyAt = time;
              }
            }
            ctx.previous = current;
          }
        }, Boolean(evidence) && root.tool !== 'grok', root.tool === 'grok' ? grokFields : undefined);
        tracked.offset = result.offset; stats.bytesRead += result.bytes;
        if (evidence) { tracked.evidenceOffset = result.offset; tracked.evidenceContext = evidenceContext; }
        remember(stat.mtimeMs);
        tracked.dev = stat.dev; tracked.ino = stat.ino; tracked.size = stat.size; tracked.mtime = stat.mtimeMs;
        budgetTick();
      } catch (error) { if (error instanceof Error && error.message === 'state_capacity') throw error; throw new Error('source_unreadable'); }
      finally { cleanup(); }
    }
  }
  const active = new Set(Object.values(state.hours).map(row => row.tool + ':' + row.sessionKey));
  for (const session of Object.values(state.sessions)) active.add(session.tool + ':' + session.sessionKey);
  projects.loadTags(active);
  for (const [key, row] of Object.entries(state.hours)) {
    const tagged = projects.tags.get(row.tool + ':' + row.sessionKey) ?? projects.folderTags.get(row.tool + ':' + row.sessionKey);
    if (tagged && (row.projectHint !== tagged.project || row.projectSource !== 'launch_tag')) {
      row.projectHint = tagged.project; row.projectSource = 'launch_tag'; state.pending[key] = true;
    }
  }
  for (const session of Object.values(state.sessions)) {
    const tagged = projects.tags.get(session.tool + ':' + session.sessionKey) ?? projects.folderTags.get(session.tool + ':' + session.sessionKey);
    if (tagged && session.projectHint !== tagged.project) { session.projectHint = tagged.project; session.folderLabel = null; state.metadataPending = true; }
  }
  if (since < cutoff) { prune(state, cutoff); for (const ids of [seen, evidenceSeen]) ids?.remove(hour => hour * HOUR < cutoff); }
  // Privacy changes also apply to metadata read from an earlier state.
  for (const session of Object.values(state.sessions)) { if (!config.sendSessionTitles) session.title = null; if (config.sendFolderLabels === false || session.projectHint) session.folderLabel = null; }
  for (const [key, session] of Object.entries(state.sessions)) {
    if (evidence) {
      const collected = state.evidence?.[key];
      const repos = topRepositories(collected?.repos ?? []), branch = collected?.gitBranch ?? null;
      if (session.gitBranch !== branch || JSON.stringify(session.reposTouched) !== JSON.stringify(repos)) state.metadataPending = true;
      session.gitBranch = branch; session.reposTouched = repos;
    } else { delete session.gitBranch; delete session.reposTouched; }
  }
  seen.finish(); evidenceSeen?.finish();
  state.version = 2;
  checkBudget();
  const rows = Object.values(state.hours).sort((a, b) => { const x = [a.hour, a.tool, a.sessionKey].join(':'), y = [b.hour, b.tool, b.sessionKey].join(':'); return x < y ? -1 : x > y ? 1 : 0; });
  stats.rows = rows.length; stats.milliseconds = performance.now() - started; stats.peakMemoryBytes = peakMemory();
  return { state, rows, stats };
}
