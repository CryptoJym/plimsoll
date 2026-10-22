import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import type { EvidenceContext, RepoTouch, State, Tool } from './types.js';
import type { Seen } from './seen.js';

// These validators also run at the upload boundary, including for old state.
// The cloud's upload schema refuses a whole batch for one bad value, so these
// match it exactly: names up to 160 characters, trimmed, without / \ or :;
// branches up to 240 characters.
export const repositoryName = (s: unknown): s is string => typeof s === 'string' && s.length > 0 && s.length <= 160 && s === s.trim() && s !== '.' && s !== '..' && !/[\/\\:\x00-\x1f\x7f]/.test(s);
export function branchName(s: unknown): string | null {
  return typeof s === 'string' && s.length > 0 && s.length <= 240 && !/[\s\x00-\x1f\x7f~^:?*\[\\]/.test(s) && !s.includes('..') && !s.includes('@{') && !s.startsWith('/') && !s.endsWith('/') ? s : null;
}
function smallFile(file: string): string {
  try { const stat = fs.statSync(file, { throwIfNoEntry: false }); if (stat?.isFile() && stat.size <= 8192) return fs.readFileSync(file, 'utf8'); } catch { /* Optional local metadata. */ }
  return '';
}
export function launchProject(cwd: string, home: string, cache = new Map<string, string | null>()): string | null {
  if (!path.isAbsolute(cwd)) return null;
  if (cache.size > 4096) cache.clear();
  let at = path.resolve(cwd), project: string | null = null;
  const walked: string[] = [];
  for (;;) {
    if (cache.has(at)) { project = cache.get(at)!; break; }
    walked.push(at);
    const text = smallFile(path.join(at, '.conductor-project.json'));
    if (text) try {
      const tag = JSON.parse(text);
      if (tag.schema === 'conductor-source-project/v1' && repositoryName(tag.project)) { project = tag.project; break; }
    } catch { /* Invalid markers do not name a project. */ }
    if (at === home || at === path.dirname(at)) break;
    at = path.dirname(at);
  }
  for (const folder of walked) cache.set(folder, project);
  return project;
}

type Repository = { name: string; branch: string | null };
class Repositories {
  private cache = new Map<string, Repository | null>();
  private canonicalHome: string;
  constructor(private home: string) { try { this.canonicalHome = fs.realpathSync(home); } catch { this.canonicalHome = path.resolve(home); } }
  find(input: string): Repository | null {
    if (!path.isAbsolute(input)) return null;
    const resolved = path.resolve(input);
    if (this.cache.has(resolved)) return this.cache.get(resolved)!;
    let at = resolved, result: Repository | null = null;
    const walked: string[] = [];
    for (;;) {
      if (this.cache.has(at)) { result = this.cache.get(at)!; break; }
      walked.push(at);
      // Resolve symlinked ancestors even when the target file does not exist yet.
      let physical = at;
      try { physical = fs.realpathSync(at); } catch { /* New file or folder. */ }
      if (physical !== at) { result = this.find(physical); break; }
      const marker = path.join(physical, '.git');
      try {
        const stat = fs.statSync(marker);
        if (stat.isDirectory() || stat.isFile()) {
          let main = physical, gitdir = marker;
          if (stat.isFile()) {
            const value = smallFile(marker).match(/^gitdir:\s*(.+)/m)?.[1]?.trim();
            if (value) {
              gitdir = path.resolve(physical, value);
              const common = smallFile(path.join(gitdir, 'commondir')).trim();
              const split = gitdir.indexOf('/.git/worktrees/');
              if (common) { const dir = path.resolve(gitdir, common); if (path.basename(dir) === '.git') main = path.dirname(dir); }
              else if (split >= 0) main = gitdir.slice(0, split);
            }
          }
          const name = path.basename(main);
          if (main !== this.canonicalHome && main !== path.parse(main).root && repositoryName(name)) result = { name, branch: branchName(smallFile(path.join(gitdir, 'HEAD')).trim().match(/^ref: refs\/heads\/(.+)$/)?.[1]) };
          break; // A home/root repository never falls through to a parent repo.
        }
      } catch { /* Walk to the nearest repository. */ }
      if (at === this.home || at === this.canonicalHome || at === path.dirname(at)) break;
      at = path.dirname(at);
    }
    if (this.cache.size > 4096) this.cache.clear();
    for (const dir of walked) this.cache.set(dir, result);
    return result;
  }
}

// Tokenize paths, not shell output. No command is executed and quoted spaces
// remain part of the path. Code strings and URLs are not treated as paths.
function shellPaths(command: string, cwd: string): string[] {
  const tokens = command.match(/(?:"(?:\\.|[^"\\])*"|'[^']*'|\\.|[^\s;&|<>"'\\])+/g) ?? [];
  const result: string[] = [];
  const unquote = (s: string) => s.replace(/"((?:\\.|[^"\\])*)"|'([^']*)'|\\(.)/g, (_, a, b, c) => a !== undefined ? a.replace(/\\(["\\$`])/g, '$1') : b ?? c);
  for (let i = 0; i < tokens.length; i++) {
    const token = unquote(tokens[i]);
    const git = tokens.slice(0, i).some(t => /(?:^|\/)git$/.test(unquote(t)));
    if (git && token === '-C' && tokens[i + 1]) result.push(path.resolve(cwd || '.', unquote(tokens[++i])));
    else if (git && token.startsWith('-C') && token.length > 2) result.push(path.resolve(cwd || '.', token.slice(2)));
    else {
      const candidate = token.replace(/^[^=/]+=/, '');
      if (path.isAbsolute(candidate)) result.push(candidate);
    }
  }
  return result;
}
const pathFields = new Set(['file_path', 'filePath', 'path', 'notebook_path', 'target_file']);
const directoryFields = ['workdir', 'cwd', 'working_directory'];
const literal = '("(?:\\\\.|[^"\\\\])*"|\'(?:\\\\.|[^\'\\\\])*\'|`(?:\\\\.|[^`\\\\])*`)';
function decodeLiteral(value: string): string | null {
  if (value[0] === '`' && value.includes('${')) return null;
  if (value[0] === '"') { try { return JSON.parse(value); } catch { return null; } }
  return value.slice(1, -1).replace(/\\(?:u([a-fA-F0-9]{4})|x([a-fA-F0-9]{2})|(\r?\n)|(.))/gs, (_, u, x, newline, c) => u || x ? String.fromCharCode(parseInt(u ?? x, 16)) : newline ? '' : ({ n: '\n', r: '\r', t: '\t', b: '\b', f: '\f', v: '\v', '0': '\0' } as Record<string, string>)[c] ?? c);
}
function embeddedCalls(script: string): { name: string; input: any }[] {
  const result: { name: string; input: any }[] = [];
  const calls = /\btools\.(exec_command|apply_patch)\s*\(/g;
  let match;
  while ((match = calls.exec(script))) {
    const start = calls.lastIndex;
    let depth = 1, quote = '', escaped = false, end = start;
    for (; end < script.length && depth; end++) {
      const c = script[end];
      if (quote) { if (escaped) escaped = false; else if (c === '\\') escaped = true; else if (c === quote) quote = ''; }
      else if (['"', "'", '`'].includes(c)) quote = c;
      else if (c === '(') depth++;
      else if (c === ')') depth--;
    }
    if (depth) continue;
    const argument = script.slice(start, end - 1);
    if (match[1] === 'apply_patch') {
      const raw = argument.match(new RegExp('^\\s*' + literal + '\\s*$'))?.[1];
      const patch = raw ? decodeLiteral(raw) : null;
      if (patch !== null) result.push({ name: 'apply_patch', input: patch });
    } else {
      const input: Record<string, string> = {};
      const fields = new RegExp('["\']?(workdir|cwd|working_directory|cmd|command)["\']?\\s*:\\s*' + literal, 'g');
      let field;
      while ((field = fields.exec(argument))) { const value = decodeLiteral(field[2]); if (value !== null) input[field[1]] = value; }
      result.push({ name: 'exec_command', input });
    }
    calls.lastIndex = end;
  }
  return result;
}
function callPaths(name: string, input: any, cwd: string, home: string): string[] {
  const result: string[] = [];
  const resolve = (s: string, base: string) => s.startsWith('~/') ? path.join(home, s.slice(2)) : path.isAbsolute(s) ? s : base && path.isAbsolute(base) ? path.resolve(base, s) : '';
  const add = (s: unknown, base: string) => { if (typeof s === 'string' && s.length <= 8192 && !s.includes('\0')) { const p = resolve(s, base); if (p) result.push(p); } };
  let base = cwd;
  // Native Codex code-mode wrappers contain literal calls to these tools. Read
  // their literal arguments without evaluating JavaScript or interpolations.
  if (/(?:^|\.)exec$/.test(name) && typeof input === 'string') for (const call of embeddedCalls(input)) result.push(...callPaths(call.name, call.input, cwd, home));
  if (input && typeof input === 'object') {
    for (const key of directoryFields) if (typeof input[key] === 'string') { base = resolve(input[key], cwd); add(base, cwd); break; }
    let nodes = 0;
    const walk = (value: any, depth: number) => {
      if (!value || typeof value !== 'object' || depth > 8 || ++nodes > 4096) return;
      for (const [key, child] of Object.entries(value)) {
        if (pathFields.has(key)) add(child, base);
        else if (child && typeof child === 'object') walk(child, depth + 1);
      }
    };
    walk(input, 0);
    for (const key of ['command', 'cmd']) {
      const command = input[key];
      if (typeof command === 'string') result.push(...shellPaths(command, base));
      else if (Array.isArray(command)) {
        const parts = command.filter((part): part is string => typeof part === 'string');
        result.push(...shellPaths(parts.map(part => JSON.stringify(part)).join(' '), base));
        for (const part of parts) result.push(...shellPaths(part, base));
      }
    }
  }
  if (/(?:^|[._])apply_patch$/.test(name)) {
    const patch = typeof input === 'string' ? input : input?.patch ?? input?.input;
    if (typeof patch === 'string') {
      for (const line of patch.split('\n')) {
        const file = line.match(/^\*\*\* (?:Add File|Update File|Delete File|Move to): (.+)$/)?.[1] ?? line.match(/^(?:---|\+\+\+) [ab]\/(.+)$/)?.[1];
        if (file) add(file, base);
      }
    }
  }
  if (!result.length && /(?:bash|shell|exec_command|shell_command)$/i.test(name)) add(base, cwd);
  return result;
}

// Names that differ only in letter case are one repository to the cloud, which
// refuses duplicates: merge their counts under the spelling seen most often.
export function topRepositories(repos: RepoTouch[]): RepoTouch[] {
  const merged = new Map<string, { name: string; best: number; events: number }>();
  for (const r of repos) {
    if (!r || !repositoryName(r.name) || !Number.isSafeInteger(r.events) || r.events < 1) continue;
    const key = r.name.toLowerCase(), entry = merged.get(key);
    if (!entry) { merged.set(key, { name: r.name, best: r.events, events: r.events }); continue; }
    entry.events = Math.min(Number.MAX_SAFE_INTEGER, entry.events + r.events);
    if (r.events > entry.best || (r.events === entry.best && r.name < entry.name)) { entry.name = r.name; entry.best = r.events; }
  }
  return [...merged.values()].map(r => ({ name: r.name, events: r.events }))
    .sort((a, b) => b.events - a.events || (a.name < b.name ? -1 : a.name > b.name ? 1 : 0)).slice(0, 20);
}

export class Evidence {
  private repositories: Repositories;
  constructor(private home: string, private state: State, private seen: Seen, private start: number, private end: number, private tick: () => void) { this.repositories = new Repositories(home); }
  observe(tool: Tool, ctx: EvidenceContext, record: Record<string, any>) {
    const p = record.payload ?? {};
    if (tool === 'claude_code') {
      if (typeof record.cwd === 'string') ctx.cwd = record.cwd.slice(0, 4096);
      if (typeof record.sessionId === 'string') ctx.session = record.sessionId.slice(0, 512);
      if (Object.hasOwn(record, 'gitBranch')) ctx.gitBranch = branchName(record.gitBranch);
    } else if (record.type === 'session_meta' || record.type === 'turn_context') {
      if (typeof p.cwd === 'string') ctx.cwd = p.cwd.slice(0, 4096);
      if (record.type === 'session_meta') ctx.session = String(p.id ?? p.session_id ?? ctx.session).slice(0, 512);
      if (p.git && Object.hasOwn(p.git, 'branch')) ctx.gitBranch = branchName(p.git.branch);
    }
    if (typeof record.timestamp === 'string' && Number.isFinite(Date.parse(record.timestamp))) ctx.time = Date.parse(record.timestamp);
    const time = ctx.time;
    if (!ctx.session || time === undefined || time < this.start || time >= this.end) return;
    const key = crypto.createHash('sha256').update(tool + ':' + ctx.session).digest('hex').slice(0, 24);
    const evidence = (this.state.evidence ??= {})[key] ??= { repos: [], gitBranch: null, branchAt: -1, lastSeen: time };
    evidence.lastSeen = Math.max(evidence.lastSeen, time);
    const setBranch = (branch: string | null) => { if (time >= evidence.branchAt) { evidence.gitBranch = branch; evidence.branchAt = time; } };
    if (ctx.gitBranch !== undefined) setBranch(ctx.gitBranch);
    else { const repo = this.repositories.find(ctx.cwd); if (repo) setBranch(repo.branch); }
    const calls: { name: string; input: any; id: unknown }[] = [];
    if (tool === 'claude_code' && record.type === 'assistant' && Array.isArray(record.message?.content)) {
      for (const call of record.message.content) if (call?.type === 'tool_use' && typeof call.name === 'string') calls.push({ name: call.name, input: call.input, id: call.id });
    } else if (tool === 'codex' && record.type === 'response_item' && ['function_call', 'custom_tool_call'].includes(p.type) && typeof p.name === 'string') {
      let input = p.input ?? p.arguments;
      if (typeof input === 'string') { try { input = JSON.parse(input); } catch { if (!/(?:^|[._])apply_patch$|(?:^|\.)exec$/.test(p.name)) input = null; } }
      calls.push({ name: p.name, input, id: p.call_id ?? p.id });
    }
    for (const [ordinal, call] of calls.entries()) {
      const id = crypto.createHash('sha256').update(JSON.stringify([tool, ctx.session, call.id || [record.message?.id ?? record.requestId ?? record.uuid ?? null, record.timestamp ?? ctx.time, ordinal, call.name, call.input]])).digest('hex').slice(0, 24);
      if (this.seen.has(id)) continue;
      this.seen.add(id, Math.floor(time / 3_600_000));
      const touched = new Map<string, Repository>();
      for (const file of callPaths(call.name, call.input, ctx.cwd, this.home)) { const repo = this.repositories.find(file); if (repo) touched.set(repo.name, repo); }
      for (const repo of touched.values()) {
        const entry = evidence.repos.find(r => r.name === repo.name);
        if (entry) entry.events = Math.min(Number.MAX_SAFE_INTEGER, entry.events + 1);
        else evidence.repos.push({ name: repo.name, events: 1 });
        if (ctx.gitBranch === undefined && !this.repositories.find(ctx.cwd)) setBranch(repo.branch);
      }
      this.tick();
    }
  }
}
