import fs from 'node:fs';
import path from 'node:path';
import { roots } from './reader.js';
import { HOUR, WINDOW, type Options, type Source, type SourceFile, type Tool } from './types.js';
import { protectedPath, resolvedPath } from './evidence.js';
import { grokFile } from './grok.js';

// At most four outstanding metadata reads. libuv retains its native pool limit
// (including any explicit UV_THREADPOOL_SIZE); no worker or daemon is launched.
async function files(root: string, visited: Set<string>, since: number, tool: Tool, home: string, resolved = resolvedPath(root)): Promise<SourceFile[]> {
  if (tool === 'antigravity') {
    // Antigravity stores compact per-step JSONL transcripts directly at
    // <brain>/<conversation-id>/.system_generated/logs/transcript.jsonl.
    // Never walk <brain>/<id>/scratch (which may hold cloned repos or node_modules).
    const out: SourceFile[] = [];
    try {
      const names = (await fs.promises.readdir(root)).sort();
      for (const name of names) {
        const candidate = path.join(root, name, '.system_generated', 'logs', 'transcript.jsonl');
        try {
          const s = await fs.promises.stat(candidate);
          if (s.isFile() && s.mtimeMs >= since) out.push({ file: candidate, stat: s });
        } catch { /* Conversation has no compact transcript yet. */ }
      }
    } catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw new Error('source_unreadable'); }
    return out;
  }
  if (tool === 'grok' && protectedPath(root, home, resolved)) return [];
  const entries: { name: string; directory?: string; resolved?: boolean; source?: SourceFile }[] = [];
  try {
    const stat = await fs.promises.stat(root), key = `${stat.dev}:${stat.ino}`;
    if (visited.has(key)) return [];
    visited.add(key);
    const directory = fs.opendirSync(root);
    let pending: Promise<void>[] = [];
    const flush = async () => {
      const results = await Promise.allSettled(pending); pending = [];
      for (const result of results) if (result.status === 'rejected') throw result.reason;
    };
    try {
      let entry;
      while ((entry = directory.readSync())) {
        const name = entry.name, file = path.join(root, name), symlink = entry.isSymbolicLink();
        // The name filter first: only folders and updates.jsonl can matter.
        if (tool === 'grok' && ((!entry.isDirectory() && !symlink && name !== 'updates.jsonl') || protectedPath(file, home, resolved && !symlink))) continue;
        if (entry.isDirectory()) { entries.push({ name, directory: file, resolved }); continue; }
        if (!symlink && !(entry.isFile() && name.endsWith('.jsonl'))) continue;
        pending.push((async () => {
          try {
            const s = await fs.promises.stat(file);
            if (symlink && s.isDirectory()) entries.push({ name, directory: file });
            else if (name.endsWith('.jsonl') && s.mtimeMs >= since && (!symlink || s.isFile()) && (tool !== 'grok' || grokFile(file, home))) entries.push({ name, source: { file: symlink ? fs.realpathSync(file) : file, stat: s } });
          } catch (e) { if (!symlink && (e as NodeJS.ErrnoException).code !== 'ENOENT') throw new Error('source_unreadable'); }
        })());
        if (pending.length === 4) await flush();
      }
      await flush();
    } finally {
      // Settle any submitted reads even when directory enumeration fails.
      await Promise.allSettled(pending); directory.closeSync();
    }
    entries.sort((a, b) => a.name < b.name ? -1 : a.name > b.name ? 1 : 0);
  } catch (error) { if ((error as NodeJS.ErrnoException).code === 'ENOENT') return []; throw new Error('source_unreadable'); }
  const result: SourceFile[] = [];
  for (const entry of entries) {
    if (entry.directory) result.push(...await files(entry.directory, visited, since, tool, home, entry.resolved === true));
    else result.push(entry.source!);
  }
  return result;
}

export async function discover(options: Options & { home: string; now: number }): Promise<Source[]> {
  const since = options.since ? Date.parse(options.since) : Math.floor((options.now - WINDOW) / HOUR) * HOUR;
  const until = options.until ? Date.parse(options.until) : options.now + HOUR;
  if (!Number.isFinite(since) || !Number.isFinite(until) || since >= until) throw new Error('invalid_window');
  const visited = new Set<string>(), result: Source[] = [];
  for (const root of roots(options.home, options.env ?? process.env, options.config ?? {})) {
    // Many short-lived SDK transcripts: enumerate one root at a time in scan,
    // rather than retain every file's Stats until the whole pass completes.
    result.push(root.aggregate ? root : { ...root, files: await files(root.path, visited, Math.floor(since / HOUR) * HOUR, root.tool, options.home) });
  }
  return result;
}
