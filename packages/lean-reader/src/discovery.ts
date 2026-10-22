import fs from 'node:fs';
import path from 'node:path';
import { roots } from './reader.js';
import { HOUR, WINDOW, type Options, type Source, type SourceFile } from './types.js';

// At most four outstanding metadata reads. libuv retains its native pool limit
// (including any explicit UV_THREADPOOL_SIZE); no worker or daemon is launched.
async function files(root: string, visited: Set<string>, since: number): Promise<SourceFile[]> {
  const entries: { name: string; directory?: string; source?: SourceFile }[] = [];
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
        if (entry.isDirectory()) { entries.push({ name, directory: file }); continue; }
        if (!symlink && !(entry.isFile() && name.endsWith('.jsonl'))) continue;
        pending.push((async () => {
          try {
            const s = await fs.promises.stat(file);
            if (symlink && s.isDirectory()) entries.push({ name, directory: file });
            else if (name.endsWith('.jsonl') && s.mtimeMs >= since && (!symlink || s.isFile())) entries.push({ name, source: { file: symlink ? fs.realpathSync(file) : file, stat: s } });
          } catch { if (!symlink) throw new Error('source_unreadable'); }
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
    if (entry.directory) result.push(...await files(entry.directory, visited, since));
    else result.push(entry.source!);
  }
  return result;
}

export async function discover(options: Options & { home: string; now: number }): Promise<Source[]> {
  const since = options.since ? Date.parse(options.since) : Math.floor((options.now - WINDOW) / HOUR) * HOUR;
  const until = options.until ? Date.parse(options.until) : options.now + HOUR;
  if (!Number.isFinite(since) || !Number.isFinite(until) || since >= until) throw new Error('invalid_window');
  const visited = new Set<string>(), result: Source[] = [];
  for (const root of roots(options.home, options.env ?? process.env, options.config ?? {})) result.push({ ...root, files: await files(root.path, visited, Math.floor(since / HOUR) * HOUR) });
  return result;
}
