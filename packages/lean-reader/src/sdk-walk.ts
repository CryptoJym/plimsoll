import fs from 'node:fs';
import path from 'node:path';
import { protectedPath, resolvedPath } from './evidence.js';
import { saveJson } from './state-json.js';
import type { SdkWalker, SdkWalkStats, SourceFile } from './types.js';

// Studio0 keeps 170,000 SDK transcripts under 137 glob roots, and the generic
// walk stat'ed every one each pass (33 s at load 40) to find the few hundred
// still young. A folder's mtime moves when a name is added or removed, never
// when a transcript is appended: an unchanged folder needs only last pass's
// young names stat'ed, a changed one only the names not already settled (mtime
// behind the root's watermark). The hourly sweep stats every name again, which
// bounds how late an append to a settled transcript is read: one hour and a
// pass. The index is a cache beside the state; lost or damaged, it costs one
// sweep, and an older bundle ignores it.
const VERSION = 1, SWEEP = 60 * 60_000, LIMIT = 32 * 1024 * 1024, SUFFIX = '.jsonl';
type Folder = { mtime: number; since: number; settled: string; young: string[]; links: string[]; dirs: string[] };
type Index = { version: number; sweptAt: number; dirs: Record<string, Folder> };
const same = (a: Folder, b: Folder) => a.mtime === b.mtime && a.since === b.since && a.settled === b.settled &&
  a.young.join('\n') === b.young.join('\n') && a.links.join('\n') === b.links.join('\n') && a.dirs.join('\n') === b.dirs.join('\n');

export class SdkWalk implements SdkWalker {
  private index: Index;
  private next: Record<string, Folder> = {};
  private sweep: boolean;
  private dirty = false;
  private counts: SdkWalkStats = { roots: 0, dirs: 0, indexed: 0, stat: 0, files: 0, walkMs: 0, swept: false };
  constructor(private file: string, private now: number) {
    let index: Index | undefined;
    try {
      if (fs.statSync(file).size <= LIMIT) index = JSON.parse(fs.readFileSync(file, 'utf8'));
      if (index?.version !== VERSION || typeof index.sweptAt !== 'number' || !index.dirs || typeof index.dirs !== 'object') index = undefined;
    } catch { /* A missing or damaged index costs one sweep. */ }
    this.sweep = !index || now - index.sweptAt >= SWEEP || now < index.sweptAt;
    this.index = index ?? { version: VERSION, sweptAt: now, dirs: {} };
    this.counts.swept = this.sweep;
  }
  stats(): SdkWalkStats { return { ...this.counts, walkMs: Math.round(this.counts.walkMs) }; }
  *files(root: string, visited: Set<string>, since: number, home: string): Generator<SourceFile> {
    const started = performance.now();
    this.counts.roots++;
    try { yield* this.walk(root, visited, since, home, resolvedPath(root)); }
    finally { this.counts.walkMs += performance.now() - started; }
  }
  private stat(file: string): fs.Stats | undefined {
    this.counts.stat++;
    try { return fs.statSync(file); } catch (e) { if ((e as NodeJS.ErrnoException).code === 'ENOENT') return undefined; throw new Error('source_unreadable'); }
  }
  private *walk(dir: string, visited: Set<string>, since: number, home: string, resolved: boolean): Generator<SourceFile> {
    if (protectedPath(dir, home, resolved)) return;
    const stat = this.stat(dir);
    if (!stat) return;
    const key = `${stat.dev}:${stat.ino}`;
    if (visited.has(key)) return;
    visited.add(key);
    this.counts.dirs++;
    const known = this.index.dirs[dir];
    // A watermark that moved back (a backfill, or a root without a complete
    // pass for eight days) makes every settled name young again.
    const trusted = !this.sweep && known && known.since <= since ? known : undefined;
    const folder: Folder = { mtime: stat.mtimeMs, since, settled: '', young: [], links: [], dirs: [] };
    const settled: string[] = [], names: string[] = [];
    if (trusted && trusted.mtime === stat.mtimeMs) {
      this.counts.indexed++;
      if (trusted.settled) settled.push(trusted.settled);
      names.push(...trusted.young); folder.links = trusted.links; folder.dirs = trusted.dirs;
    } else {
      const old = new Set(trusted?.settled ? trusted.settled.split('\n') : []);
      let entries: fs.Dirent[];
      try { entries = fs.readdirSync(dir, { withFileTypes: true }); } catch (e) { if ((e as NodeJS.ErrnoException).code === 'ENOENT') return; throw new Error('source_unreadable'); }
      for (const entry of entries) {
        if (entry.isDirectory()) folder.dirs.push(entry.name);
        else if (entry.isSymbolicLink()) folder.links.push(entry.name);
        else if (entry.isFile() && entry.name.endsWith(SUFFIX)) (old.has(entry.name) ? settled : names).push(entry.name);
      }
      folder.dirs.sort(); folder.links.sort(); names.sort();
    }
    const found: SourceFile[] = [], linked: string[] = [];
    for (const name of names) {
      const file = path.join(dir, name), s = this.stat(file);
      if (!s || !s.isFile()) continue;
      if (s.mtimeMs < since) { settled.push(name); continue; }
      folder.young.push(name); found.push({ file, stat: s });
    }
    // Aliases are stat'ed every pass; their targets never settle here.
    for (const name of folder.links) {
      const file = path.join(dir, name);
      if (protectedPath(file, home)) continue;
      let s: fs.Stats;
      try { this.counts.stat++; s = fs.statSync(file); } catch { continue; }
      if (s.isDirectory()) linked.push(file);
      else if (s.isFile() && name.endsWith(SUFFIX) && s.mtimeMs >= since) { try { found.push({ file: fs.realpathSync(file), stat: s }); } catch { /* Alias target disappeared. */ } }
    }
    folder.settled = settled.join('\n');
    if (!known || !same(known, folder)) this.dirty = true;
    this.next[dir] = folder;
    this.counts.files += found.length;
    yield* found;
    for (const name of folder.dirs) yield* this.walk(path.join(dir, name), visited, since, home, resolved);
    for (const file of linked) yield* this.walk(file, visited, since, home, false);
  }
  save(): void {
    // Folders a capped root did not reach keep last pass's record until the
    // next sweep, which rebuilds the index from the folders that still exist.
    const dirs = this.sweep ? this.next : { ...this.index.dirs, ...this.next };
    if (!this.sweep && !this.dirty && Object.keys(dirs).length === Object.keys(this.index.dirs).length) return;
    try { saveJson(this.file, { version: VERSION, sweptAt: this.sweep ? this.now : this.index.sweptAt, dirs }, LIMIT); }
    catch { try { fs.unlinkSync(this.file); } catch { /* Already absent. */ } }
  }
}
