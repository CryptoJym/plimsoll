import fs from 'node:fs';
import path from 'node:path';
import { protectedPath } from './evidence.js';

/** Asterisks match within one path component; never traverse secret stores. */
export function globRoots(pattern: string, home: string): string[] {
  const parts = path.resolve(pattern).split(path.sep).filter(Boolean);
  let roots = [path.parse(path.resolve(pattern)).root];
  for (const part of parts) {
    const next: string[] = [];
    const expression = new RegExp('^' + part.split('*').map(p => p.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).join('[^/]*') + '$');
    for (const root of roots) {
      if (protectedPath(root, home)) continue;
      let names: string[];
      try { names = part.includes('*') ? fs.readdirSync(root).filter(n => expression.test(n)).sort() : [part]; } catch (e) {
        // Another user's temp folder under /var/folders is not ours to read; skip it like a missing one.
        if (['ENOENT', 'EACCES', 'EPERM'].includes((e as NodeJS.ErrnoException).code ?? '')) continue;
        throw new Error('source_unreadable');
      }
      for (const name of names) {
        const candidate = path.join(root, name);
        try {
          if (protectedPath(candidate, home) || !fs.statSync(candidate).isDirectory()) continue;
          const real = fs.realpathSync(candidate);
          if (!protectedPath(real, home)) next.push(real);
        } catch (e) { if (!['ENOENT', 'EACCES', 'EPERM'].includes((e as NodeJS.ErrnoException).code ?? '')) throw new Error('source_unreadable'); }
      }
    }
    roots = [...new Set(next)];
  }
  return roots;
}
