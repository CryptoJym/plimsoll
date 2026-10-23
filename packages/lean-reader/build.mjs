import { build } from 'esbuild';
import fs from 'node:fs';
import { builtinModules } from 'node:module';
const result = await build({ entryPoints: [new URL('./src/cli.ts', import.meta.url).pathname], outfile: new URL('./dist/plimsoll-lean.mjs', import.meta.url).pathname, bundle: true, platform: 'node', target: 'node22', format: 'esm', metafile: true });
const builtins = new Set(builtinModules.map(n => n.replace(/^node:/, '')));
for (const output of Object.values(result.metafile.outputs)) for (const item of output.imports) if (!item.external || !builtins.has(item.path.replace(/^node:/, ''))) throw new Error('runtime dependency: ' + item.path);
fs.chmodSync(new URL('./dist/plimsoll-lean.mjs', import.meta.url), 0o755);
console.log('Built dist/plimsoll-lean.mjs (ESM, Node 22 built-ins only)');
