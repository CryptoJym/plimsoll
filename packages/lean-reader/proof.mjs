import './build.mjs';
import { spawnSync } from 'node:child_process';
import { readdirSync } from 'node:fs';
const tests = readdirSync(new URL('./test/', import.meta.url)).filter(name => name.endsWith('.test.mjs')).sort().map(name => new URL('./test/' + name, import.meta.url).pathname);
const result = spawnSync(process.execPath, ['--test', ...tests], { stdio: 'inherit' });
if (result.error) throw result.error;
process.exitCode = result.status ?? 1;
if (process.exitCode === 0) {
  const pilot = spawnSync(process.execPath, [new URL('./pilot-check.mjs', import.meta.url).pathname], { stdio: 'inherit' });
  if (pilot.error) throw pilot.error;
  process.exitCode = pilot.status ?? 1;
}
