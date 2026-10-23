import './build.mjs';
import { spawnSync } from 'node:child_process';
const result = spawnSync(process.execPath, ['--test', new URL('./test/reader.test.mjs', import.meta.url).pathname], { stdio: 'inherit' });
if (result.error) throw result.error;
process.exitCode = result.status ?? 1;
