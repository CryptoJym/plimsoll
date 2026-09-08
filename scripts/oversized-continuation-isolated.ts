/** Use the existing disposable-home helper; source proofs must never resolve
 * default native identity/configuration paths from an ambient agent session. */
import fs from 'node:fs';
import path from 'node:path';
import {spawnSync} from 'node:child_process';
import {isolatedEnvironment} from './run-proof';
const source=path.resolve(__dirname,'..');
const entry=process.argv[2];if(!entry)throw new Error('oversized proof entry required');
const base=process.env.PLIMSOLL_OVERSIZED_TEST_ROOT;if(!base)throw new Error('owned disposable test root required');
fs.mkdirSync(base,{recursive:true});const root=fs.mkdtempSync(path.join(base,'isolated-'));
const env=isolatedEnvironment(root);
for(const key of ['PLIMSOLL_OVERSIZED_LEGACY_SOURCE','PLIMSOLL_OVERSIZED_INPUT_MANIFEST','SOURCE_DATE_EPOCH'])if(process.env[key])env[key]=process.env[key];
const result=spawnSync(process.execPath,['--import',path.join(source,'node_modules/tsx/dist/loader.mjs'),path.resolve(source,entry),...process.argv.slice(3)],{cwd:source,env,stdio:'inherit',timeout:240000});
console.log(JSON.stringify({schema:'oversized-proof-isolation.v1',entry,disposableHome:root,ambientEnvironment:'allowlist',exitCode:result.status,signal:result.signal,error:result.error?.message??null}));
process.exitCode=result.status??1;
