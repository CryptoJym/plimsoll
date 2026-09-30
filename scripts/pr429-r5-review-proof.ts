import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import path from "node:path";

import { createProofCompletion } from "./lib/proof-completion";

const cases:Record<string,{script:string;args?:string[]}>= {
  "timeout-release-busy":{script:"timeout-release-busy-default-review"},
  "clock-skew-reconcile":{script:"clock-skew-reconcile-review"},
  "held-root-removal-restart":{script:"held-root-removal-restart-review"},
  "held-restart-same-roots":{script:"held-root-removal-restart-review",args:["keep-b"]},
  "startup-eof-growth":{script:"startup-eof-growth-review"},
  "promotion-rowid-overflow":{script:"promotion-rowid-overflow-review"},
  "promotion-cursor-corruption":{script:"promotion-cursor-corruption-review"},
};

const name=process.argv[2];
const scenario=name&&cases[name];
if(!scenario) throw new Error("unknown PR #429 round five proof scenario");
const proof=createProofCompletion(`pr429-r5-${name}`,1);
const repo=path.resolve(import.meta.dirname,"..");
const result=spawnSync(process.execPath,["--import",path.join(repo,"node_modules/tsx/dist/loader.mjs"),
  path.join(repo,"scripts",`pr429-r5-${scenario.script}.ts`),...(scenario.args??[])],{
  cwd:repo,env:{...process.env,NEXT_TELEMETRY_DISABLED:"1"},encoding:"utf8",
  timeout:60_000,maxBuffer:8*1024*1024,
});
if(result.stdout) process.stdout.write(result.stdout);
if(result.stderr) process.stderr.write(result.stderr);
assert.equal(result.error,undefined,`PR #429 round five fixture failed to start: ${String(result.error)}`);
assert.equal(result.signal,null,`PR #429 round five fixture terminated by ${result.signal}`);
assert.equal(result.status,0,`PR #429 round five fixture ${name} failed`);
proof.check(name);
proof.complete();
