/** The real claim proof must reject the old prefix-only non-usage decision.
 * This mutant is written only beneath the CI fixture home. */
import assert from "node:assert/strict";
import {spawnSync} from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const repo = path.resolve(import.meta.dirname,"../..");
const root = fs.mkdtempSync(path.join(fs.realpathSync(process.env.PLIMSOLL_PROOF_HOME ?? os.tmpdir()),"duplicate-type-mutant-"));
try {
  const mutant=path.join(root,"source");
  fs.cpSync(repo,mutant,{recursive:true,filter:(candidate)=>
    !path.relative(repo,candidate).split(path.sep).some(part=>[".git","node_modules","ci-home","dist"].includes(part))});
  fs.symlinkSync(path.join(repo,"node_modules"),path.join(mutant,"node_modules"),"dir");
  const target=path.join(mutant,"packages/collector-cli/src/capture-record-loss.ts");
  const source=fs.readFileSync(target,"utf8");
  const before='if (!proof || !probe || probe.escaped || probe.scanned !== recordBytes ||\n    probe.typeCount >= 3 || probe.payloadCount >= 2) return false;\n  return proof === "top_type" ? probe.typeCount === 1 :\n    probe.typeCount === 2 && probe.payloadCount === 1;';
  assert(source.includes(before),"prefix-only mutation anchor missing");
  fs.writeFileSync(target,source.replace(before,"return true;"));
  const run=spawnSync(process.execPath,[path.join(repo,"node_modules/tsx/dist/cli.mjs"),
    "scripts/lib/oversized-duplicate-type-claim-case.mts"],{cwd:mutant,
    env:{...process.env,PLIMSOLL_SKIP_CLASSIFIER_CASES:"1"},
    encoding:"utf8",timeout:1_800_000,maxBuffer:1024*1024});
  let rejected=false;
  try {
    const result=JSON.parse(run.stdout) as {passed:boolean;results:Array<{eof:boolean;receipts:Array<{usagePossible:number}>;gaps:unknown[];passed:boolean}>};
    rejected=run.status===1 && result.passed===false && result.results.length===5 &&
      result.results.every(row=>row.eof && !row.passed && row.receipts.length===1 &&
        row.receipts[0]?.usagePossible===0 && row.gaps.length===0);
  } catch { /* A crash is not a rejected behavioral mutation. */ }
  console.log(JSON.stringify({schema:"plimsoll.duplicate-type-mutation/v1",rejected,
    exitCode:run.status,stderr:rejected?undefined:run.stderr?.slice(-500)},null,2));
  assert(rejected,"prefix-only classification escaped the end-to-end claim proof");
} finally {fs.rmSync(root,{recursive:true,force:true});}
