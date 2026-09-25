/** Restore the shared early-stop guard in an isolated CI fixture copy. */
import assert from "node:assert/strict";
import {spawnSync} from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const repo=path.resolve(import.meta.dirname,"../..");
const root=fs.mkdtempSync(path.join(fs.realpathSync(process.env.PLIMSOLL_PROOF_HOME ?? os.tmpdir()),"payload-cap-mutant-"));
try {
  const mutant=path.join(root,"source");
  fs.cpSync(repo,mutant,{recursive:true,filter:(candidate)=>
    !path.relative(repo,candidate).split(path.sep).some(part=>[".git","node_modules","ci-home","dist"].includes(part))});
  fs.symlinkSync(path.join(repo,"node_modules"),path.join(mutant,"node_modules"),"dir");
  const target=path.join(mutant,"packages/collector-cli/src/capture-record-loss.ts");
  const source=fs.readFileSync(target,"utf8");
  const before=`if (!probe.escaped) {
    probe.escaped=bytes.includes(0x5c);
    if (!probe.escaped) {
      // Each key must keep scanning until its own counter saturates. A full
      // payload counter must not stop a later usage type from being seen.
      if (probe.typeCount<3) count(TYPE_KEY,"typeMatch","typeCount",3);
      if (probe.payloadCount<2) count(PAYLOAD_KEY,"payloadMatch","payloadCount",2);
    }
  }`;
  const after=`if (!probe.escaped && probe.typeCount<3 && probe.payloadCount<2) {
    probe.escaped=bytes.includes(0x5c);
    if (!probe.escaped) {count(TYPE_KEY,"typeMatch","typeCount",3);count(PAYLOAD_KEY,"payloadMatch","payloadCount",2);}
  }`;
  assert(source.includes(before),"early-stop mutation anchor missing");
  fs.writeFileSync(target,source.replace(before,after));
  const run=spawnSync(process.execPath,[path.join(repo,"node_modules/tsx/dist/cli.mjs"),
    "scripts/lib/oversized-duplicate-type-claim-case.mts"],{cwd:mutant,
    encoding:"utf8",timeout:1_800_000,maxBuffer:1024*1024});
  const rejected=run.status===1 && run.stderr?.includes("type must keep scanning after the payload cap") &&
    run.stderr?.includes("1 !== 2");
  console.log(JSON.stringify({schema:"plimsoll.payload-cap-mutation/v1",rejected,
    exitCode:run.status,reason:rejected?"type counter stopped after payload cap":run.stderr?.slice(-500)},null,2));
  assert(rejected,"the restored early stop escaped the discriminator probe proof");
} finally {fs.rmSync(root,{recursive:true,force:true});}
