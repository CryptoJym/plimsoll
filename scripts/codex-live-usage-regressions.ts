/** Run unchanged affected proofs with the existing isolated environment helper. */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import crypto from "node:crypto";
import {spawnSync} from "node:child_process";
import {isolatedEnvironment} from "./run-proof";
const repo=process.cwd(), out=path.join(repo,"evidence","live-intake-regressions");
fs.mkdirSync(out,{recursive:true});
const proofs=["live-usage-metadata-proof","authenticated-ingestion-proof","usage-dedupe-proof","local-http-boundary-proof","privacy-mode-proof","outbox-proof"];
const receipts:unknown[]=[];
for(const name of proofs) {
  const entry=path.join(repo,"scripts",name+".ts"), hash=()=>crypto.createHash("sha256").update(fs.readFileSync(entry)).digest("hex"), before=hash();
  const root=fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(),"live-regression-"))), env=isolatedEnvironment(root);
  const startedAt=new Date().toISOString();
  try {
    const result=spawnSync(process.execPath,["--max-old-space-size=1536","--import",path.join(repo,"node_modules/tsx/dist/loader.mjs"),entry],
      {cwd:repo,env:{...env,GOMAXPROCS:"2"},encoding:"utf8",timeout:180000,maxBuffer:8*1024*1024});
    fs.writeFileSync(path.join(out,name+".txt"),result.stdout+"\n"+result.stderr);
    assert.equal(result.status,0,name+" exit");assert.equal(hash(),before,name+" source unchanged");
    let summary:any;
    if(name==="authenticated-ingestion-proof") {
      const done=JSON.parse(fs.readFileSync(env.PLIMSOLL_PROOF_RECEIPT!,"utf8"));
      assert.equal(done.completed,true);assert.equal(done.status,"passed");assert.equal(done.expectedChecks,10);
      assert.equal(done.counts.total,10);assert.equal(done.counts.failed,0);assert.ok(done.checks.every((c:any)=>c.passed===true));
      summary={checks:10,passed:10,failed:0};
    } else if(name==="usage-dedupe-proof") {
      const matches=[...result.stdout.matchAll(/(?:usage-dedupe-proof|issue-193 fixtures|rework fixtures): (\d+) checks green/g)];
      assert.equal(matches.length,3);summary={checks:matches.reduce((n,m)=>n+Number(m[1]),0),passed:true};
    } else if(name==="outbox-proof") {
      const start=result.stdout.lastIndexOf('{\n  "schema": "plimsoll.outbox-proof.v1"');assert.ok(start>=0);
      summary=JSON.parse(result.stdout.slice(start).trim());assert.equal(summary.status,"pass");assert.equal(summary.failed,0);assert.equal(summary.names.length,summary.checks);
    } else {
      const line=result.stdout.trim().split("\n").reverse().find(v=>v.startsWith("{"));assert.ok(line);summary=JSON.parse(line);
      if(name==="live-usage-metadata-proof") {assert.equal(summary.state,"PASS_SHARED_LIVE_INTERFACE_ONLY");assert.equal(summary.checks.length,12);}
      else if(name==="privacy-mode-proof") {assert.equal(summary.passed,true);assert.deepEqual(summary.failures,[]);}
      else {assert.equal(summary.failed,0);assert.equal(summary.passed,summary.checks);assert.ok(summary.checks>0);}
    }
    receipts.push({name,sourceSha256:before,startedAt,finishedAt:new Date().toISOString(),status:"passed",summary});
    fs.writeFileSync(path.join(out,"receipt.json"),JSON.stringify({schema:"plimsoll.live-intake-regressions.v1",complete:receipts.length===proofs.length,receipts},null,2)+"\n");
    console.log(JSON.stringify({name,status:"passed",checks:Array.isArray(summary.checks)?summary.checks.length:summary.checks}));
  } finally {fs.rmSync(root,{recursive:true,force:true});}
}
