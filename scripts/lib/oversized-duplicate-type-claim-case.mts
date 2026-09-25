import crypto from "node:crypto";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {pathToFileURL} from "node:url";

const source = path.resolve(import.meta.dirname,"../..");
const load = (relative:string) => import(pathToFileURL(path.join(source,relative)).href);
const [{LocalEventBuffer},{CollectorMaintenance},{RolloutTailer},{TranscriptTailer},
  {captureBaselineStatus},{captureFrontier},{DEFAULT_JSONL_TAILER_IO,jsonlScanStateKey},
  {rootCursorKey},{classifySkippedRecord,newSkippedDiscriminatorProbe,observeSkippedDiscriminators,proveSkippedNonUsage}] = await Promise.all([
  load("packages/collector-cli/src/buffer.ts"),load("packages/collector-cli/src/maintenance.ts"),
  load("packages/collector-cli/src/rollout-tailer.ts"),load("packages/collector-cli/src/transcript-tailer.ts"),
  load("packages/collector-cli/src/capture-baseline.ts"),load("packages/collector-cli/src/capture-frontier.ts"),
  load("packages/collector-cli/src/jsonl-byte-tailer.ts"),load("packages/collector-cli/src/capture-root-inventory.ts"),
  load("packages/collector-cli/src/capture-record-loss.ts"),
]);

if (process.env.PLIMSOLL_SKIP_CLASSIFIER_CASES !== "1") {
  assert.deepEqual(classifySkippedRecord("codex",Buffer.from('{"type":"session_meta","padding":"'+"x".repeat(2048))),
    {kind:"unknown",usagePossible:true},"a truncated prefix cannot prove non-usage");
  const splitProbe=newSkippedDiscriminatorProbe();
  observeSkippedDiscriminators(splitProbe,Buffer.from('{"type":"user","padding":"abc","ty'));
  observeSkippedDiscriminators(splitProbe,Buffer.from('pe":"assistant"}'));
  assert.equal(proveSkippedNonUsage("top_type",splitProbe),false,"a key split across slices must remain visible");
  const plainProbe=newSkippedDiscriminatorProbe();
  observeSkippedDiscriminators(plainProbe,Buffer.from('{"type":"user","padding":"plain"}'));
  assert.equal(proveSkippedNonUsage("top_type",plainProbe),true,"a complete unique type is known non-usage");
}

type Provider = "codex"|"claude";
type Case = {name:string;provider:Provider;build:(id:string,stamp:string,pad:string)=>string};
const usage = {type:"token_count",info:{total_token_usage:{input_tokens:2,cached_input_tokens:0,output_tokens:0,reasoning_output_tokens:0}}};
const codexPayload=(pad:string)=>({...usage,rate_limits:{plan_type:pad.length?"q".repeat(5000):"q"}});
const claudeModel=(pad:string)=>pad.length?"q".repeat(5000):"claude-opus-5";
const cases:Case[] = [
  {name:"duplicate_codex_type",provider:"codex",build:(id,s,p)=>
    `{"type":"session_meta","padding":${JSON.stringify(p)},"type":"event_msg","timestamp":${JSON.stringify(s)},"payload":${JSON.stringify(codexPayload(p))}}`},
  {name:"duplicate_claude_type",provider:"claude",build:(id,s,p)=>
    `{"type":"user","padding":${JSON.stringify(p)},"type":"assistant","sessionId":${JSON.stringify(id)},"timestamp":${JSON.stringify(s)},"message":{"id":"usage-1","model":${JSON.stringify(claudeModel(p))},"usage":{"input_tokens":11,"output_tokens":1}}}`},
  {name:"duplicate_codex_payload_type",provider:"codex",build:(id,s,p)=>
    `{"type":"event_msg","timestamp":${JSON.stringify(s)},"payload":{"type":"user_message","padding":${JSON.stringify(p)},"type":"token_count","info":${JSON.stringify(usage.info)},"rate_limits":${JSON.stringify(codexPayload(p).rate_limits)}}}`},
];

const fixtureRoot=fs.mkdtempSync(path.join(fs.realpathSync(process.env.PLIMSOLL_PROOF_HOME ?? os.tmpdir()),"duplicate-type-claim-"));
const results:unknown[]=[];
try {
  for (const c of cases) {
    const home=path.join(fixtureRoot,c.name), codexHome=path.join(home,"codex"),claudeHome=path.join(home,"claude");
    fs.mkdirSync(codexHome,{recursive:true});fs.mkdirSync(claudeHome,{recursive:true});
    const options={workspaceId:"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",delivery:{enabled:true}};
    const buffer=new LocalEventBuffer(path.join(home,"ledger.sqlite"),options);
    const epoch=buffer.workspaceBinding().currentInstallationEpochId;
    const roots=[{source:"codex",directory:codexHome,rootId:"codex",profileId:"codex",installationEpochId:epoch},
      {source:"claude_code",directory:claudeHome,rootId:"claude",profileId:"claude",installationEpochId:epoch}];
    const maintenance=new CollectorMaintenance(buffer,
      new RolloutTailer(buffer,undefined,()=>[],DEFAULT_JSONL_TAILER_IO,roots.slice(0,1)),
      new TranscriptTailer(buffer,undefined,DEFAULT_JSONL_TAILER_IO,roots.slice(1)),
      undefined,undefined,{captureCoverageIntervalMs:0,captureCoverageTurnMs:250});
    try {
      for(let i=0;i<40&&captureBaselineStatus(buffer.database).status!=="complete";i++) await maintenance.runRecent();
      if(captureBaselineStatus(buffer.database).status!=="complete") throw new Error(`${c.name}: baseline incomplete`);
      await new Promise(resolve=>setTimeout(resolve,10));
      const id=crypto.randomUUID(), controlId=crypto.randomUUID(),stamp=new Date().toISOString();
      const old=new Date(Date.now()-15*86_400_000).toISOString().slice(0,10).split("-");
      const folder=c.provider==="codex"?path.join(codexHome,...old):path.join(claudeHome,"project");
      fs.mkdirSync(folder,{recursive:true});
      const name=(x:string)=>path.join(folder,c.provider==="codex"?`rollout-${x}.jsonl`:`${x}.jsonl`);
      const file=name(id),control=name(controlId);
      const pre=(x:string)=>c.provider==="codex"?
        [`{"type":"session_meta","timestamp":${JSON.stringify(stamp)},"payload":{"id":${JSON.stringify(x)}}}`,
         `{"type":"turn_context","timestamp":${JSON.stringify(stamp)},"payload":{"model":"gpt-6-sol"}}`,
         `{"type":"event_msg","timestamp":${JSON.stringify(stamp)},"payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":0,"cached_input_tokens":0,"output_tokens":0,"reasoning_output_tokens":0}}}}`]:[];
      const large=c.build(id,stamp,"x".repeat(17*1024*1024));
      const small=c.build(controlId,stamp,"");
      const parsed=JSON.parse(small);
      const validUsage=c.provider==="codex"?parsed.type==="event_msg"&&parsed.payload?.type==="token_count":parsed.type==="assistant"&&!!parsed.message?.usage;
      fs.writeFileSync(file,[...pre(id),large].join("\n")+"\n");
      fs.writeFileSync(control,[...pre(controlId),small].join("\n")+"\n");
      const cursor=(f:string)=>(buffer.database.prepare("select committed_offset as offset from rollout_scan_state where file=?")
        .get(jsonlScanStateKey(rootCursorKey(roots,f))) as {offset:number}|undefined)?.offset;
      let turns=0,bytesRead=0;
      const started=process.cpuUsage();
      for(;turns<220&&(cursor(file)!==fs.statSync(file).size||cursor(control)!==fs.statSync(control).size);turns++) {
        const run=await maintenance.runRecent();
        bytesRead+=run.rollout.bytesRead+run.transcript.bytesRead;
      }
      const run=await maintenance.runRecent();
      bytesRead+=run.rollout.bytesRead+run.transcript.bytesRead;
      const cpu=process.cpuUsage(started),cpuMs=(cpu.user+cpu.system)/1000;
      const hasLossTable=!!buffer.database.prepare("select 1 from sqlite_master where type='table' and name='capture_record_losses'").get();
      const receipts=hasLossTable?buffer.database.prepare(`select source,kind,usage_possible as usagePossible,skipped_bytes as bytes from capture_record_losses`).all():[];
      const event=(x:string)=>buffer.database.prepare(`select count(*) as rows,coalesce(sum(input_tokens),0) as tokens from buffered_events where session_id=?`).get(x);
      const gaps=captureFrontier(buffer.database)?.gaps??[];
      const eof=cursor(file)===fs.statSync(file).size&&cursor(control)===fs.statSync(control).size;
      const row=receipts[0] as {usagePossible:number,kind:string}|undefined;
      const passed=validUsage&&eof&&receipts.length===1&&row?.usagePossible===1&&gaps.length>0&&
        (event(controlId) as {tokens:number}).tokens===(c.provider==="codex"?2:11);
      const debug={cursor:cursor(file),controlCursor:cursor(control),
        continuations:buffer.database.prepare("select provider,envelope_json from jsonl_continuations").all().length};
      results.push({name:c.name,provider:c.provider,bytes:Buffer.byteLength(large),bytesRead,cpuMs,validUsage,turns,eof,receipts,debug,
        skippedEvent:event(id),controlEvent:event(controlId),gaps,passed});
    } finally {maintenance.close();buffer.close();fs.rmSync(home,{recursive:true,force:true});}
  }
  console.log(JSON.stringify({schema:"plimsoll.oversized-duplicate-type-claim/v1",results,passed:results.every((r:any)=>r.passed)},null,2));
  if(results.some((r:any)=>!r.passed)) process.exitCode=1;
} finally {fs.rmSync(fixtureRoot,{recursive:true,force:true});}
