import assert from "node:assert/strict";
import fs from "node:fs";import path from "node:path";
import childProcess from "node:child_process";
import { syncBuiltinESMExports } from "node:module";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { beginAutomaticCaptureBaseline,completeAutomaticCaptureBaseline,sealCaptureBaselineGenerations } from "../packages/collector-cli/src/capture-baseline";
import { deriveCaptureRootIdentity } from "../packages/collector-cli/src/capture-root-inventory";
import { applyCaptureHistory } from "../packages/collector-cli/src/capture-history-import";
import { proofProcessIdentity } from "./lib/proof-process-identity";
import { proofTempRoot } from "./lib/legacy-reader";
import { createProofCompletion } from "./lib/proof-completion";
const root=proofTempRoot("process-identity"),completion=createProofCompletion("codex-history-process-identity",4);
const AT=Date.now()-300000,SESSION="22222222-2222-4222-8222-222222222222",MODEL="gpt-6.1-sol";
const original=childProcess.execFileSync;
let mode="delay";const identities:string[]=[];
// Delay the actual owner-query child by 3 seconds: the product's existing 2s
// bound refuses it. This isolates scheduling sensitivity; the review's exact
// OS failure cause is unknown. No identity output or importer result is faked.
const delayed:typeof original=((file:string,args:string[],options:any)=>{
  if(file!=="/bin/ps"||args?.[3]!=="lstart=")return original(file,args,options);
  if(mode==="denied")throw Object.assign(new Error("fixture OS query denied"),{code:"EACCES"});
  const value=mode==="delay"?original("/usr/bin/python3",["-c",
    'import os,sys,time;time.sleep(3);os.execv("/bin/ps",["/bin/ps",*sys.argv[1:]])',...args],options):original(file,args,options);
  identities.push(String(value).trim());return value;
}) as typeof original;
async function main(){let b:any,robust:ReturnType<typeof proofProcessIdentity>|undefined;try{
 const now=new Date(AT+2000);b=new LocalEventBuffer(path.join(root,"ledger.sqlite"),{workspaceId:"11111111-1111-4111-8111-111111111111",deviceId:"identity-proof",enrollmentNow:()=>new Date(AT-1000000),delivery:{enabled:true,now:()=>now}});
 const sessions=path.join(root,'sessions'),dir=path.join(sessions,...new Date(AT).toISOString().slice(0,10).split('-'));fs.mkdirSync(dir,{recursive:true});const file=path.join(dir,`rollout-review-${SESSION}.jsonl`);
 const row=(offset:number,type:string,payload:any)=>JSON.stringify({timestamp:new Date(AT+offset).toISOString(),type,payload});
 fs.writeFileSync(file,[row(10000,'session_meta',{id:SESSION}),row(10000,'turn_context',{turn_id:'review-native-turn',model:MODEL}),row(10000,'event_msg',{type:'token_count',info:{total_token_usage:{input_tokens:0,output_tokens:0,cached_input_tokens:0}}}),row(11000,'event_msg',{type:'token_count',info:{total_token_usage:{input_tokens:19,output_tokens:2,cached_input_tokens:0}}})].join('\n')+'\n');
 const r={...deriveCaptureRootIdentity('identity-proof','codex',sessions),directory:sessions,source:'codex' as const,installationEpochId:b.workspaceBinding().currentInstallationEpochId};
 const start=beginAutomaticCaptureBaseline(b.database,'codex',{startedAt:new Date(AT-2000).toISOString(),filesDiscovered:0});completeAutomaticCaptureBaseline(b.database,'codex',{runId:start.latestRun!.runId,completedAt:new Date(AT-1000).toISOString()});const stat=fs.statSync(file,{bigint:true});sealCaptureBaselineGenerations(b.database,'codex',[{path:file,device:stat.dev,inode:stat.ino,size:stat.size,birthtimeNs:stat.birthtimeNs}],new Date(AT+64000).toISOString());

 const snapshot=()=>JSON.stringify([b.database.prepare("select * from buffered_events order by rowid").all(),b.database.prepare("select * from upload_outbox order by delivery_id").all()]);
 childProcess.execFileSync=delayed;syncBuiltinESMExports();const before=snapshot();
 await assert.rejects(applyCaptureHistory(b,r),/capture_history_refused:process_identity_unavailable/);
 assert.equal(snapshot(),before);completion.check("product 2s OS-query timeout remains a financial refusal");
 robust=proofProcessIdentity();const receipt=await applyCaptureHistory(b,r);assert.equal(receipt.importedRows,1);
 const actual=original("/bin/ps",["-p",String(process.pid),"-o","lstart="],{encoding:"utf8",timeout:15000,env:{...process.env,TZ:"UTC",LC_ALL:"C"}}).trim();
 assert.ok(identities.length>0);assert.ok(identities.every(value=>value===actual));completion.check("proof scheduling allowance reads the real unchanged owner identity");
 mode="denied";const imported=snapshot();await assert.rejects(applyCaptureHistory(b,r),/capture_history_refused:process_identity_unavailable/);
 assert.equal(snapshot(),imported);completion.check("OS permission failure still refuses with robust proof scheduling");
 mode="normal";b.database.prepare("insert into capture_history_import_lock (singleton,root_id,owner_pid,owner_start,owner_attempt_id) values(1,?,0,'unknown','fixture')").run(r.rootId);
 await assert.rejects(applyCaptureHistory(b,r),/capture_history_refused:import_holder_identity_unknown/);
 assert.equal(snapshot(),imported);completion.check("an unknown holder is never treated as a dead holder");
 console.log(JSON.stringify({processIdentity:robust.stats(),actualOwnerIdentity:actual,productTimeoutMs:2000,simulatedSchedulingDelayMs:3000}));completion.complete();
}finally{robust?.restore();childProcess.execFileSync=original;syncBuiltinESMExports();b?.close();fs.rmSync(root,{recursive:true,force:true});}}
main().catch(e=>{console.error(e);process.exitCode=1;});
