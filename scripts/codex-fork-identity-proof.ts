import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { deriveCaptureRootIdentity, rootCursorKey } from "../packages/collector-cli/src/capture-root-inventory";
import { DEFAULT_JSONL_TAILER_IO, jsonlScanStateKey, readJsonlTail as readTail, prepareJsonlCommittedPrefixHash as hashPrefix, rememberJsonlScanCursor as saveCursor } from "../packages/collector-cli/src/jsonl-byte-tailer";
import { deterministicEventId } from "../packages/collector-cli/src/normalizer";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { createProofCompletion } from "./lib/proof-completion";

const completion=createProofCompletion("codex-fork-identity",4), started=performance.now();
const eq:typeof assert.deepEqual=assert.deepEqual;
const base=fs.mkdtempSync(path.join(os.tmpdir(),"fork-")), errors:string[]=[], original=console.error;
const parent="01990000-0000-7000-8000-000000000001", child="01990000-0000-7000-8000-000000000002";
const timestamp=new Date().toISOString(), line=(type:string,payload:object)=>JSON.stringify({timestamp,type,payload})+"\n";
const meta=(id:string)=>line("session_meta",{id,originator:"codex-conductor"});
const count=(n:number)=>line("event_msg",{type:"token_count",info:{total_token_usage:{input_tokens:n,cached_input_tokens:n/2,output_tokens:n/10,reasoning_output_tokens:n/20}}});
const eventId=(session:string,index:number)=>deterministicEventId(["codex-rollout",session,String(index)]);
async function fixture(name:string,body:(f:any)=>Promise<void>) {
  const sessions=path.join(base,name,"sessions"), day=path.join(sessions,...timestamp.slice(0,10).split("-"));fs.mkdirSync(day,{recursive:true});
  const buffer=new LocalEventBuffer(path.join(base,name,"ledger.sqlite"),{workspaceId:parent,enrollmentNow:()=>new Date(Date.parse(timestamp)-60_000)}), db=buffer.database;
  const roots=[{...deriveCaptureRootIdentity("synthetic","codex",sessions),source:"codex" as const,directory:sessions,installationEpochId:buffer.workspaceBinding()!.currentInstallationEpochId!}];
  const file=(id:string)=>path.join(day,`rollout-proof-${id}.jsonl`), key=(id:string)=>rootCursorKey(roots,file(id));
  const io={...DEFAULT_JSONL_TAILER_IO};let tailer=new RolloutTailer(buffer,sessions,()=>[],io,roots);
  const scan=()=>tailer.scan({scope:"full"}), rows=()=>db.prepare("select payload_json from buffered_events order by session_id,rowid").all().map((r:any)=>JSON.parse(r.payload_json));
  const cursor=(id:string)=>db.prepare("select * from rollout_scan_state where file=?").get(jsonlScanStateKey(key(id))) as any;
  const receipt=(id:string)=>db.prepare("select * from rollout_scan_state where file=?").get(jsonlScanStateKey(`codex-commit-failure:${key(id)}`)) as any;
  try {await body({buffer,db,io,file,key,scan,rows,cursor,receipt,put(id:string,n:number){fs.writeFileSync(file(id),meta(id)+(id===child?meta(parent):"")+count(0)+count(n));},
    restart(){tailer.close();tailer=new RolloutTailer(buffer,sessions,()=>[],io,roots);}});}finally{tailer.close();buffer.close();}
}
async function main(){console.error=value=>errors.push(String(value));try{
  for(const order of [[parent,child],[child,parent]])await fixture(order[0],async f=>{
    for(const id of order){f.put(id,id===child?200:100);await f.scan();}
    eq(f.rows().map((e:any)=>[e.sessionId,e.id,e.inputTokens,e.cacheReadTokens,e.outputTokens]),
      [[parent,eventId(parent,1),100,50,10],[child,eventId(child,1),200,100,20]]);
    await f.scan();eq(f.rows().length,2);
  });completion.check("both_ingestion_orders");
  await fixture("saved",async f=>{
    f.put(parent,100);await f.scan();f.put(child,100);
    const read=readTail(f.file(child),fs.statSync(f.file(child)),undefined)!;
    hashPrefix(f.db,f.file(child),f.key(child),undefined,read);
    saveCursor(f.db,f.key(child),"codex-rollout-v2",2,read,JSON.parse(f.cursor(parent).parser_state_json));read.close();
    fs.appendFileSync(f.file(child),count(160));await f.scan();
    eq(f.rows().map((e:any)=>[e.id,e.inputTokens]),[[eventId(parent,1),100],[eventId(child,2),60]]);
    const state=JSON.parse(f.cursor(child).parser_state_json);eq([state.conversationId,state.tokenCountIndex,state.previous.input],[child,2,160]);
  });completion.check("foreign_cursor_keeps_prior_totals_and_rows");
  await fixture("conflict",async f=>{
    f.put(parent,100);await f.scan();f.buffer.append({...f.rows()[0],id:eventId(parent,2),inputTokens:999},[]);
    fs.appendFileSync(f.file(parent),count(160));await f.scan();await f.scan();
    const losses=f.db.prepare("select * from capture_record_losses where reason='logical_source_conflict'").all();eq(losses.length,1);
    eq([losses[0].input_tokens,losses[0].cached_input_tokens,losses[0].output_tokens,losses[0].reasoning_output_tokens],[60,30,6,3]);
    eq(f.db.prepare("select count(*) n from capture_root_observations where state='conflict'").get().n,1);
    eq(f.rows().map((e:any)=>e.inputTokens),[100,999]);eq(f.cursor(parent).committed_offset,fs.statSync(f.file(parent)).size);
  });completion.check("conflict_loss_once_and_cursor_advances");eq(errors.length,0);
  await fixture("park",async f=>{
    f.put(child,100);await f.scan();const offset=fs.statSync(f.file(child)).size;fs.appendFileSync(f.file(child),count(200));let attempts=0;
    f.io.readTail=(...args:Parameters<typeof readTail>)=>{const read=readTail(...args);if(read)read.assertStableForCommit=()=>{attempts++;throw new Error("JSONL file generation changed before cursor commit");};return read;};
    await f.scan();await f.scan();f.restart();await f.scan();eq([attempts,errors.length],[2,2]);eq(f.receipt(child).unresolved_kind,"commit_error_parked");
    const receipt=JSON.parse(f.receipt(child).commit_failure_json);eq([receipt.attempts,receipt.offset],[2,offset]);assert.match(receipt.messageHash,/^sha256:[a-f0-9]{64}$/);
    fs.appendFileSync(f.file(child),count(300));await f.scan();eq(attempts,3);await f.scan();eq(attempts,4);
    f.db.prepare("update rollout_scan_state set commit_failure_json=json_set(commit_failure_json,'$.version','0.7.55')").run();f.io.readTail=readTail;
    await f.scan();eq(f.rows().reduce((n:number,e:any)=>n+e.inputTokens,0),300);eq(f.receipt(child),undefined);
  });completion.check("park_survives_restart_file_and_version_fences");assert.ok(performance.now()-started<60_000);completion.complete();
}finally{console.error=original;fs.rmSync(base,{recursive:true,force:true});}}
main().catch(error=>{console.error(error);process.exitCode=1;});
