import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { deriveCaptureRootIdentity, rootCursorKey } from "../packages/collector-cli/src/capture-root-inventory";
import { DEFAULT_JSONL_TAILER_IO, jsonlScanStateKey, readJsonlTail as readTail, prepareJsonlCommittedPrefixHash as hashPrefix, rememberJsonlScanCursor as saveCursor } from "../packages/collector-cli/src/jsonl-byte-tailer";
import { deterministicEventId } from "../packages/collector-cli/src/normalizer";
import { readJsonlContinuation } from "../packages/collector-cli/src/jsonl-continuation";
import { loadJsonlScanCursor } from "../packages/collector-cli/src/jsonl-byte-tailer";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { createProofCompletion } from "./lib/proof-completion";

const completion=createProofCompletion("codex-fork-identity",6), started=performance.now();
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
  try {await body({buffer,db,io,file,key,scan,rows,cursor,receipt,root:roots[0],sessions,put(id:string,n:number){fs.writeFileSync(file(id),meta(id)+(id===child?meta(parent):"")+count(0)+count(n));},
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
  });completion.check("park_survives_restart_file_and_version_fences");
  for(const changed of [false,true])await fixture(`static-${changed}`,async f=>{
    f.put(child,100);fs.appendFileSync(f.file(child),line("event_msg",{type:"other",padding:"X".repeat(4096)}));await f.scan();
    const file=f.file(child),offset=fs.statSync(file).size,oldEvents=f.rows().length;
    fs.appendFileSync(file,count(200)+count(300));
    if(changed){const bytes=fs.readFileSync(file);bytes[2048]=89;fs.writeFileSync(file,bytes);}
    const stat=fs.statSync(file,{bigint:true}),snapshot=()=>{const s=fs.lstatSync(file,{bigint:true});return [s.dev,s.ino,s.birthtimeNs,s.size,s.mtimeNs,s.ctimeNs];};
    // A durable mid-file cursor, with all bytes already present before scans.
    f.db.prepare("update rollout_scan_state set size=?,deferred_bytes=?,work_remaining=1,mtime_ms=?,ctime_ms=? where file=?")
      .run(Number(stat.size),Number(stat.size)-offset,Number(stat.mtimeNs)/1e6,Number(stat.ctimeNs)/1e6,jsonlScanStateKey(f.key(child)));
    const before=snapshot(),logs=errors.length;let message="first_failure",attempts=0;
    f.io.readTail=(...args:Parameters<typeof readTail>)=>{const read=readTail(...args);if(read)read.assertStableForCommit=()=>{attempts++;throw new Error(message);};return read;};
    await f.scan();message="source_changed";await f.scan();eq(JSON.parse(f.receipt(child).commit_failure_json).attempts,1);
    await f.scan();const parked=f.receipt(child);eq(JSON.parse(parked.commit_failure_json).attempts,2);
    await f.scan();f.restart();await f.scan();eq([attempts,errors.length-logs],[3,3]);eq(f.receipt(child),parked);eq(snapshot(),before);
    eq(f.rows().length,oldEvents);eq(f.cursor(child).committed_offset,offset);
    f.db.prepare("update rollout_scan_state set commit_failure_json=json_set(commit_failure_json,'$.version','0.7.55') where commit_failure_json is not null").run();f.io.readTail=readTail;
    const pending=readJsonlContinuation(file,fs.statSync(file),loadJsonlScanCursor(f.db,f.key(child),"codex-rollout-v2",2,x=>x),{maxBytes:1025},f.io,
      {database:f.db,provider:"codex",cursorKey:f.key(child),root:f.root,directory:f.sessions,retryCommittedPrefix:true,deadline:performance.now()+200,eligible:()=>true})!;
    eq(pending.continuation?.reason,"prefix_verification_pending");eq(pending.bytesRead,1025);pending.close();
    await f.scan();eq(errors.length-logs,3);
    if(changed){eq(f.cursor(child).committed_offset,offset);eq(f.rows().length,oldEvents);
      eq(JSON.parse(f.db.prepare("select envelope_json from jsonl_continuations").get().envelope_json).reason,"prefix_changed");
      await f.scan();eq(errors.length-logs,3);
    }else{eq(f.cursor(child).committed_offset,Number(stat.size));eq(f.rows().reduce((n:number,e:any)=>n+e.inputTokens,0),300);eq(f.receipt(child),undefined);}
  });completion.check("static_mid_file_identical_failures_park_and_prefix_gates_readmission");
  await fixture("source-fence",async f=>{
    f.put(child,100);await f.scan();const file=f.file(child),offset=fs.statSync(file).size;
    fs.appendFileSync(file,line("event_msg",{padding:"X".repeat(180_000),type:"token_count",info:{total_token_usage:{input_tokens:200,output_tokens:20}}}));
    const cursor=loadJsonlScanCursor(f.db,f.key(child),"codex-rollout-v2",2,x=>x);
    const read=readJsonlContinuation(file,fs.statSync(file),cursor,{maxBytes:65536},f.io,
      {database:f.db,provider:"codex",cursorKey:f.key(child),root:f.root,directory:f.sessions,deadline:performance.now()+200,eligible:()=>true})!;
    read.assertStableForCommit();read.continuation!.applyCheckpoint();read.close();
    const row=f.db.prepare("select file_key,envelope_json from jsonl_continuations").get(),body=JSON.parse(row.envelope_json);delete body.sha256;body.reason="source_changed";
    const sealed={...body,sha256:crypto.createHash("sha256").update(JSON.stringify(body)).digest("hex")};
    f.db.prepare("update jsonl_continuations set envelope_json=? where file_key=?").run(JSON.stringify(sealed),row.file_key);
    await f.scan();eq(f.cursor(child).committed_offset,fs.statSync(file).size);assert(f.cursor(child).committed_offset>offset);
    eq(f.rows().reduce((n:number,e:any)=>n+e.inputTokens,0),200);
  });
  await fixture("uncommitted-park",async f=>{
    f.put(child,100);f.db.exec("create trigger fail_new before insert on buffered_events begin select raise(abort,'new_file_failure'); end");
    await f.scan();await f.scan();assert(f.receipt(child));eq(f.cursor(child),undefined);
    f.db.exec("drop trigger fail_new; update rollout_scan_state set commit_failure_json=json_set(commit_failure_json,'$.version','0.7.55') where commit_failure_json is not null");
    await f.scan();eq(f.rows().reduce((n:number,e:any)=>n+e.inputTokens,0),100);eq(f.receipt(child),undefined);
  });completion.check("source_changed_envelope_and_uncommitted_file_readmission");
  assert.ok(performance.now()-started<60_000);completion.complete();
}finally{console.error=original;fs.rmSync(base,{recursive:true,force:true});}}
main().catch(error=>{console.error(error);process.exitCode=1;});
