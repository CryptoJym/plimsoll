import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { explodeOtlpPayload } from "../packages/collector-cli/src/otlp";
import { createProofCompletion } from "./lib/proof-completion";
import { proofTempRoot } from "./lib/legacy-reader";

const SESSION="22222222-2222-4222-8222-222222222222", MODEL="gpt-6.1-sol";
const worker=process.argv.includes("--worker");
const root=worker?process.argv[process.argv.indexOf("--worker")+1]!:proofTempRoot("hold-crash");
const at=worker?Number(process.argv[process.argv.indexOf("--worker")+2]):Date.now();
const file=path.join(root,"ledger.sqlite"),sessions=path.join(root,"sessions");
const attribute=(key:string,value:string|number)=>({key,value:typeof value==="number"?{intValue:String(value)}:{stringValue:value}});
function event(model:string,counters=false) {
  return explodeOtlpPayload({resourceLogs:[{resource:{attributes:[attribute("service.name","codex-app-server")]},scopeLogs:[{
    logRecords:[{timeUnixNano:String(BigInt(at+(counters?1000:3000))*1000000n),traceId:"a".repeat(32),attributes:[
      attribute("event.name","codex.sse_event"),attribute("conversation.id",SESSION),attribute("model",model),
      ...(counters?[attribute("input_token_count",19),attribute("output_token_count",2)]:[])]}]}]}]},
    {source:"codex",transportPath:"/v1/logs"}).events[0]!.event;
}
function open(now:()=>Date) {
  return new LocalEventBuffer(file,{workspaceId:"11111111-1111-4111-8111-111111111111",deviceId:"crash-fixture",
    enrollmentNow:()=>new Date(at-1_000_000),delivery:{enabled:true,now}});
}
async function tail(buffer:LocalEventBuffer,now:Date) {
  const tailer=new RolloutTailer(buffer,sessions,()=>[]);
  try { return await tailer.scan({scope:"full",now}); } finally {tailer.close();}
}
async function main() {
  if(worker) {
    const b=open(()=>new Date(at+2000));assert.equal(b.append(event(MODEL,true)),true);
    const day=path.join(sessions,...new Date(at).toISOString().slice(0,10).split("-"));fs.mkdirSync(day,{recursive:true});
    const row=(type:string,payload:unknown)=>JSON.stringify({timestamp:new Date(at+1000).toISOString(),type,payload});
    fs.writeFileSync(path.join(day,`rollout-crash-${SESSION}.jsonl`),[
      row("session_meta",{id:SESSION}),row("turn_context",{turn_id:"crash-turn",model:MODEL}),
      row("event_msg",{type:"token_count",info:{total_token_usage:{input_tokens:0,output_tokens:0,cached_input_tokens:0}}}),
      row("event_msg",{type:"token_count",info:{total_token_usage:{input_tokens:19,output_tokens:2,cached_input_tokens:0}}}),
    ].join("\n")+"\n");
    await tail(b,new Date(at+2000));
    const frozen=b.database.prepare("select delivery_id as id,sealed_envelope_json as bytes from upload_outbox where sealed_envelope_json is not null").all();
    assert.equal(frozen.length,1);assert.equal(b.delivery.lease({now:new Date(at+2000)}).items.length,0);
    process.send!({frozen});
    // Parent kills this owned fixture PID while SQLite is open, before close
    // or a transport flush. Keep the IPC worker alive until that SIGKILL.
    await new Promise<void>(()=>{setInterval(()=>{},1000);});
    return;
  }
  const completion=createProofCompletion("codex-capture-hold-crash",1);
  const child=spawn(process.execPath,["--import",path.join(process.cwd(),"node_modules/tsx/dist/loader.mjs"),
    path.join(process.cwd(),"scripts/codex-capture-hold-crash-proof.ts"),"--worker",root,String(at)],
    {env:process.env,stdio:["ignore","inherit","inherit","ipc"]});
  let b:LocalEventBuffer|undefined;
  try {
    const frozen=await new Promise<any[]>((resolve,reject)=>{
      child.once("message",(value:any)=>resolve(value.frozen));child.once("error",reject);
      child.once("exit",code=>reject(new Error(`fixture exited before crash boundary: ${code}`)));
    });
    const exited=new Promise<{code:number|null;signal:string|null}>(resolve=>child.once("exit",(code,signal)=>resolve({code,signal})));
    assert.equal(child.kill("SIGKILL"),true);assert.equal((await exited).signal,"SIGKILL");
    let now=new Date(at+4000);b=open(()=>now);
    assert.equal(b.delivery.lease({now}).items.length,0,"restart preserves transport hold");
    assert.equal(b.append(event("gpt-6-astra")),true);
    assert.equal((await tail(b,now)).eventsAppended,0,"durable native coverage prevents a duplicate");
    now=new Date(at+62000);const lease=b.delivery.lease({now});
    const named=lease.items.filter(i=>i.envelope.event.inputTokens===19&&i.envelope.event.outputTokens===2);
    assert.equal(named.length,1);assert.equal(named[0]!.deliveryId,frozen[0].id);
    assert.equal(named[0]!.envelopeJson,frozen[0].bytes);assert.equal(named[0]!.envelope.event.model,MODEL);
    b.delivery.acknowledge(lease.leaseId,lease.items.map(i=>i.deliveryId),now);
    assert.equal(b.delivery.lease({now:new Date(at+184000)}).items.length,0);
    completion.check("sigkill-after-native-coverage-before-held-flush");completion.complete();
  } finally {
    b?.close();if(child.exitCode===null&&child.signalCode===null)child.kill("SIGKILL");
    fs.rmSync(root,{recursive:true,force:true});
  }
}
main().catch(error=>{console.error(error);process.exitCode=1;});
