import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { explodeOtlpPayload } from "../packages/collector-cli/src/otlp";
import { aiInteractionEventSchema, type AiInteractionEvent } from "../packages/shared/src/index";
import { createProofCompletion } from "./lib/proof-completion";

const completion=createProofCompletion("codex-admitted-projection",68);
const root=fs.mkdtempSync(path.join(os.tmpdir(),"codex-admitted-projection-"));
const at=Date.now()-120_000,iso=new Date(at).toISOString();
const workspace="11111111-1111-4111-8111-111111111111",device="admitted-projection";
const session="22222222-2222-4222-8222-222222222222";
const uuid=(n:number)=>`00000000-0000-4000-8000-${String(n).padStart(12,"0")}`;
const options={workspaceId:workspace,deviceId:device,enrollmentNow:()=>new Date(at-1_000_000),delivery:{enabled:true}};
const fields=["inputTokens","outputTokens","cacheReadTokens","cacheCreationTokens","costUsd"] as const;
const attr=(key:string,value:string|number)=>({key,value:typeof value==="string"?{stringValue:value}:{intValue:String(value)}});
let serial=0;

function projectionCases() {
  const profiles=[
    {name:"known",amount:{inputTokens:2400,outputTokens:510,cacheReadTokens:0},cost:.0273},
    {name:"reported-zero",amount:{inputTokens:0,outputTokens:0,costUsd:0,costKind:"reported"},cost:0},
    {name:"unknown",amount:{},cost:null},
    {name:"partial",amount:{inputTokens:0},cost:null},
  ] as const;
  for(const evidence of ["self-native-attribute","same-trace-tool"] as const)for(const profile of profiles) {
    const b=new LocalEventBuffer(path.join(root,String(++serial)+".sqlite"),options);
    try {
      const traceId=serial.toString(16).padStart(32,"0");
      const raw=aiInteractionEventSchema.parse({id:uuid(serial*10),source:"codex",eventType:"assistant_response",
        observedAt:iso,sessionId:session,...profile.amount,
        metadata:{otelEventName:"handle_responses",traceId,
          ...(evidence==="self-native-attribute"?{model:"gpt-5.5"}:{})}});
      assert.equal(b.append(raw),true);
      if(evidence==="same-trace-tool")assert.equal(b.append(aiInteractionEventSchema.parse({
        id:uuid(serial*10+1),source:"codex",eventType:"tool_result",observedAt:iso,sessionId:session,
        model:"gpt-5.5",metadata:{traceId,model:"gpt-5.5"}})),true);
      for(let n=0;n<20;n++) {
        const state=b.projection.status();
        if(state.ready&&state.parityReady&&!state.dirty&&Object.values(state.backlog).every(x=>x===0))break;
        b.projection.runMaintenance(new Date(Date.now()));
      }
      const fact=b.database.prepare(`select model,input_tokens as input,output_tokens as output,
        cost_nanos as cost from dashboard_event_facts where raw_rowid=(select rowid from buffered_events where id=?)`)
        .get(raw.id) as {model:string|null;input:number|null;output:number|null;cost:number|null};
      assert.ok(fact);
      const amounts=profile.amount as Partial<Record<typeof fields[number],number>>;
      const hasUsage=fields.some(k=>amounts[k]!==undefined);
      completion.check(evidence+"/"+profile.name+"/admitted-model",fact.model===(hasUsage?"gpt-5.5":null));
      completion.check(evidence+"/"+profile.name+"/known-zero-partial-tokens",
        fact.input===(raw.inputTokens??null)&&fact.output===(raw.outputTokens??null));
      completion.check(evidence+"/"+profile.name+"/known-zero-unknown-price",
        fact.cost===(profile.cost===null?null:Math.round(profile.cost*1e9)));
      const original=JSON.parse((b.database.prepare("select payload_json as payload from buffered_events where id=?")
        .get(raw.id) as {payload:string}).payload) as AiInteractionEvent;
      const snapshot=b.projection.readSnapshot(30);
      completion.check(evidence+"/"+profile.name+"/parity-and-raw-diagnostics",
        original.model===undefined&&original.costUsd===raw.costUsd&&snapshot.kind==="ready"&&
        snapshot.snapshot.projection.parityReady===true);
    }finally{b.close();}
  }
}

async function twins() {
  for(const order of ["native-first","rollout-first"] as const)
    for(const state of ["pending","sealed","retry","terminal-replay","restart"] as const) {
      const dir=path.join(root,String(++serial));fs.mkdirSync(dir);
      const file=path.join(dir,"ledger.sqlite");let b=new LocalEventBuffer(file,options);
      const frozen=new Map<string,{bytes:string;event:AiInteractionEvent}>();
      const record=(lease:ReturnType<LocalEventBuffer["delivery"]["lease"]>)=>{
        assert.equal(lease.locallyDead,0);
        for(const item of lease.items) {
          const prior=frozen.get(item.deliveryId);
          if(prior)assert.equal(item.envelopeJson,prior.bytes,"retry/replay must retain frozen bytes");
          else frozen.set(item.deliveryId,{bytes:item.envelopeJson,event:item.envelope.event});
        }
      };
      const native=explodeOtlpPayload({resourceLogs:[{resource:{attributes:[attr("service.name","codex-app-server")]},scopeLogs:[{logRecords:[{
        traceId:serial.toString(16).padStart(32,"0"),timeUnixNano:String(BigInt(at)*1_000_000n),
        attributes:[attr("event.name","codex.sse_event"),attr("model","gpt-5.5"),attr("conversation.id",session),
          attr("turn.id","priced-turn"),attr("input_token_count",2400),attr("output_token_count",510),attr("cached_token_count",0)],
      }]}]}]}, {source:"codex",transportPath:"/v1/logs"}).events[0]!.event;
      const sessions=path.join(dir,"sessions"),day=path.join(sessions,...iso.slice(0,10).split("-"));
      fs.mkdirSync(day,{recursive:true});
      const line=(type:string,payload:unknown)=>JSON.stringify({timestamp:iso,type,payload});
      const usage=(i:number,o:number)=>({type:"token_count",info:{total_token_usage:{
        input_tokens:i,output_tokens:o,cached_input_tokens:0,reasoning_output_tokens:0}}});
      fs.writeFileSync(path.join(day,`rollout-${session}.jsonl`),[
        line("session_meta",{id:session}),line("turn_context",{turn_id:"priced-turn",model:"gpt-5.5"}),
        line("event_msg",usage(0,0)),line("event_msg",usage(2400,510)),
      ].join("\n")+"\n");
      const rollout=async()=>{
        const tailer=new RolloutTailer(b,sessions,()=>[]);
        try {const scan=await tailer.scan({scope:"full",now:new Date(at+20_000)});assert.equal(scan.parseErrors,0);}
        finally {tailer.close();}
      };
      try {
        if(order==="native-first")assert.equal(b.append(native),true);else await rollout();
        let now=new Date(Date.now()+123_000);
        if(state!=="pending") {
          const first=b.delivery.lease({now});record(first);assert.ok(first.items.length>0);
          if(state==="retry")b.delivery.retry(first.leaseId,first.items,"remote_transient",now);
          if(state==="terminal-replay")b.delivery.deadLetterRemote(first.leaseId,first.items.map(i=>i.deliveryId),now);
          if(state==="restart") {b.close();b=new LocalEventBuffer(file,options);}
        }
        if(order==="native-first")await rollout();else assert.equal(b.append(native),true);
        if(state==="terminal-replay")assert.ok(b.delivery.replayDeadLetters({reason:"remote_validation_rejected",now}).requeued>0);
        now=new Date(now.getTime()+600_000);
        for(let n=0;n<4;n++) {
          const lease=b.delivery.lease({now});record(lease);
          assert.equal(b.delivery.acknowledge(lease.leaseId,lease.items.map(i=>i.deliveryId),now).acknowledged,lease.items.length);
          if(!lease.items.length)break;
        }
        const named=[...frozen.values()].map(f=>f.event).filter(e=>e.model&&e.metadata.usageSource!=="capture_gap");
        completion.check(order+"/"+state+"/tokens-once",
          named.reduce((n,e)=>n+(e.inputTokens??0),0)===2400&&named.reduce((n,e)=>n+(e.outputTokens??0),0)===510);
        completion.check(order+"/"+state+"/derived-price-once",
          Math.abs(named.reduce((n,e)=>n+(e.costUsd??0),0)-.0273)<1e-10);
        completion.check(order+"/"+state+"/named-not-gap-and-drained",
          named.length>0&&[...frozen.values()].every(f=>f.event.metadata.usageSource!=="capture_gap")&&
          b.delivery.status(now).remainingDelivery===0);
      }finally{b.close();}
    }
}

async function nativePrefixBeforeLateOtlp() {
  for(const custody of ["pending","acknowledged-restarted"] as const) {
    const dir=path.join(root,String(++serial));fs.mkdirSync(dir);
    const file=path.join(dir,"ledger.sqlite");let b=new LocalEventBuffer(file,options);
    const sessions=path.join(dir,"sessions"),day=path.join(sessions,...iso.slice(0,10).split("-"));
    fs.mkdirSync(day,{recursive:true});
    const row=(type:string,payload:unknown)=>JSON.stringify({timestamp:iso,type,payload});
    const tokens=(n:number)=>({type:"token_count",info:{total_token_usage:{
      input_tokens:n,output_tokens:n*2,cached_input_tokens:0,reasoning_output_tokens:0}}});
    const lines=[row("session_meta",{id:session}),row("turn_context",{turn_id:"prefix-turn",model:"gpt-5.5"}),
      ...Array.from({length:65},(_,n)=>row("event_msg",tokens(n)))];
    fs.writeFileSync(path.join(day,`rollout-${session}.jsonl`),lines.join("\n")+"\n");
    const frozen=new Map<string,AiInteractionEvent>();
    const drain=()=>{
      for(let n=0;n<4;n++) {
        const lease=b.delivery.lease({now:new Date(Date.now()+600_000)});
        assert.equal(lease.locallyDead,0);
        for(const item of lease.items){assert.equal(frozen.has(item.deliveryId),false);frozen.set(item.deliveryId,item.envelope.event);}
        assert.equal(b.delivery.acknowledge(lease.leaseId,lease.items.map(i=>i.deliveryId),new Date(Date.now()+600_000)).acknowledged,lease.items.length);
        if(!lease.items.length)break;
      }
    };
    try {
      const tailer=new RolloutTailer(b,sessions,()=>[]);
      try{assert.equal((await tailer.scan({scope:"full",now:new Date(at+20_000)})).parseErrors,0);}finally{tailer.close();}
      const prefix=b.database.prepare(`select count(*) as n,sum(input_tokens) as input,sum(output_tokens) as output
        from buffered_events where event_type='usage_rollout'`).get() as {n:number;input:number;output:number};
      completion.check(custody+"/native-prefix-retains-all-marginals",prefix.n===64&&prefix.input===64&&prefix.output===128);
      if(custody==="acknowledged-restarted") {drain();b.close();b=new LocalEventBuffer(file,options);}
      const late=explodeOtlpPayload({resourceLogs:[{resource:{attributes:[attr("service.name","codex-app-server")]},scopeLogs:[{logRecords:[{
        traceId:serial.toString(16).padStart(32,"0"),timeUnixNano:String(BigInt(at+10_000)*1_000_000n),
        attributes:[attr("event.name","codex.sse_event"),attr("model","gpt-5.5"),attr("conversation.id",session),
          attr("turn.id","prefix-turn"),attr("input_token_count",64),attr("output_token_count",128),attr("cached_token_count",0)],
      }]}]}]}, {source:"codex",transportPath:"/v1/logs"}).events[0]!.event;
      assert.equal(b.append(late),true);drain();
      const admitted=[...frozen.values()].filter(e=>e.model&&e.metadata.usageSource!=="capture_gap");
      completion.check(custody+"/late-otlp-keeps-every-prefix-token-once",
        admitted.reduce((n,e)=>n+(e.inputTokens??0),0)===64&&admitted.reduce((n,e)=>n+(e.outputTokens??0),0)===128);
      completion.check(custody+"/late-otlp-keeps-prefix-price-once",
        Math.abs(admitted.reduce((n,e)=>n+(e.costUsd??0),0)-.00416)<1e-10&&
        [...frozen.values()].every(e=>e.metadata.usageSource!=="capture_gap")&&b.delivery.status().remainingDelivery===0);
    }finally{b.close();}
  }
}

async function main(){try{projectionCases();await twins();await nativePrefixBeforeLateOtlp();completion.complete();}
  finally{fs.rmSync(root,{recursive:true,force:true});}}
main().catch(error=>{console.error(error);process.exitCode=1;});
