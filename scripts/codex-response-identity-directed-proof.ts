import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { explodeOtlpPayload } from "../packages/collector-cli/src/otlp";
import { aiInteractionEventSchema } from "../packages/shared/src/index";
import { createProofCompletion } from "./lib/proof-completion";
import { proofTempRoot, withReader } from "./lib/legacy-reader";

const FIELDS=["inputTokens","outputTokens","cacheReadTokens","cacheCreationTokens","costUsd"] as const;
type Amounts=Partial<Record<typeof FIELDS[number],number>>;
const root=proofTempRoot("identity-directed"),AT=Date.now()-10_000_000;
const SESSION="22222222-2222-4222-8222-222222222222",MODEL="gpt-6.1-sol";
const minimal=JSON.parse(fs.readFileSync("scripts/fixtures/codex-response-identities/minimal-counterexamples.json","utf8"));
const completion=createProofCompletion("codex-response-identity-directed",35+minimal.length*2);
let next=0;
const attr=(key:string,value:string|number)=>({key,value:typeof value==="string"?{stringValue:value}:Number.isInteger(value)?{intValue:String(value)}:{doubleValue:value}});
const aliases={inputTokens:"input_token_count",outputTokens:"output_token_count",cacheReadTokens:"cached_token_count",cacheCreationTokens:"gen_ai.usage.cache_creation_input_tokens",costUsd:"cost_usd"};
function event(identity:Record<string,string>,amount:Amounts,ordinal:number,account?:string) {
  return explodeOtlpPayload({resourceLogs:[{resource:{attributes:[attr("service.name","codex-app-server")]},scopeLogs:[{logRecords:[{
    timeUnixNano:String(BigInt(AT+ordinal*3000)*1_000_000n),traceId:(ordinal+1).toString(16).padStart(32,"0"),
    attributes:[attr("event.name","codex.sse_event"),attr("conversation.id",SESSION),attr("model",MODEL),
      ...Object.entries(identity).map(([k,v])=>attr(k,v)),...Object.entries(amount).map(([k,v])=>attr(aliases[k as keyof typeof aliases],v)),
      ...(account?[attr("user.account_id",account)]:[])],
  }]}]}]}, {source:"codex",transportPath:"/v1/logs"}).events[0]!.event;
}
class Fixture {
  dir=path.join(root,String(++next)); file=path.join(this.dir,"ledger.sqlite");now=new Date(AT+20_000);b:any;
  frozen=new Map<string,{bytes:string;event:any;rawId:string;at:number}>();appended=0;acks=0;leases=0;
  constructor(Buffer:any=LocalEventBuffer){fs.mkdirSync(this.dir);this.b=new Buffer(this.file,{workspaceId:"11111111-1111-4111-8111-111111111111",
    deviceId:"identity-directed",enrollmentNow:()=>new Date(AT-1_000_000),delivery:{enabled:true,now:()=>this.now}});}
  append(e:any){assert.equal(this.b.append(e),true);this.appended++;}
  drain(advance=true){if(advance)this.now=new Date(this.now.getTime()+123_000);
    for(let n=0;n<4;n++){const lease=this.b.delivery.lease({now:this.now});this.leases++;assert.equal(lease.locallyDead,0);
      for(const item of lease.items){const old=this.frozen.get(item.deliveryId);if(old)assert.equal(item.envelopeJson,old.bytes);
        else this.frozen.set(item.deliveryId,{bytes:item.envelopeJson,event:item.envelope.event,rawId:item.rawId,at:this.now.getTime()});}
      const ack=this.b.delivery.acknowledge(lease.leaseId,lease.items.map((i:any)=>i.deliveryId),this.now);assert.equal(ack.acknowledged,lease.items.length);this.acks+=ack.acknowledged;
      if(!lease.items.length)break;
    }
  }
  total(){const named=[...this.frozen.values()].filter(f=>f.event.model&&f.event.metadata.usageSource!=="capture_gap");return Object.fromEntries(
    FIELDS.filter(k=>named.some(f=>f.event[k]!==undefined)).map(k=>[k,named.reduce((n,f)=>n+(f.event[k]??0),0)])) as Amounts;}
  verify(){for(const [id,item] of this.frozen)if(item.event.model){const row=this.b.database.prepare("select envelope_json as bytes from codex_named_captures where delivery_id=?").get(id);assert.equal(row?.bytes,item.bytes);}}
  reopen(){this.b.close();this.b=new LocalEventBuffer(this.file,{workspaceId:"11111111-1111-4111-8111-111111111111",deviceId:"identity-directed",
    enrollmentNow:()=>new Date(AT-1_000_000),delivery:{enabled:true,now:()=>this.now}});this.verify();}
  close(){this.b.close();fs.rmSync(this.dir,{recursive:true,force:true});}
}
function same(actual:Amounts,expected:Amounts){for(const k of FIELDS){assert.equal(actual[k]===undefined,expected[k]===undefined,k+" known versus absent");if(expected[k]!==undefined)assert.ok(Math.abs(actual[k]!-expected[k]!)<1e-10,k+": "+JSON.stringify({actual,expected}));}}
const orders=[[0,1,2],[0,2,1],[1,0,2],[1,2,0],[2,0,1],[2,1,0]];
async function main(){
  const native=new Fixture();try{
    const sessions=path.join(native.dir,"sessions"),day=path.join(sessions,...new Date(AT).toISOString().slice(0,10).split("-"));fs.mkdirSync(day,{recursive:true});
    const row=(type:string,payload:any)=>JSON.stringify({timestamp:new Date(AT+9000).toISOString(),type,payload});
    const tokens=(i:number,o:number)=>({type:"token_count",info:{total_token_usage:{input_tokens:i,output_tokens:o,cached_input_tokens:0}}});
    fs.writeFileSync(path.join(day,`rollout-${SESSION}.jsonl`),[row("session_meta",{id:SESSION}),row("turn_context",{turn_id:"T",model:MODEL}),row("event_msg",tokens(0,0)),row("event_msg",tokens(19,2))].join("\n")+"\n");
    const tailer=new RolloutTailer(native.b,sessions,()=>[]);try{assert.equal((await tailer.scan({scope:"full",now:native.now})).parseErrors,0);}finally{tailer.close();}
    native.drain();native.append(event({request_id:"R",call_id:"C","turn.id":"T"},{inputTokens:19,outputTokens:2},3));native.drain();native.reopen();
    native.append(event({call_id:"C"},{inputTokens:29,outputTokens:7},1));native.drain();same(native.total(),{inputTokens:29,outputTokens:7,cacheReadTokens:0});native.verify();
    completion.check("covered-native-request-call-turn-bridge-survives-restart-and-growing-call-only");
  }finally{native.close();}
  for(const order of orders){const f=new Fixture();try{
    const obs=[event({request_id:"R1",call_id:"C"},{inputTokens:19},0),event({request_id:"R2",call_id:"C"},{outputTokens:2},1),event({call_id:"C"},{inputTokens:19,outputTokens:2},2)];
    for(const n of order){f.append(obs[n]);f.drain();f.reopen();}same(f.total(),{inputTokens:19,outputTokens:2});completion.check("shared-call-partials-"+order.join(""));
  }finally{f.close();}}
  for(const kind of ["separate-namespaces","account-boundary","zero","claude-zero"]){const f=new Fixture();try{
    if(kind==="separate-namespaces"){f.append(event({request_id:"same-string"},{inputTokens:19,outputTokens:2},0));f.drain();f.append(event({call_id:"same-string"},{inputTokens:19,outputTokens:2},1));f.drain();same(f.total(),{inputTokens:38,outputTokens:4});}
    if(kind==="account-boundary"){f.append(event({request_id:"R"},{inputTokens:19,outputTokens:2},0,"account-A"));f.drain();f.append(event({"turn.id":"T"},{inputTokens:19,outputTokens:2},1,"account-B"));f.drain();f.append(event({request_id:"R","turn.id":"T"},{inputTokens:19,outputTokens:2},2,"account-A"));f.drain();same(f.total(),{inputTokens:38,outputTokens:4});}
    if(kind==="zero"){f.append(event({call_id:"C"},{inputTokens:0,outputTokens:0,cacheReadTokens:0,cacheCreationTokens:0,costUsd:0},0));f.drain();same(f.total(),{inputTokens:0,outputTokens:0,cacheReadTokens:0,cacheCreationTokens:0,costUsd:0});assert.equal(f.frozen.size,1);}
    if(kind==="claude-zero"){f.append(aiInteractionEventSchema.parse({id:"11111111-2222-4333-8444-555555555555",source:"claude_code",dataMode:"metadata",eventType:"assistant_response",observedAt:new Date(AT).toISOString(),model:"claude-opus-4-1",inputTokens:0,outputTokens:0,metadata:{usageSource:"hook"}}));f.drain(false);same(f.total(),{inputTokens:0,outputTokens:0});assert.equal(f.frozen.size,1);}
    assert.ok(f.appended&&f.leases&&f.acks);completion.check(kind);
  }finally{f.close();}}
  // Measurement adapter only: adjust the existing queue due time in a private
  // fixture. Product code has no additional SSE hold. A bridge releases this
  // response's pending rows; every actual append/lease/ACK still uses production.
  const measurements:any[]=[];
  for(const bridgeAt of [40_000,120_000])for(const hold of [0,60_000])for(const order of orders){const f=new Fixture();try{
    const arrivals=new Map<string,number>();let bridged=false;
    const pending=()=>f.b.database.prepare("select min(next_attempt_at) as at from upload_outbox where state='pending'").get()?.at;
    const flushUntil=(at:number)=>{for(let n=0;n<8;n++){const due=pending();if(!due||Date.parse(due)>AT+at)break;f.now=new Date(due);f.drain(false);}};
    const obs=[event({request_id:"R"},{inputTokens:19,outputTokens:2},0),event({"turn.id":"T"},{inputTokens:19,outputTokens:2},1),event({request_id:"R","turn.id":"T"},{inputTokens:19,outputTokens:2},2)];
    for(const [position,n] of order.entries()){
      const arrival=position===2?bridgeAt:position*20_000;flushUntil(arrival);f.now=new Date(AT+arrival);arrivals.set(obs[n]!.id,arrival);f.append(obs[n]);
      if(n===2)bridged=true;
      if(hold&&!bridged)f.b.database.prepare("update upload_outbox set next_attempt_at=? where raw_id=? and sealed_envelope_json is null")
        .run(new Date(AT+arrival+hold).toISOString(),obs[n]!.id);
      else f.b.database.prepare("update upload_outbox set next_attempt_at=? where sealed_envelope_json is null").run(f.now.toISOString());
      f.drain(false);
    }
    flushUntil(bridgeAt+hold);f.verify();const total=f.total();const exposed=total.inputTokens!==19||total.outputTokens!==2;
    const delays=[...f.frozen.values()].filter(v=>v.event.model).map(v=>v.at-AT-(arrivals.get(v.rawId)??0));
    measurements.push({order:order.join(""),bridgeAt,hold,total,exposed,deliveryDelayMs:delays});
    assert.equal(exposed,order[2]===2&&(hold===0||bridgeAt>hold));
    completion.check(`hold-measurement-${bridgeAt}-${hold}-${order.join("")}`);
  }finally{f.close();}}
  // Every newly shrunk counterexample executes again as a permanent minimal
  // producer case on both actual readers. The ideal maximum remains recorded
  // beside its irretractable paid-prefix result, rather than being waived.
  await withReader("121b55437555c3a3c34bafe5889f4d6d8870509f",async({Buffer})=>{
    for(const [index,fixture] of minimal.entries())for(const [label,Reader] of [["head",LocalEventBuffer],["0.7.50",Buffer]] as const){const f=new Fixture(Reader);try{
      for(const o of fixture.observations){const identity={...(o.mask&1?{"turn.id":"T"}:{}),...(o.mask&2?{request_id:"R"}:{}),...(o.mask&4?{call_id:"C"}:{})};f.append(event(identity,o.amount,o.ordinal));if(fixture.ackEach)f.drain();}
      f.drain();if(label==="head")same(f.total(),fixture.paidPrefix);
      for(const k of FIELDS)assert.ok((f.total()[k]??0)>=(fixture.expected[k]??0)-1e-10,"known minimal field retained");
      assert.equal(f.appended,fixture.observations.length);assert.ok(f.leases&&f.acks);completion.check(`minimal-${index}-${label}`);
    }finally{f.close();}}
  });
  const evidence=path.resolve("evidence/codex-response-identities");fs.mkdirSync(evidence,{recursive:true});fs.writeFileSync(path.join(evidence,"hold-measurements.json"),JSON.stringify(measurements,null,2)+"\n");
  console.log(JSON.stringify({measurements,minimalReplays:minimal.length*2,productHoldChanged:false}));completion.complete();
}
main().catch(error=>{console.error(error);process.exitCode=1;}).finally(()=>fs.rmSync(root,{recursive:true,force:true}));
