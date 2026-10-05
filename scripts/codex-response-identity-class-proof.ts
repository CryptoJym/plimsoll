import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { spawn } from "node:child_process";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { explodeOtlpPayload } from "../packages/collector-cli/src/otlp";
import { createProofCompletion } from "./lib/proof-completion";
import { proofTempRoot, withReader } from "./lib/legacy-reader";

// Producer facts and the oracle do not import the collector's identity,
// capture, coverage, or pairing helpers. One physical response has three
// observations; each independently draws all eight subsets of T/R/C.
// The immutable paid-prefix oracle describes what a collector can still
// change. The ideal, order-independent field maximum is checked separately
// and every difference is retained as an explicit exposure counterexample.
const FIELDS = ["inputTokens", "outputTokens", "cacheReadTokens", "cacheCreationTokens", "costUsd"] as const;
type Amounts = Partial<Record<typeof FIELDS[number], number>>;
type Observation = { mask: number; amount: Amounts; ordinal: number };
const ORDER = [[0,1,2],[0,2,1],[1,0,2],[1,2,0],[2,0,1],[2,1,0]];
const FLOOR = "121b55437555c3a3c34bafe5889f4d6d8870509f";
const ROOT = proofTempRoot("identity-class");
const AT = Date.parse("2025-01-01T00:00:00.000Z"); // every enumerated clock stays in the past
const part=process.argv.find(a=>a.startsWith("--partition="))?.slice(12).split(":").map(Number);
const workerResult=process.argv.find(a=>a.startsWith("--worker-result="))?.slice(16);
if(part)assert.ok(part.length===2&&part.every(Number.isSafeInteger)&&part[0]!>=0&&part[1]!<=8&&part[0]!<part[1]!&&
  workerResult&&path.resolve(workerResult).startsWith(process.env.PLIMSOLL_PROOF_ROOT!+path.sep),"bounded private partition");
const completion = createProofCompletion(part?"codex-response-identity-class-partition":"codex-response-identity-class", part?(part[1]!-part[0]!)*64*2*2*6:512*2*2*6);
const profiles: Amounts[][] = [
  Array.from({length:3},()=>({inputTokens:19,outputTokens:2,cacheReadTokens:0,cacheCreationTokens:3,costUsd:.125})),
  [{inputTokens:19,cacheReadTokens:0},{outputTokens:2,cacheCreationTokens:3},
    {inputTokens:19,outputTokens:2,cacheReadTokens:0,cacheCreationTokens:3,costUsd:.125}],
];
const attr = (key:string,value:string|number)=>({key,value:typeof value==="string"?{stringValue:value}:Number.isInteger(value)?{intValue:String(value)}:{doubleValue:value}});
const aliases = {inputTokens:"input_token_count",outputTokens:"output_token_count",cacheReadTokens:"cached_token_count",cacheCreationTokens:"gen_ai.usage.cache_creation_input_tokens",costUsd:"cost_usd"};
function nodes(o:Observation) {
  return [[1,"turn:T"],[2,"request:R"],[4,"call:C"]] .filter(([bit])=>o.mask & Number(bit)).map(([,node])=>String(node));
}
function maximum(observations:Observation[]):Amounts {
  return Object.fromEntries(FIELDS.filter(k=>observations.some(o=>o.amount[k]!==undefined))
    .map(k=>[k,Math.max(...observations.map(o=>o.amount[k]??0))]));
}
function oracle(observations:Observation[], ackEach:boolean) {
  const parent = new Map<string,string>();
  const find=(node:string):string=>{if(!parent.has(node))parent.set(node,node);const p=parent.get(node)!;return p===node?node:find(p);};
  const identities = (o:Observation)=>nodes(o).length?nodes(o):["anonymous:"+o.ordinal];
  const link=(o:Observation)=>{const ids=identities(o),root=find(ids[0]!);for(const id of ids)parent.set(find(id),root);};
  const seen:Observation[]=[],paid:Array<{node:string;amount:Amounts}>=[];
  if(!ackEach) for(const o of observations)link(o);
  for(const o of observations) {
    link(o);seen.push(o);const root=find(identities(o)[0]!);
    const max=maximum(seen.filter(p=>find(identities(p)[0]!)===root));
    const amounts:Amounts={};
    for(const k of FIELDS)if(o.amount[k]!==undefined) {
      const previous=paid.filter(p=>find(p.node)===root).reduce((n,p)=>n+(p.amount[k]??0),0);
      amounts[k]=Math.max(0,(max[k]??0)-previous);
    }
    paid.push({node:identities(o)[0]!,amount:amounts});
  }
  const total=Object.fromEntries(FIELDS.filter(k=>observations.some(o=>o.amount[k]!==undefined))
    .map(k=>[k,paid.reduce((n,p)=>n+(p.amount[k]??0),0)])) as Amounts;
  return {total,components:new Set(observations.map(o=>find(identities(o)[0]!))).size};
}
function event(o:Observation,caseNumber:number) {
  const session=`00000000-0000-4000-8000-${String(caseNumber+1).padStart(12,"0")}`;
  const e=explodeOtlpPayload({resourceLogs:[{resource:{attributes:[attr("service.name","codex-app-server")]},scopeLogs:[{logRecords:[{
    timeUnixNano:String(BigInt(AT+caseNumber*1_800_000+o.ordinal*3000)*1_000_000n),traceId:(caseNumber*3+o.ordinal+1).toString(16).padStart(32,"0"),
    attributes:[attr("event.name","codex.sse_event"),attr("conversation.id",session),attr("model","gpt-6.1-sol"),
      ...(o.mask&1?[attr("turn.id","T")]:[]),...(o.mask&2?[attr("request_id","R")]:[]),...(o.mask&4?[attr("call_id","C")]:[]),
      ...Object.entries(o.amount).map(([k,v])=>attr(aliases[k as keyof typeof aliases],v))],
  }]}]}]}, {source:"codex",transportPath:"/v1/logs"}).events[0]!.event;
  for(const k of FIELDS)assert.equal(e[k],o.amount[k],"normalization preserves "+k);
  return e;
}
class Reader {
  b:any; now=new Date(AT+120_000); chunk=-1; file="";
  constructor(readonly Buffer:any,readonly label:string) {}
  open(caseNumber:number) {
    if(Math.floor(caseNumber/128)===this.chunk)return;
    this.close();this.chunk=Math.floor(caseNumber/128);this.file=path.join(ROOT,`${this.label}-${this.chunk}.sqlite`);
    this.b=new this.Buffer(this.file,{workspaceId:"11111111-1111-4111-8111-111111111111",deviceId:"identity-class",
      enrollmentNow:()=>new Date(AT-1_000_000),delivery:{enabled:true,now:()=>this.now}});
  }
  run(observations:Observation[],ackEach:boolean,caseNumber:number) {
    this.open(caseNumber);this.now=new Date(AT+caseNumber*1_800_000+120_000);
    return this.b.database.transaction(()=>{const frozen=new Map<string,{rawId:string;bytes:string;event:any}>();
    let appended=0,leases=0,acked=0;
    const drain=()=>{
      for(let attempt=0;attempt<4;attempt++) {
        this.now=new Date(this.now.getTime()+123_000);
        const lease=this.b.delivery.lease({now:this.now,maxRows:500,maxBytes:2_000_000});leases++;
        assert.equal(lease.locallyDead,0,this.label+" unexpected retirement");
        for(const item of lease.items) {
          const previous=frozen.get(item.deliveryId);if(previous)assert.equal(item.envelopeJson,previous.bytes);
          else frozen.set(item.deliveryId,{rawId:item.rawId,bytes:item.envelopeJson,event:item.envelope.event});
          assert.equal(item.envelope.event.metadata.usageSource==="capture_gap",false,"a native model cannot become a gap "+JSON.stringify({reader:this.label,caseNumber,observations,event:item.envelope.event}));
        }
        const receipt=this.b.delivery.acknowledge(lease.leaseId,lease.items.map((item:any)=>item.deliveryId),this.now);
        assert.equal(receipt.acknowledged,lease.items.length);acked+=receipt.acknowledged;
        if(!lease.items.length)break;
      }
    };
    for(const o of observations) {assert.equal(this.b.append(event(o,caseNumber)),true);appended++;if(ackEach)drain();}
    drain();assert.equal(appended,3);assert.ok(leases>0&&acked>0);
    if(this.label==="head")for(const [id,item] of frozen) {
      const row=this.b.database.prepare("select envelope_json as bytes from codex_named_captures where delivery_id=?").get(id);
      assert.equal(row?.bytes,item.bytes,"ACKed named bytes stay frozen");
    }
    const total=Object.fromEntries(FIELDS.filter(k=>[...frozen.values()].some(item=>item.event[k]!==undefined))
      .map(k=>[k,[...frozen.values()].reduce((n,item)=>n+(item.event[k]??0),0)])) as Amounts;
    return {total,appended,leases,acked};
    })();
  }
  close() {if(this.b){this.b.close();this.b=undefined;for(const suffix of ["","-wal","-shm"])fs.rmSync(this.file+suffix,{force:true});}}
}
function equal(a:Amounts,b:Amounts) {return FIELDS.every(k=>a[k]!==undefined=== (b[k]!==undefined) && Math.abs((a[k]??0)-(b[k]??0))<1e-10);}
async function enumerate() {
  const records:any[]=[],counterexamples:any[]=[],shrunk=new Map<string,any>();
  const start=performance.now();
  await withReader(FLOOR,async ({Buffer})=>{
    const head=new Reader(LocalEventBuffer,"head"),floor=new Reader(Buffer,"0.7.50");
    let caseNumber=0;
    try {for(let a=part![0]!;a<part![1]!;a++)for(let b=0;b<8;b++)for(let c=0;c<8;c++)for(let profile=0;profile<profiles.length;profile++)for(const ackEach of [false,true]) {
      const source=[a,b,c].map((mask,ordinal)=>({mask,ordinal,amount:profiles[profile]![ordinal]!}));
      const shape=`${a}-${b}-${c}/${profile===0?"complete":"partial"}/${ackEach?"ACK-each":"bridge-before-ACK"}`;
      for(const [orderNumber,order] of ORDER.entries()) {
        const observations=order.map(i=>source[i]!);const expected=maximum(source),paid=oracle(observations,ackEach);
        const h=head.run(observations,ackEach,caseNumber),f=floor.run(observations,ackEach,caseNumber++);
        assert.ok(equal(h.total,paid.total),"independent paid-prefix oracle "+JSON.stringify({shape,order,h,paid}));
        for(const k of FIELDS) {
          assert.ok((h.total[k]??0)>=(expected[k]??0)-1e-10,"known field lost "+shape+"/"+k);
          assert.ok(Math.abs((h.total[k]??0)-(expected[k]??0))<=Math.abs((f.total[k]??0)-(expected[k]??0))+1e-10,
            "REGRESSION against released 0.7.50 "+JSON.stringify({shape,order,k,h,f,expected}));
        }
        const ideal=equal(h.total,expected);
        const relation=equal(h.total,f.total)?"equal":"better";
        const row={shape,order:order.join(""),expected,head:h.total,released050:f.total,relation,idealMaximum:ideal,
          components:paid.components,operations:{head:{appended:h.appended,leases:h.leases,acked:h.acked},released050:{appended:f.appended,leases:f.leases,acked:f.acked}}};
        records.push(row);
        if(!ideal) {
          const limit=paid.components>1?"no-linking-evidence":"late-bridge-after-paid-overlap";
          counterexamples.push({...row,limit});
          // Remove every observation which is unnecessary to expose the
          // immutable-prefix mismatch; save each distinct minimal shape as
          // a permanent producer fixture, rather than dropping repetitions.
          let minimal=observations;
          for(const o of [...minimal]) {
            const candidate=minimal.filter(p=>p!==o);
            if(candidate.length>1) {
              const reduced=oracle(candidate,ackEach);
              const reducedLimit=reduced.components>1?"no-linking-evidence":"late-bridge-after-paid-overlap";
              // An accepted-overlap reproducer must retain its explicit
              // bridge. Removing it changes the failure to missing evidence.
              if(reducedLimit===limit&&!equal(reduced.total,maximum(candidate)))minimal=candidate;
            }
          }
          const key=JSON.stringify({ackEach,observations:minimal.map(o=>({mask:o.mask,amount:o.amount}))});
          shrunk.set(key,{ackEach,observations:minimal,limit,expected:maximum(minimal),paidPrefix:oracle(minimal,ackEach).total});
        }
        completion.check(shape+"/"+orderNumber);
      }
      if(caseNumber%768===0)console.log(JSON.stringify({progress:caseNumber,elapsedMs:Math.round(performance.now()-start)}));
    }} finally {head.close();floor.close();}
  });
  const result={records,counterexamples,minimal:[...shrunk.values()],elapsedMs:performance.now()-start};
  fs.writeFileSync(workerResult!,JSON.stringify(result)+"\n");
  completion.complete();
}
async function main() {
  if(part)return enumerate();
  const start=performance.now(),children:ReturnType<typeof spawn>[]=[],ranges=[[0,3],[3,6],[6,8]];
  let summaries:any[];
  try {
    const settled=await Promise.allSettled(ranges.map(async ([from,to])=>{
      const receipt=path.join(process.env.PLIMSOLL_PROOF_ROOT!,`identity-${from}-${to}-receipt.json`);
      const summary=path.join(process.env.PLIMSOLL_PROOF_ROOT!,`identity-${from}-${to}-summary.json`);
      const child=spawn(process.execPath,["--import","tsx",path.resolve("scripts/codex-response-identity-class-proof.ts"),
        `--partition=${from}:${to}`,`--worker-result=${summary}`],{cwd:process.cwd(),
        env:{...process.env,PLIMSOLL_PROOF_RECEIPT:receipt},stdio:["ignore","ignore","inherit"]});
      children.push(child);
      await new Promise<void>((resolve,reject)=>{child.once("error",reject);child.once("exit",(code,signal)=>
        code===0?resolve():reject(new Error(`identity partition ${from}:${to} exit ${code}/${signal}`)));});
      const checked=JSON.parse(fs.readFileSync(receipt,"utf8"));
      assert.ok(checked.completed&&checked.status==="passed"&&checked.runId===process.env.PLIMSOLL_PROOF_RUN_ID&&
        checked.counts.passed===(to!-from!)*1536&&checked.counts.failed===0,"complete owned worker receipt");
      return {from,to,receipt:checked,...JSON.parse(fs.readFileSync(summary,"utf8"))};
    }));
    for(const result of settled)if(result.status==="rejected")throw result.reason;
    summaries=settled.map(result=>(result as PromiseFulfilledResult<any>).value);
  } finally {
    for(const child of children)if(child.exitCode===null&&child.signalCode===null) {
      const done=new Promise<void>(resolve=>child.once("exit",()=>resolve()));child.kill("SIGTERM");await done;
    }
  }
  const records=summaries.flatMap(s=>s.records),counterexamples=summaries.flatMap(s=>s.counterexamples);
  const unique=new Set(records.map(r=>r.shape+"/"+r.order));assert.equal(records.length,12288);assert.equal(unique.size,12288);
  for(const summary of summaries)for(const check of summary.receipt.checks)completion.check(check.name,check.passed);
  const shrunk=new Map<string,any>();
  for(const summary of summaries)for(const fixture of summary.minimal) {
    const key=JSON.stringify({ackEach:fixture.ackEach,observations:fixture.observations.map((o:any)=>({mask:o.mask,amount:o.amount}))});
    shrunk.set(key,fixture);
  }
  const evidence=path.resolve("evidence/codex-response-identities");fs.mkdirSync(evidence,{recursive:true});
  fs.writeFileSync(path.join(evidence,"comparisons.json"),JSON.stringify(records)+"\n");
  fs.writeFileSync(path.join(evidence,"ideal-counterexamples.json"),JSON.stringify(counterexamples)+"\n");
  const permanent=path.resolve("scripts/fixtures/codex-response-identities/minimal-counterexamples.json");
  const minimal=[...shrunk.values()];
  if(process.argv.includes("--record-counterexamples"))fs.writeFileSync(permanent,JSON.stringify(minimal,null,2)+"\n");
  else assert.deepEqual(JSON.parse(fs.readFileSync(permanent,"utf8")),minimal,"all enumerated counterexamples remain permanent");
  console.log(JSON.stringify({comparisons:records.length,equal:records.filter(r=>r.relation==="equal").length,better:records.filter(r=>r.relation==="better").length,
    worse:0,idealMaximum:records.filter(r=>r.idealMaximum).length,idealCounterexamples:counterexamples.length,minimalCounterexamples:minimal.length,
    partitions:summaries.map(s=>({from:s.from,to:s.to,checks:s.records.length,elapsedMs:s.elapsedMs})),
    oracle:"producer field maximum checked; immutable paid-prefix exact; known-limit failures separately retained",floor:FLOOR,elapsedMs:performance.now()-start}));
  completion.complete();
}
main().catch(error=>{console.error(error);process.exitCode=1;}).finally(()=>fs.rmSync(ROOT,{recursive:true,force:true}));
