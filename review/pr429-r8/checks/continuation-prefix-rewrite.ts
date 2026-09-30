import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { LocalEventBuffer } from "../../../work/plimsoll-r5/packages/collector-cli/src/buffer";
import { startClaudeReplayBarrier } from "../../../work/plimsoll-r5/packages/collector-cli/src/claude-replay-barrier";
import { dispatchBindingSchema, durableClaudeRootSessionSightings, rootCursorKey } from
  "../../../work/plimsoll-r5/packages/collector-cli/src/capture-root-inventory";
import { collectorConfigSchema } from "../../../work/plimsoll-r5/packages/collector-cli/src/config";
import { appendForwardedHook } from "../../../work/plimsoll-r5/packages/collector-cli/src/forwarder";
import { jsonlScanStateKey } from "../../../work/plimsoll-r5/packages/collector-cli/src/jsonl-byte-tailer";
import { TranscriptTailer } from "../../../work/plimsoll-r5/packages/collector-cli/src/transcript-tailer";

const mode=process.argv[2]??"oversized";
assert.ok(mode==="oversized"||mode==="normal");
const now=Date.now(),sessionA=crypto.randomUUID(),sessionC=crypto.randomUUID();
const A={rootId:"claude-a",profileId:"profile-a",installationEpochId:"epoch-a",
  source:"claude_code" as const,directory:path.join(process.env.HOME!,"claude-a","projects")};
const B={rootId:"claude-b",profileId:"profile-b",installationEpochId:"epoch-b",
  source:"claude_code" as const,directory:path.join(process.env.HOME!,"claude-b","projects")};
const file=path.join(B.directory,"project","startup.jsonl");
fs.mkdirSync(A.directory,{recursive:true});
fs.mkdirSync(path.dirname(file),{recursive:true});
fs.mkdirSync(process.env.PLIMSOLL_HOME!,{recursive:true});
const record=(sessionId:string,id:string,padding="")=>JSON.stringify({type:"assistant",sessionId,
  timestamp:new Date(now-1_000).toISOString(),message:{id,
    model:"claude-sonnet-4-20250514",content:[],usage:{input_tokens:1,output_tokens:1,
      cache_read_input_tokens:0,cache_creation_input_tokens:0}},padding})+"\n";
const edge=JSON.stringify({type:"progress",padding:"x".repeat(70_000)})+"\n";
const original=edge+record(sessionC,"message-old")+edge;
const replacement=edge+record(sessionA,"message-old")+edge;
const appended=record(sessionC,"message-new","z".repeat(mode==="oversized"?150_000:1_000));
assert.equal(Buffer.byteLength(replacement),Buffer.byteLength(original));
assert.equal(replacement.slice(0,64*1024),original.slice(0,64*1024));
assert.equal(replacement.slice(-64*1024),original.slice(-64*1024));
fs.writeFileSync(file,original);
const binding=dispatchBindingSchema.parse({sessionId:sessionA,
  workItemId:"beads:eco-6hoxj.165.97",projectKey:`sha256:${"a".repeat(64)}`,
  companyRef:null,attemptId:crypto.randomUUID(),parentAttemptId:null,
  acceptedOutcomeId:null,validFrom:new Date(now-60_000).toISOString(),
  validUntil:new Date(now+60_000).toISOString(),evidenceRef:"review:r8-continuation-prefix"});
const config=collectorConfigSchema.parse({deviceId:"dev-r8-continuation-prefix",
  uploadUrl:"http://127.0.0.1:1/unused",captureRoots:[{...A,dispatch:[binding]},B]});
fs.writeFileSync(path.join(process.env.PLIMSOLL_HOME!,"collector.config.json"),JSON.stringify(config)+"\n");
const dbPath=path.join(process.env.PLIMSOLL_HOME!,"continuation.sqlite");
const options={workspaceId:config.tenantId,deviceId:config.deviceId,
  enrollmentNow:()=>new Date(now-3_600_000),delivery:{enabled:true},databaseBusyTimeoutMs:0};
let buffer=new LocalEventBuffer(dbPath,options);
const key=jsonlScanStateKey(rootCursorKey(config.captureRoots??[],file));
const cursor=()=>buffer.database.prepare(`select committed_offset as offset,
  committed_prefix_hash as digest from rollout_scan_state where file=?`).get(key) as
  {offset:number|null;digest:string|null}|undefined;

async function main() {
  const prior=new TranscriptTailer(buffer,A.directory,undefined,config.captureRoots??[]);
  try { const scan=await prior.scan({scope:"full"}); assert.equal(scan.parseErrors,0); }
  finally { prior.close(); }
  const oldCursor=cursor();
  assert.equal(oldCursor?.offset,Buffer.byteLength(original));
  assert.equal(oldCursor.digest,crypto.createHash("sha256").update(original).digest("hex"));
  assert.equal(durableClaudeRootSessionSightings(buffer.database,sessionC).size,1);
  assert.equal(durableClaudeRootSessionSightings(buffer.database,sessionA).size,0);
  const oldStat=fs.statSync(file,{bigint:true});
  buffer.close();
  const fd=fs.openSync(file,"r+");
  try {fs.writeSync(fd,replacement,0,"utf8");}
  finally {fs.closeSync(fd);}
  fs.appendFileSync(file,appended);
  const newStat=fs.statSync(file,{bigint:true});
  assert.equal(newStat.ino,oldStat.ino);
  assert.equal(newStat.birthtimeNs,oldStat.birthtimeNs);
  buffer=new LocalEventBuffer(dbPath,options);
  const resume=new TranscriptTailer(buffer,A.directory,undefined,config.captureRoots??[]);
  const scans:unknown[]=[];
  try {
    for(let i=0;i<12&&cursor()?.offset!==Buffer.byteLength(replacement+appended);i++) {
      const spans:Array<{offset:number;spanOffset:number|null;spanBytes:number|null}>=[];
      const scan=await resume.scan({scope:"full",onCommittedSourceSpan:(_,span,offset)=>{
        spans.push({offset,spanOffset:span?.offset??null,spanBytes:span?.bytes.length??null});
      }});
      scans.push({recordsCommitted:scan.recordsCommitted,slicesCommitted:scan.slicesCommitted,
        parseErrors:scan.parseErrors,continuationReasons:scan.continuationReasons,
        cursor:cursor()?.offset,spans});
    }
  } finally {resume.close();}
  const afterCursor=cursor();
  const actualDigest=crypto.createHash("sha256").update(fs.readFileSync(file)).digest("hex");
  const barrier=startClaudeReplayBarrier(buffer,config.captureRoots??[],{timeoutMs:800});
  const hook=appendForwardedHook({id:crypto.randomUUID(),hook_event_name:"AssistantResponse",
    session_id:sessionA,timestamp:new Date(now-1_000).toISOString()},
    {config,buffer,source:"claude_code",now:()=>now}).event;
  const receipt=await barrier.done;
  const row=buffer.database.prepare("select payload_json as payload from buffered_events where id=?")
    .get(hook.id) as {payload:string};
  const savedWork=JSON.parse(row.payload).metadata.workItemId??null;
  const evidence={mode,oldCursor,afterCursor,oldSize:Buffer.byteLength(original),
    finalSize:Buffer.byteLength(replacement+appended),appendedBytes:Buffer.byteLength(appended),
    inodeSame:newStat.ino===oldStat.ino,ctimeChanged:newStat.ctimeNs!==oldStat.ctimeNs,
    actualDigest,digestMatches:afterCursor?.digest===actualDigest,scans,
    rootBSawA:durableClaudeRootSessionSightings(buffer.database,sessionA).size>0,
    rootBSawC:durableClaudeRootSessionSightings(buffer.database,sessionC).size>0,
    receipt,savedWork};
  console.log(JSON.stringify(evidence));
  assert.equal(savedWork,null,"B's changed historical session must veto A's work stamp");
}
main().catch(error=>{console.error(error);process.exitCode=1;}).finally(()=>buffer.close());
