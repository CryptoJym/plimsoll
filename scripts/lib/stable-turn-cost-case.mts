/** Compare one unchanged 20-root, 248-file fixture on base and head. This
 * optional measurement writes no system-e2e receipt or normalizer artifact. */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const sourceRoot = process.env.PLIMSOLL_SOURCE_ROOT;
assert(sourceRoot,"PLIMSOLL_SOURCE_ROOT is required");
const load = (relative:string) => import(pathToFileURL(path.join(sourceRoot,relative)).href);
const [{LocalEventBuffer},{CollectorMaintenance},{RolloutTailer},{TranscriptTailer},{captureBaselineStatus}] = await Promise.all([
  load("packages/collector-cli/src/buffer.ts"),load("packages/collector-cli/src/maintenance.ts"),
  load("packages/collector-cli/src/rollout-tailer.ts"),load("packages/collector-cli/src/transcript-tailer.ts"),
  load("packages/collector-cli/src/capture-baseline.ts"),
]);
const home = fs.mkdtempSync(path.join(fs.realpathSync(process.env.PLIMSOLL_PROOF_HOME ?? os.tmpdir()),"stable-turn-cost-"));
const roots:Array<{source:"codex"|"claude_code";directory:string;rootId:string;profileId:string;installationEpochId:string}> = [];
const date = new Date(Date.now()-15*86_400_000).toISOString().slice(0,10).split("-");
for (const [source,count,files] of [["codex",13,173],["claude_code",7,75]] as const) {
  for (let i=0;i<count;i++) {
    const directory=path.join(home,source,String(i));
    const folder=path.join(directory,...(source==="codex"?date:["project"]));
    fs.mkdirSync(folder,{recursive:true});
    for (let j=i;j<files;j+=count) fs.writeFileSync(path.join(folder,`session-${j}.jsonl`),
      "PRIVATE_EXCLUDED_HISTORY\n");
    roots.push({source,directory,rootId:`${source}-${i}`,profileId:`${source}-${i}`,installationEpochId:""});
  }
}
let buffer:any,maintenance:any;
try {
  buffer=new LocalEventBuffer(path.join(home,"ledger.sqlite"),{
    workspaceId:"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",delivery:{enabled:true},
  });
  const epoch=buffer.workspaceBinding().currentInstallationEpochId;
  for (const root of roots) root.installationEpochId=epoch;
  maintenance=new CollectorMaintenance(buffer,
    new RolloutTailer(buffer,undefined,()=>[],undefined,roots.filter(r=>r.source==="codex")),
    new TranscriptTailer(buffer,undefined,undefined,roots.filter(r=>r.source==="claude_code")));
  for(let i=0;i<80&&captureBaselineStatus(buffer.database).status!=="complete";i++) await maintenance.runRecent();
  assert.equal(captureBaselineStatus(buffer.database).status,"complete","fixture baseline must finish");
  for(let i=0;i<10;i++) await maintenance.runRecent();
  const methods=["statSync","lstatSync","readdirSync","realpathSync","openSync"] as const;
  const originals=Object.fromEntries(methods.map(name=>[name,(fs as any)[name]])) as Record<string,Function>;
  const originalPrepare=buffer.database.prepare;
  let counts:Record<string,number>={};
  for(const name of methods) (fs as any)[name]=function(...args:unknown[]){counts[name]=(counts[name]??0)+1;
    return originals[name]!.apply(fs,args);};
  buffer.database.prepare=function(...args:unknown[]){counts.sqlitePrepare=(counts.sqlitePrepare??0)+1;
    return originalPrepare.apply(this,args);};
  const turns:Array<Record<string,number>>=[];
  try {
    for(let i=0;i<30;i++) {
      counts={};
      const cpu=process.cpuUsage();
      const wall=performance.now();
      const run=await maintenance.runRecent();
      const used=process.cpuUsage(cpu);
      turns.push({cpuMs:(used.user+used.system)/1000,wallMs:performance.now()-wall,
        bytesRead:run.rollout.bytesRead+run.transcript.bytesRead,
        filesRead:run.rollout.filesRead+run.transcript.filesRead,
        ...counts});
    }
  } finally {
    for(const name of methods) (fs as any)[name]=originals[name];
    buffer.database.prepare=originalPrepare;
  }
  const p95=(values:number[])=>values.slice().sort((a,b)=>a-b)[Math.ceil(values.length*.95)-1]??0;
  const metric=(key:string)=>{const values=turns.map(row=>row[key]??0);
    return {p95:p95(values),max:Math.max(...values),mean:values.reduce((a,b)=>a+b,0)/values.length};};
  const metrics=Object.fromEntries(["cpuMs","wallMs","bytesRead","filesRead",...methods,"sqlitePrepare"].map(key=>[key,metric(key)]));
  assert(turns.every(row=>row.bytesRead===0&&row.filesRead===0),"unchanged turns must not read source bodies");
  console.log(JSON.stringify({schema:"plimsoll.stable-turn-cost.v1",sourceRoot,roots:roots.length,
    excludedFiles:248,turns:turns.length,metrics,raw:turns,passed:true},null,2));
} finally {maintenance?.close();buffer?.close();fs.rmSync(home,{recursive:true,force:true});}
