/** The unadvertised request body must remain literal 0.7.44 v1 wire. */
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

import { deliveryAcknowledgement, deliveryExpectation } from "../packages/collector-cli/src/delivery-ack";

const oldCommit = "375f277b85f7d4ede7db77bf4359c371c0e8a4aa"; // 0.7.44 rollback build
const repo = path.resolve(import.meta.dirname,"..");
const root = fs.mkdtempSync(path.join(process.env.PLIMSOLL_PROOF_HOME ?? os.tmpdir(),"b22-v1-wire-"));
const oldRoot = path.join(root,"old-0744");
const tenantId = "00000000-0000-4000-8000-000000000081";
const installKey = "pli_b22_v1_wire_fixture_install";
const observedAt = "2029-01-01T00:00:00.000Z";
const now = () => new Date("2030-09-28T00:00:00.000Z");
const uuid = /[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}/gi;
const timestamp = /\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z/g;
const normalize = (body:string) => body.replace(uuid,"<uuid>").replace(timestamp,"<timestamp>");

async function capture(sourceRoot:string,url:string):Promise<void> {
  const load = (relative:string) => import(pathToFileURL(path.join(sourceRoot,relative)).href);
  const [{LocalEventBuffer},{collectorConfigSchema},{uploadBufferedEvents},{aiInteractionEventSchema}] = await Promise.all([
    load("packages/collector-cli/src/buffer.ts"),
    load("packages/collector-cli/src/config.ts"),
    load("packages/collector-cli/src/upload.ts"),
    load("packages/shared/src/index.ts"),
  ]);
  const fixtureName = sourceRoot===oldRoot ? "old" : "head";
  const home = path.join(root,fixtureName);
  fs.mkdirSync(home,{recursive:true});
  const buffer = new LocalEventBuffer(path.join(home,"ledger.sqlite"));
  try {
    buffer.useWorkspace(tenantId);
    assert.equal(buffer.append(aiInteractionEventSchema.parse({
      id:"00000000-0000-4000-8000-000000000001",
      sessionId:"00000000-0000-4000-8000-000000000002",
      actorId:"sha256:sessionproofaccount0000000000000000000001",
      source:"codex",dataMode:"metadata",eventType:"assistant_response",
      observedAt,actionClass:"other",inputTokens:1,outputTokens:1,
    })),true);
    const config = collectorConfigSchema.parse({tenantId,installKey,uploadUrl:url,
      delivery:{maxOldestAgeDays:3650}});
    const result = await uploadBufferedEvents(config,buffer,{spoolHome:path.join(home,"spools"),
      developmentLoopbackUrl:true,now,appVersion:"0.7.44"});
    assert.ok(result.uploadedEvents>0,`${fixtureName} sent no event: ${JSON.stringify(result)}`);
  } finally { buffer.close(); }
}

async function main() {
  let server:http.Server|null=null;
  try {
    execFileSync("git",["worktree","add","--detach",oldRoot,oldCommit],{cwd:repo,stdio:"pipe"});
    fs.symlinkSync(path.join(repo,"node_modules"),path.join(oldRoot,"node_modules"),"dir");
    assert.equal(JSON.parse(fs.readFileSync(path.join(oldRoot,"packages/collector-cli/package.json"),"utf8")).version,"0.7.44");
    // Build the rollback source in this disposable checkout before measuring.
    execFileSync("pnpm",["--dir",path.join(oldRoot,"packages/collector-cli"),"build"],
      {cwd:repo,stdio:"pipe",timeout:180_000});
    const received:string[]=[];
    server=http.createServer(async(request,response)=>{
      const chunks:Buffer[]=[];
      for await (const chunk of request) chunks.push(Buffer.from(chunk));
      const body=Buffer.concat(chunks).toString("utf8");
      received.push(body);
      const expected=deliveryExpectation(body,installKey);
      response.writeHead(200,{"content-type":"application/json"});
      response.end(JSON.stringify({ok:true,accepted:expected.itemIds.length,
        ack:deliveryAcknowledgement(expected,expected.itemIds)}));
    });
    await new Promise<void>((resolve,reject)=>server!.once("error",reject).listen(0,"127.0.0.1",resolve));
    const address=server.address();
    assert.ok(address && typeof address!=="string");
    const url=`http://127.0.0.1:${address.port}/api/work-intelligence/ingest`;
    await capture(oldRoot,url);
    const oldBodies=received.splice(0);
    await capture(repo,url);
    const headBodies=received.splice(0);
    assert.ok(oldBodies.length>0,"0.7.44 sent no event batch");
    assert.equal(headBodies.length,oldBodies.length,"request count changed");
    assert.deepEqual(headBodies.map(normalize),oldBodies.map(normalize));
    assert.notEqual(normalize(headBodies[0]!.replace('"appVersion":"0.7.44"','"appVersion":"mutated"')),
      normalize(oldBodies[0]!),"comparator missed a non-id, non-timestamp change");
    console.log(JSON.stringify({status:"PASS",oldCommit,requests:oldBodies.length,
      normalizedBodiesIdentical:true,nonIdMutationRejected:true}));
  } finally {
    if (server) await new Promise<void>((resolve)=>server!.close(()=>resolve()));
    try { execFileSync("git",["worktree","remove","--force",oldRoot],{cwd:repo,stdio:"pipe"}); }
    catch { /* The initial add may have failed; leave the original error intact. */ }
    fs.rmSync(root,{recursive:true,force:true});
  }
}
main().catch((error)=>{console.error(error);process.exitCode=1;});
