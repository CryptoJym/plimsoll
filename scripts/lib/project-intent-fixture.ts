import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import type { AddressInfo } from "node:net";
import { spawn } from "node:child_process";
import { createRequire } from "node:module";
import { collectorConfigSchema, type CollectorConfig } from "../../packages/collector-cli/src/config";
import { LocalEventBuffer } from "../../packages/collector-cli/src/buffer";
import { ProjectIntentProducer, type IntentChoice } from "../../packages/collector-cli/src/project-intent-producer";
import type { ProjectIntentReceipt } from "../../packages/shared/src/project-intent";

export const INTENT_FIXTURE_PROJECTS: IntentChoice[] = [
  { projectKey: `sha256:${"a".repeat(64)}`, projectLabel: "Client research", projectRegistryRevision: 1 },
  { projectKey: `sha256:${"b".repeat(64)}`, projectLabel: "Company overhead", projectRegistryRevision: 1 },
];
export function fixtureIntentAck(receipt: ProjectIntentReceipt, revision = 1, replayed = false) {
  return { schema: "plimsoll-project-intent-ack/v1", acknowledged: true,
    receiptId: receipt.receiptId, sessionId: receipt.sessionId, replayed, revision, receiptRevision: 1,
    project: { state: receipt.projectKey ? "known" : "unknown", projectKey: receipt.projectKey,
      company: receipt.projectKey ? "Fixture company" : null, registryRevision: receipt.projectRegistryRevision,
      reason: receipt.projectKey ? null : "needs_project", companyReason: receipt.projectKey ? null : "needs_project" } };
}
export async function createIntentFixture() {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-p04-")));
  fs.chmodSync(root, 0o700);
  for (const dir of ["collector", "codex", "codex-rotated", "claude", "home", "non-repo", "other", "bin"])
    fs.mkdirSync(path.join(root, dir), { mode: 0o700 });
  const calls: Array<{ method: string; url: string; body: string; headers: http.IncomingHttpHeaders }> = [];
  let projects = INTENT_FIXTURE_PROJECTS.map(choice => ({ ...choice }));
  let getStatus = 200;
  let post: (body: { receipt: ProjectIntentReceipt; expectedRevision: number }) => { status: number; body: unknown } = body =>
    ({ status: 202, body: fixtureIntentAck(body.receipt) });
  const server = http.createServer(async (req, res) => {
    const chunks: Buffer[] = [];
    for await (const chunk of req) chunks.push(Buffer.from(chunk));
    const raw = Buffer.concat(chunks).toString("utf8");
    const timestamp = req.headers["x-plimsoll-upload-timestamp"];
    const signature = `sha256=${crypto.createHmac("sha256", "p04-synthetic-signing-key").update(`${timestamp}.${raw}`).digest("hex")}`;
    calls.push({ method: req.method!, url: req.url!, body: raw, headers: req.headers });
    if (req.headers["x-plimsoll-install-key"] !== "p04-synthetic-install-key" || req.headers["x-plimsoll-upload-signature"] !== signature) {
      res.writeHead(401); res.end(JSON.stringify({ error: "unauthorized" })); return;
    }
    res.setHeader("content-type", "application/json");
    if (req.method === "GET") {
      res.writeHead(getStatus);
      res.end(JSON.stringify(getStatus === 200 ? { schema: "plimsoll-project-intent-projects/v1", projects, nextCursor: null } : { error: "device_revoked" }));
    } else {
      const result = post(JSON.parse(raw)); res.writeHead(result.status); res.end(JSON.stringify(result.body));
    }
  });
  await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
  const port = (server.address() as AddressInfo).port;
  const config: CollectorConfig = collectorConfigSchema.parse({
    tenantId: "24000000-0000-4000-8000-000000000010", cloudDeviceId: "24000000-0000-4000-8000-000000000002",
    deviceId: "dev_p04_synthetic_fixture", installKey: "p04-synthetic-install-key", uploadSigningSecret: "p04-synthetic-signing-key",
    uploadUrl: `http://127.0.0.1:${port}/api/work-intelligence/ingest`, port: 49997,
  });
  fs.writeFileSync(path.join(root, "collector", "collector.config.json"), JSON.stringify(config), { mode: 0o600 });
  const buffer = new LocalEventBuffer(path.join(root, "collector", "work-ledger.sqlite"), {
    workspaceId: config.tenantId, deviceId: config.deviceId, delivery: { enabled: false },
  });
  buffer.close();
  const p = new ProjectIntentProducer(config, { directory: path.join(root, "collector") });
  const cli = path.resolve("packages/collector-cli/src/cli.ts");
  const loader = createRequire(import.meta.url).resolve("tsx");
  const env: NodeJS.ProcessEnv = { ...process.env, HOME: path.join(root, "home"), USERPROFILE: path.join(root, "home"),
    PLIMSOLL_HOME: path.join(root, "collector"), CODEX_HOME: path.join(root, "codex"), CLAUDE_CONFIG_DIR: path.join(root, "claude"),
    XDG_CONFIG_HOME: path.join(root, "home", ".config"), XDG_CACHE_HOME: path.join(root, "home", ".cache"),
    XDG_STATE_HOME: path.join(root, "home", ".local", "state"), PATH: `${path.join(root, "bin")}:${process.env.PATH}` };
  const run = (args: string[], options: { cwd?: string; env?: NodeJS.ProcessEnv; input?: string } = {}) => new Promise<{ code: number | null; stdout: string; stderr: string }>((resolve, reject) => {
    const child = spawn(process.execPath, ["--import", loader, cli, ...args], {
      cwd: options.cwd ?? path.join(root, "non-repo"), env: { ...env, ...options.env }, stdio: "pipe",
    });
    let stdout = "", stderr = "";
    const timer = setTimeout(() => { child.kill("SIGKILL"); reject(new Error("fixture_cli_timeout")); }, 20_000);
    child.stdout.on("data", data => { stdout += data; }); child.stderr.on("data", data => { stderr += data; });
    child.once("error", error => { clearTimeout(timer); reject(error); });
    child.once("close", code => { clearTimeout(timer); resolve({ code, stdout, stderr }); });
    child.stdin.end(options.input ?? "");
  });
  const provider = `#!${process.execPath}\n${String.raw`
const fs=require('node:fs');
const cp=require('node:child_process');
const args=process.argv.slice(2);
const sessionFlag=args.indexOf('--session-id');
const nativeSession=sessionFlag>=0?args[sessionFlag+1]:process.env.FIXTURE_NATIVE_SESSION;
const i=args.indexOf('--settings');
if(i>=0) {
 const hooks=JSON.parse(args[i+1]).hooks.SessionStart[0].hooks;
 for(const source of ['startup','resume','clear','compact']) for(const hook of hooks) {
  const sessionId=['clear','compact'].includes(source)?process.env.FIXTURE_CLEAR_NATIVE_SESSION??nativeSession:nativeSession;
  const input=JSON.stringify({hook_event_name:'SessionStart',session_id:sessionId,source,
    cwd:process.cwd(),transcript_path:'/private/PROMPT_PATH_DO_NOT_EXPORT',prompt:'PROMPT_DO_NOT_EXPORT',secret:'SECRET_DO_NOT_EXPORT'});
  const result=cp.spawnSync('/bin/sh',['-c',hook.command],{input,env:process.env,encoding:'utf8',timeout:10000});
  if(result.status!==0) { console.error(result.stdout,result.stderr); process.exit(91); }
  if(result.stdout.trim()) console.log(result.stdout.trim());
 }
}
console.log('FIXTURE:'+JSON.stringify({cwd:process.cwd(),staleProject:process.env.PLIMSOLL_PROJECT_KEY??null,
 defaultProject:process.env.PLIMSOLL_FOLDER_PROJECT??null,launchId:process.env.PLIMSOLL_INTENT_LAUNCH_ID}));
process.exit(Number(process.env.FIXTURE_EXIT_CODE??0));
`}`;
  for (const name of ["claude", "codex"]) fs.writeFileSync(path.join(root, "bin", name), provider, { mode: 0o700 });
  return { root, p, config, calls, env, run,
    sourceRoot: path.join(root, "codex"),
    setProjects(value: IntentChoice[]) { projects = value; },
    setGetStatus(value: number) { getStatus = value; },
    setPost(value: typeof post) { post = value; },
    async close() {
      server.closeAllConnections(); await new Promise<void>(resolve => server.close(() => resolve()));
      fs.rmSync(root, { recursive: true, force: true });
    },
    assertInstallAuth() { assert.ok(calls.every(call => call.headers["x-plimsoll-install-key"] === "p04-synthetic-install-key")); },
  };
}
