/** A proxy must never see a collector request addressed to a loopback host. */
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import fs from "node:fs";
import http from "node:http";
import type net from "node:net";
import path from "node:path";

import { createProofCompletion } from "./lib/proof-completion";
import { deliveryAcknowledgement, deliveryExpectation } from "../packages/collector-cli/src/delivery-ack";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { aiInteractionEventSchema } from "../packages/shared/src/index";

const repo = path.resolve(import.meta.dirname, "..");
const fixtureRoot = path.join(process.env.PLIMSOLL_PROOF_ROOT ?? "", "loopback-proxy");
const tenantId = "00000000-0000-4000-8000-000000000001";
const installKey = "fixture-loopback-install-key";
const uuid = (n: number) => `00000000-0000-4000-8000-${String(n).padStart(12, "0")}`;

async function client(mode: string, url: string, home: string) {
  if (mode === "native-proxy-probe") {
    await fetch(url).catch(() => undefined);
  } else if (mode === "join") {
    const { performJoin } = await import("../packages/collector-cli/src/join");
    const result = await performJoin({ target: "pljt_fixture-token", baseUrl: url, homeDir: home,
      temporaryRoot: path.join(home, "temporary") });
    assert.equal(result.joined, false);
  } else if (mode === "salt") {
    const { syncAccountActorSalt } = await import("../packages/collector-cli/src/account-salt");
    const result = await syncAccountActorSalt({ collectorHome: path.join(home, ".plimsoll"), tenantId,
      cloudDeviceId: uuid(2), uploadUrl: url, installKey });
    assert.equal(result.reason, "unallocated");
  } else if (mode === "listener") {
    const { observeCollectorListener } = await import("../packages/collector-cli/src/runtime-ownership");
    assert.equal((await observeCollectorListener(Number(new URL(url).port))).kind, "unrelated");
  } else if (mode === "hook") {
    const { forwardHookOverLoopback } = await import("../packages/collector-cli/src/local-hook-client");
    const result = await forwardHookOverLoopback("{}", { source: "codex", port: Number(new URL(url).port),
      auth: { version: 1, claudeCodeProducer: "fixture-claude", codexProducer: "fixture-codex",
        managementRead: "fixture-management" } });
    assert.deepEqual(result, { accepted: true });
  } else if (mode === "hosted") {
    const { postJson } = await import("../packages/collector-cli/src/http-transport");
    await assert.rejects(postJson({ url, body: "{}", timeoutMs: 3_000 }), /network_error/);
  } else {
    throw new Error(`unknown client mode: ${mode}`);
  }
}

async function listen(server: http.Server, host = "127.0.0.1") {
  await new Promise<void>((resolve) => server.listen(0, host, resolve));
  return `http://${host.includes(":") ? `[${host}]` : host}:${(server.address() as net.AddressInfo).port}`;
}

async function close(server: http.Server) {
  server.closeAllConnections();
  await new Promise<void>((resolve) => server.close(() => resolve()));
}

type ProxyMechanism = "native NODE_USE_ENV_PROXY" | "undici EnvHttpProxyAgent";

async function runChild(args: string[], home: string, proxyUrl: string, mechanism?: ProxyMechanism) {
  const env: NodeJS.ProcessEnv = { ...process.env, HOME: home, USERPROFILE: home,
    PLIMSOLL_HOME: path.join(home, ".plimsoll"), CODEX_HOME: path.join(home, ".codex"),
    CLAUDE_CONFIG_DIR: path.join(home, ".claude"), TMPDIR: path.join(home, "tmp"),
    NODE_USE_ENV_PROXY: "1", HTTP_PROXY: proxyUrl, HTTPS_PROXY: proxyUrl };
  delete env.NO_PROXY;
  delete env.no_proxy;
  fs.mkdirSync(env.PLIMSOLL_HOME!, { recursive: true, mode: 0o700 });
  fs.mkdirSync(env.CODEX_HOME!, { recursive: true, mode: 0o700 });
  fs.mkdirSync(env.CLAUDE_CONFIG_DIR!, { recursive: true, mode: 0o700 });
  fs.mkdirSync(env.TMPDIR!, { recursive: true, mode: 0o700 });
  const imports = mechanism === "undici EnvHttpProxyAgent"
    ? ["--import", path.join(repo, "scripts/fixtures/undici-env-proxy-agent.mjs")]
    : [];
  const child = spawn(process.execPath, [...imports, "--import", path.join(repo, "node_modules/tsx/dist/loader.mjs"), ...args],
    { cwd: repo, env, stdio: ["ignore", "pipe", "pipe"] });
  let stdout = "", stderr = "";
  child.stdout.setEncoding("utf8");
  child.stderr.setEncoding("utf8");
  child.stdout.on("data", (chunk: string) => { stdout += chunk; });
  child.stderr.on("data", (chunk: string) => { stderr += chunk; });
  const timer = setTimeout(() => child.kill("SIGKILL"), 20_000);
  try {
    const code = await new Promise<number | null>((resolve, reject) => {
      child.once("error", reject);
      child.once("exit", resolve);
    });
    return { code, stdout, stderr };
  } finally {
    clearTimeout(timer);
  }
}

function homeFor(name: string) {
  const home = path.join(fixtureRoot, name);
  fs.mkdirSync(home, { recursive: true, mode: 0o700 });
  return home;
}

function seedUpload(home: string, uploadUrl?: string) {
  const collectorHome = path.join(home, ".plimsoll");
  fs.mkdirSync(collectorHome, { recursive: true, mode: 0o700 });
  fs.writeFileSync(path.join(collectorHome, "collector.config.json"), JSON.stringify({ tenantId, installKey,
    uploadSigningSecret: "fixture-loopback-signing-secret-0123456789", ...(uploadUrl ? { uploadUrl } : {}) }), { mode: 0o600 });
  const buffer = new LocalEventBuffer(path.join(collectorHome, "work-ledger.sqlite"), { workspaceId: tenantId });
  try {
    buffer.append(aiInteractionEventSchema.parse({ id: uuid(100), sessionId: uuid(101), source: "codex",
      eventType: "assistant_response", observedAt: new Date().toISOString(), inputTokens: 7 }));
  } finally {
    buffer.close();
  }
}

async function proof() {
  const completion = createProofCompletion("loopback-proxy", 15);
  fs.mkdirSync(fixtureRoot, { recursive: true, mode: 0o700 });
  const direct: string[] = [];
  const forwarded: string[] = [];
  const connects: string[] = [];
  const sinkHandler: http.RequestListener = (request, response) => {
    direct.push(request.url ?? "");
    const chunks: Buffer[] = [];
    request.on("data", (chunk: Buffer) => chunks.push(chunk));
    request.on("end", () => {
      const body = Buffer.concat(chunks).toString("utf8");
      response.setHeader("content-type", "application/json");
      if (request.url === "/api/work-intelligence/ingest") {
        const expected = deliveryExpectation(body, installKey);
        response.end(JSON.stringify({ ok: true, accepted: expected.itemIds.length,
          inserted: expected.itemIds.length, ack: deliveryAcknowledgement(expected, expected.itemIds) }));
      } else if (request.url === "/api/work-intelligence/join") {
        response.writeHead(400); response.end(JSON.stringify({ reason: "unknown" }));
      } else if (request.url === "/api/work-intelligence/account-actor-salt") {
        response.writeHead(404); response.end("{}");
      } else if (request.url === "/hooks/codex") {
        response.writeHead(202); response.end("{}");
      } else {
        response.end("{}");
      }
    });
  };
  const sink = http.createServer(sinkHandler);
  const ipv6Sink = http.createServer(sinkHandler);
  const proxy = http.createServer((request, response) => {
    forwarded.push(request.url ?? "");
    request.resume(); response.writeHead(502); response.end();
  });
  proxy.on("connect", (request, socket) => {
    connects.push(request.url ?? "");
    socket.end("HTTP/1.1 502 Bad Gateway\r\n\r\n");
  });
  const results: Array<Record<string, unknown>> = [];
  let proxyMechanism: ProxyMechanism = "native NODE_USE_ENV_PROXY";
  let caseIndex = 0;
  const check = async (name: string, action: () => Promise<{ pass: boolean; detail: Record<string, unknown> }>) => {
    try {
      const result = await action();
      completion.check(name, result.pass);
      results.push({ name, proxyMechanism, ...result });
    } catch (error) {
      completion.check(name, false);
      results.push({ name, proxyMechanism, pass: false, error: String(error) });
    }
  };
  try {
    const sinkUrl = await listen(sink);
    const ipv6Url = await listen(ipv6Sink, "::1");
    const proxyUrl = await listen(proxy);
    const uploadUrl = `${sinkUrl}/api/work-intelligence/ingest`;
    const ipv6UploadUrl = `${ipv6Url}/api/work-intelligence/ingest`;
    const localhostUploadUrl = `http://localhost:${new URL(ipv6Url).port}/api/work-intelligence/ingest`;
    const counts = () => ({ direct: direct.length, forwarded: forwarded.length, connects: connects.length });
    // Detect the actual Node capability with local servers. Older CI Nodes
    // ignore NODE_USE_ENV_PROXY, so their children install undici's proxy
    // dispatcher explicitly. No external DNS or hosted service is involved.
    const probeHome = homeFor("native-proxy-probe");
    const probeBefore = counts();
    const probe = await runChild([import.meta.filename, "--client", "native-proxy-probe",
      `${sinkUrl}/native-proxy-probe`, probeHome], probeHome, proxyUrl);
    const probeAfter = counts();
    if (probe.code !== 0) throw new Error(`native proxy capability probe failed: ${probe.stderr}`);
    const nativeEnvProxySupported = probeAfter.forwarded > probeBefore.forwarded ||
      probeAfter.connects > probeBefore.connects;
    proxyMechanism = nativeEnvProxySupported ? "native NODE_USE_ENV_PROXY" : "undici EnvHttpProxyAgent";
    const run = async (args: string[], joinedUrl?: string) => {
      const home = homeFor(`case-${++caseIndex}`);
      seedUpload(home, joinedUrl);
      const before = counts();
      const child = await runChild(["packages/collector-cli/src/cli.ts", ...args], home, proxyUrl, proxyMechanism);
      const after = counts();
      const forwardedRequests = after.forwarded - before.forwarded;
      const connectRequests = after.connects - before.connects;
      return { code: child.code, direct: after.direct - before.direct,
        forwardedRequests, connectRequests, proxy: forwardedRequests + connectRequests,
        flagMessage: `${child.stdout}\n${child.stderr}`.includes("--dev-loopback-url allows only"),
        warning: child.stderr.includes("WARNING: --dev-loopback-url") };
    };
    for (const [name, args, joined] of [
      ["flag_upload_direct", ["upload-history", "--full", "--dev-loopback-url", "--url", uploadUrl], undefined],
      ["localhost_upload_direct", ["upload-history", "--full", "--dev-loopback-url", "--url", localhostUploadUrl], undefined],
      ["ipv6_upload_direct", ["upload-history", "--full", "--dev-loopback-url", "--url", ipv6UploadUrl], undefined],
      ["joined_upload_direct", ["upload-history", "--full"], uploadUrl],
    ] as Array<[string, string[], string | undefined]>) {
      await check(name, async () => {
        const detail = await run(args, joined);
        return { pass: detail.code === 0 && detail.direct === 1 && detail.proxy === 0 &&
          detail.warning === (name !== "joined_upload_direct"), detail };
      });
    }
    for (const [name, mode, url] of [
      ["join_direct", "join", sinkUrl],
      ["salt_direct", "salt", uploadUrl],
      ["listener_direct", "listener", sinkUrl],
      ["hook_forward_direct", "hook", sinkUrl],
      ["hosted_https_uses_proxy", "hosted", "https://workspace.example/api/work-intelligence/ingest"],
    ]) {
      await check(name, async () => {
        const home = homeFor(`case-${++caseIndex}`);
        const before = counts();
        const child = await runChild([import.meta.filename, "--client", mode!, url!, home], home, proxyUrl, proxyMechanism);
        const after = counts();
        const detail = { code: child.code, direct: after.direct - before.direct,
          forwardedRequests: after.forwarded - before.forwarded,
          connectRequests: after.connects - before.connects, stderr: child.stderr.slice(-300) };
        return { pass: child.code === 0 && (mode === "hosted"
          ? detail.connectRequests > 0 && detail.forwardedRequests === 0 && detail.direct === 0
          : detail.direct === 1 && detail.connectRequests === 0 && detail.forwardedRequests === 0), detail };
      });
    }
    const refusals: Array<[string, string]> = [
      ["localhost_dot", "http://localhost.:3000/ingest"],
      ["localhost_subdomain", "http://x.localhost:3000/ingest"],
      ["localtest_me", "http://localtest.me:3000/ingest"],
      ["nip_io", "http://127.0.0.1.nip.io:3000/ingest"],
      ["ipv4_mapped", "http://[::ffff:127.0.0.1]:3000/ingest"],
      ["embedded_credentials", "http://user:pass@localhost:3000/ingest"],
    ];
    for (const [name, url] of refusals) {
      await check(`flag_refuses_${name}`, async () => {
        const detail = await run(["upload-history", "--full", "--dev-loopback-url", "--url", url]);
        return { pass: detail.code !== 0 && detail.flagMessage && detail.direct === 0 && detail.proxy === 0,
          detail: { ...detail, input: url } };
      });
    }
    console.log(JSON.stringify({ schema: "plimsoll.loopback-proxy-proof/v1", node: process.version,
      nativeEnvProxySupported, proxyMechanism, results }, null, 2));
    completion.complete();
  } finally {
    await close(sink);
    await close(ipv6Sink);
    await close(proxy);
  }
}

if (process.argv[2] === "--client") {
  void client(process.argv[3]!, process.argv[4]!, process.argv[5]!).catch((error) => {
    console.error(error); process.exitCode = 1;
  });
} else {
  void proof().catch((error) => { console.error(error); process.exitCode = 1; });
}
