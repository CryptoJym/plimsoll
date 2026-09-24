/** Generated local curl hooks must not expose their payload or token to a proxy. */
import { spawn } from "node:child_process";
import fs from "node:fs";
import http from "node:http";
import path from "node:path";
import type { AddressInfo } from "node:net";

import { generateHookForwardCommand } from "../packages/collector-config/src/templates";
import { createProofCompletion } from "./lib/proof-completion";

const completion = createProofCompletion("managed-curl-proxy", 6);
const root = path.join(process.env.PLIMSOLL_PROOF_ROOT!, "managed-curl-proxy");
const body = '{"fixture":"local-hook"}';

function listen(server: http.Server) {
  return new Promise<number>((resolve) => server.listen(0, "127.0.0.1", () =>
    resolve((server.address() as AddressInfo).port)));
}

function runHook(command: string, env: NodeJS.ProcessEnv) {
  return new Promise<number | null>((resolve, reject) => {
    const child = spawn("/bin/sh", ["-c", command], { env, stdio: ["pipe", "ignore", "pipe"] });
    child.stderr.resume();
    child.once("error", reject);
    child.once("close", resolve);
    child.stdin.end(body);
  });
}

async function main() {
  fs.mkdirSync(root, { recursive: true, mode: 0o700 });
  const bin = path.join(root, "bin");
  fs.mkdirSync(bin, { mode: 0o700 });
  fs.writeFileSync(path.join(bin, "uuidgen"),
    "#!/bin/sh\nprintf '%s\\n' 00000000-0000-4000-8000-000000000001\n", { mode: 0o700 });
  const direct: Array<{ url: string; token: string | undefined; body: string }> = [];
  const proxyRequests: string[] = [];
  const sink = http.createServer((request, response) => {
    const chunks: Buffer[] = [];
    request.on("data", (chunk: Buffer) => chunks.push(chunk));
    request.on("end", () => {
      direct.push({ url: request.url ?? "", token: request.headers["x-plimsoll-token"] as string | undefined,
        body: Buffer.concat(chunks).toString("utf8") });
      response.writeHead(202); response.end();
    });
  });
  const proxy = http.createServer((request, response) => {
    proxyRequests.push(`forwarded:${request.url}`);
    request.resume(); response.writeHead(202); response.end();
  });
  proxy.on("connect", (request, socket) => {
    proxyRequests.push(`connect:${request.url}`);
    socket.end("HTTP/1.1 502 Bad Gateway\r\n\r\n");
  });
  const results: Array<Record<string, unknown>> = [];
  try {
    const sinkPort = await listen(sink);
    const proxyPort = await listen(proxy);
    const proxyUrl = `http://127.0.0.1:${proxyPort}`;
    for (const source of ["codex", "grok"] as const) {
      const headerFile = path.join(root, `${source}.headers`);
      fs.writeFileSync(headerFile, "x-plimsoll-token: fixture-token\n", { mode: 0o600 });
      const hook = generateHookForwardCommand({ repoRoot: root, port: sinkPort,
        codexHeaderFile: headerFile, grokHeaderFile: headerFile, grokCurlCommand: "/usr/bin/curl" }, source);
      for (const scenario of ["http_proxy", "ALL_PROXY", "both_with_localhost_no_proxy"] as const) {
        const env: NodeJS.ProcessEnv = { ...process.env, PATH: `${bin}:${process.env.PATH ?? ""}` };
        for (const key of ["http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY", "all_proxy", "ALL_PROXY",
          "no_proxy", "NO_PROXY"]) delete env[key];
        env.HTTPS_PROXY = proxyUrl;
        if (scenario !== "ALL_PROXY") env.http_proxy = proxyUrl;
        if (scenario !== "http_proxy") env.ALL_PROXY = proxyUrl;
        if (scenario === "both_with_localhost_no_proxy") env.no_proxy = "localhost";
        const beforeDirect = direct.length;
        const beforeProxy = proxyRequests.length;
        const status = await runHook(hook, env);
        const newDirect = direct.slice(beforeDirect);
        const newProxy = proxyRequests.length - beforeProxy;
        const pass = status === 0 && newProxy === 0 && newDirect.length === 1 &&
          newDirect[0]?.url === `/hooks/${source}` && newDirect[0]?.token === "fixture-token" &&
          newDirect[0]?.body === body;
        completion.check(`${source}_${scenario}_direct`, pass);
        results.push({ source, scenario, pass, status, direct: newDirect.length, proxy: newProxy });
      }
    }
    console.log(JSON.stringify({ schema: "plimsoll.managed-curl-proxy-proof/v1", results }, null, 2));
    completion.complete();
  } finally {
    sink.closeAllConnections();
    proxy.closeAllConnections();
    await Promise.all([
      new Promise<void>((resolve) => sink.close(() => resolve())),
      new Promise<void>((resolve) => proxy.close(() => resolve())),
    ]);
  }
}

void main().catch((error) => { console.error(error); process.exitCode = 1; });
