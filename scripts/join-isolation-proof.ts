#!/usr/bin/env node

import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import crypto from "node:crypto";
import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import {
  collectorBufferPath,
  collectorConfigPath,
  collectorConfigSchema,
  type CollectorConfig,
} from "../packages/collector-cli/src/config";
import { appendForwardedHook } from "../packages/collector-cli/src/forwarder";
import {
  COLLECTOR_APP_VERSION,
  performJoin,
} from "../packages/collector-cli/src/join";

type Check = { name: string; detail: Record<string, unknown> };
type RequestRecord = { init?: RequestInit; url: string; body: Record<string, unknown> };

const checks: Check[] = [];
const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-join-isolation-proof-"));
const originalPlimsollHome = process.env.PLIMSOLL_HOME;
delete process.env.PLIMSOLL_HOME;

const TENANT_A = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
const TENANT_B = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";
const INSTALL_A = "pli_workspace_a_install";
const INSTALL_B = "pli_workspace_b_install";
const SECRET_A = "workspace-a-signing-secret-0123456789";
const TOKEN = "pljt_never-print-this-single-use-token";

function check(name: string, condition: unknown, detail: Record<string, unknown>) {
  assert.ok(condition, `${name}: ${JSON.stringify(detail)}`);
  checks.push({ name, detail });
}

function home(name: string) {
  return path.join(root, name);
}

function oldConfig(): CollectorConfig {
  return collectorConfigSchema.parse({
    port: 49123,
    tenantId: TENANT_A,
    installKey: INSTALL_A,
    ingestKey: "workspace-a-legacy-ingest-key",
    uploadSigningSecret: SECRET_A,
    uploadUrl: "https://workspace-a.example/api/work-intelligence/ingest",
    retentionDays: 37,
  });
}

function writeConfig(homeDir: string, config = oldConfig()) {
  const configPath = collectorConfigPath(homeDir);
  fs.mkdirSync(path.dirname(configPath), { recursive: true, mode: 0o700 });
  // Deliberately non-canonical whitespace makes byte preservation stronger
  // than merely comparing parsed values.
  const bytes = `${JSON.stringify(config, null, 4)}\n\n`;
  fs.writeFileSync(configPath, bytes, { mode: 0o600 });
  return { bytes, configPath };
}

function responseJson(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

function requestUrl(input: Parameters<typeof fetch>[0]) {
  if (typeof input === "string") return new URL(input);
  if (input instanceof URL) return input;
  return new URL(input.url);
}

function requestBody(init?: RequestInit) {
  return JSON.parse(String(init?.body ?? "{}")) as Record<string, unknown>;
}

function successfulFetch(options: {
  configPath: string;
  configBytes: string;
  grant?: Record<string, unknown>;
  requests: RequestRecord[];
}) {
  return (async (input, init) => {
    const url = requestUrl(input);
    const body = requestBody(init);
    options.requests.push({ url: url.href, init, body });
    assert.equal(
      fs.readFileSync(options.configPath, "utf8"),
      options.configBytes,
      "active config changed before handshake completion",
    );
    if (url.pathname.endsWith("/join")) {
      return responseJson(
        options.grant ?? {
          ok: true,
          tenantId: TENANT_B,
          installKey: INSTALL_B,
          uploadUrl: "https://workspace-b.example/api/work-intelligence/ingest",
        },
        201,
      );
    }
    return responseJson({ ok: true, accepted: 1 }, 200);
  }) as typeof fetch;
}

async function expectRejected(
  action: () => Promise<unknown>,
  pattern: RegExp,
) {
  let message = "";
  try {
    await action();
  } catch (error) {
    message = error instanceof Error ? error.message : String(error);
  }
  assert.match(message, pattern);
  return message;
}

function hashFile(file: string) {
  return crypto.createHash("sha256").update(fs.readFileSync(file)).digest("hex");
}

async function runChild(args: string[], input: string, env: NodeJS.ProcessEnv) {
  const child = spawn(process.execPath, args, {
    cwd: process.cwd(),
    env,
    stdio: ["pipe", "pipe", "pipe"],
  });
  let stdout = "";
  let stderr = "";
  child.stdout.setEncoding("utf8");
  child.stderr.setEncoding("utf8");
  child.stdout.on("data", (chunk: string) => (stdout += chunk));
  child.stderr.on("data", (chunk: string) => (stderr += chunk));
  child.stdin.end(input);
  const exit = await new Promise<{ code: number | null; signal: NodeJS.Signals | null }>(
    (resolve, reject) => {
      child.once("error", reject);
      child.once("exit", (code, signal) => resolve({ code, signal }));
    },
  );
  return { child, exit, stdout, stderr };
}

async function main() {
try {
  // Workspace A has real unsent history. Joining B may see neither its bytes
  // nor its outbox; only one isolated synthetic probe is eligible.
  const isolatedHome = home("workspace-a-backlog");
  const { bytes: configABytes, configPath: configAPath } = writeConfig(isolatedHome);
  const ledgerPath = collectorBufferPath(isolatedHome);
  const activeBuffer = new LocalEventBuffer(ledgerPath);
  const backlog = appendForwardedHook(
    {
      id: "workspace_a_private_backlog_event",
      source: "claude_code",
      event_type: "UserPromptSubmit",
    },
    { config: oldConfig(), buffer: activeBuffer, source: "claude_code" },
  );
  activeBuffer.close();
  const ledgerHashBefore = hashFile(ledgerPath);
  const requests: RequestRecord[] = [];
  const temporaryRoot = path.join(root, "successful-temporary-state");
  fs.mkdirSync(temporaryRoot);
  const joined = await performJoin({
    target: TOKEN,
    baseUrl: "https://workspace-b.example",
    homeDir: isolatedHome,
    temporaryRoot,
    fetchImpl: successfulFetch({
      configPath: configAPath,
      configBytes: configABytes,
      requests,
    }),
  });
  assert.equal(joined.joined, true);
  const handshakeBody = requests[1]?.body;
  const handshakeEvents = handshakeBody?.events as Array<{
    event?: { id?: string };
  }>;
  const activated = JSON.parse(fs.readFileSync(configAPath, "utf8")) as Record<string, unknown>;
  check(
    "workspace_backlog_isolated_and_success_activates_after_one_probe",
    requests.length === 2 &&
      requests.every((request) => request.init?.redirect === "manual") &&
      handshakeBody?.tenantId === TENANT_B &&
      handshakeBody?.installKey === INSTALL_B &&
      handshakeBody?.appVersion === COLLECTOR_APP_VERSION &&
      handshakeEvents.length === 1 &&
      handshakeEvents[0]?.event?.id === joined.handshake.selfTestEventId &&
      handshakeEvents[0]?.event?.id !== backlog.event.id &&
      joined.handshake.uploadedEvents === 1 &&
      hashFile(ledgerPath) === ledgerHashBefore &&
      fs.readdirSync(temporaryRoot).length === 0 &&
      activated.tenantId === TENANT_B &&
      activated.installKey === INSTALL_B &&
      activated.port === 49123 &&
      activated.retentionDays === 37,
    {
      requests: requests.length,
      handshakeEvents: handshakeEvents.length,
      activeLedgerUnchanged: hashFile(ledgerPath) === ledgerHashBefore,
      appVersion: handshakeBody?.appVersion,
    },
  );
  check(
    "grant_absence_removes_stale_credentials",
    !("uploadSigningSecret" in activated) && !("ingestKey" in activated),
    {
      signingSecretPresent: "uploadSigningSecret" in activated,
      ingestKeyPresent: "ingestKey" in activated,
    },
  );

  // A 5xx handshake must delete every temporary ledger file and preserve the
  // unusual original bytes, despite the preceding successful grant.
  const failedHome = home("handshake-failure");
  const { bytes: failedBytes, configPath: failedConfigPath } = writeConfig(failedHome);
  const failedTemp = path.join(root, "failed-temporary-state");
  fs.mkdirSync(failedTemp);
  let failedCalls = 0;
  const failedMessage = await expectRejected(
    () =>
      performJoin({
        target: TOKEN,
        baseUrl: "https://workspace-b.example",
        homeDir: failedHome,
        temporaryRoot: failedTemp,
        fetchImpl: (async (input, init) => {
          failedCalls += 1;
          const url = requestUrl(input);
          assert.equal(fs.readFileSync(failedConfigPath, "utf8"), failedBytes);
          assert.equal(init?.redirect, "manual");
          if (url.pathname.endsWith("/join")) {
            return responseJson({
              ok: true,
              tenantId: TENANT_B,
              installKey: INSTALL_B,
              uploadUrl: "https://workspace-b.example/api/work-intelligence/ingest",
            }, 201);
          }
          return responseJson({ ok: false }, 503);
        }) as typeof fetch,
      }),
    /not activated.*handshake failed/i,
  );
  check(
    "handshake_failure_preserves_config_and_cleans_temp_state",
    failedCalls === 2 &&
      fs.readFileSync(failedConfigPath, "utf8") === failedBytes &&
      fs.readdirSync(failedTemp).length === 0,
    { failedCalls, failedMessage, temporaryEntries: fs.readdirSync(failedTemp) },
  );

  let zeroAcceptedCalls = 0;
  const zeroAcceptedMessage = await expectRejected(
    () =>
      performJoin({
        target: TOKEN,
        baseUrl: "https://workspace-b.example",
        homeDir: failedHome,
        temporaryRoot: failedTemp,
        fetchImpl: (async (input) => {
          zeroAcceptedCalls += 1;
          if (requestUrl(input).pathname.endsWith("/join")) {
            return responseJson({
              ok: true,
              tenantId: TENANT_B,
              installKey: INSTALL_B,
              uploadUrl: "https://workspace-b.example/api/work-intelligence/ingest",
            }, 201);
          }
          return responseJson({ ok: true, accepted: 0 }, 200);
        }) as typeof fetch,
      }),
    /did not explicitly acknowledge exactly/i,
  );
  check(
    "two_xx_without_probe_acknowledgement_does_not_activate",
    zeroAcceptedCalls === 2 &&
      fs.readFileSync(failedConfigPath, "utf8") === failedBytes &&
      fs.readdirSync(failedTemp).length === 0,
    { zeroAcceptedCalls, zeroAcceptedMessage },
  );

  // Transport policy rejects insecure external HTTP before any network call.
  const transportHome = home("transport-failures");
  const { bytes: transportBytes, configPath: transportConfigPath } = writeConfig(transportHome);
  let insecureCalls = 0;
  const insecureMessage = await expectRejected(
    () =>
      performJoin({
        target: TOKEN,
        baseUrl: "http://workspace-b.example",
        homeDir: transportHome,
        fetchImpl: (async () => {
          insecureCalls += 1;
          return responseJson({});
        }) as typeof fetch,
      }),
    /must use HTTPS/i,
  );
  check(
    "external_http_rejected_before_network",
    insecureCalls === 0 && fs.readFileSync(transportConfigPath, "utf8") === transportBytes,
    { insecureCalls, insecureMessage },
  );

  // Manual redirect mode plus an explicit 3xx rejection prevents token or
  // upload credentials from being replayed to a Location target.
  let redirectMode: RequestRedirect | undefined;
  const redirectMessage = await expectRejected(
    () =>
      performJoin({
        target: TOKEN,
        baseUrl: "https://workspace-b.example",
        homeDir: transportHome,
        fetchImpl: (async (_input, init) => {
          redirectMode = init?.redirect;
          return new Response(null, {
            status: 302,
            headers: { location: "https://attacker.example/join" },
          });
        }) as typeof fetch,
      }),
    /redirects are rejected/i,
  );
  check(
    "join_redirect_rejected_without_config_change",
    redirectMode === "manual" && fs.readFileSync(transportConfigPath, "utf8") === transportBytes,
    { redirectMode, redirectMessage },
  );

  let crossOriginCalls = 0;
  const crossOriginMessage = await expectRejected(
    () =>
      performJoin({
        target: TOKEN,
        baseUrl: "https://workspace-b.example",
        homeDir: transportHome,
        fetchImpl: (async () => {
          crossOriginCalls += 1;
          return responseJson({
            ok: true,
            tenantId: TENANT_B,
            installKey: INSTALL_B,
            uploadUrl: "https://attacker.example/api/work-intelligence/ingest",
          }, 201);
        }) as typeof fetch,
      }),
    /same origin/i,
  );
  check(
    "cross_origin_upload_audience_rejected",
    crossOriginCalls === 1 && fs.readFileSync(transportConfigPath, "utf8") === transportBytes,
    { crossOriginCalls, crossOriginMessage },
  );

  let uploadRedirectCalls = 0;
  const uploadRedirectTemp = path.join(root, "upload-redirect-temporary-state");
  fs.mkdirSync(uploadRedirectTemp);
  const uploadRedirectMessage = await expectRejected(
    () =>
      performJoin({
        target: TOKEN,
        baseUrl: "https://workspace-b.example",
        homeDir: transportHome,
        temporaryRoot: uploadRedirectTemp,
        fetchImpl: (async (input, init) => {
          uploadRedirectCalls += 1;
          assert.equal(init?.redirect, "manual");
          if (requestUrl(input).pathname.endsWith("/join")) {
            return responseJson({
              ok: true,
              tenantId: TENANT_B,
              installKey: INSTALL_B,
              uploadUrl: "https://workspace-b.example/api/work-intelligence/ingest",
            }, 201);
          }
          return new Response(null, {
            status: 307,
            headers: { location: "https://workspace-b.example/redirected-ingest" },
          });
        }) as typeof fetch,
      }),
    /not activated.*handshake failed/i,
  );
  check(
    "upload_redirect_rejected_and_temp_cleaned",
    uploadRedirectCalls === 2 &&
      fs.readFileSync(transportConfigPath, "utf8") === transportBytes &&
      fs.readdirSync(uploadRedirectTemp).length === 0,
    { uploadRedirectCalls, uploadRedirectMessage },
  );

  // The public CLI can keep the token out of argv via stdin. Its success
  // output contains neither token nor install/signing credentials, and the
  // active config remains absent during both network exchanges.
  const cliHome = path.join(root, "cli-stdin-home");
  const cliRequests: Array<Record<string, unknown>> = [];
  const cliSecret = "cli-stdin-signing-secret-0123456789";
  const cliInstallKey = "pli_cli_stdin_install_secret";
  const cliServer = http.createServer((request, response) => {
    let body = "";
    request.setEncoding("utf8");
    request.on("data", (chunk) => (body += chunk));
    request.on("end", () => {
      assert.equal(fs.existsSync(path.join(cliHome, "collector.config.json")), false);
      const parsed = JSON.parse(body) as Record<string, unknown>;
      cliRequests.push(parsed);
      if (request.url?.endsWith("/join")) {
        const address = cliServer.address();
        assert.ok(address && typeof address !== "string");
        response.writeHead(201, { "content-type": "application/json" });
        response.end(JSON.stringify({
          ok: true,
          tenantId: TENANT_B,
          installKey: cliInstallKey,
          uploadUrl: `http://127.0.0.1:${address.port}/api/work-intelligence/ingest`,
          uploadSigningSecret: cliSecret,
        }));
        return;
      }
      response.writeHead(200, { "content-type": "application/json" });
      response.end(JSON.stringify({ ok: true, accepted: 1 }));
    });
  });
  await new Promise<void>((resolve) => cliServer.listen(0, "127.0.0.1", resolve));
  try {
    const address = cliServer.address();
    assert.ok(address && typeof address !== "string");
    const child = await runChild(
      [
        "node_modules/tsx/dist/cli.mjs",
        "packages/collector-cli/src/cli.ts",
        "join",
        "--token-stdin",
        "--url",
        `http://127.0.0.1:${address.port}`,
      ],
      `${TOKEN}\n`,
      { ...process.env, PLIMSOLL_HOME: cliHome },
    );
    const combinedOutput = `${child.stdout}\n${child.stderr}`;
    const childConfig = JSON.parse(
      fs.readFileSync(path.join(cliHome, "collector.config.json"), "utf8"),
    ) as Record<string, unknown>;
    check(
      "cli_stdin_keeps_secret_out_of_argv_and_output",
      child.exit.code === 0 &&
        child.exit.signal === null &&
        cliRequests.length === 2 &&
        cliRequests[0]?.token === TOKEN &&
        cliRequests[0]?.appVersion === COLLECTOR_APP_VERSION &&
        (cliRequests[1]?.events as unknown[]).length === 1 &&
        !child.child.spawnargs.includes(TOKEN) &&
        !combinedOutput.includes(TOKEN) &&
        !combinedOutput.includes(cliInstallKey) &&
        !combinedOutput.includes(cliSecret) &&
        childConfig.installKey === cliInstallKey &&
        /"status": "joined"/.test(child.stdout),
      {
        exit: child.exit,
        requests: cliRequests.length,
        tokenInArgv: child.child.spawnargs.includes(TOKEN),
        secretInOutput:
          combinedOutput.includes(TOKEN) ||
          combinedOutput.includes(cliInstallKey) ||
          combinedOutput.includes(cliSecret),
      },
    );
  } finally {
    await new Promise<void>((resolve, reject) =>
      cliServer.close((error) => (error ? reject(error) : resolve())),
    );
  }

  console.log(
    JSON.stringify(
      {
        ok: true,
        proof: "join-isolation",
        appVersion: COLLECTOR_APP_VERSION,
        node: process.version,
        checks,
      },
      null,
      2,
    ),
  );
} finally {
  if (originalPlimsollHome === undefined) delete process.env.PLIMSOLL_HOME;
  else process.env.PLIMSOLL_HOME = originalPlimsollHome;
  fs.rmSync(root, { recursive: true, force: true });
}
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
