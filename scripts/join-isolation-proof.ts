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
  pendingJoinPath,
  performJoin,
  resumePendingJoin,
} from "../packages/collector-cli/src/join";
import { uploadBufferedEvents } from "../packages/collector-cli/src/upload";

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

async function waitFor(predicate: () => boolean, message: string, timeoutMs = 10_000) {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error(message);
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
}

async function main() {
try {
  // Workspace A has real unsent history. Joining B may see neither its bytes
  // nor its outbox; only one isolated synthetic probe is eligible.
  const isolatedHome = home("workspace-a-backlog");
  const { bytes: configABytes, configPath: configAPath } = writeConfig(isolatedHome);
  const ledgerPath = collectorBufferPath(isolatedHome);
  const configA = oldConfig();
  const activeBuffer = new LocalEventBuffer(ledgerPath, {
    workspaceId: TENANT_A,
    delivery: { enabled: true, limits: configA.delivery },
  });
  const backlog = appendForwardedHook(
    {
      id: "workspace_a_private_backlog_event",
      source: "claude_code",
      event_type: "UserPromptSubmit",
    },
    { config: configA, buffer: activeBuffer, source: "claude_code" },
  );
  activeBuffer.delivery.openCircuit("auth_blocked");
  activeBuffer.close();
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
  const activatedConfig = collectorConfigSchema.parse(activated);
  const postActivationBuffer = new LocalEventBuffer(ledgerPath, {
    workspaceId: TENANT_B,
    delivery: { enabled: true, limits: activatedConfig.delivery },
  });
  const ordinaryBodies: Array<Record<string, unknown>> = [];
  const ordinaryFetch = (async (_input, init) => {
    ordinaryBodies.push(requestBody(init));
    return responseJson({ ok: true, accepted: 1 }, 200);
  }) as typeof fetch;
  const firstOrdinaryUpload = await uploadBufferedEvents(
    activatedConfig,
    postActivationBuffer,
    { fetchImpl: ordinaryFetch },
  );
  const postJoinCircuit = postActivationBuffer.delivery.status().circuit.kind;
  const preservedARow = postActivationBuffer.database
    .prepare(
      `select workspace_id as workspaceId, uploaded_at as uploadedAt
       from buffered_events where id = ?`,
    )
    .get(backlog.event.id) as { workspaceId: string | null; uploadedAt: string | null };
  const workspaceBEvent = appendForwardedHook(
    {
      id: "workspace_b_post_join_event",
      source: "claude_code",
      event_type: "UserPromptSubmit",
    },
    { config: activatedConfig, buffer: postActivationBuffer, source: "claude_code" },
  );
  const secondOrdinaryUpload = await uploadBufferedEvents(
    activatedConfig,
    postActivationBuffer,
    { fetchImpl: ordinaryFetch },
  );
  const ordinaryEvents = ordinaryBodies[0]?.events as Array<{ event?: { id?: string } }>;
  postActivationBuffer.close();
  check(
    "workspace_backlog_bound_and_post_activation_upload_isolated",
    requests.length === 2 &&
      requests.every((request) => request.init?.redirect === "manual") &&
      handshakeBody?.tenantId === TENANT_B &&
      handshakeBody?.installKey === INSTALL_B &&
      handshakeBody?.appVersion === COLLECTOR_APP_VERSION &&
      handshakeEvents.length === 1 &&
      handshakeEvents[0]?.event?.id === joined.handshake.selfTestEventId &&
      handshakeEvents[0]?.event?.id !== backlog.event.id &&
      joined.handshake.uploadedEvents === 1 &&
      joined.workspaceBoundary.fromWorkspaceId === TENANT_A &&
      joined.workspaceBoundary.toWorkspaceId === TENANT_B &&
      firstOrdinaryUpload.uploadedEvents === 0 &&
      postJoinCircuit === "none" &&
      preservedARow.workspaceId === TENANT_A &&
      preservedARow.uploadedAt === null &&
      ordinaryBodies.length === 1 &&
      ordinaryEvents.length === 1 &&
      ordinaryEvents[0]?.event?.id === workspaceBEvent.event.id &&
      ordinaryEvents[0]?.event?.id !== backlog.event.id &&
      secondOrdinaryUpload.uploadedEvents === 1 &&
      fs.readdirSync(temporaryRoot).length === 0 &&
      !fs.existsSync(pendingJoinPath(isolatedHome)) &&
      activated.tenantId === TENANT_B &&
      activated.installKey === INSTALL_B &&
      activated.port === 49123 &&
      activated.retentionDays === 37,
    {
      requests: requests.length,
      handshakeEvents: handshakeEvents.length,
      firstOrdinaryUploaded: firstOrdinaryUpload.uploadedEvents,
      postJoinCircuit,
      ordinaryRequests: ordinaryBodies.length,
      preservedWorkspaceA: preservedARow,
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
      fs.readdirSync(failedTemp).length === 0 &&
      fs.existsSync(pendingJoinPath(failedHome)) &&
      (fs.statSync(pendingJoinPath(failedHome)).mode & 0o777) === 0o600,
    {
      failedCalls,
      failedMessage,
      temporaryEntries: fs.readdirSync(failedTemp),
      pendingGrant: fs.existsSync(pendingJoinPath(failedHome)),
      pendingMode: (fs.statSync(pendingJoinPath(failedHome)).mode & 0o777).toString(8),
    },
  );

  let resumeCalls = 0;
  const resumeBodies: Array<Record<string, unknown>> = [];
  const resumed = await resumePendingJoin({
    homeDir: failedHome,
    temporaryRoot: failedTemp,
    fetchImpl: (async (_input, init) => {
      resumeCalls += 1;
      resumeBodies.push(requestBody(init));
      return responseJson({ ok: true, accepted: 1 }, 200);
    }) as typeof fetch,
  });
  check(
    "redeemed_grant_resumes_without_second_join_redemption",
    resumed.joined &&
      resumeCalls === 1 &&
      (resumeBodies[0]?.events as unknown[]).length === 1 &&
      !fs.existsSync(pendingJoinPath(failedHome)) &&
      collectorConfigSchema.parse(
        JSON.parse(fs.readFileSync(failedConfigPath, "utf8")),
      ).tenantId === TENANT_B,
    {
      resumeCalls,
      pendingAfterResume: fs.existsSync(pendingJoinPath(failedHome)),
    },
  );

  const zeroAcceptedHome = home("zero-accepted-failure");
  const {
    bytes: zeroAcceptedBytes,
    configPath: zeroAcceptedConfigPath,
  } = writeConfig(zeroAcceptedHome);
  const zeroAcceptedTemp = path.join(root, "zero-accepted-temporary-state");
  fs.mkdirSync(zeroAcceptedTemp);
  let zeroAcceptedCalls = 0;
  const zeroAcceptedMessage = await expectRejected(
    () =>
      performJoin({
        target: TOKEN,
        baseUrl: "https://workspace-b.example",
        homeDir: zeroAcceptedHome,
        temporaryRoot: zeroAcceptedTemp,
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
      fs.readFileSync(zeroAcceptedConfigPath, "utf8") === zeroAcceptedBytes &&
      fs.readdirSync(zeroAcceptedTemp).length === 0 &&
      fs.existsSync(pendingJoinPath(zeroAcceptedHome)),
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

  // Unsupported preview must fail before token consumption or network and a
  // normal CLI startup scavenges only dead-owner handshake directories.
  const dryRunHome = path.join(root, "dry-run-home");
  const dryRunTemp = path.join(root, "dry-run-temp");
  fs.mkdirSync(dryRunTemp);
  fs.mkdirSync(path.join(dryRunTemp, "plimsoll-join-handshake-99999999-stale"));
  let dryRunRequests = 0;
  const dryRunServer = http.createServer((_request, response) => {
    dryRunRequests += 1;
    response.writeHead(500).end();
  });
  await new Promise<void>((resolve) => dryRunServer.listen(0, "127.0.0.1", resolve));
  try {
    const address = dryRunServer.address();
    assert.ok(address && typeof address !== "string");
    const dryRun = await runChild(
      [
        "node_modules/tsx/dist/cli.mjs",
        "packages/collector-cli/src/cli.ts",
        "join",
        "--dry-run",
        "--token-stdin",
        "--url",
        `http://127.0.0.1:${address.port}`,
      ],
      `${TOKEN}\n`,
      { ...process.env, PLIMSOLL_HOME: dryRunHome, TMPDIR: dryRunTemp },
    );
    check(
      "join_dry_run_rejected_before_token_network_or_mutation",
      dryRun.exit.code === 1 &&
        dryRunRequests === 0 &&
        !fs.existsSync(path.join(dryRunHome, "collector.config.json")) &&
        !`${dryRun.stdout}\n${dryRun.stderr}`.includes(TOKEN) &&
        /dry-run is unsupported/.test(dryRun.stderr) &&
        !fs.readdirSync(dryRunTemp).some((entry) =>
          entry.startsWith("plimsoll-join-handshake-"),
        ),
      {
        exit: dryRun.exit,
        requests: dryRunRequests,
        configCreated: fs.existsSync(path.join(dryRunHome, "collector.config.json")),
        handshakeEntries: fs.readdirSync(dryRunTemp).filter((entry) =>
          entry.startsWith("plimsoll-join-handshake-"),
        ),
      },
    );
  } finally {
    await new Promise<void>((resolve, reject) =>
      dryRunServer.close((error) => (error ? reject(error) : resolve())),
    );
  }

  // SIGTERM while the upload response is held must synchronously remove the
  // temporary SQLite directory before honoring the signal. Active config and
  // ledger are not opened until the probe is acknowledged, so both stay exact.
  const signalHome = path.join(root, "signal-home");
  const signalTemp = path.join(root, "signal-temp");
  fs.mkdirSync(signalHome);
  fs.mkdirSync(signalTemp);
  const signalConfig = oldConfig();
  const signalConfigBytes = `${JSON.stringify(signalConfig, null, 4)}\n\n`;
  const signalConfigPath = path.join(signalHome, "collector.config.json");
  fs.writeFileSync(signalConfigPath, signalConfigBytes, { mode: 0o600 });
  const signalLedgerPath = path.join(signalHome, "work-ledger.sqlite");
  const signalSeed = new LocalEventBuffer(signalLedgerPath);
  appendForwardedHook(
    { id: "signal_workspace_a_row", source: "claude_code", event_type: "UserPromptSubmit" },
    { config: signalConfig, buffer: signalSeed, source: "claude_code" },
  );
  signalSeed.close();
  const signalLedgerHash = hashFile(signalLedgerPath);
  let heldHandshake = false;
  let resumeMode = false;
  let cliResumeRequests = 0;
  let heldProbeId = "";
  let resumedProbeId = "";
  const heldServer = http.createServer((request, response) => {
    let body = "";
    request.setEncoding("utf8");
    request.on("data", (chunk) => (body += chunk));
    request.on("end", () => {
      if (request.url?.endsWith("/join")) {
        const address = heldServer.address();
        assert.ok(address && typeof address !== "string");
        response.writeHead(201, { "content-type": "application/json" });
        response.end(JSON.stringify({
          ok: true,
          tenantId: TENANT_B,
          installKey: INSTALL_B,
          uploadUrl: `http://127.0.0.1:${address.port}/api/work-intelligence/ingest`,
        }));
        return;
      }
      const events = (JSON.parse(body) as { events?: Array<{ event?: { id?: string } }> }).events ?? [];
      if (resumeMode) {
        cliResumeRequests += 1;
        resumedProbeId = events[0]?.event?.id ?? "";
        response.writeHead(200, { "content-type": "application/json" });
        response.end(JSON.stringify({ ok: true, accepted: 1 }));
        return;
      }
      heldHandshake = true;
      heldProbeId = events[0]?.event?.id ?? "";
      // Intentionally hold the response until SIGTERM closes the connection.
    });
  });
  await new Promise<void>((resolve) => heldServer.listen(0, "127.0.0.1", resolve));
  try {
    const address = heldServer.address();
    assert.ok(address && typeof address !== "string");
    const child = spawn(
      process.execPath,
      [
        "node_modules/tsx/dist/cli.mjs",
        "packages/collector-cli/src/cli.ts",
        "join",
        "--token-stdin",
        "--url",
        `http://127.0.0.1:${address.port}`,
      ],
      {
        cwd: process.cwd(),
        env: { ...process.env, PLIMSOLL_HOME: signalHome, TMPDIR: signalTemp },
        stdio: ["pipe", "pipe", "pipe"],
      },
    );
    child.stdin.end(`${TOKEN}\n`);
    await waitFor(
      () =>
        heldHandshake &&
        fs.readdirSync(signalTemp).some((entry) => entry.startsWith("plimsoll-join-handshake-")),
      "held handshake did not create temporary state",
    );
    child.kill("SIGTERM");
    const exit = await new Promise<{ code: number | null; signal: NodeJS.Signals | null }>(
      (resolve, reject) => {
        child.once("error", reject);
        child.once("exit", (code, signal) => resolve({ code, signal }));
      },
    );
    check(
      "sigterm_during_handshake_cleans_temp_and_preserves_active_bytes",
      (exit.signal === "SIGTERM" || exit.code === 143) &&
        fs.readFileSync(signalConfigPath, "utf8") === signalConfigBytes &&
        hashFile(signalLedgerPath) === signalLedgerHash &&
        fs.existsSync(path.join(signalHome, "join.pending.json")) &&
        !fs.readdirSync(signalTemp).some((entry) =>
          entry.startsWith("plimsoll-join-handshake-"),
        ),
      {
        exit,
        configByteIdentical: fs.readFileSync(signalConfigPath, "utf8") === signalConfigBytes,
        ledgerByteIdentical: hashFile(signalLedgerPath) === signalLedgerHash,
        pendingGrant: fs.existsSync(path.join(signalHome, "join.pending.json")),
        temporaryEntries: fs.readdirSync(signalTemp).filter((entry) =>
          entry.startsWith("plimsoll-join-handshake-"),
        ),
      },
    );
    resumeMode = true;
    const cliResume = await runChild(
      [
        "node_modules/tsx/dist/cli.mjs",
        "packages/collector-cli/src/cli.ts",
        "join",
        "--resume",
      ],
      "",
      { ...process.env, PLIMSOLL_HOME: signalHome, TMPDIR: signalTemp },
    );
    const resumedConfig = collectorConfigSchema.parse(
      JSON.parse(fs.readFileSync(signalConfigPath, "utf8")),
    );
    check(
      "cli_resume_replays_stable_probe_without_redeeming_token",
      cliResume.exit.code === 0 &&
        cliResumeRequests === 1 &&
        heldProbeId.length > 0 &&
        resumedProbeId === heldProbeId &&
        resumedConfig.tenantId === TENANT_B &&
        !fs.existsSync(path.join(signalHome, "join.pending.json")),
      {
        exit: cliResume.exit,
        resumeRequests: cliResumeRequests,
        stableProbe: resumedProbeId === heldProbeId,
        pendingAfterResume: fs.existsSync(path.join(signalHome, "join.pending.json")),
      },
    );
  } finally {
    heldServer.closeAllConnections();
    await new Promise<void>((resolve, reject) =>
      heldServer.close((error) => (error ? reject(error) : resolve())),
    );
  }

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
    if (!fs.existsSync(path.join(cliHome, "collector.config.json"))) {
      throw new Error(`CLI stdin join did not activate config: ${JSON.stringify({
        exit: child.exit,
        stdout: child.stdout,
        stderr: child.stderr,
      })}`);
    }
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
