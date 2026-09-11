#!/usr/bin/env node

/** Contract proof for hosted tenant account-actor salt acquisition. */
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import http from "node:http";

import {
  ACCOUNT_ASSERTION_SALT_FILE,
  ACCOUNT_ASSERTION_SALT_META_FILE,
  deriveAccountActorHash,
  readAccountAssertionAdapterState,
  readAccountAssertionSaltForTenant,
} from "../packages/collector-cli/src/account-assertion";
import { CLOUD_ACCOUNT_SALT_PATH, syncAccountActorSalt } from "../packages/collector-cli/src/account-salt";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import {
  collectorConfigSchema,
  collectorHome,
  reconcileCloudDeviceIdFromIngest,
} from "../packages/collector-cli/src/config";
import { performJoin } from "../packages/collector-cli/src/join";
import { uploadBufferedEvents } from "../packages/collector-cli/src/upload";
import { aiInteractionEventSchema } from "../packages/shared/src/index";
import { acknowledgingFetch } from "./fixtures/delivery-ack-fixture";

const TENANT_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
const DEVICE_ID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";
const CONFLICTING_DEVICE_ID = "cccccccc-cccc-4ccc-8ccc-cccccccccccc";
const INSTALL_KEY = "pli_account_salt_proof_install";
const INGEST_KEY = "account-salt-proof-ingest";
const SIGNING_SECRET = "account-salt-proof-signing-secret-0123456789";
const ACCOUNT_ID = "33334444-5555-4666-8777-888899990000";
const SALT_V1 = Buffer.from("fedcba9876543210fedcba9876543210", "utf8");
const SALT_V2 = Buffer.from("0123456789abcdef0123456789abcdef", "utf8");

function response(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
}

function saltBody(salt: Uint8Array = SALT_V1, saltVersion = "salt-v1") {
  return { ok: true, schema: "account-actor-salt/v1", tenantId: TENANT_ID,
    salt: Buffer.from(salt).toString("base64"), saltVersion };
}

function privateHome(label: string) {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), `plimsoll-account-salt-${label}-`));
  fs.chmodSync(home, 0o700);
  return home;
}

async function child(args: string[], env: NodeJS.ProcessEnv) {
  const proc = spawn(process.execPath, args, { cwd: process.cwd(), env, stdio: ["ignore", "pipe", "pipe"] });
  let stdout = "";
  let stderr = "";
  proc.stdout.setEncoding("utf8");
  proc.stderr.setEncoding("utf8");
  proc.stdout.on("data", (chunk: string) => { stdout += chunk; });
  proc.stderr.on("data", (chunk: string) => { stderr += chunk; });
  const exitCode = await new Promise<number | null>((resolve, reject) => {
    proc.once("error", reject);
    proc.once("exit", resolve);
  });
  return { exitCode, stdout, stderr };
}

function startChild(args: string[], env: NodeJS.ProcessEnv) {
  const proc = spawn(process.execPath, args, { cwd: process.cwd(), env, stdio: ["ignore", "pipe", "pipe"] });
  let stdout = "";
  let stderr = "";
  proc.stdout.setEncoding("utf8");
  proc.stderr.setEncoding("utf8");
  proc.stdout.on("data", (chunk: string) => { stdout += chunk; });
  proc.stderr.on("data", (chunk: string) => { stderr += chunk; });
  return new Promise<{ exitCode: number | null; stdout: string; stderr: string }>((resolve, reject) => {
    proc.once("error", reject);
    proc.once("exit", exitCode => resolve({ exitCode, stdout, stderr }));
  });
}

const waitState = new Int32Array(new SharedArrayBuffer(4));
function waitForFile(file: string, timeoutMs: number) {
  const deadline = Date.now() + timeoutMs;
  while (!fs.existsSync(file) && Date.now() < deadline) Atomics.wait(waitState, 0, 0, 10);
  return fs.existsSync(file);
}

function waitForAnyFile(files: string[], timeoutMs: number) {
  const deadline = Date.now() + timeoutMs;
  while (!files.some(file => fs.existsSync(file)) && Date.now() < deadline) {
    Atomics.wait(waitState, 0, 0, 10);
  }
  return files.some(file => fs.existsSync(file));
}

async function cloudDeviceIdRaceWorker() {
  const [home, deviceId, ready, release] = process.argv.slice(3);
  assert.ok(home && deviceId && ready && release);
  const configPath = path.join(home, "collector.config.json");
  const originalRename = fs.renameSync.bind(fs);
  Object.defineProperty(fs, "renameSync", {
    configurable: true,
    value: (oldPath: fs.PathLike, newPath: fs.PathLike) => {
      if (path.resolve(String(newPath)) === path.resolve(configPath)) {
        fs.writeFileSync(ready, "ready\n", { flag: "wx", mode: 0o600 });
        if (!waitForFile(release, 10_000)) throw new Error("cloud_device_id_race_barrier_timeout");
      }
      return originalRename(oldPath, newPath);
    },
  });
  try {
    const config = collectorConfigSchema.parse(JSON.parse(fs.readFileSync(configPath, "utf8")));
    const result = reconcileCloudDeviceIdFromIngest(config, deviceId, { homeDir: home });
    process.stdout.write(`${JSON.stringify({ result })}\n`);
  } finally {
    Object.defineProperty(fs, "renameSync", { configurable: true, value: originalRename });
  }
}

async function main() {
  const originalPlimsollHome = process.env.PLIMSOLL_HOME;
  delete process.env.PLIMSOLL_HOME;
  const directA = privateHome("direct-a");
  const directB = privateHome("direct-b");
  const directLedger = new LocalEventBuffer(path.join(directA, "work-ledger.sqlite"), {
    workspaceId: TENANT_ID, deviceId: DEVICE_ID,
  });
  try {
    assert.equal(collectorConfigSchema.parse({ cloudDeviceId: DEVICE_ID }).cloudDeviceId, DEVICE_ID);
    assert.throws(() => collectorConfigSchema.parse({ cloudDeviceId: "dev_not_a_cloud_uuid" }));

    const requests: Array<{ url: string; headers: Headers; body: Record<string, unknown> }> = [];
    const fetchSalt = (salt: Buffer, version: string): typeof fetch => async (input, init) => {
      const headers = new Headers(init?.headers);
      const body = JSON.parse(String(init?.body ?? "{}")) as Record<string, unknown>;
      requests.push({ url: String(input), headers, body });
      return response(saltBody(salt, version));
    };
    const sync = (home: string, salt = SALT_V1, version = "salt-v1") => syncAccountActorSalt({
      collectorHome: home,
      tenantId: TENANT_ID,
      cloudDeviceId: DEVICE_ID,
      uploadUrl: "https://tenant.example/api/work-intelligence/ingest",
      installKey: INSTALL_KEY,
      ingestKey: INGEST_KEY,
      signingSecret: SIGNING_SECRET,
      fetchImpl: fetchSalt(salt, version),
    });

    const first = await sync(directA);
    const second = await sync(directB);
    assert.equal(first.synced, true);
    assert.equal(second.synced, true);
    assert.equal(new URL(requests[0].url).pathname, CLOUD_ACCOUNT_SALT_PATH);
    assert.equal(requests[0].headers.get("x-plimsoll-install-key"), INSTALL_KEY);
    assert.equal(requests[0].headers.get("x-plimsoll-ingest-key"), INGEST_KEY);
    assert.match(requests[0].headers.get("x-plimsoll-upload-signature") ?? "", /^sha256=[a-f0-9]{64}$/);
    assert.equal(requests[0].body.tenantId, TENANT_ID);
    assert.equal(requests[0].body.deviceId, DEVICE_ID);
    assert.equal(JSON.stringify(requests[0].body).includes(SALT_V1.toString("base64")), false);
    const directSaltA = readAccountAssertionSaltForTenant(directA, TENANT_ID);
    const directSaltB = readAccountAssertionSaltForTenant(directB, TENANT_ID);
    assert.ok(directSaltA);
    assert.ok(directSaltB);
    assert.equal(deriveAccountActorHash(ACCOUNT_ID, directSaltA), deriveAccountActorHash(ACCOUNT_ID, directSaltB));
    assert.equal(readAccountAssertionSaltForTenant(directA, "different-tenant"), null);
    assert.equal(fs.statSync(path.join(directA, ACCOUNT_ASSERTION_SALT_FILE)).mode & 0o777, 0o600);
    assert.equal(fs.statSync(path.join(directA, ACCOUNT_ASSERTION_SALT_META_FILE)).mode & 0o777, 0o600);
    assert.deepEqual(readAccountAssertionAdapterState(directLedger.database).salt,
      { tenantId: TENANT_ID, version: "salt-v1" });

    let sameVersionFailure = "";
    try { await sync(directA, SALT_V2, "salt-v1"); }
    catch (error) { sameVersionFailure = error instanceof Error ? error.message : String(error); }
    assert.match(sameVersionFailure, /account_assertion_salt_version_conflict/);
    assert.deepEqual(readAccountAssertionSaltForTenant(directA, TENANT_ID), SALT_V1);
    await sync(directA, SALT_V2, "salt-v2");
    assert.deepEqual(readAccountAssertionSaltForTenant(directA, TENANT_ID), SALT_V2);
    assert.deepEqual(readAccountAssertionAdapterState(directLedger.database).salt,
      { tenantId: TENANT_ID, version: "salt-v2" });

    let unboundCalls = 0;
    const unbound = await syncAccountActorSalt({
      collectorHome: directA,
      tenantId: TENANT_ID,
      cloudDeviceId: undefined,
      uploadUrl: "https://tenant.example/api/work-intelligence/ingest",
      installKey: INSTALL_KEY,
      fetchImpl: async () => {
        unboundCalls += 1;
        return response(saltBody());
      },
    });
    assert.deepEqual(unbound, {
      synced: false,
      tenantId: TENANT_ID,
      saltVersion: null,
      reason: "unbound",
    });
    assert.equal(unboundCalls, 0);

    // Join fetches exactly once after the authenticated handshake and stores
    // into the canonical collector home, not the caller's OS home.
    const joinHome = privateHome("join-os-home");
    const joinRequests: string[] = [];
    const joinFetch = acknowledgingFetch(async (input, init) => {
      const url = new URL(String(input));
      joinRequests.push(url.pathname);
      if (url.pathname.endsWith("/join")) {
        return response({
          ok: true,
          tenantId: TENANT_ID,
          deviceId: DEVICE_ID,
          installKey: INSTALL_KEY,
          uploadUrl: "https://tenant.example/api/work-intelligence/ingest",
          accountActorSaltEndpoint: `https://tenant.example${CLOUD_ACCOUNT_SALT_PATH}`,
        }, 201);
      }
      if (url.pathname === CLOUD_ACCOUNT_SALT_PATH) return response(saltBody());
      const uploaded = JSON.parse(String(init?.body ?? "{}")) as { events?: unknown[] };
      return response({ ok: true, accepted: uploaded.events?.length ?? 0 });
    });
    const joined = await performJoin({
      target: "pljt_account-salt-proof-token",
      baseUrl: "https://tenant.example",
      homeDir: joinHome,
      temporaryRoot: path.join(joinHome, "temporary"),
      fetchImpl: joinFetch,
    });
    assert.equal(joined.joined, true);
    assert.equal(joined.joined && joined.accountSaltSynced, true);
    assert.equal(joinRequests.filter(value => value === CLOUD_ACCOUNT_SALT_PATH).length, 1);
    assert.equal(joined.joined && JSON.parse(fs.readFileSync(joined.configPath, "utf8")).accountActorSaltEndpoint,
      `https://tenant.example${CLOUD_ACCOUNT_SALT_PATH}`);
    const joinedConfig = JSON.parse(fs.readFileSync(joined.joined ? joined.configPath : "", "utf8")) as Record<string, unknown>;
    assert.equal(joinedConfig.cloudDeviceId, DEVICE_ID);
    assert.match(String(joinedConfig.deviceId), /^dev_[0-9a-f-]{36}$/i);
    assert.notEqual(joinedConfig.deviceId, joinedConfig.cloudDeviceId);
    assert.ok(readAccountAssertionSaltForTenant(collectorHome(joinHome), TENANT_ID));
    assert.equal(fs.existsSync(path.join(joinHome, ACCOUNT_ASSERTION_SALT_FILE)), false);

    // Older grants remain valid and do not synthesize a cloud device id.
    const legacyJoinHome = privateHome("legacy-join-os-home");
    const legacyJoined = await performJoin({
      target: "pljt_account-salt-proof-legacy-token",
      baseUrl: "https://tenant.example",
      homeDir: legacyJoinHome,
      temporaryRoot: path.join(legacyJoinHome, "temporary"),
      fetchImpl: acknowledgingFetch(async (input, init) => {
        if (new URL(String(input)).pathname.endsWith("/join")) {
          return response({
            ok: true,
            tenantId: TENANT_ID,
            installKey: INSTALL_KEY,
            uploadUrl: "https://tenant.example/api/work-intelligence/ingest",
          }, 201);
        }
        const uploaded = JSON.parse(String(init?.body ?? "{}")) as { events?: unknown[] };
        return response({ ok: true, accepted: uploaded.events?.length ?? 0 });
      }),
    });
    assert.equal(legacyJoined.joined, true);
    const legacyJoinConfig = JSON.parse(fs.readFileSync(legacyJoined.joined ? legacyJoined.configPath : "", "utf8"));
    assert.equal(Object.hasOwn(legacyJoinConfig, "cloudDeviceId"), false);

    // A pre-v1.1 config learns the cloud UUID from the first acknowledged
    // ingest response. Equal echoes do not rewrite it; conflicts preserve the
    // stored UUID and emit one rate-limited, identifier-free diagnostic.
    const backfillHome = privateHome("ingest-backfill");
    process.env.PLIMSOLL_HOME = backfillHome;
    const backfillConfig = collectorConfigSchema.parse({
      managed: true,
      tenantId: TENANT_ID,
      deviceId: "dev_11111111-1111-4111-8111-111111111111",
      installKey: INSTALL_KEY,
      uploadUrl: "https://tenant.example/api/work-intelligence/ingest",
    });
    const backfillConfigPath = path.join(backfillHome, "collector.config.json");
    fs.writeFileSync(backfillConfigPath, `${JSON.stringify(backfillConfig, null, 2)}\n`, { mode: 0o600 });
    const backfillBuffer = new LocalEventBuffer(path.join(backfillHome, "work-ledger.sqlite"), {
      workspaceId: TENANT_ID,
      deviceId: backfillConfig.deviceId,
      enrollmentNow: () => new Date("2026-09-11T10:59:00.000Z"),
      delivery: { enabled: true, limits: backfillConfig.delivery },
    });
    const appendBackfillEvent = (id: string) => backfillBuffer.append(aiInteractionEventSchema.parse({
      id,
      sessionId: "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
      source: "codex",
      dataMode: "metadata",
      eventType: "assistant_response",
      observedAt: "2026-09-11T11:00:00.000Z",
      actionClass: "other",
      inputTokens: 1,
      outputTokens: 1,
      metadata: { proof: true },
    }));
    const echoed = (deviceId: string): typeof fetch => {
      const acknowledged = acknowledgingFetch(async (_input, init) => {
        const uploaded = JSON.parse(String(init?.body ?? "{}")) as { events?: unknown[] };
        return response({ ok: true, accepted: uploaded.events?.length ?? 0, deviceId });
      });
      return async (input, init) => {
        const result = await acknowledged(input, init);
        assert.equal((await result.clone().json() as Record<string, unknown>).deviceId, deviceId);
        return result;
      };
    };
    try {
      appendBackfillEvent("eeeeeeee-eeee-4eee-8eee-eeeeeeeeeee1");
      const firstBackfill = await uploadBufferedEvents(backfillConfig, backfillBuffer, {
        fetchImpl: echoed(DEVICE_ID),
      });
      assert.equal(firstBackfill.uploadedEvents, 1);
      assert.equal(JSON.parse(fs.readFileSync(backfillConfigPath, "utf8")).cloudDeviceId, DEVICE_ID);
      assert.equal(JSON.stringify(firstBackfill.response).includes(DEVICE_ID), false);
      const afterFirst = fs.statSync(backfillConfigPath, { bigint: true });

      appendBackfillEvent("eeeeeeee-eeee-4eee-8eee-eeeeeeeeeee2");
      await uploadBufferedEvents(backfillConfig, backfillBuffer, { fetchImpl: echoed(DEVICE_ID) });
      const afterEqual = fs.statSync(backfillConfigPath, { bigint: true });
      assert.equal(afterEqual.ino, afterFirst.ino);
      assert.equal(afterEqual.mtimeNs, afterFirst.mtimeNs);

      const diagnostics: string[] = [];
      const originalWarn = console.warn;
      console.warn = (...values: unknown[]) => { diagnostics.push(values.map(String).join(" ")); };
      try {
        for (const suffix of ["3", "4"]) {
          appendBackfillEvent(`eeeeeeee-eeee-4eee-8eee-eeeeeeeeeee${suffix}`);
          await uploadBufferedEvents(backfillConfig, backfillBuffer, {
            fetchImpl: echoed(CONFLICTING_DEVICE_ID),
          });
        }
      } finally {
        console.warn = originalWarn;
      }
      assert.equal(JSON.parse(fs.readFileSync(backfillConfigPath, "utf8")).cloudDeviceId, DEVICE_ID);
      assert.deepEqual(diagnostics, [JSON.stringify({ status: "cloud_device_id_conflict" })]);
      assert.equal(diagnostics[0].includes(DEVICE_ID), false);
      assert.equal(diagnostics[0].includes(CONFLICTING_DEVICE_ID), false);
      const afterConflict = fs.statSync(backfillConfigPath, { bigint: true });
      assert.equal(afterConflict.ino, afterFirst.ino);
      assert.equal(afterConflict.mtimeNs, afterFirst.mtimeNs);
    } finally {
      backfillBuffer.close();
      delete process.env.PLIMSOLL_HOME;
    }

    // Interprocess reconciliation is one mutation: exactly one concurrent
    // writer stores its echo and the other observes that authoritative value.
    const raceHome = privateHome("ingest-backfill-race");
    const raceConfigPath = path.join(raceHome, "collector.config.json");
    const raceSeed = { ...backfillConfig };
    delete raceSeed.cloudDeviceId;
    fs.writeFileSync(raceConfigPath, `${JSON.stringify(raceSeed, null, 2)}\n`, { mode: 0o600 });
    const raceRelease = path.join(raceHome, "release");
    const raceReady = [path.join(raceHome, "ready-a"), path.join(raceHome, "ready-b")];
    const tsx = path.join(process.cwd(), "node_modules", "tsx", "dist", "cli.mjs");
    const script = path.join(process.cwd(), "scripts", "account-salt-proof.ts");
    const raceValues = [DEVICE_ID, CONFLICTING_DEVICE_ID];
    const racers = raceValues.map((deviceId, index) => startChild([
      tsx,
      script,
      "--cloud-device-id-race-worker",
      raceHome,
      deviceId,
      raceReady[index],
      raceRelease,
    ], { ...process.env, PLIMSOLL_HOME: raceHome }));
    if (!waitForAnyFile(raceReady, 10_000)) {
      assert.fail(`race workers did not reach publication: ${JSON.stringify(await Promise.all(racers))}`);
    }
    const secondReadyDeadline = Date.now() + 2_000;
    while (!raceReady.every(file => fs.existsSync(file)) && Date.now() < secondReadyDeadline) {
      Atomics.wait(waitState, 0, 0, 10);
    }
    fs.writeFileSync(raceRelease, "release\n", { flag: "wx", mode: 0o600 });
    const raceResults = await Promise.all(racers);
    for (const result of raceResults) assert.equal(result.exitCode, 0, result.stderr);
    const dispositions = raceResults.map(result =>
      (JSON.parse(result.stdout) as { result: string }).result,
    );
    assert.equal(dispositions.filter(value => value === "stored").length, 1);
    assert.equal(dispositions.filter(value => value === "conflict").length, 1);
    const storedIndex = dispositions.indexOf("stored");
    const finalRaceConfig = collectorConfigSchema.parse(JSON.parse(fs.readFileSync(raceConfigPath, "utf8")));
    assert.equal(finalRaceConfig.cloudDeviceId, raceValues[storedIndex]);
    const raceDiagnostics = raceResults.map(result => result.stderr.trim()).filter(Boolean);
    assert.deepEqual(raceDiagnostics, [JSON.stringify({ status: "cloud_device_id_conflict" })]);
    assert.equal(raceDiagnostics.some(line => raceValues.some(value => line.includes(value))), false);
    assert.equal(fs.statSync(raceConfigPath).mode & 0o777, 0o600);
    assert.equal(fs.readdirSync(raceHome).some(name => name.startsWith(".collector.config-")), false);
    assert.equal(fs.readdirSync(raceHome).some(name => name.endsWith(".mutation.lock")), false);

    // An already joined collector can run the shipped CLI command against the
    // same authenticated channel. The receipt exposes version only.
    const cliHome = privateHome("cli");
    let cliCalls = 0;
    const cliDeviceIds: unknown[] = [];
    const server = http.createServer((request, result) => {
      cliCalls += 1;
      assert.equal(request.headers["x-plimsoll-install-key"], INSTALL_KEY);
      const chunks: Buffer[] = [];
      request.on("data", chunk => { chunks.push(Buffer.from(chunk)); });
      request.on("end", () => {
        cliDeviceIds.push((JSON.parse(Buffer.concat(chunks).toString("utf8")) as Record<string, unknown>).deviceId);
        if (request.url === "/forbidden-account-salt") {
          result.writeHead(403, { "content-type": "application/json" });
          result.end(JSON.stringify({ ok: false }));
          return;
        }
        if (request.url === "/unallocated-account-salt") {
          result.writeHead(404, { "content-type": "application/json" });
          result.end(JSON.stringify({ ok: false }));
          return;
        }
        result.writeHead(200, { "content-type": "application/json" });
        result.end(JSON.stringify(request.url === "/mismatched-account-salt"
          ? { ...saltBody(), tenantId: "cccccccc-cccc-4ccc-8ccc-cccccccccccc" }
          : saltBody()));
      });
    });
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(0, "127.0.0.1", resolve);
    });
    try {
      const address = server.address();
      assert.ok(address && typeof address === "object");
      const runSyncCommand = async (endpointPath: string, includeCloudDeviceId = true) => {
        const config = collectorConfigSchema.parse({
          managed: true,
          tenantId: TENANT_ID,
          deviceId: "dev_22222222-2222-4222-8222-222222222222",
          ...(includeCloudDeviceId ? { cloudDeviceId: DEVICE_ID } : {}),
          installKey: INSTALL_KEY,
          uploadUrl: `http://127.0.0.1:${address.port}/api/work-intelligence/ingest`,
          accountActorSaltEndpoint: `http://127.0.0.1:${address.port}${endpointPath}`,
        });
        fs.writeFileSync(path.join(cliHome, "collector.config.json"), `${JSON.stringify(config, null, 2)}\n`, { mode: 0o600 });
        return child([
          path.join(process.cwd(), "node_modules", "tsx", "dist", "cli.mjs"),
          path.join(process.cwd(), "packages", "collector-cli", "src", "cli.ts"),
          "sync-account-salt",
        ], { ...process.env, PLIMSOLL_HOME: cliHome });
      };
      const run = await runSyncCommand("/custom-account-salt");
      assert.equal(run.exitCode, 0, run.stderr);
      const receipt = JSON.parse(run.stdout) as Record<string, unknown>;
      assert.equal(receipt.status, "account_salt_synced");
      assert.equal(cliDeviceIds[0], DEVICE_ID);
      assert.equal(receipt.saltVersion, "salt-v1");
      assert.equal(run.stdout.includes(SALT_V1.toString("base64")), false);
      assert.equal(run.stderr.includes(SALT_V1.toString("base64")), false);
      assert.ok(readAccountAssertionSaltForTenant(cliHome, TENANT_ID));

      const forbidden = await runSyncCommand("/forbidden-account-salt");
      assert.notEqual(forbidden.exitCode, 0);
      assert.equal(JSON.parse(forbidden.stdout).status, "account_salt_refused");

      const unallocated = await runSyncCommand("/unallocated-account-salt");
      assert.equal(unallocated.exitCode, 0, unallocated.stderr);
      assert.equal(JSON.parse(unallocated.stdout).status, "account_salt_unallocated");

      const mismatched = await runSyncCommand("/mismatched-account-salt");
      assert.notEqual(mismatched.exitCode, 0);
      assert.equal(JSON.parse(mismatched.stdout).status, "account_salt_refused");
      assert.equal(cliCalls, 4);

      const unboundCli = await runSyncCommand("/must-not-be-called", false);
      assert.equal(unboundCli.exitCode, 1, unboundCli.stderr);
      assert.deepEqual(JSON.parse(unboundCli.stdout), {
        status: "account_salt_device_unbound",
        tenantId: TENANT_ID,
        saltVersion: null,
        synced: false,
        action: "upload once with delivery enabled, or rejoin",
      });
      assert.equal(cliCalls, 4);
    } finally {
      await new Promise<void>((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
    }

    console.log(JSON.stringify({
      proof: "account-salt",
      completion: "passed",
      checks: {
        authenticatedEndpointContract: true,
        sameTenantCrossHostHash: true,
        ownerOnlyTenantBoundStorage: true,
        versionedRotationFailClosed: true,
        joinFetchesOnceAfterHandshake: true,
        syncCommandForJoinedDevice: true,
        unboundSkipsEndpoint: true,
        joinPersistsCloudDeviceId: true,
        legacyJoinLeavesCloudDeviceIdAbsent: true,
        acknowledgedIngestBackfillsOnce: true,
        conflictingEchoPreservesStoredId: true,
        conflictDiagnosticRateLimitedAndIdentifierFree: true,
        concurrentBackfillStoresExactlyOnce: true,
        refusalDistinctFromUnallocated: true,
        saltAbsentFromReceipts: true,
      },
    }));
  } finally {
    directLedger.close();
    if (originalPlimsollHome === undefined) delete process.env.PLIMSOLL_HOME;
    else process.env.PLIMSOLL_HOME = originalPlimsollHome;
  }
}

const operation = process.argv[2] === "--cloud-device-id-race-worker"
  ? cloudDeviceIdRaceWorker()
  : main();

operation.catch(error => {
  console.error(error instanceof Error ? error.stack : String(error));
  process.exitCode = 1;
});
