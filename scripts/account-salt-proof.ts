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
import { collectorConfigSchema, collectorHome } from "../packages/collector-cli/src/config";
import { performJoin } from "../packages/collector-cli/src/join";
import { acknowledgingFetch } from "./fixtures/delivery-ack-fixture";

const TENANT_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
const DEVICE_ID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";
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

async function main() {
  const originalPlimsollHome = process.env.PLIMSOLL_HOME;
  delete process.env.PLIMSOLL_HOME;
  const directA = privateHome("direct-a");
  const directB = privateHome("direct-b");
  const directLedger = new LocalEventBuffer(path.join(directA, "work-ledger.sqlite"), {
    workspaceId: TENANT_ID, deviceId: DEVICE_ID,
  });
  try {
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
      deviceId: DEVICE_ID,
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
    assert.ok(readAccountAssertionSaltForTenant(collectorHome(joinHome), TENANT_ID));
    assert.equal(fs.existsSync(path.join(joinHome, ACCOUNT_ASSERTION_SALT_FILE)), false);

    // An already joined collector can run the shipped CLI command against the
    // same authenticated channel. The receipt exposes version only.
    const cliHome = privateHome("cli");
    let cliCalls = 0;
    const server = http.createServer((request, result) => {
      cliCalls += 1;
      assert.equal(request.url, "/custom-account-salt");
      assert.equal(request.headers["x-plimsoll-install-key"], INSTALL_KEY);
      request.resume();
      result.writeHead(200, { "content-type": "application/json" });
      result.end(JSON.stringify(saltBody()));
    });
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(0, "127.0.0.1", resolve);
    });
    try {
      const address = server.address();
      assert.ok(address && typeof address === "object");
      const config = collectorConfigSchema.parse({
        managed: true,
        tenantId: TENANT_ID,
        deviceId: DEVICE_ID,
        installKey: INSTALL_KEY,
        uploadUrl: `http://127.0.0.1:${address.port}/api/work-intelligence/ingest`,
        accountActorSaltEndpoint: `http://127.0.0.1:${address.port}/custom-account-salt`,
      });
      fs.writeFileSync(path.join(cliHome, "collector.config.json"), `${JSON.stringify(config, null, 2)}\n`, { mode: 0o600 });
      const run = await child([
        path.join(process.cwd(), "node_modules", "tsx", "dist", "cli.mjs"),
        path.join(process.cwd(), "packages", "collector-cli", "src", "cli.ts"),
        "sync-account-salt",
      ], { ...process.env, PLIMSOLL_HOME: cliHome });
      assert.equal(run.exitCode, 0, run.stderr);
      const receipt = JSON.parse(run.stdout) as Record<string, unknown>;
      assert.equal(receipt.status, "account_salt_synced");
      assert.equal(receipt.saltVersion, "salt-v1");
      assert.equal(cliCalls, 1);
      assert.equal(run.stdout.includes(SALT_V1.toString("base64")), false);
      assert.equal(run.stderr.includes(SALT_V1.toString("base64")), false);
      assert.ok(readAccountAssertionSaltForTenant(cliHome, TENANT_ID));
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
        saltAbsentFromReceipts: true,
      },
    }));
  } finally {
    directLedger.close();
    if (originalPlimsollHome === undefined) delete process.env.PLIMSOLL_HOME;
    else process.env.PLIMSOLL_HOME = originalPlimsollHome;
  }
}

main().catch(error => {
  console.error(error instanceof Error ? error.stack : String(error));
  process.exitCode = 1;
});
