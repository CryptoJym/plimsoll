/** Focused collector proof for the 0.7.46 non-event install heartbeat. */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import {
  buildInstallContactPayload,
  INSTALL_CONTACT_INTERVAL_MS,
  postInstallContact,
  reportedMachineName,
  startInstallContactScheduler,
} from "../packages/collector-cli/src/install-contact";
import { performJoin } from "../packages/collector-cli/src/join";

const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-install-contact-proof-"));
const TENANT = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
const CLOUD_DEVICE = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";
const LOCAL_DEVICE = "cccccccc-cccc-4ccc-8ccc-cccccccccccc";
const INSTALL_KEY = "pli_install_contact_proof_key";
const CLOUD = "https://install-contact.fixture.test";

function config(overrides: Record<string, unknown> = {}) {
  return collectorConfigSchema.parse({
    tenantId: TENANT,
    deviceId: LOCAL_DEVICE,
    cloudDeviceId: CLOUD_DEVICE,
    installKey: INSTALL_KEY,
    uploadUrl: `${CLOUD}/api/work-intelligence/ingest`,
    reportMachineName: true,
    ...overrides,
  });
}

function response(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
}

async function main() {
  const ledgerPath = path.join(root, "ledger.sqlite");
  const buffer = new LocalEventBuffer(ledgerPath, { workspaceId: TENANT, deviceId: LOCAL_DEVICE, delivery: { enabled: false } });
  try {
    const machine = reportedMachineName({ platform: "darwin", runScutil: () => `Studio 8\u0000\n` });
    assert.equal(machine, "Studio 8");
    assert.equal(reportedMachineName({ platform: "other", hostname: "host.local" }), "host");
    assert.equal(reportedMachineName({ platform: "other", hostname: `x${"a".repeat(80)}` })?.length, 64);

    const enabled = buildInstallContactPayload(config(), buffer.database, { machineName: "Studio 8" });
    assert.equal(enabled.machineName, "Studio 8");
    const disabled = buildInstallContactPayload(config({ reportMachineName: false }), buffer.database, { machineName: "Studio 8" });
    assert.equal(Object.hasOwn(disabled, "machineName"), false);
    const before = {
      events: (buffer.database.prepare("select count(*) as n from buffered_events").get() as { n: number }).n,
      outbox: (buffer.database.prepare("select count(*) as n from upload_outbox").get() as { n: number }).n,
    };
    let contactBody = "";
    const contact = await postInstallContact({
      config: config(), payload: enabled,
      fetchImpl: async (_input, init) => {
        contactBody = String(init?.body ?? "");
        return response({ ok: true, acknowledged: true, serverTime: "2026-10-01T08:00:00.000Z" });
      },
    });
    assert.equal(contact.kind, "accepted");
    assert.equal(JSON.parse(contactBody).machineName, "Studio 8");
    const after = {
      events: (buffer.database.prepare("select count(*) as n from buffered_events").get() as { n: number }).n,
      outbox: (buffer.database.prepare("select count(*) as n from upload_outbox").get() as { n: number }).n,
    };
    assert.deepEqual(after, before, "heartbeat must not create event or upload rows");

    let timerCallback: (() => void) | undefined;
    let timerDelay = 0;
    let fetchCalls = 0;
    const scheduler = startInstallContactScheduler({
      config: config(), database: buffer.database, machineName: "Studio 8",
      fetchImpl: async () => { fetchCalls += 1; return response({ ok: true, acknowledged: true }); },
      setTimer: (callback, delay) => { timerCallback = callback; timerDelay = delay; return setTimeout(() => undefined, 1); },
      clearTimer: (handle) => clearTimeout(handle),
    });
    await new Promise((resolve) => setImmediate(resolve));
    await new Promise((resolve) => setImmediate(resolve));
    assert.equal(fetchCalls, 1, "scheduler sends once at start");
    assert.equal(timerDelay, INSTALL_CONTACT_INTERVAL_MS, "successful contact uses the 15 minute cadence");
    scheduler.stop();
    assert.ok(timerCallback);

    const unavailable = await postInstallContact({
      config: config(), payload: enabled,
      fetchImpl: async () => response({ error: "not_found" }, 404),
    });
    assert.equal(unavailable.kind, "not_available");

    const requests: Array<{ url: string; body: Record<string, unknown> }> = [];
    const joined = await performJoin({
      target: "pljt_contact_fixture", baseUrl: CLOUD, homeDir: path.join(root, "join-home"),
      temporaryRoot: path.join(root, "join-tmp"), appVersion: "0.7.46", reassign: true,
      fetchImpl: async (input, init) => {
        const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
        const body = JSON.parse(String(init?.body ?? "{}")) as Record<string, unknown>;
        requests.push({ url, body });
        if (url.endsWith("/join")) return response({
          ok: true, tenantId: TENANT, deviceId: CLOUD_DEVICE, installKey: INSTALL_KEY,
          uploadUrl: `${CLOUD}/api/work-intelligence/ingest`,
          installContactEndpoint: `${CLOUD}/api/work-intelligence/install-contact`,
        }, 201);
        if (url.endsWith("/install-contact")) return response({ ok: true, acknowledged: true, serverTime: "2026-10-01T08:00:00.000Z" });
        throw new Error(`unexpected_join_url:${url}`);
      },
    });
    assert.equal(joined.joined, true);
    if (joined.joined) {
      assert.equal(joined.handshake.uploadedEvents, 0);
      assert.equal(joined.handshake.selfTestEventId, null);
      assert.equal(requests.filter((request) => request.url.endsWith("/ingest")).length, 0,
        "contact acknowledgement must replace the synthetic event upload");
    }

    console.log(JSON.stringify({ proof: "install-contact", status: "passed", fetchCalls, contactRequests: requests.length }));
  } finally {
    buffer.close();
    fs.rmSync(root, { recursive: true, force: true });
  }
}

main().catch((error) => { console.error(error); process.exitCode = 1; });
