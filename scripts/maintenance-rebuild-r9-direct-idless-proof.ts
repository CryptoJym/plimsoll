/** An authenticated direct hook retry without a client ID keeps the pause
 * receipt's exact ledger key after the daemon resumes. */
import assert from "node:assert/strict";
import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { captureSpoolState } from "../packages/collector-cli/src/capture-spool-state";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { loadOrCreateLocalIngestAuth } from "../packages/collector-cli/src/local-auth";
import { createCollectorServer } from "../packages/collector-cli/src/server";
import { releaseStopWindowListener, runStopWindowListener } from "../packages/collector-cli/src/stop-window-listener";

async function freePort() {
  const server = http.createServer();
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const port = (server.address() as { port: number }).port;
  await new Promise<void>((resolve) => server.close(() => resolve()));
  return port;
}
async function ready(port: number) {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    try { if ((await fetch(`http://127.0.0.1:${port}/healthz`)).status === 200) return; }
    catch { /* listener still binding */ }
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  assert.fail("fixture listener did not bind");
}

async function main() {
  const home = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "r9-direct-idless-")));
  const ledger = path.join(home, "ledger.sqlite");
  const auth = loadOrCreateLocalIngestAuth(home);
  const config = collectorConfigSchema.parse({ port: await freePort() });
  const seed = new LocalEventBuffer(ledger);
  seed.close();
  const body = JSON.stringify({ hook_event_name: "UserPromptSubmit", session_id: randomUUID(),
    timestamp: new Date().toISOString(), cwd: "/fixture", prompt: "synthetic" });
  const headers = { "content-type": "application/json", "x-plimsoll-source": "claude_code",
    "x-plimsoll-token": auth.claudeCodeProducer };
  const listener = runStopWindowListener(config, home, { mode: "maintenance_rebuild" });
  let released = false;
  let buffer: LocalEventBuffer | null = null;
  let server: ReturnType<typeof createCollectorServer> | null = null;
  try {
    await ready(config.port);
    const paused = await fetch(`http://127.0.0.1:${config.port}/hooks/claude-code`,
      { method: "POST", headers, body });
    assert.equal(paused.status, 503);
    assert.equal(paused.headers.get("retry-after"), "1");
    const receiptDir = path.join(home, "maintenance-rebuild-refusals");
    const receiptFile = path.join(receiptDir, fs.readdirSync(receiptDir)[0]!);
    const receipt = JSON.parse(fs.readFileSync(receiptFile, "utf8")) as { eventId: string };
    assert.match(receipt.eventId, /^[0-9a-f-]{36}$/);
    await releaseStopWindowListener(config.port, home);
    await listener; released = true;
    assert.equal(captureSpoolState(home).maintenanceRebuildPending, true);
    buffer = new LocalEventBuffer(ledger);
    server = createCollectorServer(config, buffer, {
      localAuth: auth, localAuthHome: home, liveProducerHome: home, hookSpoolHome: home,
    });
    await new Promise<void>((resolve) => server!.listen(0, "127.0.0.1", resolve));
    const port = (server.address() as { port: number }).port;
    const retry = await fetch(`http://127.0.0.1:${port}/hooks/claude-code`,
      { method: "POST", headers, body });
    assert.equal(retry.status, 202);
    const accepted = await retry.json() as { eventId?: string };
    assert.equal(accepted.eventId, receipt.eventId,
      "the direct ID-less retry must commit the paused receipt's stable ledger key");
    const row = buffer.database.prepare("select count(*) as n from buffered_events where id=?")
      .get(receipt.eventId) as { n: number };
    assert.equal(row.n, 1);
    assert.equal(captureSpoolState(home).maintenanceRebuildPending, false);
    assert.equal(fs.readdirSync(receiptDir).filter((entry) => entry.endsWith(".receipt")).length, 0);
    console.log(JSON.stringify({ check: "r9_direct_idless_hook_retry", paused: paused.status,
      retry: retry.status, ledgerId: accepted.eventId, rows: row.n }));
  } finally {
    if (server?.listening) await new Promise<void>((resolve) => server!.close(() => resolve()));
    buffer?.close();
    if (!released) await releaseStopWindowListener(config.port, home).catch(() => false);
    await listener;
    fs.rmSync(home, { recursive: true, force: true });
  }
}
main().catch((error) => { console.error(error); process.exitCode = 1; });
