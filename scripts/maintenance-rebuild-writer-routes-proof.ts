import assert from "node:assert/strict";
import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { loadOrCreateLocalIngestAuth } from "../packages/collector-cli/src/local-auth";
import { LearningMaterializationStateStore } from "../packages/collector-cli/src/learning-materializer";
import { OutcomeTimelineStore } from "../packages/collector-cli/src/outcome-timeline-store";
import { connectionOwnershipClosed, observeRebuildConnectionOwnership, rebuildLedger } from
  "../packages/collector-cli/src/maintenance-rebuild";
import { releaseStopWindowListener, runStopWindowListener } from
  "../packages/collector-cli/src/stop-window-listener";
import { exerciseRebuildWriterRoutes } from "./maintenance-rebuild-writer-routes";

async function freePort(): Promise<number> {
  const server = http.createServer();
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const port = (server.address() as { port: number }).port;
  await new Promise<void>((resolve) => server.close(() => resolve()));
  return port === 48271 ? freePort() : port;
}

async function main() {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "b13-writers-")));
  const home = path.join(root, "home");
  const ledger = path.join(root, "work-ledger.sqlite");
  fs.mkdirSync(home, { mode: 0o700 });
  loadOrCreateLocalIngestAuth(home);
  const config = collectorConfigSchema.parse({ port: await freePort() });
  const listener = runStopWindowListener(config, home, { mode: "maintenance_rebuild" });
  try {
    let ready = false;
    for (let attempt = 0; attempt < 100; attempt += 1) {
      try { ready = (await fetch(`http://127.0.0.1:${config.port}/healthz`)).status === 200; }
      catch { /* listener is binding */ }
      if (ready) break;
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    assert.equal(ready, true);
    const buffer = new LocalEventBuffer(ledger);
    try {
      const routes = await exerciseRebuildWriterRoutes(buffer, ledger, home);
      const held = observeRebuildConnectionOwnership(ledger);
      assert.equal(routes.length, 29);
      assert.equal(held.openTokens.length, 1);
      assert.ok(held.writerLeases.some((lease) => lease.owner === "local_event_buffer"));
      await assert.rejects(rebuildLedger({ ledgerPath: ledger, stage: "S10", walHighWaterBytes: 0,
        copyDrill: true,
        quiesce: async () => ({ before: held, after: held, connectionsClosed: connectionOwnershipClosed(held) }),
        resume: async () => undefined,
      }), /writer_not_quiesced/);
      console.log(JSON.stringify({ check: "r2_4_real_writer_entries_held", routes, ownership: held }));
    } finally { buffer.close(); }
    assert.equal(connectionOwnershipClosed(observeRebuildConnectionOwnership(ledger)), true);
    const lock = `${ledger}.maintenance-rebuild.lock`;
    fs.writeFileSync(lock, "fixture\n");
    try {
      assert.throws(() => new LocalEventBuffer(ledger), /maintenance_rebuild_paused/);
      assert.throws(() => new OutcomeTimelineStore(ledger), /maintenance_rebuild_paused/);
      assert.throws(() => new LearningMaterializationStateStore(ledger), /maintenance_rebuild_paused/);
    } finally { fs.unlinkSync(lock); }
    console.log(JSON.stringify({ check: "r2_4_all_reopen_paths_fenced", sharedRoutes: 29,
      independentAliases: 2, afterClose: observeRebuildConnectionOwnership(ledger) }));
  } finally {
    await releaseStopWindowListener(config.port, home).catch(() => false);
    await listener;
    fs.rmSync(root, { recursive: true, force: true });
  }
}
main().catch((error) => { console.error(error); process.exitCode = 1; });
