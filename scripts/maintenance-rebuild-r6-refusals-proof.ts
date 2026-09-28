import assert from "node:assert/strict";
import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { advanceCaptureFrontier, CAPTURE_WRITE_LAG_MS } from "../packages/collector-cli/src/capture-frontier";
import { captureSpoolState } from "../packages/collector-cli/src/capture-spool-state";
import { provisionLiveProducer } from "../packages/collector-cli/src/codex-live-usage-auth";
import { canonicalJson } from "../packages/collector-cli/src/codex-live-usage-protocol";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { listHookSpoolFiles } from "../packages/collector-cli/src/hook-spool";
import { loadOrCreateLocalIngestAuth } from "../packages/collector-cli/src/local-auth";
import { OtlpIntakeSpool } from "../packages/collector-cli/src/otlp-spool";
import { createCollectorServer } from "../packages/collector-cli/src/server";
import { releaseStopWindowListener, runStopWindowListener } from "../packages/collector-cli/src/stop-window-listener";

const root = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "b13-r6-refusals-")));
const workspaceId = "33333333-3333-7333-8333-333333333333";
const deviceId = "44444444-4444-7444-8444-444444444444";
const enrolledAt = "2026-09-08T08:00:00.000Z";

async function freePort(): Promise<number> {
  const server = http.createServer();
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const port = (server.address() as { port: number }).port;
  await new Promise<void>((resolve) => server.close(() => resolve()));
  return port === 48271 ? freePort() : port;
}

async function ready(port: number) {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    try { if ((await fetch(`http://127.0.0.1:${port}/healthz`)).status === 200) return; }
    catch { /* binding */ }
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  assert.fail("fixture listener did not bind");
}

function claim(buffer: LocalEventBuffer, home: string) {
  buffer.delivery.migrateLegacy({ now: new Date() });
  return buffer.delivery.captureClaim([], captureSpoolState(home));
}

function completeFrontier(buffer: LocalEventBuffer) {
  const observed = new Date(Date.now() + CAPTURE_WRITE_LAG_MS).toISOString();
  for (const source of ["codex", "claude_code", "grok"] as const) {
    advanceCaptureFrontier(buffer.database, source, { complete: true, files: [] }, observed);
  }
}

async function scenario(route: "otlp" | "live", terminal: "malformed" | "enrollment" | null = null) {
  const home = path.join(root, terminal ? `terminal-${terminal}` : route);
  fs.mkdirSync(home, { mode: 0o700 });
  const auth = loadOrCreateLocalIngestAuth(home);
  const ledger = path.join(home, "ledger.sqlite");
  const initial = new LocalEventBuffer(ledger, {
    workspaceId, deviceId, enrollmentNow: () => new Date(enrolledAt), delivery: { enabled: true },
  });
  const captureRoot = { rootId: "synthetic-root", profileId: "synthetic-profile", source: "codex" as const,
    installationEpochId: initial.workspaceBinding()!.currentInstallationEpochId!,
    directory: path.join(home, "empty-source"), dispatch: [] };
  fs.mkdirSync(captureRoot.directory, { mode: 0o700 });
  const port = await freePort();
  const config = collectorConfigSchema.parse({ tenantId: workspaceId, deviceId, captureRoots: [captureRoot], port });
  let body: string;
  let url: string;
  let headers: Record<string, string>;
  if (route === "live") {
    const golden = JSON.parse(fs.readFileSync(new URL("./fixtures/codex-live-usage-golden-r4.json", import.meta.url),
      "utf8")) as { vectors: Array<{ canonicalUtf8: string; packet: { producerId: string; credentialId: string } }> };
    const first = golden.vectors[0]!;
    const enrolled = provisionLiveProducer({ home, buffer: initial, config, producerId: first.packet.producerId,
      credentialId: first.packet.credentialId, captureRootId: captureRoot.rootId, enrolledAt });
    body = terminal === "malformed" ? "{invalid live JSON" : terminal === "enrollment"
      ? canonicalJson({ ...first.packet, credentialId: "unbound-credential" }) : first.canonicalUtf8;
    url = "/hooks/codex";
    headers = { "content-type": "application/json", "x-plimsoll-producer-id": first.packet.producerId,
      "x-plimsoll-token": fs.readFileSync(enrolled.credentialFile, "utf8") };
  } else {
    body = terminal === "malformed" ? "{invalid OTLP JSON" : JSON.stringify({ resourceSpans: [{ scopeSpans: [{ spans: [{
      name: "handle_responses", traceId: "1".padStart(32, "0"), spanId: "1".padStart(16, "0"),
      startTimeUnixNano: String(BigInt(Date.now()) * 1_000_000n),
      attributes: [{ key: "gen_ai.usage.input_tokens", value: { intValue: "5" } }],
    }] }] }] });
    url = "/v1/traces";
    headers = { "content-type": "application/json", "x-plimsoll-source": "codex",
      "x-plimsoll-token": auth.codexProducer };
  }
  initial.close();
  const listener = runStopWindowListener(config, home, { mode: "maintenance_rebuild" });
  try {
    await ready(port);
    const paused = await fetch(`http://127.0.0.1:${port}${url}`, { method: "POST", headers, body });
    assert.equal(paused.status, 503, `${route} receives a retryable maintenance refusal`);
    assert.equal(paused.headers.get("retry-after"), "1");
    const repeated = await fetch(`http://127.0.0.1:${port}${url}`, { method: "POST", headers, body });
    assert.equal(repeated.status, 503);
    const refusalDir = path.join(home, "maintenance-rebuild-refusals");
    assert.equal(fs.readdirSync(refusalDir).length, 1, "repeated 503s for one payload share a receipt");
    assert.equal(fs.statSync(refusalDir).mode & 0o777, 0o700);
    const receipt = path.join(refusalDir, fs.readdirSync(refusalDir)[0]!);
    assert.equal(fs.statSync(receipt).mode & 0o777, 0o600);
    assert.equal(fs.readFileSync(receipt, "utf8").includes(body), false,
      "the refusal receipt contains no producer payload");
    assert.equal(listHookSpoolFiles(home).length, 0, "no server hook spool is written for a 503");
    assert.equal(new OtlpIntakeSpool({ home }).status().pendingFiles, 0,
      "no server OTLP spool is written for a 503");
    await releaseStopWindowListener(port, home);
    await listener;

    // Reopen the ledger and server as the resumed daemon would do. A producer
    // owns the refused payload; nothing has arrived in either server spool.
    const buffer = new LocalEventBuffer(ledger, {
      workspaceId, deviceId, enrollmentNow: () => new Date(enrolledAt), delivery: { enabled: true },
    });
    const server = createCollectorServer(config, buffer, {
      localAuth: auth, localAuthHome: home, liveProducerHome: home, hookSpoolHome: home,
    });
    try {
      completeFrontier(buffer);
      const before = claim(buffer, home);
      assert.equal(before?.unattested, "maintenance_rebuild", `${route} refusal survives restart`);
      assert.equal(before?.through, null, `${route} refusal cannot publish an attested claim`);
      assert.equal(captureSpoolState(home).maintenanceRebuildPending, true,
        "an absent producer retry keeps the durable refusal unresolved");
      await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
      const normalPort = (server.address() as { port: number }).port;
      if (terminal) {
        const rejected = await fetch(`http://127.0.0.1:${normalPort}${url}`, {
          method: "POST", headers, body,
        });
        assert.equal(rejected.status, terminal === "malformed" ? 400 : 403,
          "the retry is terminally rejected");
        assert.equal(fs.readdirSync(refusalDir).length, 0,
          "the exact refused payload's terminal rejection retires its receipt");
        assert.equal(captureSpoolState(home).maintenanceRebuildPending, false);
        const afterTerminal = claim(buffer, home);
        assert.equal(afterTerminal?.unattested, undefined);
        assert.notEqual(afterTerminal?.through, null);
        console.log(JSON.stringify({ check: route === "live" ? `r7_live_${terminal}_terminal_retirement` :
          `review_${route}_${terminal}_terminal_retirement`,
          paused: paused.status, rejected: rejected.status, before: before?.unattested,
          after: afterTerminal?.through }));
        return;
      }
      const differentBody = route === "otlp" ? body.replace("handle_responses", "different_responses") : "{}";
      const different = await fetch(`http://127.0.0.1:${normalPort}${url}`, {
        method: "POST", headers, body: differentBody,
      });
      assert.equal(different.status, route === "otlp" ? 202 : 400);
      assert.equal(claim(buffer, home)?.unattested, "maintenance_rebuild",
        "a different payload cannot retire the refused payload's claim hold");
      const retry = await fetch(`http://127.0.0.1:${normalPort}${url}`, { method: "POST", headers, body });
      assert.equal(retry.status, route === "live" ? 200 : 202, `${route} retry lands on normal intake`);
      if (route === "live") {
        const receipt = await retry.json() as { committed?: boolean; disposition?: string };
        assert.equal(receipt.committed, true, "the live retry is durable");
        assert.equal(receipt.disposition, "baseline_only");
      } else {
        const accepted = await retry.json() as { accepted?: boolean };
        assert.equal(accepted.accepted, true);
        assert.equal((buffer.database.prepare("select count(*) as n from buffered_events").get() as { n: number }).n,
          2, "the refused OTLP event and the unrelated accepted event are both in the ledger");
      }
      const after = claim(buffer, home);
      assert.equal(captureSpoolState(home).maintenanceRebuildPending, false,
        `${route} receipt retires only after the matching retry commits`);
      assert.equal(fs.readdirSync(refusalDir).length, 0);
      assert.equal(after?.unattested, undefined);
      assert.notEqual(after?.through, null);
      console.log(JSON.stringify({ check: `r6_${route}_503_restart_retry_claim`, paused: paused.status,
        retry: retry.status, before: before?.unattested, after: after?.through }));
    } finally {
      if (server.listening) await new Promise<void>((resolve) => server.close(() => resolve()));
      buffer.close();
    }
  } finally {
    await releaseStopWindowListener(port, home).catch(() => false);
    await listener;
  }
}

async function main() {
  const selected = process.argv[2] ?? "all";
  try {
    if (selected === "all" || selected === "otlp") await scenario("otlp");
    if (selected === "all" || selected === "live") await scenario("live");
    if (selected === "terminal-live" || selected === "terminal-all") await scenario("live", "malformed");
    if (selected === "terminal-enrollment" || selected === "terminal-all") await scenario("live", "enrollment");
    if (selected === "terminal-otlp") await scenario("otlp", "malformed");
  } finally { fs.rmSync(root, { recursive: true, force: true }); }
}

main().catch((error) => { console.error(error); process.exitCode = 1; });
