import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import http from "node:http";
import net from "node:net";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { loadOrCreateLocalIngestAuth } from "../packages/collector-cli/src/local-auth";
import { createCollectorServer } from "../packages/collector-cli/src/server";
import { ensureSessionSummarySchema, updateSessionSummary, type SessionReadQuery } from "../packages/collector-cli/src/session-summary";
import { aiInteractionEventSchema } from "../packages/shared/src/index";
import { createProofCompletion } from "./lib/proof-completion";

const completion = createProofCompletion("status-pending-reasons");
const home = process.env.PLIMSOLL_HOME!;
const ledgerPath = path.join(home, "work-ledger.sqlite");
const sessionIds = [
  "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa31",
  "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa32",
  "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa33",
];

function cliStatus() {
  const child = spawnSync(process.execPath, ["--import", path.resolve("node_modules/tsx/dist/loader.mjs"),
    path.resolve("packages/collector-cli/src/cli.ts"), "status", "--json"], {
    cwd: path.resolve("."), env: process.env, encoding: "utf8", timeout: 30_000,
  });
  if (child.error) throw child.error;
  assert.equal(child.status, 0, child.stderr);
  return { body: JSON.parse(child.stdout) as Record<string, unknown>, text: child.stdout };
}

async function unusedPort(): Promise<number> {
  const listener = net.createServer();
  await new Promise<void>((resolve) => listener.listen(0, "127.0.0.1", resolve));
  const address = listener.address();
  assert.ok(address && typeof address !== "string");
  const port = address.port;
  await new Promise<void>((resolve) => listener.close(() => resolve()));
  assert.notEqual(port, 48271);
  return port;
}

async function httpStatus(port: number, token: string) {
  return new Promise<{ status: number; body: Record<string, unknown>; text: string }>((resolve, reject) => {
    const request = http.get({ host: "127.0.0.1", port, path: "/status",
      headers: { "x-plimsoll-token": token, connection: "close" } }, (response) => {
      const chunks: Buffer[] = [];
      response.on("data", (chunk) => chunks.push(Buffer.from(chunk)));
      response.on("end", () => {
        const text = Buffer.concat(chunks).toString("utf8");
        resolve({ status: response.statusCode ?? 0, body: JSON.parse(text), text });
      });
    });
    request.setTimeout(5_000, () => request.destroy(new Error("status_timeout")));
    request.on("error", reject);
  });
}

async function main() {
  const directory = path.join(process.env.CODEX_HOME!, "sessions");
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  const config = collectorConfigSchema.parse({ port: await unusedPort(), captureRoots: [{
    rootId: "status-root", profileId: "status-profile",
    installationEpochId: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
    source: "codex", directory,
    dispatch: [{ sessionId: "unlinkable-session", workItemId: "beads:invalid-work",
      projectKey: `sha256:${"a".repeat(64)}`, attemptId: "lane-1", parentAttemptId: null,
      companyRef: null, acceptedOutcomeId: null, evidenceRef: "status-proof",
      validFrom: new Date(Date.now() - 60_000).toISOString(), validUntil: null, role: "author" }],
  }] });
  fs.writeFileSync(path.join(home, "collector.config.json"), `${JSON.stringify(config, null, 2)}\n`, { mode: 0o600 });
  const buffer = new LocalEventBuffer(ledgerPath);
  const auth = loadOrCreateLocalIngestAuth(home);
  const refresher: { refresh?: () => boolean } = {};
  const server = createCollectorServer(config, buffer, { localAuth: auth,
    registerStatusRefresher: (registered) => { refresher.refresh = registered; } });
  try {
    ensureSessionSummarySchema(buffer.database);
    const event = aiInteractionEventSchema.parse({ id: "00000000-0000-4000-8000-000000000131",
      sessionId: sessionIds[0], source: "codex", eventType: "assistant_response",
      observedAt: new Date(Date.now() - 60_000).toISOString(), inputTokens: 2, outputTokens: 1 });
    buffer.appendMany([{ event, suppressedFields: [] }]);
    const insert = buffer.database.prepare(`insert into session_sync_summary_pending
      (session_id, reason, mutation_revision, queued_high_water,
        consecutive_zero_progress, next_retry_at, updated_at)
      values (?, ?, 0, 0, ?, null, ?)`);
    insert.run(sessionIds[0], "rows_read_timeout", 2, new Date(Date.now() - 180_000).toISOString());
    insert.run(sessionIds[1], "ledger_mutation", 0, new Date(Date.now() - 120_000).toISOString());
    insert.run(sessionIds[2], `secret-prompt-${sessionIds[2]}`, 0, new Date(Date.now() - 60_000).toISOString());
    completion.check("fixture_has_timeout_mutation_and_unknown_pending_reasons");

    await new Promise<void>((resolve) => server.listen(config.port, "127.0.0.1", resolve));
    assert.ok(refresher.refresh);
    refresher.refresh();
    const cli = cliStatus();
    const pending = cli.body.summaryPending as Record<string, unknown>;
    assert.equal(pending.pendingCount, 3);
    assert.equal(cli.body.unlinkableBindCount, 1);
    assert.deepEqual(pending.reasonCounts, { rows_read_timeout: 1, ledger_mutation: 1, other: 1 });
    const freshness = pending.freshness as Record<string, unknown>;
    assert.ok(Number.isFinite(Date.parse(String(freshness.observedAt))));
    assert.ok(Number.isFinite(Date.parse(String(freshness.oldestPendingAt))));
    assert.ok(Number.isFinite(Date.parse(String(freshness.latestUpdateAt))));
    for (const id of sessionIds) assert.equal(cli.text.includes(id), false);
    assert.equal(cli.text.includes("secret-prompt"), false);
    assert.equal(cli.text.includes("unlinkable-session"), false);
    console.log(JSON.stringify({ statusExcerpt: { summaryPending: pending,
      unlinkableBindCount: cli.body.unlinkableBindCount } }));
    completion.check("cli_status_reports_bounded_private_reasons_pending_and_freshness");

    const httpBefore = await httpStatus(config.port, auth.managementRead);
    assert.equal(httpBefore.status, 200);
    assert.deepEqual((httpBefore.body.summaryPending as Record<string, unknown>).reasonCounts, pending.reasonCounts);
    assert.equal((httpBefore.body.summaryPending as Record<string, unknown>).pendingCount, 3);
    assert.equal(httpBefore.body.unlinkableBindCount, 1);
    for (const id of sessionIds) assert.equal(httpBefore.text.includes(id), false);
    assert.equal(httpBefore.text.includes("secret-prompt"), false);
    assert.equal(httpBefore.text.includes("unlinkable-session"), false);
    completion.check("http_status_mirrors_private_pending_summary_from_cache");

    const until = new Date().toISOString();
    let recovered;
    for (let pass = 0; pass < 5; pass += 1) {
      recovered = await updateSessionSummary(buffer.database, sessionIds[0], until, {
        read: async <T>(queries: SessionReadQuery[]) => queries.flatMap((query) =>
          buffer.database.prepare(query.sql).all(query.params) as T[]),
      });
      if (recovered.complete) break;
    }
    assert.equal(recovered?.complete, true, JSON.stringify(recovered));
    refresher.refresh();
    const after = cliStatus();
    assert.equal((after.body.summaryPending as Record<string, unknown>).pendingCount, 2);
    assert.deepEqual((after.body.summaryPending as Record<string, unknown>).reasonCounts,
      { ledger_mutation: 1, other: 1 });
    const httpAfter = await httpStatus(config.port, auth.managementRead);
    assert.deepEqual((httpAfter.body.summaryPending as Record<string, unknown>).reasonCounts,
      (after.body.summaryPending as Record<string, unknown>).reasonCounts);
    completion.check("recovery_clears_timeout_reason_on_cli_and_http");
    completion.complete();
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()));
    buffer.close();
  }
}

main().catch((error) => { console.error(error); process.exitCode = 1; });
