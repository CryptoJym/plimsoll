/** Blocking regressions from the independent PR449 review; every value is synthetic. */
import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { AddressInfo } from "node:net";
import test from "node:test";

import { LocalEventBuffer } from "../../../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../../../packages/collector-cli/src/config";
import { loadOrCreateLocalIngestAuth } from "../../../packages/collector-cli/src/local-auth";
import { explodeOtlpPayload } from "../../../packages/collector-cli/src/otlp";
import { RolloutTailer } from "../../../packages/collector-cli/src/rollout-tailer";
import { createCollectorServer } from "../../../packages/collector-cli/src/server";
import { reviewCodexPayloads, reviewObservedAt } from "./_pr449";

// Recorded by the pinned 4b47d330 normalizer under a Codex credential before the fix.
const legacyCodexIds = {
  span: "cd26b686-5423-5ac3-9dbf-3100bc0aa0c7",
  log: "0b3fbf6c-6726-586b-932e-35258e87ff2a",
};

test("one_OTLP_response_keeps_one_usage_total_across_valid_credential_switch", async () => {
  for (const fixture of reviewCodexPayloads) {
    const legacyId = legacyCodexIds[fixture.kind];
    assert.equal(explodeOtlpPayload(fixture.payload, { source: "codex" }).events[0]?.event.id, legacyId,
      "previously admitted Codex identities must remain byte-for-byte unchanged");
    for (const firstSource of ["claude_code", "codex"] as const) {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-credential-identity-"));
      const authHome = path.join(root, "auth");
      fs.mkdirSync(authHome, { mode: 0o700 });
      const auth = loadOrCreateLocalIngestAuth(authHome);
      const buffer = new LocalEventBuffer(path.join(root, "ledger.sqlite"));
      const server = createCollectorServer(collectorConfigSchema.parse({}), buffer, { localAuth: auth });
      const warnings: string[] = [];
      const originalWarn = console.warn;
      console.warn = (...values: unknown[]) => warnings.push(values.map(String).join(" "));
      try {
        await new Promise<void>((resolve, reject) => {
          server.once("error", reject);
          server.listen(0, "127.0.0.1", resolve);
        });
        const port = (server.address() as AddressInfo).port;
        const post = async (source: string | null, token: string | null) => {
          const headers: Record<string, string> = { "content-type": "application/json", connection: "close" };
          if (source !== null) headers["x-plimsoll-source"] = source;
          if (token !== null) headers["x-plimsoll-token"] = token;
          const response = await fetch(`http://127.0.0.1:${port}${fixture.route}`, {
            method: "POST", headers, body: JSON.stringify(fixture.payload),
          });
          return { status: response.status, body: await response.json() };
        };
        for (const [source, token] of [
          ["claude_code", null], ["claude_code", "invalid-synthetic-token"],
          ["claude_code", "Bearer malformed"], ["claude_code", auth.codexProducer],
          ["unknown", auth.claudeCodeProducer], [null, auth.claudeCodeProducer],
        ]) assert.equal((await post(source, token)).status, 401, "credentials and source headers authorize first");
        assert.equal((buffer.database.prepare("select count(*) as rows from buffered_events").get() as { rows: number }).rows, 0);
        const sources = firstSource === "codex" ? ["codex", "claude_code"] : ["claude_code", "codex"];
        for (const source of [...sources, sources[1]]) {
          const token = source === "codex" ? auth.codexProducer : auth.claudeCodeProducer;
          assert.equal((await post(source, token)).status, 202);
        }
        const totals = buffer.database.prepare(`select count(*) as rows, sum(input_tokens) as input,
          sum(output_tokens) as output, count(distinct id) as ids from buffered_events`).get();
        assert.deepEqual(totals, { rows: 1, input: 29, output: 7, ids: 1 }, `${fixture.kind}, ${firstSource} first`);
        const row = buffer.database.prepare("select id, source, usage_duplicate_reason as duplicateReason from buffered_events").get();
        assert.deepEqual(row, { id: legacyId, source: "codex", duplicateReason: null });
        const mismatchWarnings = warnings.filter(line => line.includes('"status":"otlp_service_source_mismatch"'));
        assert.equal(mismatchWarnings.length, 1);
        assert.deepEqual(JSON.parse(mismatchWarnings[0]), {
          status: "otlp_service_source_mismatch", credentialSource: "claude_code", serviceSource: "codex",
        });
      } finally {
        console.warn = originalWarn;
        try {
          if (server.listening) await new Promise<void>((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
        } finally {
          buffer.close();
          fs.rmSync(root, { recursive: true, force: true });
        }
      }
    }
  }
});

test("diagnostic_never_copies_an_arbitrary_error_message", async () => {
  // Include the formerly accepted internal-looking prefix and native SQLite syntax.
  for (const message of ["rollout_synthetic_private_prompt_token_123",
    "no such table: synthetic_private_prompt_token_123",
    "NOT NULL constraint failed: synthetic_private_prompt_token_123", "database is locked"]) {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-diagnostic-identity-"));
    const sessions = path.join(root, "sessions");
    const day = path.join(sessions, "2026/10/01");
    fs.mkdirSync(day, { recursive: true });
    const sessionId = "11111111-1111-4111-8111-111111111111";
    const file = path.join(day, `rollout-2026-10-01T00-00-00-${sessionId}.jsonl`);
    fs.writeFileSync(file, [
      { type: "session_meta", payload: { id: sessionId } },
      { type: "turn_context", payload: { model: "gpt-5.4" } },
      { type: "event_msg", payload: { type: "token_count", info: {
        total_token_usage: { input_tokens: 20, cached_input_tokens: 5, output_tokens: 3, total_tokens: 23 },
      } } },
    ].map(record => JSON.stringify({ timestamp: reviewObservedAt, ...record }) + "\n").join(""));
    const buffer = new LocalEventBuffer(path.join(root, "ledger.sqlite"));
    const tailer = new RolloutTailer(buffer, sessions, () => []);
    const logs: string[] = [];
    const originalError = console.error;
    try {
      buffer.database.exec(`create trigger review_abort before insert on buffered_events begin
        select raise(abort, '${message}'); end`);
      console.error = (...values: unknown[]) => logs.push(values.map(String).join(" "));
      const scan = await tailer.scan({ scope: "full" });
      assert.equal(scan.readErrors, 1);
      assert.equal(scan.eventsAppended, 0);
      assert.equal(logs.length, 1);
      assert.ok(!logs[0].includes(message) && !logs[0].includes(file), "arbitrary message and raw handle must stay private");
      const diagnostic = JSON.parse(logs[0]);
      const hash = (value: string) => "sha256:" + crypto.createHash("sha256")
        .update("plimsoll-maintenance-candidate-v1\0").update(value).digest("hex");
      assert.deepEqual(diagnostic, {
        status: "rollout_commit_error", errorClass: "SqliteError", message: "[redacted error message]",
        messageHash: hash(message), fileHandleHash: hash(file), offset: 0,
      }, "native SQLite errors obey the same exact six-field protocol");
      assert.equal((buffer.database.prepare("select count(*) as rows from rollout_scan_state").get() as { rows: number }).rows, 0);
    } finally {
      console.error = originalError;
      tailer.close();
      buffer.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  }
});
