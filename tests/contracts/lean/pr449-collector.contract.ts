/** Blocking regressions from the independent PR449 review; every value is synthetic. */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { AddressInfo } from "node:net";
import test from "node:test";

import { LocalEventBuffer } from "../../../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../../../packages/collector-cli/src/config";
import { loadOrCreateLocalIngestAuth } from "../../../packages/collector-cli/src/local-auth";
import { explodeOtlpPayload } from "../../../packages/collector-cli/src/otlp";
import { createCollectorServer } from "../../../packages/collector-cli/src/server";
import { reviewCodexPayloads } from "./_pr449";

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

