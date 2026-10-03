import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { AddressInfo } from "node:net";

import { LocalEventBuffer } from "../../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../../packages/collector-cli/src/config";
import { loadOrCreateLocalIngestAuth } from "../../packages/collector-cli/src/local-auth";
import { explodeOtlpPayload } from "../../packages/collector-cli/src/otlp";
import { createCollectorServer } from "../../packages/collector-cli/src/server";

function attribute(key: string, value: string | number) {
  return { key, value: typeof value === "number"
    ? { intValue: String(value) } : { stringValue: value } };
}

/** Invented exporters, credentials and usage; every listener uses port zero. */
export async function verifyOtlpServiceSourceCases() {
  const cases = [
    { signal: "spans", service: "Codex_Desktop", source: "codex" },
    { signal: "logs", service: "codex_exec", source: "codex" },
    { signal: "spans", service: "codex-cli", source: "codex" },
    { signal: "logs", service: "Codex_Desktop", source: "codex" },
    { signal: "spans", service: "codex-cli", key: "service_name", source: "codex" },
    { signal: "logs", service: "codex_exec", key: "serviceName", source: "codex" },
    { signal: "spans", service: "claude-code", source: "claude_code" },
    { signal: "logs", service: "claude_code", source: "claude_code" },
    { signal: "logs", service: undefined, source: "claude_code" },
    { signal: "logs", service: "unrecognized-producer", source: "unknown" },
  ] as const;
  const warnings: string[] = [];
  const originalWarn = console.warn;
  const credentials: string[] = [];
  console.warn = (...values: unknown[]) => warnings.push(values.map(String).join(" "));
  let requests = 0;
  try {
    // A second server in the same process proves the warning is process-wide.
    for (let instance = 0; instance < 2; instance += 1) {
      const home = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-otlp-service-source-"));
      const config = collectorConfigSchema.parse({});
      const auth = loadOrCreateLocalIngestAuth(home);
      credentials.push(auth.claudeCodeProducer, auth.codexProducer, auth.managementRead);
      const buffer = new LocalEventBuffer(path.join(home, "ledger.sqlite"));
      const server = createCollectorServer(config, buffer, { localAuth: auth });
      try {
        await new Promise<void>((resolve, reject) => {
          server.once("error", reject);
          server.listen(0, "127.0.0.1", resolve);
        });
        const port = (server.address() as AddressInfo).port;
        for (const [index, fixture] of cases.entries()) {
          const attributes = [
            attribute("gen_ai.usage.input_tokens", 29),
            attribute("gen_ai.usage.output_tokens", 7),
            attribute("gen_ai.usage.cache_read_tokens", 5),
            attribute("gen_ai.usage.cache_creation_input_tokens", 3),
            attribute("gen_ai.request.model", "gpt-5.4"),
            { key: "gen_ai.usage.cost_usd", value: { doubleValue: 0.0023 } },
          ];
          const time = String(BigInt(Date.now() - 10_000 + index) * 1_000_000n);
          const resource = { attributes: fixture.service === undefined ? [] : [
            attribute("key" in fixture ? fixture.key : "service.name", fixture.service),
          ] };
          const payload = fixture.signal === "spans" ? {
            resourceSpans: [{ resource, scopeSpans: [{ spans: [{
              name: "handle_responses", startTimeUnixNano: time,
              traceId: (index + 1).toString(16).padStart(32, "0"),
              spanId: (index + 1).toString(16).padStart(16, "0"), attributes,
            }] }] }],
          } : {
            resourceLogs: [{ resource, scopeLogs: [{ logRecords: [{
              timeUnixNano: time, attributes,
            }] }] }],
          };
          const route = fixture.signal === "spans" ? "/v1/traces" : "/v1/logs";
          const expected = explodeOtlpPayload(payload, {
            policy: config.policy, source: "claude_code", transportPath: route,
          }).events[0]?.event;
          assert.ok(expected, "synthetic exporter normalizes to one usage event");
          const post = async () => {
            requests += 1;
            const response = await fetch(`http://127.0.0.1:${port}${route}`, {
              method: "POST", headers: {
                "content-type": "application/json",
                "x-plimsoll-source": "claude_code",
                "x-plimsoll-token": auth.claudeCodeProducer,
              }, body: JSON.stringify(payload),
            });
            await response.text();
            assert.equal(response.status, 202, `${fixture.signal} ${fixture.service ?? "unnamed"}`);
          };
          await post();
          const row = buffer.database.prepare(
            "select payload_json as payloadJson from buffered_events where id = ?",
          ).get(expected.id) as { payloadJson: string } | undefined;
          assert.ok(row, "deterministic event id is preserved");
          const event = JSON.parse(row.payloadJson);
          assert.equal(event.source, fixture.source);
          for (const [field, value] of Object.entries({
            inputTokens: 29, outputTokens: 7, cacheReadTokens: 5,
            cacheCreationTokens: 3, costUsd: 0.0023,
          })) assert.equal(event[field], value, field);
          assert.equal(event.costKind, expected.costKind);
          await post();
          assert.equal((buffer.database.prepare(
            "select count(*) as count from buffered_events where id = ?",
          ).get(expected.id) as { count: number }).count, 1, "retry dedupes");
        }
      } finally {
        if (server.listening) await new Promise<void>(resolve => server.close(() => resolve()));
        buffer.close();
        fs.rmSync(home, { recursive: true, force: true });
      }
    }
    const mismatchWarnings = warnings.map(line => JSON.parse(line) as Record<string, unknown>)
      .filter(line => line.status === "otlp_service_source_mismatch");
    assert.deepEqual(mismatchWarnings, [{
      status: "otlp_service_source_mismatch",
      credentialSource: "claude_code", serviceSource: "codex",
    }]);
    assert.ok(credentials.every(credential => !warnings.join("\n").includes(credential)));
    return { cases: cases.length, serverInstances: 2, requests, mismatchWarnings: 1 };
  } finally {
    console.warn = originalWarn;
  }
}
