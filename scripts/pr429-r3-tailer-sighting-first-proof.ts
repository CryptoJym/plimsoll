/** A known-root transcript persists one sighting before its first raw row. */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { performance } from "node:perf_hooks";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { durableClaudeRootSessionSightings, recordClaudeRootSessionSighting } from
  "../packages/collector-cli/src/capture-root-inventory";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { TranscriptTailer } from "../packages/collector-cli/src/transcript-tailer";
import { createProofCompletion } from "./lib/proof-completion";

const home = process.env.HOME!;
const plimsoll = process.env.PLIMSOLL_HOME!;
const now = Date.now();
const observedAt = new Date(now - 60_000).toISOString();
const sessionId = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
const root = { rootId: "claude-b", profileId: "profile-b", installationEpochId: "epoch-b",
  source: "claude_code" as const, directory: path.join(home, ".claude-b", "projects") };
const project = path.join(root.directory, "-synthetic-project");
fs.mkdirSync(project, { recursive: true, mode: 0o700 });
fs.mkdirSync(plimsoll, { recursive: true, mode: 0o700 });
const config = collectorConfigSchema.parse({ deviceId: "dev_pr429-tailer-sighting",
  uploadUrl: "http://127.0.0.1:1/unused", captureRoots: [root] });
fs.writeFileSync(path.join(plimsoll, "collector.config.json"), `${JSON.stringify(config)}\n`);
const line = (index: number) => JSON.stringify({ type: "assistant", sessionId,
  timestamp: observedAt, message: { id: `message-${index}`, model: "claude-sonnet-4-20250514",
    content: [], usage: { input_tokens: index, output_tokens: index,
      cache_read_input_tokens: 0, cache_creation_input_tokens: 0 } } });
fs.writeFileSync(path.join(project, `${sessionId}.jsonl`), `${line(1)}\n${line(2)}\n`);

async function main() {
  const buffer = new LocalEventBuffer(path.join(plimsoll, "tailer-sighting.sqlite"), {
    workspaceId: config.tenantId, deviceId: config.deviceId,
    enrollmentNow: () => new Date(now - 3_600_000), delivery: { enabled: true },
  });
  const originalPrepare = buffer.database.prepare.bind(buffer.database);
  const rawCountsAtSighting: number[] = [];
  buffer.database.prepare = ((sql: string) => {
    if(sql.trimStart().startsWith("insert into capture_root_session_sightings")) {
      const row = originalPrepare("select count(*) as n from buffered_events where session_id=?")
        .get(sessionId) as { n: number };
      rawCountsAtSighting.push(row.n);
    }
    return originalPrepare(sql);
  }) as typeof buffer.database.prepare;
  try {
    const tailer = new TranscriptTailer(buffer, root.directory, undefined, [root]);
    let scan;
    try { scan = await tailer.scan({ scope: "full" }); }
    finally { tailer.close(); }
    const rawCount = (originalPrepare("select count(*) as n from buffered_events where session_id=?")
      .get(sessionId) as { n: number }).n;
    const sightings = durableClaudeRootSessionSightings(buffer.database, sessionId).size;
    const actual = { rawCount, sightings, rawCountsAtSighting, eventsAppended: scan.eventsAppended };
    console.log(JSON.stringify({ scenario: "two usage rows from one known-root file", actual }));
    assert.equal(rawCount, 2);
    assert.equal(sightings, 1);
    assert.deepEqual(rawCountsAtSighting, [0], "sighting must commit once before any raw row");

    // Report the additive cost without making wall-clock time a CI gate.
    const samples: number[] = [];
    for(let i=0;i<100;i++) {
      const start = performance.now();
      assert.equal(recordClaudeRootSessionSighting(buffer,root,`cost-session-${i}`,observedAt),true);
      samples.push(performance.now()-start);
    }
    samples.sort((a,b) => a-b);
    console.log(JSON.stringify({ scenario: "first_sighting_cost", samples: samples.length,
      medianMs: samples[Math.floor(samples.length/2)],
      p95Ms: samples[Math.floor(samples.length*0.95)],
      writesPerNewRootSession: 1, writesForLaterEvents: 0 }));
  } finally {
    buffer.database.prepare = originalPrepare;
    buffer.close();
  }
  const proof = createProofCompletion("pr429-r3-tailer-sighting-first", 1);
  proof.check("one_durable_sighting_before_two_transcript_rows");
  proof.complete();
}
main().catch(error => { console.error(error); process.exitCode = 1; });
