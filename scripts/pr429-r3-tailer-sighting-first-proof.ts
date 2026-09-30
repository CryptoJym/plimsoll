import { fixtureEpochId } from "./lib/fixture-epoch-id";
/** A known-root transcript persists one sighting before its first raw row. */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { performance } from "node:perf_hooks";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { captureRootDigest, durableClaudeRootSessionSightings, recordClaudeRootSessionSighting } from
  "../packages/collector-cli/src/capture-root-inventory";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { TranscriptTailer } from "../packages/collector-cli/src/transcript-tailer";
import { createProofCompletion } from "./lib/proof-completion";
import { aiInteractionEventSchema } from "../packages/shared/src/schemas";

const home = process.env.HOME!;
const plimsoll = process.env.PLIMSOLL_HOME!;
const now = Date.now();
const observedAt = new Date(now - 60_000).toISOString();
const sessionId = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
const root = { rootId: "claude-b", profileId: "profile-b", installationEpochId: fixtureEpochId("epoch-b"),
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

    // An older ledger may have a raw root receipt but no compact sighting.
    // Re-reading its file must migrate the observation before raw retention
    // can remove the only evidence that this root saw the session.
    const legacySession = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";
    const legacyId = "cccccccc-cccc-4ccc-8ccc-cccccccccccc";
    const legacyEvent = aiInteractionEventSchema.parse({ id: legacyId,source:"claude_code",
      dataMode:"metadata",eventType:"assistant_response",observedAt,sessionId:legacySession,
      metadata:{ captureRootId:root.rootId,captureProfileId:root.profileId } });
    assert.equal(buffer.append(legacyEvent,[]),true);
    buffer.database.prepare("insert into capture_root_observations values(?,?,?,?,?)")
      .run(captureRootDigest(root),legacyId,"legacy-digest",observedAt,"admitted");
    assert.equal(durableClaudeRootSessionSightings(buffer.database,legacySession).size,1);
    assert.equal(recordClaudeRootSessionSighting(buffer,root,legacySession,observedAt),true);
    const compact = buffer.database.prepare(`select count(*) as n from capture_root_session_sightings
      where source='claude_code' and session_id=? and root_digest=?`)
      .get(legacySession,captureRootDigest(root)) as { n:number };
    assert.equal(compact.n,1,"legacy observation must gain an independent durable sighting");
    console.log(JSON.stringify({ scenario:"legacy_observation_promoted",compactSightings:compact.n }));

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
  const proof = createProofCompletion("pr429-r3-tailer-sighting-first", 2);
  proof.check("one_durable_sighting_before_two_transcript_rows");
  proof.check("legacy_observation_promoted_before_raw_retention");
  proof.complete();
}
main().catch(error => { console.error(error); process.exitCode = 1; });
