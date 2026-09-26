import assert from "node:assert/strict";
import Database from "better-sqlite3";

import { LearningFactStore } from "../packages/collector-cli/src/learning-facts";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { aggregateToolStatsWeek, boundedRuntimeVersion, ensureWeeklyToolStatsSchema, recordToolStatsDimension } from "../packages/collector-cli/src/weekly-tool-stats";
import { uploadCompletedToolStatsWeek } from "../packages/collector-cli/src/weekly-tool-stats-upload";

async function main() {
const db = new Database(":memory:");
new LearningFactStore(db);
db.exec(`create table buffered_events (
  id text primary key, source text, session_id text, event_type text, observed_at text,
  workspace_id text, device_id text, data_mode text, metadata text
);
insert into buffered_events values
  ('e1','codex','s1','tool_use','2026-09-21T12:00:00.000Z','tenant-1','device-1','metadata','PRIVATE_PROMPT'),
  ('e2','codex','s1','tool_result','2026-09-21T12:01:00.000Z','tenant-1','device-1','metadata','PRIVATE_OUTPUT'),
  ('e3','codex','s2','message','2026-09-22T12:00:00.000Z','tenant-1','device-1','metadata','PRIVATE_PATH'),
  ('e4','codex','s3','tool_use','2026-09-22T13:00:00.000Z','other-tenant','device-1','metadata','PRIVATE_OTHER'),
  ('e5','codex','s4','tool_use','2026-09-22T14:00:00.000Z','tenant-1','device-1','evidence','PRIVATE_EVIDENCE');`);
ensureWeeklyToolStatsSchema(db);
const insert = db.prepare(`insert into tool_attempt_facts
  (operation_id,source,session_id,tool_class,tool_name,started_at,ended_at,duration_ms,result_status,error_category,retry_of,created_at,updated_at)
  values (?,?,?,?,?,?,?,?,?,?,?,?,?)`);
for (const [id, at, status, retry] of [
  ['op1','2026-09-21T12:00:00.000Z','failure',null],
  ['op2','2026-09-21T12:02:00.000Z','unknown','op1'],
  ['op3','2026-09-21T12:03:00.000Z','success','op2'],
  ['op0','2026-09-21T12:03:00.000Z','success','op3'],
] as const) {
  insert.run(id,'codex','s1','compute','shell',at,status === 'unknown' ? null : at,
    status === 'unknown' ? null : 0,status,status === 'failure' ? 'tool' : 'unknown',retry,at,at);
  recordToolStatsDimension(db, { operationId:id, eventId:'e1', runtimeVersion:'0.153.0', collectorVersion:'0.7.43' });
}
insert.run('op-evidence','codex','s4','compute','shell','2026-09-22T14:00:00.000Z',null,
  null,'unknown','unknown',null,'2026-09-22T14:00:00.000Z','2026-09-22T14:00:00.000Z');
recordToolStatsDimension(db, { operationId:'op-evidence', eventId:'e5', runtimeVersion:'private-branch', collectorVersion:'0.7.43' });
assert.equal(boundedRuntimeVersion('private-branch'), 'unknown');
assert.equal(boundedRuntimeVersion('0.153.0-private-branch'), 'unknown');
const report = aggregateToolStatsWeek(db, { workspaceId:'tenant-1', deviceId:'device-1', weekStart:'2026-09-21' });
assert.deepEqual(report.coverage, [{ runtime:'codex', sessions:2, sessionsWithToolEvents:1 }]);
assert.equal(report.cells.length,1);
assert.deepEqual(Object.fromEntries(['attempts','failures','unknown','retries','longestChain'].map((key) => [key, report.cells[0]![key as keyof typeof report.cells[0]]])),
  { attempts:4, failures:1, unknown:1, retries:3, longestChain:4 });
assert.deepEqual(report.cells[0]!.sessions.map((session) => session.sessionId), ['s1']);
assert.doesNotMatch(JSON.stringify(report), /PRIVATE_|other-tenant|e1|op1|metadata|path|prompt|output/i);
assert.equal(aggregateToolStatsWeek(db, { workspaceId:'tenant-1', deviceId:'device-1', weekStart:'2026-09-28' }).cells.length, 0);
db.prepare(`insert into weekly_tool_stats_control(workspace_id,device_id,first_week) values('tenant-1','device-1','2026-09-21')`).run();
const config = collectorConfigSchema.parse({ tenantId:'tenant-1', deviceId:'device-1',
  cloudDeviceId:'11111111-1111-4111-8111-111111111111', installKey:'pli_fixture_key',
  uploadUrl:'http://127.0.0.1:61777/api/work-intelligence/ingest' });
const bodies:string[] = [];
let requests = 0;
const fetchImpl = (async (_url: RequestInfo | URL, init?: RequestInit) => {
  bodies.push(String(init?.body ?? ''));
  requests += 1;
  if (requests === 1) throw new Error('lost response');
  const request = JSON.parse(bodies.at(-1)!) as { reportSequence:number; deviceId:string };
  const stored = db.prepare(`select digest from weekly_tool_stats_uploads where week_start='2026-09-21'`).get() as { digest:string };
  return new Response(JSON.stringify({ schema:'fleet-device-report-receipt/v1', deviceId:request.deviceId,
    reportSequence:request.reportSequence, disposition:'replay_ignored', nextReportSequence:request.reportSequence + 1,
    toolStatsDigest:stored.digest }), { status:200, headers:{'content-type':'application/json'} });
}) as typeof fetch;
await assert.rejects(uploadCompletedToolStatsWeek(config, db, { now:() => new Date('2026-09-28T00:05:00.000Z'), fetchImpl }));
assert.equal(await uploadCompletedToolStatsWeek(config, db, { now:() => new Date('2026-09-28T00:06:00.000Z'), fetchImpl }), 'accepted');
assert.equal(bodies.length, 2);
assert.equal(bodies[0], bodies[1], 'an ambiguous send retries the same body and sequence');
assert.doesNotMatch(bodies.join(''), /PRIVATE_|other-tenant|metadata|path|prompt|output/i);
assert.equal((db.prepare(`select delivered from weekly_tool_stats_uploads where week_start='2026-09-21'`).get() as {delivered:number}).delivered, 1);
db.close();
console.log('weekly tool statistics: 2/2 pass');
}

void main().catch((error) => { console.error(error); process.exitCode = 1; });
