import assert from 'node:assert/strict';
import { performance } from 'node:perf_hooks';
import { LocalEventBuffer } from '../packages/collector-cli/src/buffer';
import { SessionAttributionBatch, SESSION_INHERIT_MAX_BATCH_ROW_READS, SESSION_INHERIT_MAX_SCANNED_ROWS } from '../packages/collector-cli/src/session-attribution';
import { sessionContextIndexComplete } from '../packages/collector-cli/src/session-context-index';
import type { AiInteractionEvent } from '../packages/shared/src/index';

const A = `sha256:${'a'.repeat(64)}`, B = `sha256:${'b'.repeat(64)}`;
const ledger = new LocalEventBuffer(':memory:', { delivery: { enabled: false } });
const eventTypes = ['usage_rollout', 'usage_transcript'] as const;
const inputsByType = Object.fromEntries(eventTypes.map((eventType) => [eventType,
  Array.from({ length: 500 }, (_, n) => ({
    repoHash: A,
    event: { id: `00000000-0000-4000-8000-${eventType === 'usage_rollout' ? '1' : '2'}${String(n).padStart(11, '0')}`,
      sessionId: `${eventType}-budget-${n}`, source: 'codex', dataMode: 'metadata', eventType,
      observedAt: '2026-09-23T12:01:00.000Z', intent: 'unknown', actionClass: 'other', inputTokens: 10, metadata: {},
    } as AiInteractionEvent,
  })),
])) as Record<typeof eventTypes[number], Array<{ repoHash: string; event: AiInteractionEvent }>>;
try {
  const insert = ledger.database.prepare(`insert into buffered_events
    (id, source, event_type, data_mode, observed_at, payload_json, suppressed_fields_json, created_at,
     session_id, repo_hash, workspace_id, device_id, privacy_generation)
    values (?, 'codex', 'tool_result', 'metadata', '2026-09-23T12:00:00.000Z', '{}', '[]',
      '2026-09-23T12:00:00.000Z', ?, ?, 'review-workspace', 'review-device', 'review-generation')`);
  ledger.database.transaction(() => {
    for (const eventType of eventTypes) {
      const inputs = inputsByType[eventType];
      for (let n = 0; n < inputs.length; n++) {
        for (let r = 0; r < 256; r++) insert.run(`${eventType}-context-${n}-${r}`, inputs[n]!.event.sessionId, B);
      }
    }
  })();
  const coldStart = performance.now();
  assert.equal(sessionContextIndexComplete(ledger.database), true);
  const coldValidationMs = performance.now() - coldStart;
  const details = [];
  for (const eventType of eventTypes) {
    const inputs = inputsByType[eventType];
    for (const contextIndex of [true, false]) {
      const start = performance.now();
      const batch = new SessionAttributionBatch(ledger.database, inputs, { contextIndex });
      const projects: Record<string, number> = {};
      for (const input of inputs) {
        const result = batch.attribute(input.event, { repoHash: input.repoHash });
        const key = result.event.projectKey === A ? 'startingFolderA' : result.event.projectKey === B ? 'priorToolB' : 'unallocated';
        projects[key] = (projects[key] ?? 0) + 1;
        if (result.event.projectKey === A) throw new Error(`${eventType}/${contextIndex ? 'indexed' : 'legacy'} charged capped lookup to starting folder`);
        if (result.event.projectKey === B) {
          assert.equal(result.event.metadata.projectBasis, 'session_inherited');
        } else {
          assert.equal(result.event.projectKey, undefined);
          assert.equal(result.event.metadata.projectBasis, 'unallocated');
        }
      }
      const elapsedMs = performance.now() - start;
      const stats = batch.stats();
      assert.equal(stats.lookups, 500);
      assert.ok(stats.contextRows <= SESSION_INHERIT_MAX_BATCH_ROW_READS + stats.lookups);
      assert.ok(stats.rowReads <= SESSION_INHERIT_MAX_BATCH_ROW_READS);
      assert.ok(stats.indexEntries <= (SESSION_INHERIT_MAX_SCANNED_ROWS + 1) * stats.lookups);
      assert.equal(projects.startingFolderA ?? 0, 0);
      assert.equal(projects.priorToolB ?? 0, 256);
      assert.equal(projects.unallocated ?? 0, 244);
      details.push({ eventType, contextIndex, elapsedMs, stats, projects });
    }
  }
  console.log(JSON.stringify({ status: 'PASS resource caps; allocation failures recorded separately',
    fixture: { sessions: 500, contextsPerSession: 256, totalContexts: 128000, ownFolder: 'A', priorTools: 'B' },
    coldValidationMs, details,
    indexedPlan: ledger.database.prepare(`explain query plan select c.source_rowid,
      (select e.event_type from buffered_events e where e.rowid = c.source_rowid) as eventType,
      exists (select 1 from buffered_events e where e.rowid = c.source_rowid and e.session_id is c.session_id
        and e.observed_at is c.observed_at and e.repo_hash is c.repo_hash and e.data_mode <> 'evidence'
        and e.privacy_disposition is null) as sourceValid from session_repo_contexts c
      where c.session_id = ? and c.observed_at >= ? and c.observed_at <= ?
      order by c.observed_at asc, c.source_rowid asc limit ?`).all('budget-0', '2026-09-23T06:01:00.000Z', '2026-09-23T18:01:00.000Z', 4097),
  }, null, 2));
} finally { ledger.close(); }
