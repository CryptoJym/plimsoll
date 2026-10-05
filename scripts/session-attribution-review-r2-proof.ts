import { nativeCodexFixture } from "./lib/native-codex-fixture";
import assert from 'node:assert/strict';
import { LocalEventBuffer } from '../packages/collector-cli/src/buffer';
import { applyProjectAttribution, SessionAttributionBatch } from '../packages/collector-cli/src/session-attribution';
import { prepareHistoryEvent, normalizeHistoryEvent } from '../packages/collector-cli/src/upload-history';
import { sealOutboundEnvelope } from '../packages/collector-cli/src/outbound-envelope';
import { extractRepoContextCwd, peekRepoContextSidecar } from '../packages/collector-cli/src/repo-context';
import { explodeOtlpPayload } from '../packages/collector-cli/src/otlp';
import { DEFAULT_POLICY, type AiInteractionEvent } from '../packages/shared/src/index';

const A = `sha256:${'a'.repeat(64)}`, B = `sha256:${'b'.repeat(64)}`, C = `sha256:${'c'.repeat(64)}`;
const at = (seconds: number) => new Date(Date.UTC(2026, 8, 23, 12) + seconds * 1000).toISOString();
const id = (n: number) => `00000000-0000-4000-8000-${String(n).padStart(12, '0')}`;
const event = (n: number, seconds = 600, extra: Partial<AiInteractionEvent> = {}): AiInteractionEvent => ({
  id: id(n), sessionId: 'review-session', source: 'codex', dataMode: 'metadata',
  eventType: 'usage_rollout', observedAt: at(seconds), intent: 'unknown', actionClass: 'other',
  inputTokens: 10, outputTokens: 20, metadata: {}, ...extra,
});
const context = (n: number, seconds: number, repoHash: string, sessionId = 'review-session') => ({
  rowid: n, observedAt: at(seconds), repoHash, sessionId, eventType: 'tool_result',
});
const checks: { name: string; pass: boolean; detail?: unknown }[] = [];
function check(name: string, run: () => unknown) {
  try { const detail = run(); checks.push({ name, pass: true, ...(detail === undefined ? {} : { detail }) }); }
  catch (error) { checks.push({ name, pass: false, detail: error instanceof Error ? error.message : String(error) }); }
}

if (process.argv.includes('--bounds')) {
  for (const eventType of ['usage_rollout', 'usage_transcript'] as const) {
    for (const toolCount of [255, 256, 4097]) {
      const ledger = new LocalEventBuffer(':memory:', {
        workspaceId: 'review-workspace', deviceId: 'review-device',
        enrollmentNow: () => new Date(at(-3600)), delivery: { enabled: true, now: () => new Date(at(700)) },
      });
      try {
        const update = ledger.database.prepare('update buffered_events set repo_hash = ? where id = ?');
        ledger.database.transaction(() => {
          for (let i = 1; i <= toolCount; i++) {
            ledger.append(event(i, i / 10, { eventType: 'tool_result', inputTokens: undefined, outputTokens: undefined }));
            update.run(B, id(i));
          }
        })();
        const usage = event(9000, 600, { eventType,
          ...nativeCodexFixture(id(9000), "attribution-proof-unpriced"),
          metadata: { ...nativeCodexFixture(id(9000), "attribution-proof-unpriced").metadata,
            installationEpochId: (ledger.database.prepare("select current_installation_epoch_id as id from collector_workspace_binding where singleton=1").get() as {id:string}).id } });
        ledger.append(usage);
        update.run(A, usage.id);
        const row = ledger.database.prepare('select rowid from buffered_events where id = ?').get(usage.id) as { rowid: number };
        ledger.delivery.fillLinkageForRawRow(row.rowid, A, null);
        const input = { payloadJson: JSON.stringify(usage), suppressedFieldsJson: '[]', repoHash: A, branchHash: null };
        const prepared = prepareHistoryEvent(input);
        assert.ok(prepared.ok && prepared.event);
        const index = new SessionAttributionBatch(ledger.database, [{ event: usage, repoHash: A }]);
        const scan = new SessionAttributionBatch(ledger.database, [{ event: usage, repoHash: A }], { contextIndex: false });
        const historyBatch = new SessionAttributionBatch(ledger.database, [{ event: prepared.event, repoHash: A }]);
        const history = normalizeHistoryEvent({ ...input, attribution: historyBatch });
        assert.ok(history.ok);
        const indexedResult = index.attribute(usage, { repoHash: A });
        const scanResult = scan.attribute(usage, { repoHash: A });
        const cappedBudget = new SessionAttributionBatch(ledger.database, [{ event: usage, repoHash: A }], { maxBatchRowReads: 0 });
        const budgetResult = cappedBudget.attribute(usage, { repoHash: A });
        const expected = toolCount === 255 ? B : undefined;
        const expectedBasis = toolCount === 255 ? 'session_inherited' : 'unallocated';
        const observed = {
          eventType, toolCount, indexed: indexedResult.event.projectKey, indexStats: index.stats(),
          legacy: scanResult.event.projectKey, legacyStats: scan.stats(),
          history: history.envelope.event.projectKey,
          budgetZero: budgetResult.event.projectKey, budgetStats: cappedBudget.stats(),
        };
        // The first two shapes fit a real 500-event lease and exercise the outbound path.
        if (toolCount < 500) {
          const lease = ledger.delivery.lease({ maxRows: 500, maxBytes: 10_000_000, now: new Date(at(701)) });
          const live = lease.items.find(item => item.envelope.event.id === usage.id);
          assert.ok(live, 'usage row absent from real live lease');
          Object.assign(observed, { live: live.envelope.event.projectKey, liveBasis: live.envelope.event.metadata.projectBasis });
          check(`${eventType}/${toolCount}: live agrees with history`, () => assert.deepEqual(live.envelope, history.envelope));
          check(`${eventType}/${toolCount}: live obeys cap`, () => {
            assert.equal(live.envelope.event.projectKey, expected);
            assert.equal(live.envelope.event.metadata.projectBasis, expectedBasis);
          });
        }
        console.log(JSON.stringify({ scenario: 'bounds', ...observed }));
        check(`${eventType}/${toolCount}: indexed lookup obeys cap`, () => {
          assert.equal(indexedResult.event.projectKey, expected);
          assert.equal(indexedResult.event.metadata.projectBasis, expectedBasis);
        });
        check(`${eventType}/${toolCount}: legacy lookup obeys cap`, () => {
          assert.equal(scanResult.event.projectKey, expected);
          assert.equal(scanResult.event.metadata.projectBasis, expectedBasis);
        });
        check(`${eventType}/${toolCount}: history obeys cap`, () => {
          assert.equal(history.envelope.event.projectKey, expected);
          assert.equal(history.envelope.event.metadata.projectBasis, expectedBasis);
        });
        check(`${eventType}/${toolCount}: exhausted batch gives unallocated`, () => {
          assert.equal(budgetResult.event.projectKey, undefined);
          assert.equal(budgetResult.event.metadata.projectBasis, 'unallocated');
        });
      } finally { ledger.close(); }
    }
  }
} else {
  const usage = event(1);
  check('nearest strictly earlier repository wins', () => assert.equal(applyProjectAttribution(usage, {
    sessionContexts: [context(1, 500, A), context(2, 590, B), context(3, 601, C)],
  }).event.projectKey, B));
  check('future-only context gives unallocated', () => assert.equal(applyProjectAttribution(usage, {
    sessionContexts: [context(2, 601, B)],
  }).event.projectKey, undefined));
  check('same timestamp gives unallocated', () => assert.equal(applyProjectAttribution(usage, {
    sessionContexts: [context(2, 600, B)],
  }).event.projectKey, undefined));
  check('different session cannot supply project', () => assert.equal(applyProjectAttribution(usage, {
    sessionContexts: [context(2, 590, B, 'other-session')],
  }).event.projectKey, undefined));
  check('own folder is the fallback', () => assert.equal(applyProjectAttribution(usage, {
    repoHash: A, sessionContexts: [context(2, 601, B)],
  }).event.projectKey, A));
  check('explicit producer key wins', () => assert.equal(applyProjectAttribution({ ...usage, projectKey: C }, {
    repoHash: A, sessionContexts: [context(2, 590, B)],
  }).event.projectKey, C));
  check('prior tool displaces rollout starting folder', () => assert.equal(applyProjectAttribution(usage, {
    repoHash: A, sessionContexts: [context(2, 590, B)],
  }).event.projectKey, B));
  check('prior tool displaces transcript starting folder', () => assert.equal(applyProjectAttribution({ ...usage, eventType: 'usage_transcript' }, {
    repoHash: A, sessionContexts: [context(2, 590, B)],
  }).event.projectKey, B));
  check('incomplete lookup clears a generated rollout fallback', () => {
    const attributed = applyProjectAttribution({ ...usage, eventType: 'usage_rollout', projectKey: A,
      metadata: { projectBasis: 'repo_context' } }, {
      repoHash: A, sessionContexts: [context(2, 590, B)], sessionContextsTruncated: true,
    });
    assert.equal(attributed.event.projectKey, undefined);
    assert.equal(attributed.event.metadata.projectBasis, 'unallocated');
  });
  check('direct tool repository evidence survives an unrelated cap', () => {
    const attributed = applyProjectAttribution({ ...usage, eventType: 'tool_result' }, {
      repoHash: A, sessionContexts: [], sessionContextsTruncated: true,
    });
    assert.equal(attributed.event.projectKey, A);
    assert.equal(attributed.event.metadata.projectBasis, 'repo_context');
  });
  check('over-six-hour context is excluded', () => assert.equal(applyProjectAttribution(usage, {
    sessionContexts: [context(2, 600 - 21601, B)],
  }).event.projectKey, undefined));
  const cwd = '/fixture/private-coordinator', workdir = '/fixture/private-tool-repository';
  const args = JSON.stringify({ workdir, command: 'PRIVATE_TOOL_ARGUMENT_SENTINEL' });
  check('hook tool workdir beats enclosing cwd', () => assert.equal(extractRepoContextCwd({ cwd, arguments: args }), workdir));
  const attr = (key: string, value: string) => ({ key, value: { stringValue: value } });
  const otlp = explodeOtlpPayload({ resourceLogs: [{ resource: { attributes: [attr('service.name', 'codex_exec')] },
    scopeLogs: [{ logRecords: [{ timeUnixNano: '1790164800000000000', attributes: [attr('cwd', cwd), attr('arguments', args), attr('session.id', 'review-session')] }] }],
  }] }, { policy: DEFAULT_POLICY, source: 'codex', transportPath: '/v1/logs' });
  check('OTLP tool workdir beats enclosing cwd', () => assert.equal(peekRepoContextSidecar(otlp.events[0]!.event)?.cwd, workdir));
  check('OTLP output and sealed envelope exclude raw path and arguments', () => {
    const sealed = sealOutboundEnvelope(otlp.events[0]);
    assert.ok(sealed.ok);
    for (const value of [JSON.stringify(otlp), JSON.stringify(sealed.envelope)]) {
      for (const secret of [cwd, workdir, 'PRIVATE_TOOL_ARGUMENT_SENTINEL']) assert.ok(!value.includes(secret));
    }
  });
  check('local lookup eventType field is not copied to the outbound envelope', () => {
    const attributed = applyProjectAttribution(usage, { sessionContexts: [{ ...context(2, 590, B), eventType: 'PRIVATE_INTERNAL_VALUE' }] });
    assert.deepEqual(Object.keys(attributed.event.metadata), ['projectBasis']);
    const sealed = sealOutboundEnvelope({ event: attributed.event, suppressedFields: [] });
    assert.ok(sealed.ok);
    assert.ok(!JSON.stringify(sealed.envelope).includes('PRIVATE_INTERNAL_VALUE'));
  });
  check('outbound validator rejects path-bearing project and basis', () => {
    assert.equal(sealOutboundEnvelope({ event: { ...usage, projectKey: workdir }, suppressedFields: [] }).ok, false);
    assert.equal(sealOutboundEnvelope({ event: { ...usage, metadata: { projectBasis: workdir } }, suppressedFields: [] }).ok, false);
  });
}
console.log(JSON.stringify({ checks, passed: checks.filter(c => c.pass).length, failed: checks.filter(c => !c.pass).length }, null, 2));
if (checks.some(c => !c.pass)) process.exitCode = 1;
