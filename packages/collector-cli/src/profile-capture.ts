import type { LocalEventBuffer } from "./buffer";
import type { CollectorConfig } from "./config";
import { validateCaptureRoots } from "./capture-root-inventory";
import { accountAssertionContains, accountAssertionV1Schema, accountAssertionAdapterEnabled,
  codexAccountAssertionBindings } from "./account-assertion";
import { RolloutTailer } from "./rollout-tailer";
import { TranscriptTailer } from "./transcript-tailer";
import { beginAutomaticCaptureBaseline,captureBaselineStatus,completeAutomaticCaptureBaseline } from "./capture-baseline";
/** Parent CLI/maintenance factory uses this instead of constructing single-default tailers.
 * An explicitly empty provider inventory disables that fallback reader. */
export function createProfileCapture(buffer: LocalEventBuffer,config: Pick<CollectorConfig,"captureRoots">) {
  const roots=config.captureRoots===undefined? null:validateCaptureRoots(config.captureRoots);
  // Live enrollment persists the assertion beside the binding.  Rehydrate it
  // into the in-memory root inventory so rollout events and live intervals use
  // the same window without reading a provider auth store.
  const hydrateRoot = (root: NonNullable<typeof roots>[number], observedAt?: string) => {
    if (root.source !== "codex") return root;
    // Adapter capability is authoritative: a disabled source must not hydrate
    // or carry any persisted assertion into subsequent observations.
    if (!accountAssertionAdapterEnabled(buffer.database, "codex")) {
      const { account: _discardedAccount, accountAssertions: _discardedIntervals,
        accountAssertionEpochs: _discardedEpochs, ...withoutAssertion } = root;
      return { ...withoutAssertion, installationEpochId:
        buffer.workspaceBinding()?.currentInstallationEpochId ?? root.installationEpochId };
    }
    // Hydrate every immutable interval so delayed observations can resolve by
    // event time after a failover; the latest interval remains the convenience
    // `account` field for callers that only need the current view.
    const accountIsVersioned = root.account ? accountAssertionV1Schema.safeParse(root.account).success : false;
    const legacyAccount = root.account && !accountIsVersioned ? root.account : undefined;
    const bindings = codexAccountAssertionBindings(buffer.database, root.rootId);
    const selected = observedAt
      ? bindings.filter(binding => accountAssertionContains(binding.assertion, observedAt))
      : bindings.filter(binding => binding.active).slice(-1);
    if (selected.length !== 1) {
      const { account: _configuredAccount, accountAssertions: _discardedIntervals,
        accountAssertionEpochs: _discardedEpochs, ...withoutAssertion } = root;
      return legacyAccount ? { ...withoutAssertion, account: legacyAccount } : withoutAssertion;
    }
    const accountAssertions = bindings.map(binding => binding.assertion);
    const accountAssertionEpochs = bindings.flatMap(binding => binding.installationEpochId ? [{
      actorHash: binding.assertion.actorHash, validFrom: binding.assertion.validFrom,
      evidenceRef: binding.assertion.evidenceRef, installationEpochId: binding.installationEpochId,
    }] : []);
    return { ...root, account: selected[0].assertion, accountAssertions, accountAssertionEpochs,
      installationEpochId: selected[0].installationEpochId ?? root.installationEpochId };
  };
  const hydratedRoots = roots?.map(root => hydrateRoot(root));
  const codex=hydratedRoots?.filter(root => root.source==="codex"),claude=hydratedRoots?.filter(root => root.source==="claude_code");
  if(roots!==null) {
    // An explicitly empty provider inventory has no authorized roots to walk.
    // Establish only an absent fence; preserve previous failures/pending work.
    for(const source of ["codex","claude_code"] as const) {
      if(roots.some(root => root.source===source))
        continue;
      const state=captureBaselineStatus(buffer.database).sources.find(row => row.source===source)!;
      if(state.status!=="not_established")
        continue;
      const at=new Date().toISOString();
      const began=beginAutomaticCaptureBaseline(buffer.database,source,{ startedAt: at,filesDiscovered: 0 });
      completeAutomaticCaptureBaseline(buffer.database,source,{ runId: began.latestRun!.runId,completedAt: at });
    }
  }
  const rollout = new RolloutTailer(buffer, undefined, codex ? () => [] : undefined, undefined, codex,
    () => accountAssertionAdapterEnabled(buffer.database, "codex"),
    (root, observedAt) => root ? hydrateRoot(root, observedAt) : undefined);
  const transcript = new TranscriptTailer(buffer, undefined, undefined, claude);
  return { rollout,transcript,close() { rollout.close(); transcript.close(); } };
}
