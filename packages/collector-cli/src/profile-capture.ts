import type { LocalEventBuffer } from "./buffer";
import type { CollectorConfig } from "./config";
import { validateCaptureRoots } from "./capture-root-inventory";
import { accountAssertionV1Schema, activeCodexAccountAssertion } from "./account-assertion";
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
  const hydratedRoots = roots?.map(root => {
    if (root.source !== "codex") return root;
    // A persisted binding wins over a stale V1 manifest entry after a
    // failover. Legacy account objects remain untouched for compatibility.
    const isVersioned = root.account ? accountAssertionV1Schema.safeParse(root.account).success : false;
    if (root.account && !isVersioned) return root;
    const assertion = activeCodexAccountAssertion(buffer.database, root.rootId);
    return assertion ? { ...root, account: assertion } : root;
  });
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
  const rollout = new RolloutTailer(buffer, undefined, codex ? () => [] : undefined, undefined, codex);
  const transcript = new TranscriptTailer(buffer, undefined, undefined, claude);
  return { rollout,transcript,close() { rollout.close(); transcript.close(); } };
}
