import type { LocalEventBuffer } from "./buffer";
import type { CollectorConfig } from "./config";
import { validateCaptureRoots } from "./capture-root-inventory";
import { RolloutTailer } from "./rollout-tailer";
import { TranscriptTailer } from "./transcript-tailer";
import { beginAutomaticCaptureBaseline,captureBaselineStatus,completeAutomaticCaptureBaseline } from "./capture-baseline";
/** Parent CLI/maintenance factory uses this instead of constructing single-default tailers.
 * An explicitly empty provider inventory disables that fallback reader. */
export function createProfileCapture(buffer: LocalEventBuffer,config: Pick<CollectorConfig,"captureRoots">) {
  const roots=config.captureRoots===undefined? null:validateCaptureRoots(config.captureRoots);
  const codex=roots?.filter(root => root.source==="codex"),claude=roots?.filter(root => root.source==="claude_code");
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
