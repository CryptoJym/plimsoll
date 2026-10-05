/** This source is a rollback BRIDGE. No public receipt, caller context or
 * read-only probe can replace L2's actual installed rollback target. Activation
 * requires a later reviewed source build after this bridge is installed as the
 * previous version and the complete real source/runtime/consumer pair qualifies.
 * Tests build a separately identified future SOURCE fixture; they never activate
 * this bridge or manufacture an installed-version receipt. */
export const DISPATCH_HISTORY_BUILD_PAIR: Readonly<{
  mode: "rollback-bridge" | "future-source-pair";
  previousSourceCommit: string; previousCollectorVersion: string;
  previousReadsHistory: boolean; qualificationScope: "installed-pair-required" | "source-fixture-only";
}> = Object.freeze({
  mode: "rollback-bridge", previousSourceCommit: "34d58bcd90865679e09fcbd1ee1703de5effda97",
  previousCollectorVersion: "0.7.48", previousReadsHistory: false,
  qualificationScope: "installed-pair-required",
});
