/** Removing and replacing a root must not lend its old rows the new root's bind. */
import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { captureRootDigest, dispatchBindingSchema } from
  "../packages/collector-cli/src/capture-root-inventory";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { restampDispatch } from "../packages/collector-cli/src/dispatch-command";
import { createProofCompletion } from "./lib/proof-completion";

const home = process.env.HOME!;
const plimsoll = process.env.PLIMSOLL_HOME!;
fs.mkdirSync(plimsoll, { recursive: true, mode: 0o700 });
const now = Date.now();
const sessionId = "r2-root-reuse";
const observedAt = new Date(now - 60_000).toISOString();
const attemptId = "11111111-1111-4111-8111-111111111111";
const binding = dispatchBindingSchema.parse({ sessionId,
  workItemId: "beads:eco-6hoxj.165.97", projectKey: `sha256:${"a".repeat(64)}`,
  companyRef: null, attemptId, parentAttemptId: null, acceptedOutcomeId: null,
  validFrom: new Date(now - 120_000).toISOString(), validUntil: null,
  evidenceRef: "dispatch:root-reuse-review" });
const old = { rootId: "claude-a", profileId: "old-profile", installationEpochId: "old-epoch",
  source: "claude_code" as const, directory: path.join(home, ".claude-old", "projects"),
  dispatch: [binding] };
const sibling = { rootId: "claude-b", profileId: "sibling-profile", installationEpochId: "sibling-epoch",
  source: "claude_code" as const, directory: path.join(home, ".claude-b", "projects"),
  dispatch: [binding] };
const replacement = { ...old, profileId: "new-profile", installationEpochId: "new-epoch",
  directory: path.join(home, ".claude-new", "projects") };
for (const root of [old, sibling, replacement])
  fs.mkdirSync(root.directory, { recursive: true, mode: 0o700 });
const config = collectorConfigSchema.parse({ deviceId: "dev_pr429-root-reuse",
  uploadUrl: "http://127.0.0.1:1/unused", captureRoots: [sibling, replacement] });
const buffer = new LocalEventBuffer(path.join(plimsoll, "root-reuse.sqlite"), {
  workspaceId: config.tenantId, deviceId: config.deviceId,
  enrollmentNow: () => new Date(now - 3_600_000), delivery: { enabled: true } });
const id = crypto.randomUUID();
try {
  assert.equal(buffer.append({ id, source: "claude_code", eventType: "assistant_response",
    dataMode: "metadata", observedAt, sessionId, actionClass: "other", intent: "unknown",
    inputTokens: 1, outputTokens: 1,
    metadata: { captureRootId: old.rootId, captureProfileId: old.profileId,
      installationEpochId: old.installationEpochId } }, []), true);
  const removed = restampDispatch(["--attempt-id", attemptId], buffer, [sibling]);
  assert.equal(removed.restamped, 0);
  const replaced = restampDispatch(["--attempt-id", attemptId], buffer,
    config.captureRoots!);
  const raw = buffer.database.prepare("select payload_json as payload from buffered_events where id=?")
    .get(id) as { payload: string };
  const metadata = JSON.parse(raw.payload).metadata as Record<string, unknown>;
  const actual = { removedRestamped: removed.restamped, replacedRestamped: replaced.restamped,
    replacedSkipped: replaced.skipped,
    oldDigest: captureRootDigest(old), replacementDigest: captureRootDigest(replacement),
    storedRootId: metadata.captureRootId, storedEpoch: metadata.installationEpochId,
    storedWorkItemId: metadata.workItemId ?? null };
  console.log(JSON.stringify({ scenario: "old root removed, new directory and epoch reuse rootId",
    expected: { removedRestamped: 0, replacedRestamped: 0, storedWorkItemId: null }, actual }));
  assert.notEqual(actual.oldDigest, actual.replacementDigest);
  assert.equal(replaced.restamped, 0, "replacement root inherited an old root's row");
  assert.equal(replaced.skipped, 1, "root with insufficient identity evidence was not counted");
  assert.equal(metadata.workItemId, undefined);
} finally {
  buffer.close();
}
const proof = createProofCompletion("pr429-r2-restamp-root-reuse", 1);
proof.check("restamp_requires_admitted_full_root_digest");
proof.complete();
