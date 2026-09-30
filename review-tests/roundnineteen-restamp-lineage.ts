import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { dispatchBindingSchema, type CaptureRoot } from "../packages/collector-cli/src/capture-root-inventory";
import { restampDispatch } from "../packages/collector-cli/src/dispatch-command";
import { aiInteractionEventSchema } from "../packages/shared/src/index";

const base = process.env.PR417_BASE_WORKTREE;
assert.ok(base, "exact 0.7.44 worktree required");
const OldBuffer = require(path.join(base,
  "packages/collector-cli/src/buffer.ts")).LocalEventBuffer as typeof LocalEventBuffer;
const root = fs.mkdtempSync(path.join(os.tmpdir(), "pr417-r19-restamp-"));
const ledger = path.join(root, "ledger.sqlite");
const id = "00000000-0000-4000-8000-000000005721";
const now = new Date();
const oldAt = new Date(now.getTime() - 45 * 86_400_000).toISOString();
const options = { workspaceId: "r19-restamp", deviceId: "r19-device",
  delivery: { enabled: false }, enrollmentNow: () => new Date(now.getTime() - 60 * 86_400_000) };
const event = aiInteractionEventSchema.parse({ id, sessionId: "r19-restamp-session",
  source: "codex", eventType: "assistant_response", dataMode: "metadata", observedAt: oldAt,
  actionClass: "other", inputTokens: 1, outputTokens: 1 });

try {
  const first = new LocalEventBuffer(ledger, options);
  try {
    assert.equal(first.append(event), true);
    first.database.prepare("update buffered_events set created_at=? where id=?").run(oldAt, id);
    first.delivery.configure({ enabled: true });
    assert.equal(first.delivery.repairRawById(id).enqueued, 1);
  } finally { first.close(); }
  const old = new OldBuffer(ledger, { ...options, delivery: { enabled: true } });
  try { assert.equal(old.prune(30, { now }).events, 1); }
  finally { old.close(); }

  const reused = new LocalEventBuffer(ledger, options);
  try {
    const db = reused.database;
    db.prepare("update upload_outbox set attempt_count=1 where raw_id=?").run(id);
    const stale = db.prepare(`select raw_rowid as rowid,raw_created_at as at,
      raw_generation as generation from upload_outbox where raw_id=?`).get(id) as {
        rowid: number; at: string; generation: string;
      };
    assert.ok(stale);
    assert.equal(reused.append(event), true);
    const current = db.prepare(`select rowid as rowid,created_at as at,
      privacy_generation as generation from buffered_events where id=?`).get(id) as {
        rowid: number; at: string; generation: string;
      };
    assert.equal(current.rowid, stale.rowid);
    assert.notEqual(current.at, stale.at);
    assert.notEqual(current.generation, stale.generation);

    const binding = dispatchBindingSchema.parse({ sessionId: event.sessionId,
      workItemId: "beads:eco-6hoxj.163.122", projectKey: `sha256:${"b".repeat(64)}`,
      attemptId: "r19-restamp", validFrom: new Date(Date.parse(oldAt) - 86_400_000).toISOString(),
      validUntil: null, companyRef: null, parentAttemptId: null, acceptedOutcomeId: null,
      evidenceRef: "r19-restamp-proof" });
    const captureRoot = { rootId: "r19-root", profileId: "r19-profile",
      installationEpochId: reused.workspaceBinding()!.currentInstallationEpochId!,
      source: "codex", directory: root, dispatch: [binding] } as CaptureRoot;
    const result = restampDispatch(["--attempt-id", binding.attemptId], reused, [captureRoot]);
    const raw = db.prepare("select payload_json as payload from buffered_events where id=?")
      .get(id) as { payload: string };
    const metadata = JSON.parse(raw.payload).metadata;
    console.log(JSON.stringify({ case: "stale_attempted_copy_does_not_block_new_raw",
      stale, current, result, workItemId: metadata?.workItemId ?? null }));
    assert.equal(result.restamped, 1,
      "a different incarnation's attempted copy must not block restamping");
    assert.equal(metadata.workItemId, binding.workItemId);
  } finally { reused.close(); }
} finally { fs.rmSync(root, { recursive: true, force: true }); }
