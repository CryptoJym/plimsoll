import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";

const base = process.env.PR417_BASE_WORKTREE;
assert.ok(base, "exact 0.7.44 worktree required");
const OldBuffer = require(path.join(base,
  "packages/collector-cli/src/buffer.ts")).LocalEventBuffer as typeof LocalEventBuffer;
const root = fs.mkdtempSync(path.join(os.tmpdir(), "pr417-r19-v2-upgrade-"));
const ledger = path.join(root, "ledger.sqlite");
const options = { workspaceId: "r19-trigger", deviceId: "r19-device", delivery: { enabled: false } };
const triggerNames = (buffer: LocalEventBuffer) => (buffer.database.prepare(`select name
  from sqlite_master where type='trigger' and name like 'trg_events_outbox_linkage_update%'
  order by name`).all() as Array<{ name: string }>).map(row => row.name);

try {
  const old = new OldBuffer(ledger, options);
  try { assert.deepEqual(triggerNames(old), ["trg_events_outbox_linkage_update_v2"]); }
  finally { old.close(); }
  const upgraded = new LocalEventBuffer(ledger, options);
  try {
    const names = triggerNames(upgraded);
    console.log(JSON.stringify({ case: "v2_trigger_upgrade", names }));
    assert.deepEqual(names, ["trg_events_outbox_linkage_update_v3"],
      "upgrade must replace the installed rowid-only trigger");
  } finally { upgraded.close(); }
} finally { fs.rmSync(root, { recursive: true, force: true }); }
