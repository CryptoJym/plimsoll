import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { provisionLiveProducer, authenticateLiveProducer } from "../packages/collector-cli/src/codex-live-usage-auth";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { loadOrCreateLocalIngestAuth } from "../packages/collector-cli/src/local-auth";
import { planFreshLedgerCutover, switchFreshLedger } from "../packages/collector-cli/src/fresh-ledger-cutover";
import { setAccountAssertionAdapterEnabled } from "../packages/collector-cli/src/account-assertion";

const home = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "pr426-live-binding-")));
const ledger = path.join(home, "work-ledger.sqlite");
const root = path.join(home, "codex-root");
const epoch = "10000000-0000-4000-8000-000000000001";
const workspace = "30000000-0000-4000-8000-000000000003";
const device = "40000000-0000-4000-8000-000000000004";
const producerId = "fixture-producer";
const credentialId = "fixture-credential";
fs.mkdirSync(root);
const config = collectorConfigSchema.parse({ tenantId: workspace, deviceId: device, captureRoots: [{
  source: "codex", rootId: "fixture-root", profileId: "fixture-profile", directory: root,
  installationEpochId: epoch,
}] });
const options = { workspaceId: workspace, deviceId: device, freshCaptureRootEpoch: epoch };
let old: LocalEventBuffer | undefined;
let replacement: LocalEventBuffer | undefined;
try {
  old = new LocalEventBuffer(ledger, options);
  loadOrCreateLocalIngestAuth(home);
  const enrolled = provisionLiveProducer({ home, buffer: old, config, producerId, credentialId,
    captureRootId: "fixture-root", enrolledAt: new Date().toISOString() });
  const token = fs.readFileSync(enrolled.credentialFile, "utf8");
  const authenticated = authenticateLiveProducer(home, old, config, producerId, token);
  assert.equal(authenticated.binding.credentialId, credentialId);
  setAccountAssertionAdapterEnabled(old.database, "codex", true);
  old.database.prepare(`insert into codex_live_pins
    (source,session_id,producer_id,context_digest,context_json,claimed_at)
    select 'codex','fixture-session',producer_id,context_digest,context_json,?
    from codex_live_producers where producer_id=?`).run(new Date().toISOString(), producerId);
  old.database.prepare(`insert into codex_live_attachments
    (scope_digest,attachment_id,thread_id,checkpoint_json,held_reason)
    values(?,?,?,null,null)`).run(authenticated.scopeDigest, "attachment", "fixture-session");
  old.database.prepare(`insert into codex_live_packet_keys
    (scope_digest,kind,attachment_id,packet_key,packet_digest)
    values(?,?,?,?,?)`).run(authenticated.scopeDigest, "usage", "attachment", "packet", "digest");
  old.database.prepare(`insert into codex_live_receipts
    (scope_digest,kind,attachment_id,packet_key,packet_digest,receipt_json)
    values(?,?,?,?,?,?)`).run(authenticated.scopeDigest, "usage", "attachment", "packet", "digest", "{}");
  old.database.prepare(`insert into session_usage_authority
    (source,session_id,authority,claimed_at) values('codex','fixture-session','live',?)`)
    .run(new Date().toISOString());
  old.close(); old = undefined;
  const archiveDirectory = path.join(home, "archive");
  fs.mkdirSync(archiveDirectory, { mode: 0o700 });
  const archivePath = path.join(archiveDirectory, "archived-ledger.sqlite");
  const plan = planFreshLedgerCutover({ ledgerPath: ledger, archivePath, config });
  assert.equal(plan.status, "ready", plan.reason ?? "");
  assert.equal(plan.carriedRows.codex_live_bindings, 1);
  for (const table of ["codex_live_pins", "codex_live_attachments",
    "codex_live_packet_keys", "codex_live_receipts", "session_usage_authority"]) {
    assert.equal(plan.carriedRows[table], 1, table);
  }
  assert.equal(plan.carriedRows.account_assertion_adapters_v1, 1);
  switchFreshLedger({ ledgerPath: ledger, archivePath, config,
    authorityRoot: path.join(home, "lifecycle-authority") });
  replacement = new LocalEventBuffer(ledger, options);
  assert.equal(replacement.workspaceBinding()!.currentInstallationEpochId, epoch);
  console.log(JSON.stringify({ oldCredentialRegistered: true, replacementEpoch: epoch,
    replacementRows: replacement.database.prepare("select count(*) as n from codex_live_bindings").get(),
    planCarried: plan.carriedRows }));
  assert.doesNotThrow(() => authenticateLiveProducer(home, replacement!, config, producerId, token),
    "a retained live producer credential must remain usable after the replacement ledger starts");
  for (const table of ["codex_live_pins", "codex_live_attachments",
    "codex_live_packet_keys", "codex_live_receipts", "session_usage_authority"]) {
    assert.equal((replacement.database.prepare(`select count(*) as n from ${table}`).get() as {n:number}).n, 1, table);
  }
} finally {
  replacement?.close(); old?.close();
  fs.rmSync(home, { recursive: true, force: true });
}
