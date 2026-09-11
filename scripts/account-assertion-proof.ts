#!/usr/bin/env node

/** Synthetic, local-only proof for the Codex account assertion boundary. */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import {
  accountAssertionStatus,
  accountAssertionV1Schema,
  closeAccountAssertionWindow,
  deriveAccountActorHash,
  ensureAccountAssertionSalt,
  formatAccountAssertionStatusLine,
  readAccountAssertionAdapterState,
  setAccountAssertionAdapterEnabled,
} from "../packages/collector-cli/src/account-assertion";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { authenticateLiveProducer, provisionLiveProducer } from "../packages/collector-cli/src/codex-live-usage-auth";
import {
  appendRootObservation,
  rootEventMetadata,
  type CaptureRoot,
} from "../packages/collector-cli/src/capture-root-inventory";
import { loadOrCreateLocalIngestAuth } from "../packages/collector-cli/src/local-auth";
import type { AiInteractionEvent } from "../packages/shared/src/schemas";

const firstAt = "2026-09-10T20:00:00.000Z";
const secondAt = "2026-09-10T21:00:00.000Z";
const thirdAt = "2026-09-10T22:00:00.000Z";
const home = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-account-assertion-proof-"));
fs.chmodSync(home, 0o700);
const workspaceId = "00000000-0000-4000-8000-000000000001";
const deviceId = "00000000-0000-4000-8000-000000000002";
const buffer = new LocalEventBuffer(path.join(home, "ledger.sqlite"), {
  workspaceId,
  deviceId,
  enrollmentNow: () => new Date(firstAt),
});
const identity = "codex-proof-account-7f9a";
const email = "account-assertion-proof@example.invalid";
const rootBase: CaptureRoot = {
  rootId: "proof-codex-root",
  profileId: "proof-codex-profile",
  installationEpochId: buffer.workspaceBinding()!.currentInstallationEpochId!,
  source: "codex",
  directory: path.join(home, "codex-root"),
};
fs.mkdirSync(rootBase.directory, { mode: 0o700 });
const config = collectorConfigSchema.parse({ tenantId: workspaceId, deviceId, captureRoots: [rootBase] });
const localAuth = loadOrCreateLocalIngestAuth(home);

function event(id: string, observedAt: string, metadata: Record<string, unknown>): AiInteractionEvent {
  return {
    id,
    source: "codex",
    dataMode: "metadata",
    eventType: "usage_rollout",
    sessionId: "proof-session",
    tenantId: "00000000-0000-4000-8000-000000000001",
    observedAt,
    intent: "unknown",
    actionClass: "other",
    inputTokens: 3,
    outputTokens: 2,
    metadata,
  } as AiInteractionEvent;
}

function payload(id: string) {
  return (buffer.database.prepare("select payload_json from buffered_events where id=?").get(id) as { payload_json: string }).payload_json;
}

try {
  // A row admitted before the adapter is enabled is captured byte-for-byte.
  const prior = event("proof-prior", firstAt, { native: "prior" });
  assert.equal(buffer.append(prior, []), true);
  const priorBytes = payload(prior.id);

  const bindingA = { providerAccountId: identity, credentialId: "credential-a", email };
  const enrollmentA = provisionLiveProducer({
    home, buffer, config, producerId: "proof-producer", credentialId: "credential-a",
    captureRootId: rootBase.rootId, enrolledAt: firstAt, accountBinding: bindingA,
  });
  const assertionA = enrollmentA.accountAssertion;
  assert.ok(assertionA);
  assert.ok(accountAssertionV1Schema.safeParse(assertionA).success);
  const tokenA = fs.readFileSync(enrollmentA.credentialFile, "utf8");
  const authenticatedA = authenticateLiveProducer(home, buffer, config, "proof-producer", tokenA, localAuth);
  assert.equal(authenticatedA.root.account && accountAssertionV1Schema.parse(authenticatedA.root.account).actorHash,
    assertionA.actorHash);
  const salt = ensureAccountAssertionSalt(home);
  assert.equal(salt.length, 32);
  assert.equal(fs.statSync(path.join(home, "account-assertion.salt")).mode & 0o777, 0o600);
  assert.equal(deriveAccountActorHash(identity, home), deriveAccountActorHash(identity, salt));
  const rootA = { ...rootBase, account: assertionA };
  const metadataA = rootEventMetadata(rootA, "proof-new", secondAt, "proof-session") as Record<string, unknown>;
  assert.match(String(metadataA.captureAccountHash), /^sha256:[a-f0-9]{64}$/);
  assert.match(String(metadataA.accountEvidenceRef), /^sha256:[a-f0-9]{64}$/);
  assert.equal(appendRootObservation(buffer, event("proof-new", secondAt, metadataA), rootA), true);
  assert.equal(payload(prior.id), priorBytes, "prior payload changed");

  // A binding rotation closes only the prior enrollment window and starts a
  // fresh hash.  Existing event payloads are never relabeled.
  const bindingB = { providerAccountId: "codex-proof-account-failover", credentialId: "credential-b", email };
  const enrollmentB = provisionLiveProducer({
    home, buffer, config, producerId: "proof-producer", credentialId: "credential-b",
    captureRootId: rootBase.rootId, enrolledAt: thirdAt, accountBinding: bindingB,
  });
  const assertionB = enrollmentB.accountAssertion;
  assert.ok(assertionB);
  assert.notEqual(assertionA.actorHash, assertionB.actorHash);
  const tokenB = fs.readFileSync(enrollmentB.credentialFile, "utf8");
  const authenticatedB = authenticateLiveProducer(home, buffer, config, "proof-producer", tokenB, localAuth);
  assert.equal(authenticatedB.root.account && accountAssertionV1Schema.parse(authenticatedB.root.account).actorHash,
    assertionB.actorHash);
  assert.equal(assertionA.validUntil, null, "source object is immutable");
  const history = readAccountAssertionAdapterState(buffer.database).bindings.codex
    .filter(binding => binding.rootId === rootBase.rootId)
    .sort((a, b) => a.createdAt.localeCompare(b.createdAt));
  assert.equal(history.length, 2);
  const storedA = accountAssertionV1Schema.parse(history[0].assertion);
  assert.equal(storedA.validUntil, thirdAt);
  assert.equal(closeAccountAssertionWindow(storedA, thirdAt).validUntil, thirdAt);
  const rootB = { ...rootBase, account: assertionB };
  const oldMetadata = rootEventMetadata(rootA, "proof-old-window", secondAt, "proof-session") as Record<string, unknown>;
  const newMetadata = rootEventMetadata(rootB, "proof-new-window", thirdAt, "proof-session") as Record<string, unknown>;
  assert.equal(oldMetadata.captureAccountHash, assertionA.actorHash);
  assert.equal(newMetadata.captureAccountHash, assertionB.actorHash);
  appendRootObservation(buffer, event("proof-old-window", secondAt, oldMetadata), rootA);
  appendRootObservation(buffer, event("proof-new-window", thirdAt, newMetadata), rootB);

  // Disable only Codex. Claude Code and conductor capability state remains
  // enabled, and no new Codex assertion is produced.
  setAccountAssertionAdapterEnabled(buffer.database, "codex", false);
  const enrollmentDisabled = provisionLiveProducer({
    home, buffer, config, producerId: "proof-producer", credentialId: "credential-c",
    captureRootId: rootBase.rootId, enrolledAt: "2026-09-10T23:00:00.000Z",
    accountBinding: { providerAccountId: "codex-proof-disabled", credentialId: "credential-c" },
  });
  assert.equal(enrollmentDisabled.accountAssertion, null);
  const tokenDisabled = fs.readFileSync(enrollmentDisabled.credentialFile, "utf8");
  const authenticatedDisabled = authenticateLiveProducer(home, buffer, config, "proof-producer", tokenDisabled, localAuth);
  assert.equal(authenticatedDisabled.root.account, undefined);
  const disabledHistory = readAccountAssertionAdapterState(buffer.database).bindings.codex
    .filter(binding => binding.rootId === rootBase.rootId)
    .sort((a, b) => a.createdAt.localeCompare(b.createdAt));
  assert.equal(accountAssertionV1Schema.parse(disabledHistory.at(-1)!.assertion).validUntil, "2026-09-10T23:00:00.000Z");
  const status = accountAssertionStatus(buffer.database);
  assert.equal(status.find(row => row.source === "codex")?.enabled, false);
  assert.equal(status.find(row => row.source === "codex")?.rootsWithAssertion, 1);
  assert.equal(status.find(row => row.source === "claude_code")?.enabled, true);
  assert.equal(status.find(row => row.source === "conductor")?.enabled, true);
  assert.match(formatAccountAssertionStatusLine(buffer.database), /codex=disabled/);

  // The sentinel identity and email are supplied to the adapter but only
  // salted hashes/digests reach durable state, receipts, or this output.
  const tables = buffer.database.prepare("select name from sqlite_master where type='table'").all() as Array<{ name: string }>;
  const ledgerDump = JSON.stringify(tables.flatMap(({ name }) => {
    const quoted = `\"${name.replace(/\"/g, "\"\"")}\"`;
    return buffer.database.prepare(`select * from ${quoted}`).all();
  }));
  assert.equal(ledgerDump.includes(identity), false);
  assert.equal(ledgerDump.includes(email), false);
  const rows = buffer.database.prepare("select payload_json from buffered_events").all() as Array<{ payload_json: string }>;
  assert.equal(rows.some(row => row.payload_json.includes(identity) || row.payload_json.includes(email)), false);
  const receipts = buffer.database.prepare("select receipt_json from codex_live_receipts").all() as Array<{ receipt_json: string }>;
  assert.equal(receipts.some(row => row.receipt_json.includes(identity) || row.receipt_json.includes(email)), false);
  const walk = (directory: string): string[] => fs.readdirSync(directory, { withFileTypes: true }).flatMap(entry => {
    const file = path.join(directory, entry.name);
    return entry.isDirectory() ? walk(file) : entry.isFile() ? [file] : [];
  });
  assert.equal(walk(home).some(file => fs.readFileSync(file).includes(identity) || fs.readFileSync(file).includes(email)), false);

  const proofOutput = JSON.stringify({
    proof: "account-assertion",
    completion: "passed",
    checks: {
      preExistingRowsByteIdentical: true,
      v1HashAndEvidence: true,
      bindingWindowClosed: storedA.validUntil === thirdAt,
      failoverHashChanged: assertionA.actorHash !== assertionB.actorHash,
      codexDisableIsolated: true,
      rawIdentityAbsent: true,
    },
  });
  // Keep the stdout receipt itself privacy-safe; this assertion guards future
  // edits that might accidentally echo a fixture binding or email.
  assert.equal(proofOutput.includes(identity), false);
  assert.equal(proofOutput.includes(email), false);
  console.log(proofOutput);
} finally {
  buffer.close();
}
