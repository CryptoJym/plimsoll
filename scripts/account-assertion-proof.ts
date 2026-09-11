#!/usr/bin/env node

/** Adversarial, local-only proof for the Codex account assertion boundary. */
import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import {
  ACCOUNT_ASSERTION_SALT_FILE,
  ACCOUNT_ASSERTION_SALT_META_FILE,
  ACCOUNT_ASSERTION_STATE_KEY,
  accountAssertionV1Schema,
  codexAccountAssertionAt,
  deriveAccountActorHash,
  hashBindingRecord,
  loadCodexNativeAccountBinding,
  persistCodexAccountAssertion,
  readAccountAssertionAdapterState,
  resolveCodexSignedNativeEvidence,
  setAccountAssertionAdapterEnabled,
  storeAccountAssertionSalt,
} from "../packages/collector-cli/src/account-assertion";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import {
  captureRootDigest,
  rootEventMetadata,
  type CaptureRoot,
} from "../packages/collector-cli/src/capture-root-inventory";
import {
  LIVE_BINDINGS_FILE,
  authenticateLiveProducer,
  provisionLiveProducer,
} from "../packages/collector-cli/src/codex-live-usage-auth";
import { liveSha256 } from "../packages/collector-cli/src/codex-live-usage-protocol";
import { createProfileCapture } from "../packages/collector-cli/src/profile-capture";
import { loadOrCreateLocalIngestAuth } from "../packages/collector-cli/src/local-auth";

const TENANT_ID = "00000000-0000-4000-8000-000000000001";
const DEVICE_ID = "00000000-0000-4000-8000-000000000002";
const FIRST_AT = "2026-09-10T20:00:00.000Z";
const DELAYED_AT = "2026-09-10T20:15:00.000Z";
const DISABLED_AT = "2026-09-10T20:30:00.000Z";
const SECOND_AT = "2026-09-10T21:00:00.000Z";
const AFTER_SECOND = "2026-09-10T21:00:01.000Z";
const TAILER_SESSION = "019f0000-1111-7222-8333-444444444444";
const FLEET_SALT = Buffer.from("0123456789abcdef0123456789abcdef", "utf8");
const RAW_EMAIL = "account-assertion-proof@example.invalid";
const PROOF_SIGNING_KEYS = crypto.generateKeyPairSync("ed25519");

type Fixture = ReturnType<typeof fixture>;

function signedCodexAuth(accountId: string) {
  const encoded = (value: unknown) => Buffer.from(JSON.stringify(value)).toString("base64url");
  const signedBytes = [
    encoded({ alg: "EdDSA", typ: "JWT", kid: "proof-key" }),
    encoded({
      iss: "https://auth.openai.com",
      "https://api.openai.com/auth": {
        chatgpt_account_id: accountId,
        chatgpt_plan_type: "pro",
      },
    }),
  ].join(".");
  const signature = crypto.sign(null, Buffer.from(signedBytes, "utf8"), PROOF_SIGNING_KEYS.privateKey);
  assert.equal(crypto.verify(null, Buffer.from(signedBytes, "utf8"), PROOF_SIGNING_KEYS.publicKey, signature), true);
  const idToken = `${signedBytes}.${signature.toString("base64url")}`;
  return {
    record: { email: RAW_EMAIL, tokens: { id_token: idToken } },
    idToken,
    evidenceRef: `sha256:${crypto.createHash("sha256").update(idToken, "utf8").digest("hex")}`,
  };
}

function readNativeCodexAuth(record: Record<string, unknown>) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-native-codex-auth-"));
  fs.chmodSync(directory, 0o700);
  const file = path.join(directory, "auth.json");
  fs.writeFileSync(file, JSON.stringify(record), { mode: 0o600 });
  fs.chmodSync(file, 0o600);
  try { return loadCodexNativeAccountBinding(file); }
  finally { fs.unlinkSync(file); fs.rmdirSync(directory); }
}

function fixture(label: string, options: { salt?: boolean } = {}) {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), `plimsoll-account-r2-${label}-`));
  fs.chmodSync(home, 0o700);
  let now = FIRST_AT;
  const buffer = new LocalEventBuffer(path.join(home, "work-ledger.sqlite"), {
    workspaceId: TENANT_ID,
    deviceId: DEVICE_ID,
    enrollmentNow: () => new Date(now),
  });
  const root: CaptureRoot = {
    rootId: `${label}-codex-root`,
    profileId: `${label}-codex-profile`,
    installationEpochId: buffer.workspaceBinding()!.currentInstallationEpochId!,
    source: "codex",
    directory: path.join(home, "codex-root"),
  };
  fs.mkdirSync(root.directory, { mode: 0o700 });
  root.directory = fs.realpathSync(root.directory);
  const config = collectorConfigSchema.parse({
    tenantId: TENANT_ID,
    deviceId: DEVICE_ID,
    captureRoots: [root],
  });
  const localAuth = loadOrCreateLocalIngestAuth(home);
  if (options.salt !== false) {
    storeAccountAssertionSalt(home, FLEET_SALT, { tenantId: TENANT_ID, version: "salt-v1" });
  }
  return { home, buffer, root, config, localAuth, setNow(value: string) { now = value; } };
}

function provision(subject: Fixture, producerId: string, credentialId: string, at: string, accountId: string,
  extra: Record<string, unknown> = {}) {
  subject.setNow(at);
  const native = signedCodexAuth(accountId);
  const value = provisionLiveProducer({
    home: subject.home,
    buffer: subject.buffer,
    config: subject.config,
    producerId,
    credentialId,
    captureRootId: subject.root.rootId,
    enrolledAt: at,
    accountBinding: readNativeCodexAuth(native.record),
    ...extra,
  } as Parameters<typeof provisionLiveProducer>[0]);
  return { ...value, native };
}

function errorMessage(action: () => unknown) {
  try { action(); } catch (error) { return error instanceof Error ? error.message : String(error); }
  return "";
}

function databaseState(subject: Fixture) {
  return JSON.stringify(readAccountAssertionAdapterState(subject.buffer.database));
}

function workspaceState(subject: Fixture) {
  return JSON.stringify(subject.buffer.workspaceBinding());
}

function privateTreeFiles(directory: string): string[] {
  return fs.readdirSync(directory, { withFileTypes: true }).flatMap(entry => {
    const file = path.join(directory, entry.name);
    return entry.isDirectory() ? privateTreeFiles(file) : entry.isFile() ? [file] : [];
  });
}

function capacityRegistry(subject: Fixture) {
  const rootDigest = captureRootDigest(subject.root);
  const epoch = subject.buffer.workspaceBinding()!.currentInstallationEpochId!;
  return {
    schema: "plimsoll.live-producer-bindings.v1",
    bindings: Array.from({ length: 64 }, (_, index) => ({
      producerId: `capacity-producer-${index}`,
      credentialId: `capacity-credential-${index}`,
      tokenSha256: crypto.createHash("sha256").update(`capacity-token-${index}`).digest("hex"),
      source: "codex" as const,
      captureRootId: subject.root.rootId,
      profileId: subject.root.profileId,
      captureRootDigest: rootDigest,
      installationEpochId: epoch,
      enabled: true,
      enrolledAt: FIRST_AT,
    })),
  };
}

function rolloutLine(timestamp: string, type: string, payload: Record<string, unknown>) {
  return JSON.stringify({ timestamp, type, payload });
}

function tokenCountLine(timestamp: string, input: number, output: number) {
  return rolloutLine(timestamp, "event_msg", {
    type: "token_count",
    info: { total_token_usage: {
      input_tokens: input, cached_input_tokens: 0, output_tokens: output,
      reasoning_output_tokens: 0, total_tokens: input + output,
    } },
    rate_limits: { plan_type: "pro" },
  });
}

const opened: Fixture[] = [];
async function main() {
try {
  const primary = fixture("primary");
  opened.push(primary);
  const accountA = "11112222-3333-4444-8555-999900001111";
  const accountB = "22223333-4444-4555-8666-000011112222";
  const nativeA = signedCodexAuth(accountA);

  // P1 evidence: only the real Codex auth.json field is accepted. The
  // evidence reference is the digest of the exact signed id-token bytes.
  assert.equal(resolveCodexSignedNativeEvidence(nativeA.record), null);
  const nativeDirectory = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-native-codex-reread-"));
  fs.chmodSync(nativeDirectory, 0o700);
  const nativeFile = path.join(nativeDirectory, "auth.json");
  fs.writeFileSync(nativeFile, JSON.stringify(nativeA.record), { mode: 0o600 });
  fs.chmodSync(nativeFile, 0o600);
  const resolvedA = resolveCodexSignedNativeEvidence(loadCodexNativeAccountBinding(nativeFile));
  assert.deepEqual(resolvedA, { identity: accountA, evidenceRef: nativeA.evidenceRef });
  assert.deepEqual(resolveCodexSignedNativeEvidence(loadCodexNativeAccountBinding(nativeFile)), resolvedA);
  fs.unlinkSync(nativeFile);
  fs.rmdirSync(nativeDirectory);
  assert.equal(resolveCodexSignedNativeEvidence({ providerAccountId: accountA, credentialId: "routing-only" }), null);

  const enrollmentA = provision(primary, "proof-producer", "credential-a", FIRST_AT, accountA);
  assert.ok(enrollmentA.accountAssertion);
  const assertionA = accountAssertionV1Schema.parse(enrollmentA.accountAssertion);
  assert.equal(assertionA.evidenceRef, nativeA.evidenceRef);
  const tokenA = fs.readFileSync(enrollmentA.credentialFile, "utf8");
  const authenticatedA = authenticateLiveProducer(primary.home, primary.buffer, primary.config,
    "proof-producer", tokenA, primary.localAuth);
  assert.equal(accountAssertionV1Schema.parse(authenticatedA.root.account).actorHash, assertionA.actorHash);
  const captureStartedBeforeDisable = createProfileCapture(primary.buffer, { captureRoots: [primary.root] });
  const runningRollout = captureStartedBeforeDisable.rollout as unknown as {
    captureRoots: CaptureRoot[];
    accountAttributionEnabled: () => boolean;
  };
  assert.equal(runningRollout.accountAttributionEnabled(), true);
  assert.equal(rootEventMetadata(runningRollout.captureRoots[0], "before-disable", DELAYED_AT, undefined,
    runningRollout.accountAttributionEnabled()).captureAccountHash, assertionA.actorHash);

  // P1 fleet salt: the same native account under the same tenant salt hashes
  // identically on another collector, while an unsalted collector stays
  // explicitly unallocated and creates no local fallback salt.
  const peer = fixture("peer");
  opened.push(peer);
  const peerEnrollment = provision(peer, "peer-producer", "peer-credential", FIRST_AT, accountA);
  assert.equal(peerEnrollment.accountAssertion?.actorHash, assertionA.actorHash);
  assert.equal(deriveAccountActorHash(accountA, FLEET_SALT), assertionA.actorHash);
  const unsalted = fixture("unsalted", { salt: false });
  opened.push(unsalted);
  const unsaltedEnrollment = provision(unsalted, "unsalted-producer", "unsalted-credential", FIRST_AT, accountA);
  assert.equal(unsaltedEnrollment.accountAssertion, null);
  assert.equal(fs.existsSync(path.join(unsalted.home, ACCOUNT_ASSERTION_SALT_FILE)), false);
  assert.equal(fs.existsSync(path.join(unsalted.home, ACCOUNT_ASSERTION_SALT_META_FILE)), false);
  // A versioned assertion copied into static config is not authority.  If the
  // hosted salt is absent and enrollment is explicitly unallocated, neither
  // live authentication nor profile capture may revive that stale assertion.
  const staleUnsaltedRoot = unsalted.config.captureRoots![0];
  staleUnsaltedRoot.account = assertionA;
  const unsaltedToken = fs.readFileSync(unsaltedEnrollment.credentialFile, "utf8");
  const unsaltedAuth = authenticateLiveProducer(unsalted.home, unsalted.buffer, unsalted.config,
    "unsalted-producer", unsaltedToken, unsalted.localAuth);
  assert.equal(unsaltedAuth.root.account, undefined);
  assert.equal(unsaltedAuth.root.accountAssertions, undefined);
  const unsaltedCapture = createProfileCapture(unsalted.buffer, { captureRoots: [staleUnsaltedRoot] });
  const unsaltedRoots = (unsaltedCapture.rollout as unknown as { captureRoots: CaptureRoot[] }).captureRoots;
  assert.equal(unsaltedRoots[0]?.account, undefined);
  assert.equal(unsaltedRoots[0]?.accountAssertions, undefined);
  unsaltedCapture.close();
  delete staleUnsaltedRoot.account;
  staleUnsaltedRoot.accountAssertions = [assertionA];
  const unsaltedIntervalAuth = authenticateLiveProducer(unsalted.home, unsalted.buffer, unsalted.config,
    "unsalted-producer", unsaltedToken, unsalted.localAuth);
  assert.equal(unsaltedIntervalAuth.root.account, undefined);
  assert.equal(unsaltedIntervalAuth.root.accountAssertions, undefined);
  const unsaltedIntervalCapture = createProfileCapture(unsalted.buffer, { captureRoots: [staleUnsaltedRoot] });
  const unsaltedIntervalRoots = (unsaltedIntervalCapture.rollout as unknown as { captureRoots: CaptureRoot[] }).captureRoots;
  assert.equal(unsaltedIntervalRoots[0]?.account, undefined);
  assert.equal(unsaltedIntervalRoots[0]?.accountAssertions, undefined);
  unsaltedIntervalCapture.close();

  // P1 disable: an already authenticated producer loses the assertion
  // immediately. Disabling closes the old interval; re-enable alone cannot
  // revive it, and the next enrollment starts a fresh non-overlapping one.
  (setAccountAssertionAdapterEnabled as unknown as (
    db: typeof primary.buffer.database, source: "codex", enabled: boolean, at?: string,
  ) => unknown)(primary.buffer.database, "codex", false, DISABLED_AT);
  assert.equal(runningRollout.accountAttributionEnabled(), false);
  assert.equal(rootEventMetadata(runningRollout.captureRoots[0], "after-disable", DELAYED_AT, undefined,
    runningRollout.accountAttributionEnabled()).captureAccountHash, undefined);
  const disabledAuth = authenticateLiveProducer(primary.home, primary.buffer, primary.config,
    "proof-producer", tokenA, primary.localAuth);
  assert.equal(disabledAuth.root.account, undefined);
  assert.equal(disabledAuth.root.accountAssertions, undefined);
  const disabledCapture = createProfileCapture(primary.buffer, { captureRoots: [primary.root] });
  const disabledRoots = (disabledCapture.rollout as unknown as { captureRoots: CaptureRoot[] }).captureRoots;
  assert.equal(disabledRoots[0]?.account, undefined);
  assert.equal(disabledRoots[0]?.accountAssertions, undefined);
  disabledCapture.close();
  const afterDisable = readAccountAssertionAdapterState(primary.buffer.database).bindings.codex;
  assert.equal(afterDisable.find(row => row.assertion.actorHash === assertionA.actorHash)?.assertion.validUntil, DISABLED_AT);
  setAccountAssertionAdapterEnabled(primary.buffer.database, "codex", true);
  const reenabledOldAuth = authenticateLiveProducer(primary.home, primary.buffer, primary.config,
    "proof-producer", tokenA, primary.localAuth);
  assert.equal(reenabledOldAuth.root.account, undefined);
  assert.equal(reenabledOldAuth.root.accountAssertions, undefined);

  // P1 failover: all immutable windows hydrate, delayed pre-failover events
  // resolve A, post-failover live events resolve B, and B has a new epoch.
  const enrollmentB = provision(primary, "proof-producer", "credential-b", SECOND_AT, accountB);
  const assertionB = accountAssertionV1Schema.parse(enrollmentB.accountAssertion);
  assert.notEqual(enrollmentB.binding.installationEpochId, enrollmentA.binding.installationEpochId);
  assert.notEqual(assertionB.actorHash, assertionA.actorHash);
  const history = readAccountAssertionAdapterState(primary.buffer.database).bindings.codex
    .filter(row => row.rootId === primary.root.rootId)
    .sort((a, b) => a.assertion.validFrom.localeCompare(b.assertion.validFrom));
  assert.equal(history.length, 2);
  assert.equal(history[0].assertion.validUntil, DISABLED_AT);
  assert.equal(history[0].active, false);
  assert.equal(history[1].assertion.validFrom, SECOND_AT);
  assert.equal(history[1].active, true);
  assert.ok(Date.parse(history[0].assertion.validUntil!) <= Date.parse(history[1].assertion.validFrom));
  assert.equal(codexAccountAssertionAt(primary.buffer.database, primary.root.rootId, DELAYED_AT)?.actorHash,
    assertionA.actorHash);
  assert.equal(codexAccountAssertionAt(primary.buffer.database, primary.root.rootId, AFTER_SECOND)?.actorHash,
    assertionB.actorHash);
  const hydratedCapture = createProfileCapture(primary.buffer, { captureRoots: [primary.root] });
  const hydratedRoot = (hydratedCapture.rollout as unknown as { captureRoots: CaptureRoot[] }).captureRoots[0];
  assert.equal(hydratedRoot.accountAssertions?.length, 2);
  assert.equal(rootEventMetadata(hydratedRoot, "delayed-event", DELAYED_AT).captureAccountHash, assertionA.actorHash);
  assert.equal(rootEventMetadata(hydratedRoot, "new-event", AFTER_SECOND).captureAccountHash, assertionB.actorHash);
  hydratedCapture.close();
  const tokenB = fs.readFileSync(enrollmentB.credentialFile, "utf8");
  const authenticatedB = authenticateLiveProducer(primary.home, primary.buffer, primary.config,
    "proof-producer", tokenB, primary.localAuth);
  assert.equal(authenticatedB.context.installationEpochId, enrollmentB.binding.installationEpochId);
  assert.equal(accountAssertionV1Schema.parse(authenticatedB.root.account).actorHash, assertionB.actorHash);
  const bindingScopes = primary.buffer.database.prepare(
    "select scope_digest from codex_live_bindings where producer_id=?",
  ).all("proof-producer") as Array<{ scope_digest: string }>;
  assert.equal(new Set(bindingScopes.map(row => row.scope_digest)).size, 2);
  assert.equal(new Set(history.map(row => row.bindingKey)).size, 2);

  // Drive the already-running tailer through its real append/admission path.
  // The delayed event stays in A's epoch; the post-failover event uses B's
  // epoch, and both immutable populations reach the ledger exactly once.
  const rolloutDay = path.join(primary.root.directory, "2026", "09", "10");
  fs.mkdirSync(rolloutDay, { recursive: true });
  const rolloutFile = path.join(rolloutDay, `rollout-2026-09-10T20-14-58-${TAILER_SESSION}.jsonl`);
  fs.writeFileSync(rolloutFile, [
    rolloutLine("2026-09-10T20:14:58.000Z", "session_meta", {
      id: TAILER_SESSION, cwd: primary.root.directory, originator: "proof",
    }),
    rolloutLine("2026-09-10T20:14:59.000Z", "turn_context", {
      model: "gpt-5.5", cwd: primary.root.directory,
    }),
    tokenCountLine(DELAYED_AT, 10, 2),
    tokenCountLine(AFTER_SECOND, 20, 4),
  ].join("\n") + "\n");
  const failoverScan = await captureStartedBeforeDisable.rollout.scan({ scope: "full", now: new Date(AFTER_SECOND) });
  assert.equal(failoverScan.eventsAppended, 2, JSON.stringify(failoverScan));
  const failoverRows = primary.buffer.database.prepare(`select payload_json as payload,
    installation_epoch_id as epoch from buffered_events where session_id=? order by observed_at`).all(TAILER_SESSION) as
    Array<{ payload: string; epoch: string }>;
  assert.equal(failoverRows.length, 2);
  const failoverEvents = failoverRows.map(row => ({ payload: JSON.parse(row.payload) as {
    metadata: Record<string, unknown>; actorId?: string;
  }, epoch: row.epoch }));
  assert.deepEqual(failoverEvents.map(event => event.payload.actorId), [assertionA.actorHash, assertionB.actorHash]);
  assert.deepEqual(failoverEvents.map(event => event.payload.metadata.installationEpochId),
    [enrollmentA.binding.installationEpochId, enrollmentB.binding.installationEpochId]);
  assert.deepEqual(failoverEvents.map(event => event.epoch),
    [enrollmentA.binding.installationEpochId, enrollmentB.binding.installationEpochId]);
  captureStartedBeforeDisable.close();

  // Source-binding epochs are per binding. Enrolling another producer cannot
  // rotate the workspace out from under an existing producer.
  const multi = fixture("multi");
  opened.push(multi);
  const multiWorkspaceEpoch = multi.buffer.workspaceBinding()!.currentInstallationEpochId;
  const multiA = provision(multi, "multi-producer-a", "multi-credential-a", FIRST_AT, accountA);
  const multiB = provision(multi, "multi-producer-b", "multi-credential-b", SECOND_AT, accountB);
  assert.notEqual(multiA.binding.installationEpochId, multiB.binding.installationEpochId);
  assert.equal(multi.buffer.workspaceBinding()!.currentInstallationEpochId, multiWorkspaceEpoch);
  for (const [producerId, enrollment] of [["multi-producer-a", multiA], ["multi-producer-b", multiB]] as const) {
    const credential = fs.readFileSync(enrollment.credentialFile, "utf8");
    assert.equal(authenticateLiveProducer(multi.home, multi.buffer, multi.config,
      producerId, credential, multi.localAuth).binding.installationEpochId, enrollment.binding.installationEpochId);
  }

  // Once closed, a historical interval can never be overlapped by a backdated
  // assertion. The refusal leaves the additive maintenance record unchanged.
  const overlap = fixture("overlap");
  opened.push(overlap);
  const overlapEnrollment = provision(overlap, "overlap-producer", "overlap-credential", FIRST_AT, accountA);
  (setAccountAssertionAdapterEnabled as unknown as (
    db: typeof overlap.buffer.database, source: "codex", enabled: boolean, at?: string,
  ) => unknown)(overlap.buffer.database, "codex", false, SECOND_AT);
  setAccountAssertionAdapterEnabled(overlap.buffer.database, "codex", true);
  const overlapBefore = databaseState(overlap);
  const overlapFailure = errorMessage(() => persistCodexAccountAssertion(overlap.buffer.database,
    overlap.root.rootId, captureRootDigest(overlap.root), {
      ...accountAssertionV1Schema.parse(overlapEnrollment.accountAssertion), validFrom: DELAYED_AT, validUntil: null,
    }, `sha256:${"b".repeat(64)}`, crypto.randomUUID()));
  assert.match(overlapFailure, /account_assertion_window_overlap/);
  assert.equal(databaseState(overlap), overlapBefore);

  // P1 atomicity (reviewer's capacity probe): a full valid registry rejects a
  // 65th producer without changing workspace epoch, assertion history, the
  // active prior window, registry bytes, or live credential rows.
  const capacity = fixture("capacity");
  opened.push(capacity);
  provision(capacity, "existing-producer", "existing-credential", FIRST_AT, accountA);
  const capacityFile = path.join(capacity.home, LIVE_BINDINGS_FILE);
  fs.writeFileSync(capacityFile, JSON.stringify(capacityRegistry(capacity)), { mode: 0o600 });
  fs.chmodSync(capacityFile, 0o600);
  const capacityBefore = {
    state: databaseState(capacity),
    workspace: workspaceState(capacity),
    registry: fs.readFileSync(capacityFile, "utf8"),
    liveBindings: capacity.buffer.database.prepare("select count(*) as count from codex_live_bindings").get(),
  };
  const capacityFailure = errorMessage(() => provision(capacity, "overflow-producer", "overflow-credential",
    SECOND_AT, accountB));
  assert.match(capacityFailure, /live_registry_capacity/);
  assert.equal(databaseState(capacity), capacityBefore.state);
  assert.equal(workspaceState(capacity), capacityBefore.workspace);
  assert.equal(fs.readFileSync(capacityFile, "utf8"), capacityBefore.registry);
  assert.deepEqual(capacity.buffer.database.prepare("select count(*) as count from codex_live_bindings").get(),
    capacityBefore.liveBindings);
  assert.equal(fs.existsSync(path.join(capacity.home,
    `live-producer-${liveSha256("overflow-producer").slice(0, 32)}.token`)), false);

  // A registration-layer refusal occurs before publication and rolls back the
  // candidate assertion. This reproduces the real context-rebinding guard,
  // rather than an artificial throw inside the transaction.
  const registration = fixture("registration");
  opened.push(registration);
  provision(registration, "registration-producer", "registration-a", FIRST_AT, accountA);
  const secondRoot: CaptureRoot = {
    ...registration.root,
    rootId: "registration-second-root",
    profileId: "registration-second-profile",
    directory: path.join(registration.home, "codex-second-root"),
  };
  fs.mkdirSync(secondRoot.directory, { mode: 0o700 });
  secondRoot.directory = fs.realpathSync(secondRoot.directory);
  const secondConfig = collectorConfigSchema.parse({
    ...registration.config, captureRoots: [registration.root, secondRoot],
  });
  const registrationBefore = {
    state: databaseState(registration),
    registry: fs.readFileSync(path.join(registration.home, LIVE_BINDINGS_FILE), "utf8"),
    liveBindings: registration.buffer.database.prepare("select count(*) as count from codex_live_bindings").get(),
  };
  const registrationNative = signedCodexAuth(accountB);
  const registrationFailure = errorMessage(() => provisionLiveProducer({
    home: registration.home, buffer: registration.buffer, config: secondConfig,
    producerId: "registration-producer", credentialId: "registration-b",
    captureRootId: secondRoot.rootId, enrolledAt: SECOND_AT,
    accountBinding: readNativeCodexAuth(registrationNative.record),
  }));
  assert.match(registrationFailure, /live_producer_rebinding_forbidden/);
  assert.equal(databaseState(registration), registrationBefore.state);
  assert.equal(fs.readFileSync(path.join(registration.home, LIVE_BINDINGS_FILE), "utf8"), registrationBefore.registry);
  assert.deepEqual(registration.buffer.database.prepare("select count(*) as count from codex_live_bindings").get(),
    registrationBefore.liveBindings);

  // P1 atomicity (publication probe): fail after credential publication but
  // before registry publication; compensation restores both files and the
  // SQLite/workspace state exactly.
  const publication = fixture("publication");
  opened.push(publication);
  const publishedA = provision(publication, "publication-producer", "publication-a", FIRST_AT, accountA);
  const publicationRegistry = path.join(publication.home, LIVE_BINDINGS_FILE);
  const publicationBefore = {
    state: databaseState(publication),
    workspace: workspaceState(publication),
    registry: fs.readFileSync(publicationRegistry, "utf8"),
    credential: fs.readFileSync(publishedA.credentialFile, "utf8"),
    liveBindings: publication.buffer.database.prepare("select count(*) as count from codex_live_bindings").get(),
  };
  let publicationCalls = 0;
  const publicationFailure = errorMessage(() => provision(publication, "publication-producer", "publication-b",
    SECOND_AT, accountB, {
      afterFilePublication: (kind: string) => {
        publicationCalls += 1;
        if (kind === "registry") throw new Error("proof_registry_post_fsync_failure");
      },
    }));
  assert.match(publicationFailure, /proof_registry_post_fsync_failure/);
  assert.equal(publicationCalls, 2);
  assert.equal(databaseState(publication), publicationBefore.state);
  assert.equal(workspaceState(publication), publicationBefore.workspace);
  assert.equal(fs.readFileSync(publicationRegistry, "utf8"), publicationBefore.registry);
  assert.equal(fs.readFileSync(publishedA.credentialFile, "utf8"), publicationBefore.credential);
  assert.deepEqual(publication.buffer.database.prepare("select count(*) as count from codex_live_bindings").get(),
    publicationBefore.liveBindings);

  const tokenFailure = errorMessage(() => provision(publication, "publication-producer", "publication-c",
    SECOND_AT, accountB, {
      beforeFilePublication: (kind: string) => {
        if (kind === "credential") throw new Error("proof_credential_publication_failure");
      },
    }));
  assert.match(tokenFailure, /proof_credential_publication_failure/);
  assert.equal(databaseState(publication), publicationBefore.state);
  assert.equal(fs.readFileSync(publicationRegistry, "utf8"), publicationBefore.registry);
  assert.equal(fs.readFileSync(publishedA.credentialFile, "utf8"), publicationBefore.credential);

  // The bounded maintenance record refuses a 1,025th active root instead of
  // evicting an unrelated root's active assertion.
  const stateCapacity = fixture("state-capacity");
  opened.push(stateCapacity);
  const fullState = readAccountAssertionAdapterState(stateCapacity.buffer.database);
  const seededAssertion = accountAssertionV1Schema.parse({
    ...assertionA, validFrom: FIRST_AT, validUntil: null,
  });
  fullState.bindings.codex = Array.from({ length: 1024 }, (_, index) => ({
    rootId: `state-root-${index}`,
    bindingDigest: `sha256:${index.toString(16).padStart(64, "0")}`,
    assertion: seededAssertion, active: true, createdAt: FIRST_AT,
  }));
  stateCapacity.buffer.database.exec(`create table if not exists maintenance_state (
    key text primary key, value text not null, updated_at text not null)`);
  stateCapacity.buffer.database.prepare(`insert into maintenance_state(key,value,updated_at) values(?,?,?)
    on conflict(key) do update set value=excluded.value,updated_at=excluded.updated_at`)
    .run(ACCOUNT_ASSERTION_STATE_KEY, JSON.stringify(fullState), FIRST_AT);
  const fullStateBefore = databaseState(stateCapacity);
  const stateCapacityFailure = errorMessage(() => persistCodexAccountAssertion(stateCapacity.buffer.database,
    "state-overflow-root", captureRootDigest(stateCapacity.root), {
      ...seededAssertion, validFrom: SECOND_AT,
    }, `sha256:${"c".repeat(64)}`, crypto.randomUUID()));
  assert.match(stateCapacityFailure, /account_assertion_state_capacity/);
  assert.equal(databaseState(stateCapacity), fullStateBefore);

  // P2 canonicalisation: depth, width, byte, and cycle attacks all fail with
  // controlled contract errors rather than RangeError/stack overflow.
  let deep: unknown = null;
  for (let index = 0; index < 20_000; index += 1) deep = [deep];
  const deepFailure = errorMessage(() => hashBindingRecord(deep));
  const wideFailure = errorMessage(() => hashBindingRecord(
    Array.from({ length: 20_000 }, (_, index) => index),
    { maxBytes: 1024 * 1024 },
  ));
  const bytesFailure = errorMessage(() => hashBindingRecord("x".repeat(20_000)));
  const escapedValue = `quoted \" slash \\ newline\n ${"a".repeat(1_015)}😀tail`;
  assert.equal(hashBindingRecord({ escapedValue }), `sha256:${crypto.createHash("sha256")
    .update(JSON.stringify({ escapedValue }), "utf8").digest("hex")}`);
  const cyclic: { self?: unknown } = {};
  cyclic.self = cyclic;
  const cycleFailure = errorMessage(() => hashBindingRecord(cyclic));
  assert.match(deepFailure, /^account_binding_record_depth_exceeded$/);
  assert.match(wideFailure, /^account_binding_record_node_budget_exceeded$/);
  assert.match(bytesFailure, /^account_binding_record_too_large$/);
  assert.match(cycleFailure, /^account_binding_record_cyclic$/);
  for (const failure of [deepFailure, wideFailure, bytesFailure, cycleFailure]) assert.doesNotMatch(failure, /RangeError|call stack/i);

  // Hash-only privacy: neither account identity, email, nor the signed token
  // is present in the ledger, registry, credentials, receipts, or stdout.
  const durableText = privateTreeFiles(primary.home).map(file => fs.readFileSync(file).toString("utf8")).join("\n");
  for (const sentinel of [accountA, accountB, RAW_EMAIL, enrollmentA.native.idToken, enrollmentB.native.idToken]) {
    assert.equal(durableText.includes(sentinel), false);
  }

  const proof = {
    proof: "account-assertion-r2",
    completion: "passed",
    checks: {
      disableAuthoritativeAndFreshReenable: true,
      eventTimeFailoverAndNewEpoch: true,
      actualTailerFailoverAndEpochDedupe: true,
      multiProducerEpochIsolation: true,
      overlapRejected: true,
      provisioningCapacityAtomic: true,
      provisioningRegistrationAtomic: true,
      provisioningPublicationCompensated: true,
      activeStateCapacityFailClosed: true,
      fleetSaltNoLocalFallback: true,
      signedNativeEvidenceStable: true,
      canonicalisationBudgetsControlled: true,
      hashOnlyPrivacy: true,
    },
  };
  const proofOutput = JSON.stringify(proof);
  for (const sentinel of [accountA, accountB, RAW_EMAIL, enrollmentA.native.idToken, enrollmentB.native.idToken]) {
    assert.equal(proofOutput.includes(sentinel), false);
  }
  console.log(proofOutput);
} finally {
  for (const subject of opened) subject.buffer.close();
}
}

void main().catch(error => {
  console.error(error);
  process.exitCode = 1;
});
