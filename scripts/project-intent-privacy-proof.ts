import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { PROJECT_INTENT_OUTBOUND_FIELDS, projectIntentRequestSchema } from "../packages/shared/src/project-intent";
import { managedLifecyclePaths } from "../packages/collector-cli/src/lifecycle-adapters";
import { ProjectIntentProducer, type IntentLaunch, type IntentSession } from "../packages/collector-cli/src/project-intent-producer";
import { IntentStore } from "../packages/collector-cli/src/project-intent-store";
import { createIntentFixture } from "./lib/project-intent-fixture";

async function main() {
const f = await createIntentFixture();
try {
  const fixture = JSON.parse(fs.readFileSync("scripts/resource-proof/fixtures/metadata-privacy-sentinels.json", "utf8"));
  const planted = Object.values(fixture.sentinels) as string[];
  const project = (await f.p.choices()).projects[0];
  const launch = f.p.declare({ source: "codex", sourceRoot: f.sourceRoot, project,
    principal: "acct_p04_private_principal", work: { authority: "beads", namespace: "eco-p04-private", id: "eco-p04-private.1" },
    // Deliberately unexpected local producer fields must never become wire facts.
    ...Object.fromEntries(Object.entries(fixture.sentinels)),
  });
  const native = "PRIVATE_NATIVE_ID_MUST_STAY_LOCAL";
  const receipt = f.p.bind(launch.launchId, native)!;
  assert.equal((await f.p.sendSession(receipt.sessionId)).queued, 0);
  const posts = f.calls.filter(call => call.method === "POST");
  assert.equal(posts.length, 1);
  const wire = projectIntentRequestSchema.parse(JSON.parse(posts[0].body));
  assert.deepEqual(Object.keys(wire.receipt), PROJECT_INTENT_OUTBOUND_FIELDS);
  const privateTerms = [...planted, f.root, native, "acct_p04_private_principal", "eco-p04-private.1", "p04-synthetic-signing-key"];
  const outbound = JSON.stringify(f.calls);
  for (const term of privateTerms) assert.ok(!outbound.includes(term), "private input escaped outbound boundary");
  for (const call of f.calls) assert.ok(!call.url.includes(f.root));
  assert.equal(f.calls[0].body, "");

  // Native hook fixtures plant raw prompt/path/secret fields. Even local queue documents exclude their envelopes.
  const helper = await f.run(["launch", "claude", "--project", project.projectKey], { env: { FIXTURE_NATIVE_SESSION: crypto.randomUUID() } });
  assert.equal(helper.code, 0, "fixture helper did not bind");
  for (const id of f.p.store.ids("sessions")) {
    const text = JSON.stringify(f.p.store.read<IntentSession>("sessions", id));
    for (const term of ["PROMPT_DO_NOT_EXPORT", "SECRET_DO_NOT_EXPORT", "PROMPT_PATH_DO_NOT_EXPORT"]) assert.ok(!text.includes(term));
  }
  for (const kind of ["launches", "sessions", "defaults", "registry"]) {
    const directory = path.join(f.p.store.root, kind);
    assert.equal(fs.statSync(directory).mode & 0o077, 0);
    for (const name of fs.readdirSync(directory)) assert.equal(fs.statSync(path.join(directory, name)).mode & 0o077, 0);
  }
  const unsafeHome = path.join(f.root, "unsafe-collector"); fs.mkdirSync(unsafeHome, { mode: 0o700 });
  fs.symlinkSync(f.p.store.root, path.join(unsafeHome, "project-intents"));
  assert.throws(() => new IntentStore(unsafeHome), /collector_state_directory_unsafe/);

  const previousHome = process.env.PLIMSOLL_HOME;
  try {
    process.env.PLIMSOLL_HOME = path.join(f.root, "collector");
    assert.ok(managedLifecyclePaths({ homeDir: path.join(f.root, "home") }).history.includes(f.p.store.root));
  } finally {
    if (previousHome === undefined) delete process.env.PLIMSOLL_HOME; else process.env.PLIMSOLL_HOME = previousHome;
  }
  const dryRun = await f.run(["purge-local-data"]);
  assert.equal(dryRun.code, 0); assert.match(dryRun.stdout, /project intent evidence and queue/);
  const purged = await f.run(["purge-local-data", "--confirm"]);
  assert.equal(purged.code, 0, "fixture-only purge failed"); assert.equal(fs.existsSync(f.p.store.root), false);
  const restored = new ProjectIntentProducer(f.config, { directory: path.join(f.root, "collector") });
  const pending = restored.declare({ source: "codex", sourceRoot: f.sourceRoot, project: null });
  assert.equal(restored.bind(pending.launchId, native, { continuation: true }), null);
  assert.equal(restored.store.read<IntentLaunch>("launches", pending.launchId)!.localState, "awaiting_native_binding");
  console.log(JSON.stringify({ status: "passed", checks: 9, outboundReceipts: posts.length,
    privacy: "strict wire fields, empty signed GET, planted content absent, raw hook envelopes discarded, private state, symlinks rejected",
    erasure: "both inventories include state; fixture purge removes it; missing continuity remains pending" }));
} finally { await f.close(); }

}
main().catch(() => { console.error("project_intent_privacy_proof_failed"); process.exitCode = 1; });
