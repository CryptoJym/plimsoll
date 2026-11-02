import assert from "node:assert/strict";
import { execFileSync, spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";

import Database from "better-sqlite3";
import ts from "typescript";

import { composeLifecycleAdapter, SqliteOnlineBackupAdapter } from "../packages/collector-cli/src/lifecycle-adapters";
import { runLifecycleSnapshotCommand } from "../packages/collector-cli/src/lifecycle-command";
import { LifecycleInterruption, LifecycleManager, type LifecycleReadiness, type RuntimeArtifact } from "../packages/collector-cli/src/lifecycle";
import { collectorBufferPath } from "../packages/collector-cli/src/config";
import type { LifecycleServiceAdapter } from "../packages/collector-cli/src/lifecycle-filesystem";
import { createProofCompletion } from "./lib/proof-completion";

const completion = createProofCompletion("lifecycle-snapshot-remove", 49);
const root = fs.mkdtempSync(path.join(process.env.TMPDIR!, "snapshot-remove-"));
const hash = (bytes: Buffer) => createHash("sha256").update(bytes).digest("hex");
function tree(directory: string): string {
  if (!fs.existsSync(directory)) return "absent";
  return JSON.stringify(fs.readdirSync(directory).sort().map(name => {
    const file = path.join(directory, name);
    const stat = fs.lstatSync(file);
    return [name, stat.isDirectory() ? tree(file) : stat.isSymbolicLink() ? fs.readlinkSync(file) : hash(fs.readFileSync(file))];
  }));
}
function check(name: string, condition: unknown) {
  completion.check(name, Boolean(condition));
  assert.ok(condition, name);
  console.log(`PASS ${name}`);
}

async function fixture(name: string) {
  const home = path.join(root, name);
  const collector = path.join(home, ".plimsoll");
  fs.mkdirSync(collector, { recursive: true, mode: 0o700 });
  process.env.PLIMSOLL_HOME = collector;
  const ledger = collectorBufferPath(home);
  const db = new Database(ledger);
  db.exec("create table immutable_proof (id integer primary key, payload text); insert into immutable_proof values (1,'history')");
  db.close();
  let active: string | null = null;
  let serviceCalls = 0;
  const service: LifecycleServiceAdapter = {
    async activate(input) { active = input.version; serviceCalls++; },
    async restore(input) { active = input.version; serviceCalls++; },
    async remove() { serviceCalls++; },
    async readiness(expectedVersion): Promise<LifecycleReadiness> {
      return { ready: active === expectedVersion, runtimeVersion: active, serviceReady: true,
        configCompatible: true, databaseCompatible: true, reason: "ready" };
    },
    async supportSnapshot() { throw new Error("unused fixture boundary"); },
  };
  const lifecycleRoot = path.join(collector, "lifecycle");
  const freshAdapter = () => composeLifecycleAdapter({ homeDir: home, lifecycleRoot, artifactSourceRoot: home,
    service, database: new SqliteOnlineBackupAdapter() });
  const adapter = freshAdapter();
  const artifact = (version: string): RuntimeArtifact => {
    const sourcePath = path.join(home, `runtime-${version}.mjs`);
    fs.writeFileSync(sourcePath, `// fixture ${version}\n`);
    return { version, platform: "darwin", architecture: process.arch === "x64" ? "x64" : "arm64",
      nodeMajor: Number(process.versions.node.split(".")[0]), sourcePath,
      sha256: `sha256:${hash(fs.readFileSync(sourcePath))}` };
  };
  const manager = new LifecycleManager(adapter);
  await manager.update({ operationId: "first", artifact: artifact("0.7.39") });
  await manager.update({ operationId: "target", artifact: artifact("0.7.47") });
  return { adapter, freshAdapter, manager, ledger, lifecycleRoot, home, collector, serviceCalls: () => serviceCalls };
}

async function main() {
try {
  const f = await fixture("preview");
  const before = tree(f.home);
  const result = await runLifecycleSnapshotCommand({ adapter: f.adapter,
    argv: ["snapshots", "remove", "--id", "target", "--operation-id", "preview"] });
  check("targeted removal defaults to a preview without a receipt", result.kind === "remove" && result.receipt === null);
  check("preview changes no fixture byte", tree(f.home) === before);
  assert.equal(result.kind, "remove");
  if (result.kind !== "remove") throw new Error("unexpected command result");
  check("only route to 0.7.39 is named in the warning", result.removal.warnings.join(" ").includes("rollback to 0.7.39 will no longer be possible"));
  check("preview selects precisely one named snapshot", result.removal.retention?.removed.length === 1 && result.removal.retention.removed[0]?.name === "target");
  check("logical size is positive", (result.removal.snapshot?.bytes ?? 0) > 0);
  check("ordinary retention still protects the newest snapshot", (await f.manager.pruneSnapshots({ operationId: "ordinary" })).retention.removed.length === 0);
  const remove = (adapter = f.adapter, id = "target", operationId = "remove", apply = false) =>
    runLifecycleSnapshotCommand({ adapter, argv: ["snapshots", "remove", "--id", id, "--operation-id", operationId,
      ...(apply ? ["--apply", "--confirm-exact", id] : [])] });
  await assert.rejects(runLifecycleSnapshotCommand({ adapter: f.adapter, argv: ["snapshots", "remove", "--id", "target", "--apply"] }), /confirm-exact/);
  check("missing confirmation changes nothing", tree(f.home) === before);
  await assert.rejects(runLifecycleSnapshotCommand({ adapter: f.adapter, argv: ["snapshots", "remove", "--id", "target", "--apply", "--confirm-exact", "other"] }), /confirm-exact/);
  check("wrong confirmation changes nothing", tree(f.home) === before);
  const missing = await remove(f.adapter, "missing");
  check("missing target refuses with a known reason", missing.kind === "remove" && missing.removal.refusal === "snapshot_not_found");
  check("missing target preview is read only", tree(f.home) === before);

  // Exercise the real packaged command after the lane's one build, against this fixture only.
  const packaged = path.resolve(import.meta.dirname, "../packages/collector-cli/dist/cli.mjs");
  const packagedEnv = { ...process.env, HOME: f.home, USERPROFILE: f.home, PLIMSOLL_HOME: f.collector,
    CODEX_HOME: path.join(f.home, ".codex"), CLAUDE_CONFIG_DIR: path.join(f.home, ".claude"),
    XDG_CONFIG_HOME: path.join(f.home, ".config"), XDG_CACHE_HOME: path.join(f.home, ".cache"), XDG_STATE_HOME: path.join(f.home, ".state") };
  const cli = spawnSync(process.execPath, [packaged, "lifecycle", "snapshots", "remove", "--id", "target", "--json"],
    { env: packagedEnv, encoding: "utf8", timeout: 60_000 });
  check("packaged CLI previews targeted removal", cli.status === 0 && JSON.parse(cli.stdout).kind === "remove");
  check("packaged preview changes no fixture byte", tree(f.home) === before);
  const ledgerBefore = hash(fs.readFileSync(f.ledger));
  const runtimesBefore = tree(path.join(f.lifecycleRoot, "versions"));
  const receiptsBefore = ["first", "target"].map(id => fs.readFileSync(path.join(f.lifecycleRoot, "completed-operations", `${id}.json`)));
  const callsBefore = f.serviceCalls();
  const applied = await remove(f.adapter, "target", "approved-removal", true);
  check("confirmed command completes with an audited receipt", applied.kind === "remove" && applied.receipt?.status === "completed");
  check("only the target snapshot was removed", !fs.existsSync(path.join(f.lifecycleRoot, "snapshots/target")) && fs.existsSync(path.join(f.lifecycleRoot, "snapshots/first")));
  check("every runtime is retained by targeted removal", tree(path.join(f.lifecycleRoot, "versions")) === runtimesBefore);
  check("live ledger bytes are unchanged", hash(fs.readFileSync(f.ledger)) === ledgerBefore);
  check("no service boundary is called during removal", f.serviceCalls() === callsBefore);
  check("old completion receipts are byte-for-byte retained", ["first", "target"].every((id, index) => fs.readFileSync(path.join(f.lifecycleRoot, "completed-operations", `${id}.json`)).equals(receiptsBefore[index]!)));
  check("durable completion receipt retains rollback warning", JSON.parse(fs.readFileSync(path.join(f.lifecycleRoot, "completed-operations/approved-removal.json"), "utf8")).snapshotRemoval.warnings[0].includes("0.7.39"));
  check("removal intent is committed after the receipt", !fs.existsSync(path.join(f.lifecycleRoot, "removals/approved-removal.json")));
  await assert.rejects(remove(f.adapter, "first", "approved-removal", true), /already completed|already used|operationId/);
  check("reuse of completed operation ID preserves other snapshot", fs.existsSync(path.join(f.lifecycleRoot, "snapshots/first")));

  const busy = await fixture("unfinished");
  for (const phase of ["prepared", "snapshotted", "staged", "switched", "verified", "rollback_required", "rollback_complete"] as const) {
    const journalPath = path.join(busy.lifecycleRoot, "journal.json");
    const journal = { schemaVersion: 1, operationId: "target", kind: "update", fromVersion: "0.7.39", toVersion: "0.7.47", phase, snapshotId: "target" };
    fs.writeFileSync(journalPath, JSON.stringify(journal));
    const snapshots = tree(path.join(busy.lifecycleRoot, "snapshots"));
    const preview = await remove(busy.adapter, "target", `pending-${phase}`);
    const blocked = await remove(busy.adapter, "target", `pending-${phase}`, true);
    check(`preview and apply refuse unfinished phase ${phase}`, preview.kind === "remove" && preview.removal.refusal === "unfinished_lifecycle_operation" && blocked.kind === "remove" && blocked.receipt?.status === "refused");
    check(`unfinished phase ${phase} preserves snapshot and journal`, tree(path.join(busy.lifecycleRoot, "snapshots")) === snapshots && fs.readFileSync(journalPath, "utf8") === JSON.stringify(journal));
  }
  fs.rmSync(path.join(busy.lifecycleRoot, "journal.json"));
  check("a competing mutation lease is admitted once", await busy.adapter.acquireLock("competing"));
  try { await assert.rejects(remove(busy.adapter, "target", "contended", true), /owns the lock/); }
  finally { await busy.adapter.releaseLock("competing"); }
  check("contended removal preserves target", fs.existsSync(path.join(busy.lifecycleRoot, "snapshots/target")));
  const marker = path.join(busy.lifecycleRoot, "completed-operations/target.json");
  fs.writeFileSync(marker, "{torn");
  const unknown = await remove(busy.adapter);
  check("unreadable operation evidence refuses deletion", unknown.kind === "remove" && unknown.removal.status === "refused");

  const crash = await fixture("rename-crash");
  const fence = crash.adapter.assertFence!.bind(crash.adapter);
  crash.adapter.assertFence = async id => {
    await fence(id);
    if (!fs.existsSync(path.join(crash.lifecycleRoot, "snapshots/target"))) throw new LifecycleInterruption("fixture interruption after rename");
  };
  await assert.rejects(remove(crash.adapter, "target", "crashed", true), /fixture interruption/);
  const intentPath = path.join(crash.lifecycleRoot, "removals/crashed.json");
  const intent = JSON.parse(fs.readFileSync(intentPath, "utf8"));
  check("crash after rename retains durable confirmed intent", intent.requiresCliVersion === "snapshot-remove-v1" && intent.snapshotRemoval.snapshotId === "target");
  check("crash leaves exactly the target in recorded trash", fs.existsSync(path.join(crash.lifecycleRoot, "trash", intent.items[0].trashName)));
  const baseSource = execFileSync("git", ["show", "71d6ff27f0d39aa31d188c9bcc31d37bf188c384:packages/collector-cli/src/lifecycle-filesystem.ts"], { encoding: "utf8" });
  const parser = baseSource.slice(baseSource.indexOf("function parseRemovalRecord("), baseSource.indexOf("/** Whether a durable receipt"));
  const javascript = ts.transpileModule(parser, { compilerOptions: { target: ts.ScriptTarget.ES2022 } }).outputText;
  const oldParse = new Function("TRASH_NAME", "MAX_COMPLETION_MARKERS", "isBoundedIdentifier", `${javascript}\nreturn parseRemovalRecord;`)(
    /^(snapshot|runtime_version)\+([A-Za-z0-9][A-Za-z0-9._-]{0,95})\+[0-9a-f]{12}$/, 100_000,
    (id: string) => /^[A-Za-z0-9][A-Za-z0-9._-]{0,95}$/.test(id) && !id.includes(".."));
  check("pinned main's exact parser refuses new removal intent", oldParse(intent, "crashed") === null);
  const recoveryAdapter = crash.freshAdapter();
  const pending = await remove(recoveryAdapter, "first");
  check("targeted removal refuses existing pending recovery", pending.kind === "remove" && pending.removal.refusal === "pending_removal_or_restore");
  const recovery = await new LifecycleManager(recoveryAdapter).pruneSnapshots({ operationId: "recover-crash", apply: true });
  check("prune recovery finishes explicit removal instead of restoring rollback", recovery.retention.recovered.some(item => item.name === "target") && !fs.existsSync(path.join(crash.lifecycleRoot, "snapshots/target")));
  check("recovered receipt preserves explicit rollback loss", recovery.receipt?.retention?.snapshotRemovals?.[0]?.rollbackUnavailable === true);
  check("recovery commits its original intent", !fs.existsSync(intentPath));

  const receiptFailure = await fixture("receipt-failure");
  const persist = receiptFailure.adapter.persistReceipt.bind(receiptFailure.adapter);
  receiptFailure.adapter.persistReceipt = async receipt => {
    if (receipt.operation === "snapshots_remove") throw new Error("fixture receipt write failed");
    await persist(receipt);
  };
  await assert.rejects(remove(receiptFailure.adapter, "target", "receipt-failed", true), /receipt write failed/);
  check("receipt failure leaves a durable record of actual removal", fs.existsSync(path.join(receiptFailure.lifecycleRoot, "removals/receipt-failed.json")) && !fs.existsSync(path.join(receiptFailure.lifecycleRoot, "snapshots/target")));
  const finish = await new LifecycleManager(receiptFailure.freshAdapter()).pruneSnapshots({ operationId: "recover-receipt", apply: true });
  check("recovery accounts for deletion before a failed receipt", finish.retention.recovered.some(item => item.name === "target") && finish.receipt?.retention?.snapshotRemovals?.[0]?.snapshotId === "target");

  const raced = await fixture("same-id-race");
  const racedFence = raced.adapter.assertFence!.bind(raced.adapter);
  let fenceCalls = 0;
  const unrelatedTrash = path.join(raced.lifecycleRoot, "trash/snapshot+unrelated+111111111111");
  raced.adapter.assertFence = async id => {
    await racedFence(id);
    if (++fenceCalls === 2) {
      fs.mkdirSync(unrelatedTrash, { recursive: true });
      fs.writeFileSync(path.join(unrelatedTrash, "keep"), "another pending operation");
      fs.mkdirSync(path.join(raced.lifecycleRoot, "removals"), { recursive: true });
      fs.writeFileSync(path.join(raced.lifecycleRoot, `removals/${id}.json`), JSON.stringify({
        schemaVersion: 1, operationId: id, requiresCliVersion: "0.7.41",
        items: [{ kind: "snapshot", name: "unrelated", bytes: 25,
          trashName: "snapshot+unrelated+111111111111", origin: "planned" }],
      }));
    }
  };
  await assert.rejects(remove(raced.adapter, "target", "raced-removal", true), /another_removal_pending/);
  check("same-ID pending intent race preserves unrelated trash", fs.readFileSync(path.join(unrelatedTrash, "keep"), "utf8") === "another pending operation");
  check("same-ID pending intent race preserves the target", fs.existsSync(path.join(raced.lifecycleRoot, "snapshots/target")));
  completion.complete();
} finally {
  fs.rmSync(root, { recursive: true, force: true });
}
}
void main().catch(error => { console.error(error); process.exitCode = 1; });
