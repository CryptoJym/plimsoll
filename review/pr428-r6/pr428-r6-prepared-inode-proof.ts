/** Recovery must leave a right-name prepared path with the wrong inode alone. */
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { collectorConfigSchema } from "../../packages/collector-cli/src/config";
import { joinRestartObligationPath, readJoinRestartObligation,
  writeJoinRestartObligation } from "../../packages/collector-cli/src/join-setup-journal";

const home = process.argv[3] ?? fs.mkdtempSync(path.join(process.cwd(), "pr428-r6-inode-"));
const final = joinRestartObligationPath(home);
if (process.argv[2] === "child") {
  const original = fs.linkSync;
  fs.linkSync = ((from: fs.PathLike, to: fs.PathLike) => {
    original(from, to);
    if (String(to) === final) process.kill(process.pid, "SIGKILL");
  }) as typeof fs.linkSync;
  const configBeforeRoots = `${JSON.stringify(collectorConfigSchema.parse({ port: 49390 }))}\n`;
  writeJoinRestartObligation(home, {
    port: 49390, configPath: path.join(home, "collector.config.json"), configBeforeRoots,
    priorManifestDigest: null, priorContent: null, priorOwnedTemplateContent: null,
    priorOwnedTemplateIdentityContent: null,
    replacementManifestDigest: `sha256:${"0".repeat(64)}`,
  });
  process.exit(0);
}
try {
  const tsx = path.join(process.cwd(), "node_modules/tsx/dist/cli.mjs");
  const child = spawnSync(process.execPath, [tsx, import.meta.filename, "child", home],
    { env: process.env, timeout: 20_000, encoding: "utf8" });
  assert.ok(child.signal === "SIGKILL" || child.status === 137,
    JSON.stringify({ status: child.status, signal: child.signal, stderr: child.stderr }));
  const journalName = fs.readdirSync(home).find((name) =>
    name.startsWith("join.restart-obligation.json.prepared-journal-"));
  assert.ok(journalName);
  const journal = JSON.parse(fs.readFileSync(path.join(home, journalName), "utf8")) as {
    preparedName: string;
  };
  const prepared = path.join(home, journal.preparedName);
  const movedOwnedLink = `${prepared}.moved-by-owner`;
  fs.renameSync(prepared, movedOwnedLink);
  fs.writeFileSync(prepared, "foreign replacement\n", { mode: 0o600 });
  assert.equal(fs.statSync(final).nlink, 2);
  let error: string | null = null;
  try { readJoinRestartObligation(home); }
  catch (caught) { error = caught instanceof Error ? caught.message : String(caught); }
  const observed = { error, preparedPreserved: fs.existsSync(prepared),
    movedOwnedLinkPreserved: fs.existsSync(movedOwnedLink),
    finalLinks: fs.statSync(final).nlink };
  console.log(JSON.stringify(observed));
  assert.match(error ?? "", /unowned hard link|changed ownership/i);
  assert.ok(observed.preparedPreserved && observed.movedOwnedLinkPreserved);
  assert.equal(observed.finalLinks, 2);
} finally { if (process.argv[2] !== "child") fs.rmSync(home, { recursive: true, force: true }); }
