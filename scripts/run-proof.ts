import { spawnSync } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { build } from "esbuild";

const scriptPath = fileURLToPath(import.meta.url);
const repoRoot = path.resolve(path.dirname(scriptPath), "..");
const hash = (file: string) => createHash("sha256").update(fs.readFileSync(file)).digest("hex");

export function isolatedEnvironment(root: string): NodeJS.ProcessEnv {
  const dirs = { HOME: "home", USERPROFILE: "home", PLIMSOLL_HOME: "home/.plimsoll",
    CODEX_HOME: "home/.codex", CLAUDE_CONFIG_DIR: "home/.claude", XDG_CONFIG_HOME: "home/.config",
    XDG_CACHE_HOME: "home/.cache", XDG_STATE_HOME: "home/.local/state", TMPDIR: "tmp", TEMP: "tmp", TMP: "tmp" };
  const env: NodeJS.ProcessEnv = { PATH: process.env.PATH, LANG: "en_US.UTF-8", TZ: "UTC",
    PLIMSOLL_PROOF_ROOT: root, PLIMSOLL_PROOF_RUN_ID: randomUUID(),
    PLIMSOLL_PROOF_RECEIPT: path.join(root, "completion.json") };
  fs.writeFileSync(path.join(root, ".proof-root.json"), JSON.stringify({
    schema: "plimsoll.disposable-proof.v1", runId: env.PLIMSOLL_PROOF_RUN_ID,
  }), { mode: 0o600, flag: "wx" });
  for (const [key, relative] of Object.entries(dirs)) {
    env[key] = path.join(root, relative);
    fs.mkdirSync(env[key]!, { recursive: true, mode: 0o700 });
  }
  // Deliberate proof inputs only; no provider credentials, loaders, live roots,
  // proxy settings, production collector configuration or inherited NODE_PATH.
  for (const key of ["CI", "REJECTION_PROOF_SCALE", "PLIMSOLL_QUALIFICATION_ARTIFACT"]) {
    if (process.env[key]) env[key] = process.env[key];
  }
  env.npm_config_cache = path.join(root, "home/.cache/npm");
  env.npm_config_userconfig = path.join(root, "home/.npmrc");
  return env;
}

export async function runProof(entry: string, options: { directNode?: boolean; args?: string[]; quiet?: boolean } = {}) {
  const absoluteEntry = path.resolve(entry);
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-proof-")));
  const env = isolatedEnvironment(root);
  const nodeBefore = hash(process.execPath);
  const entryBefore = hash(absoluteEntry);
  const runnerBefore = hash(scriptPath);
  const sentinel = path.join(root, "sentinel-home");
  fs.mkdirSync(sentinel, { mode: 0o700 });
  fs.writeFileSync(path.join(sentinel, "must-remain"), "unrelated-home-sentinel\n");
  const sentinelBefore = hash(path.join(sentinel, "must-remain"));
  let receipt: any = null;
  let outcome: Record<string, unknown>;
  try {
    let childEntry = absoluteEntry;
    if (options.directNode) {
      childEntry = path.join(root, "proof.mjs");
      await build({ entryPoints: [absoluteEntry], bundle: true, platform: "node", target: "node20",
        format: "esm", outfile: childEntry, logLevel: "silent", packages: "external" });
    }
    const args = options.directNode ? [childEntry] : ["--import", path.join(repoRoot, "node_modules/tsx/dist/loader.mjs"), childEntry];
    const child = spawnSync(process.execPath, [...args, ...(options.args ?? [])], {
      cwd: repoRoot, env, encoding: "utf8", stdio: options.quiet ? "pipe" : "inherit",
      timeout: 600_000, maxBuffer: 16 * 1024 * 1024,
    });
    try { receipt = JSON.parse(fs.readFileSync(env.PLIMSOLL_PROOF_RECEIPT!, "utf8")); } catch { /* Missing completion fails. */ }
    const nodeUnchanged = hash(process.execPath) === nodeBefore;
    const sourceUnchanged = hash(absoluteEntry) === entryBefore && hash(scriptPath) === runnerBefore;
    const sentinelUnchanged = hash(path.join(sentinel, "must-remain")) === sentinelBefore;
    const valid = receipt?.schema === "plimsoll.proof-completion.v1" && receipt.runId === env.PLIMSOLL_PROOF_RUN_ID &&
      receipt.completed === true && receipt.status === "passed" && Array.isArray(receipt.checks) &&
      receipt.counts?.total > 0 && receipt.counts.total === receipt.checks.length &&
      receipt.counts.passed === receipt.checks.length && receipt.counts.failed === 0 &&
      receipt.checks.every((c: any) => typeof c.name === "string" && c.passed === true) &&
      (receipt.expectedChecks === null || receipt.expectedChecks === receipt.checks.length);
    outcome = { schema: "plimsoll.proof-run.v1", status: child.status === 0 && valid && nodeUnchanged && sentinelUnchanged && sourceUnchanged ? "passed" : "failed",
      entry: path.relative(repoRoot, absoluteEntry), entrySha256: entryBefore, runnerSha256: runnerBefore, sourceUnchanged, directNode: Boolean(options.directNode),
      runtime: { node: process.versions.node, abi: process.versions.modules, platform: process.platform, arch: process.arch, sha256: nodeBefore },
      exitCode: child.status, signal: child.signal, error: child.error?.message ?? null,
      ...(child.status !== 0 && options.quiet ? { diagnostic: (child.stderr ?? "").slice(-4000) } : {}),
      isolation: { nodeUnchanged, sentinelUnchanged, disposableHome: true, ambientEnvironment: "allowlist" }, receipt };
  } finally { fs.rmSync(root, { recursive: true, force: true }); }
  return outcome;
}

async function main() {
  const args = process.argv.slice(2);
  const directNode = args[0] === "--direct-node";
  if (directNode) args.shift();
  const entry = args.shift();
  if (!entry) throw new Error("usage: run-proof.ts [--direct-node] scripts/proof.ts [proof arguments]");
  const result = await runProof(entry, { directNode, args });
  const out = path.join(repoRoot, "evidence/completion", `${path.basename(entry, path.extname(entry))}${directNode ? "-direct-node" : ""}.json`);
  fs.mkdirSync(path.dirname(out), { recursive: true });
  fs.writeFileSync(out, `${JSON.stringify(result, null, 2)}\n`);
  console.log(JSON.stringify({ status: result.status, receipt: path.relative(repoRoot, out) }));
  if (result.status !== "passed") process.exitCode = 1;
}
if (process.argv[1] && path.resolve(process.argv[1]) === scriptPath) main().catch(error => { console.error(error); process.exitCode = 1; });
