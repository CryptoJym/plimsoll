/** Darwin regression: a long tsx IPC name aliases an uncleared native pipe. */
import assert from "node:assert/strict";
import { spawnSync as rawSpawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn, spawnSync } from "./lib/proof-child-process";

const repo = path.resolve(import.meta.dirname, "..");
const cli = path.join(repo, "node_modules/tsx/dist/cli.mjs");
const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "ipc-")));

async function main() {
  try {
    assert.equal(process.platform, "darwin", "this regression targets the self-hosted Mac proof job");
    // libuv truncates Darwin socket names to 104 bytes. Keep the truncated
    // name inside our fixture, with three PID digits as its final component.
    const padding = 92 - Buffer.byteLength(root) - 1;
    assert.ok(padding > 0, "fixture parent must leave room for a 92-byte TMPDIR");
    const tmp = path.join(root, "t".repeat(padding));
    const fakePid = 424242;
    const uid = process.geteuid!();
    const requestedPipe = path.join(tmp, `tsx-${uid}`, `${fakePid}.pipe`);
    const stalePipe = Buffer.from(requestedPipe).subarray(0, 104).toString();
    fs.mkdirSync(path.dirname(requestedPipe), { recursive: true, mode: 0o700 });
    fs.writeFileSync(stalePipe, "stale native IPC pipe\n", { mode: 0o600, flag: "wx" });
    const preload = path.join(root, "reuse-pid.cjs");
    fs.writeFileSync(preload, `Object.defineProperty(process, "pid", { value: ${fakePid} });\n`);
    const entry = path.join(root, "entry.ts");
    fs.writeFileSync(entry, 'const argument: string = process.argv[2]; console.log(JSON.stringify({ argument, pid: process.pid, input: require("node:fs").readFileSync(0, "utf8"), marker: process.env.PROOF_CHILD_MARKER }));\n');
    const env = { ...process.env, TMPDIR: tmp, TMP: tmp, TEMP: tmp,
      TSX_DISABLE_CACHE: "1", PROOF_CHILD_MARKER: "preserved", NODE_OPTIONS: `--require ${JSON.stringify(preload)}` };
    const options = { cwd: repo, env, encoding: "utf8" as const, timeout: 5000, input: "fixture stdin" };
    const expected = { argument: "fixture-argument", pid: fakePid, input: "fixture stdin", marker: "preserved" };
    const staleInode = fs.lstatSync(stalePipe).ino;

    // Real pinned CLI, real unlink and real bind: only the PID is faked.
    const collision = rawSpawnSync(process.execPath, [cli, entry, "fixture-argument"], options);
    assert.notEqual(collision.status, 0, "unprotected CLI must reproduce the collision");
    assert.match(collision.stderr, /listen EADDRINUSE/);
    assert.equal(fs.existsSync(requestedPipe), false, "full-path cleanup misses the native name");
    console.log(JSON.stringify({ control: "EADDRINUSE", fakePid, requestedPipe, stalePipe,
      requestedBytes: Buffer.byteLength(requestedPipe), boundBytes: Buffer.byteLength(stalePipe) }));

    for (const [command, args] of [
      [process.execPath, [cli, entry, "fixture-argument"]],
      ["pnpm", ["exec", "tsx", entry, "fixture-argument"]],
    ] as const) {
      const child = spawnSync(command, [...args], options);
      assert.equal(child.status, 0, `proof child must execute despite the stale IPC pipe: ${child.stderr}`);
      assert.deepEqual(JSON.parse(child.stdout), expected);
    }
    const child = spawn(path.join(repo, "node_modules/.bin/tsx"), [entry, "fixture-argument"], {
      cwd: repo, env, stdio: ["pipe", "pipe", "pipe"], timeout: 5000,
    });
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", chunk => { stdout += chunk; });
    child.stderr.on("data", chunk => { stderr += chunk; });
    child.stdin.end("fixture stdin");
    const code = await new Promise<number | null>((resolve, reject) => {
      child.once("error", reject);
      child.once("close", resolve);
    });
    assert.equal(code, 0, stderr);
    assert.deepEqual(JSON.parse(stdout), expected);
    assert.equal(fs.lstatSync(stalePipe).ino, staleInode, "the unrelated stale socket must remain intact");
    assert.equal(fs.readFileSync(stalePipe, "utf8"), "stale native IPC pipe\n");
    console.log(JSON.stringify({ proof: "proof-tsx-ipc-pipe", status: "passed", invocations: 3 }));
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
}
main().catch(error => { console.error(error); process.exitCode = 1; });
