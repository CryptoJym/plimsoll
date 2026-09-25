/** Published, checksum-pinned older CLIs against interrupted current retention. */
import { spawnSync } from "node:child_process";
import path from "node:path";
import { createProofCompletion } from "./lib/proof-completion";

const completion = createProofCompletion("lifecycle-cross-version", 1);
const root = path.resolve(import.meta.dirname, "..");
const result = spawnSync("python3", [path.join(root, "scripts/lib/lifecycle-cross-version.py")], {
  cwd: root, encoding: "utf8", timeout: 900_000, maxBuffer: 10 * 1024 * 1024,
});
if (result.stdout) process.stdout.write(result.stdout);
if (result.stderr) process.stderr.write(result.stderr);
const summary = (() => {
  try { return JSON.parse(result.stdout?.trim().split("\n").at(-1) ?? ""); } catch { return null; }
})();
completion.check("all_twelve_pinned_release_states_preserve_every_protected_byte",
  result.status === 0 && summary?.passed === 12 && summary?.total === 12 && summary?.liveStateTouched === false);
completion.complete();
