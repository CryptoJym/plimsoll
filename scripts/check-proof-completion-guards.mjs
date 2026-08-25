#!/usr/bin/env node
/**
 * Static audit for issue #210: every async proof script under scripts/ must
 * install the shared completion guard (scripts/lib/completion-guard.ts).
 * Mirrors the issue's population method (async/await usage) plus a guard check.
 */
import fs from "node:fs";
import path from "node:path";
import url from "node:url";

const scriptsDir = path.join(path.dirname(url.fileURLToPath(import.meta.url)));
const files = [];
const walk = (dir) => {
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) walk(full);
    else if (entry.name.endsWith(".ts") && entry.name.includes("proof")) files.push(full);
  }
};
walk(scriptsDir);

const asyncProofs = [];
for (const file of files.sort()) {
  const src = fs.readFileSync(file, "utf8");
  const isAsync = /\basync\b/.test(src) && /\bawait\b/.test(src);
  if (!isAsync) continue;
  // The guard's own self-proof imports it; other proofs must import + install.
  const guarded =
    /installProofCompletionGuard\s*\(/.test(src) ||
    /from "[^"]*completion-guard"/.test(src);
  asyncProofs.push({ file: path.relative(scriptsDir, file), guarded });
}

const unguarded = asyncProofs.filter((entry) => !entry.guarded);
console.log(JSON.stringify({
  audit: "proof-completion-guards",
  asyncProofs: asyncProofs.length,
  guarded: asyncProofs.length - unguarded.length,
  unguarded: unguarded.map((entry) => entry.file),
}, null, 2));
process.exitCode = unguarded.length === 0 ? 0 : 1;
