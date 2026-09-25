"use strict";
// Test-only fault injection. Each child receives exact fixture-root patterns.
const fs = require("node:fs");
const { syncBuiltinESMExports } = require("node:module");
const source = new RegExp(process.env.PLS_PROOF_RENAME_SOURCE);
const destination = new RegExp(process.env.PLS_PROOF_RENAME_DESTINATION);
const mode = process.env.PLS_PROOF_RENAME_MODE;
const rename = fs.renameSync;
let fired = false;
fs.renameSync = function (from, to) {
  const hit = !fired && source.test(String(from)) && destination.test(String(to));
  if (hit) fired = true;
  if (hit && mode === "kill-before") process.kill(process.pid, "SIGKILL");
  if (hit && mode === "throw") throw new Error("fixture rename interruption");
  const result = rename.apply(this, arguments);
  if (hit && mode === "kill-after") process.kill(process.pid, "SIGKILL");
  return result;
};
syncBuiltinESMExports();
