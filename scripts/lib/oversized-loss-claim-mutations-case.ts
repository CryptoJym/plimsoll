/** Two old classifier failures must be rejected by the real claim proof.
 * Mutants live only under the CI proof home; the checkout is never edited. */
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const repo = path.resolve(import.meta.dirname, "../..");
const root = fs.mkdtempSync(path.join(fs.realpathSync(process.env.PLIMSOLL_PROOF_HOME ?? os.tmpdir()), "loss-claim-mutants-"));
const removeWholeRecordProof = {
  before: 'if (!proof || !probe || probe.escaped || probe.scanned !== recordBytes ||\n    probe.typeCount >= 3 || probe.payloadCount >= 2) return false;\n  return proof === "top_type" ? probe.typeCount === 1 :\n    probe.typeCount === 2 && probe.payloadCount === 1;',
  after: 'return true;',
};
const mutations = [
  {
    name: "lexical_first",
    changes: [{before: "const {type,payloadType}=recordTypes(prefix);\n  return classifyTypes(provider,type,payloadType);",
      after: `const text = prefix.subarray(0, 2048).toString("utf8");
  const type = text.match(/"type"\\s*:\\s*"([a-z_]+)"/)?.[1];
  const payloadType = text.match(/"payload"\\s*:\\s*\\{\\s*"type"\\s*:\\s*"([a-z_]+)"/)?.[1];
  return classifyTypes(provider,type,payloadType);`,
    },removeWholeRecordProof],
    falseKind: "codex_non_usage", source: "codex",
  },
  {
    name: "ambiguous_non_usage",
    changes: [{before:'return { kind: "unknown", usagePossible: true };',
      after:'return { kind: "unknown", usagePossible: false };'},removeWholeRecordProof],
    falseKind: "unknown", source: "claude_code",
  },
] as const;
const results: Array<{name:string;rejected:boolean;exitCode:number|null}> = [];
try {
  for (const mutation of mutations) {
    const mutant = path.join(root, mutation.name);
    fs.cpSync(repo, mutant, {recursive:true,filter:(candidate) =>
      !path.relative(repo,candidate).split(path.sep).some(part => [".git","node_modules","ci-home","dist"].includes(part))});
    fs.symlinkSync(path.join(repo,"node_modules"),path.join(mutant,"node_modules"),"dir");
    const target = path.join(mutant,"packages/collector-cli/src/capture-record-loss.ts");
    let source = fs.readFileSync(target,"utf8");
    for (const change of mutation.changes) {
      assert(source.includes(change.before),`missing mutation anchor: ${mutation.name}`);
      source=source.replace(change.before,change.after);
    }
    fs.writeFileSync(target,source);
    const execution = spawnSync(process.execPath,[path.join(repo,"node_modules/tsx/dist/cli.mjs"),
      "scripts/lib/oversized-loss-claim-case.mts"],{cwd:mutant,
      env:{...process.env,PLIMSOLL_SKIP_CLASSIFIER_CASES:"1"},encoding:"utf8",timeout:1_800_000,maxBuffer:1024*1024});
    let rejected = false;
    try {
      const proof = JSON.parse(execution.stdout) as {passed:boolean;eof:boolean;receipts:Array<{source:string;kind:string;usagePossible:number}>};
      rejected = execution.status === 1 && proof.passed === false && proof.eof &&
        proof.receipts.some(row => row.source === mutation.source && row.kind === mutation.falseKind && row.usagePossible === 0);
    } catch { /* A crash is not mutation rejection. */ }
    results.push({name:mutation.name,rejected,exitCode:execution.status});
    assert(rejected,`mutation escaped or fixture crashed: ${mutation.name}: ${(execution.stderr || execution.stdout).slice(-500)}`);
  }
  console.log(JSON.stringify({schema:"plimsoll.oversized-loss-claim-mutations.v1",results,passed:true},null,2));
} finally { fs.rmSync(root,{recursive:true,force:true}); }
