/** Run the catch-up fixture against three isolated source regressions. The
 * checkout is never modified; every mutant is copied under the CI proof home. */
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const repo = path.resolve(import.meta.dirname, "..");
const proofHome = fs.realpathSync(process.env.PLIMSOLL_PROOF_HOME ?? os.tmpdir());
const root = fs.mkdtempSync(path.join(proofHome, "uncovered-mutants-"));
const mutations = [
  {
    name: "recency_only",
    changes: [
      ["rollout-tailer.ts", "this.revisit.next()", "[]"],
      ["transcript-tailer.ts", "this.revisit.next()", "[]"],
    ],
    failingCheck: "no_tailer_row Codex old day",
  },
  {
    name: "oversized_stall",
    changes: [["jsonl-continuation.ts",
      'if (skippedBytes <= ABSOLUTE_MAX_READ_BYTES && skip.reason !== "enrollment_boundary_fragment")',
      'if (skip.reason !== "enrollment_boundary_fragment")']],
    failingCheck: "oversized non-usage record skips and later usage arrives",
  },
  {
    name: "rewrite_double_count",
    changes: [["rollout-tailer.ts",
      'id: deterministicEventId(["codex-rollout", state.conversationId, String(entry.index)]),',
      'id: crypto.randomUUID(),']],
    failingCheck: "same-inode rewrite completes without duplicate usage",
    addCrypto: true,
  },
] as const;

const results: Array<{ name: string; rejected: boolean; failedCheck: string; exitCode: number | null; detail?: string }> = [];
try {
  for (const mutation of mutations) {
    const mutant = path.join(root, mutation.name);
    fs.cpSync(repo, mutant, {recursive:true,filter:(candidate) => {
      const relative=path.relative(repo,candidate);
      return !relative.split(path.sep).some((part) => [".git","node_modules","ci-home","dist"].includes(part));
    }});
    fs.symlinkSync(path.join(repo,"node_modules"),path.join(mutant,"node_modules"),"dir");
    for (const [file,before,after] of mutation.changes) {
      const target=path.join(mutant,"packages/collector-cli/src",file);
      const source=fs.readFileSync(target,"utf8");
      assert(source.includes(before),`missing mutation anchor ${mutation.name}: ${file}`);
      fs.writeFileSync(target,source.replace(before,after));
    }
    if ("addCrypto" in mutation) {
      const target=path.join(mutant,"packages/collector-cli/src/rollout-tailer.ts");
      fs.writeFileSync(target,'import crypto from "node:crypto";\n'+fs.readFileSync(target,"utf8"));
    }
    const execution=spawnSync(process.execPath,[path.join(repo,"node_modules/tsx/dist/cli.mjs"),
      "scripts/uncovered-catchup-case.ts"],{cwd:mutant,env:{...process.env,PLIMSOLL_LARGE_CATCHUP:"0"},
      encoding:"utf8",timeout:1_800_000,maxBuffer:4*1024*1024});
    let rejected=false,detail="";
    try {
      const proof=JSON.parse(execution.stdout) as {checks:Array<{name:string;passed:boolean}>;passed:boolean};
      rejected=execution.status===1&&!proof.passed&&proof.checks.some((item)=>item.name===mutation.failingCheck&&!item.passed);
      detail=proof.checks.filter((item)=>!item.passed).map((item)=>item.name).join(", ");
    } catch {detail=(execution.stderr||execution.stdout).slice(-500);}
    results.push({name:mutation.name,rejected,failedCheck:mutation.failingCheck,exitCode:execution.status,detail});
    assert(rejected,`mutation escaped or fixture crashed: ${mutation.name}: ${detail}`);
  }
  console.log(JSON.stringify({schema:"plimsoll.uncovered-catchup-mutations.v1",results,passed:true},null,2));
} finally {fs.rmSync(root,{recursive:true,force:true});}
