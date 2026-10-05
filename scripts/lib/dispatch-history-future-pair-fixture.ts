import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { spawnSync } from "node:child_process";
import { requireIsolatedProofEnvironment } from "./proof-completion";
import { DISPATCH_HISTORY_BUILD_PAIR } from "../../packages/collector-cli/src/dispatch-history-build-pair";
import { DISPATCH_HISTORY_ROLLBACK_READER } from "../../packages/collector-cli/src/dispatch-history-adoption";

/** Repository-local equivalent of the visible `rg --files packages scripts`
 * source set, including untracked/generated files. Only the repository's simple
 * positive basename ignore rules (optional root anchor/directory suffix) are
 * supported; new syntax refuses instead of silently omitting source. Ambient
 * Git excludes are deliberately not source authority. Paths are sorted because
 * rg's parallel enumeration order is unspecified; the three metadata files
 * remain appended by the caller. Like rg without --follow, file and directory
 * links are omitted; a listed path replaced by a link still fails the copy guard. */
export function futureDispatchHistorySourceFiles(repo: string): string[] {
  type Ignore = { base: string; anchored: boolean; directory: boolean; name: RegExp };
  const names: string[] = [];
  let visited = 0, ignoreBytes = 0, nameBytes = 0;
  function rules(relative: string, inherited: Ignore[]): Ignore[] {
    const result = [...inherited];
    for (const name of [".gitignore", ".ignore", ".rgignore"]) {
      const file = path.join(repo, relative, name);
      let fd: number;
      try { fd = fs.openSync(file, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW); }
      catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") continue; throw error; }
      let text: string;
      try {
        const stat = fs.fstatSync(fd);
        if (!stat.isFile() || stat.size > 64 * 1024 || (ignoreBytes += stat.size) > 1024 * 1024) throw new Error("future fixture ignore byte bound");
        const bytes = Buffer.alloc(stat.size + 1); let read = 0;
        while (read < bytes.length) { const n = fs.readSync(fd, bytes, read, bytes.length - read, null); if (!n) break; read += n; }
        if (read !== stat.size || fs.fstatSync(fd).size !== stat.size) throw new Error("future fixture ignore changed");
        text = bytes.subarray(0, read).toString("utf8");
      } finally { fs.closeSync(fd); }
      for (const line of text.split(/\r?\n/)) {
        if (!line || line.startsWith("#")) continue;
        const match = /^(\/)?([\w.*-]+)(\/)?$/.exec(line);
        if (!match) throw new Error("future fixture unsupported ignore rule: " + file);
        result.push({ base: relative, anchored: Boolean(match[1]), directory: Boolean(match[3]),
          name: new RegExp("^" + match[2].split("*").map(part => part.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")).join(".*") + "$") });
      }
    }
    return result;
  }
  function walk(relative: string, inherited: Ignore[], depth: number) {
    if (depth > 64 || !fs.lstatSync(path.join(repo, relative)).isDirectory()) throw new Error("future fixture directory refused");
    const ignored = rules(relative, inherited), directory = fs.opendirSync(path.join(repo, relative));
    try {
      let entry: fs.Dirent | null;
      while ((entry = directory.readSync())) {
        if (++visited > 16384) throw new Error("future fixture entry bound");
        if (entry.name.startsWith(".") || entry.isSymbolicLink()) continue;
        const child = path.posix.join(relative, entry.name);
        if (ignored.some(rule => (!rule.directory || entry!.isDirectory()) && (!rule.anchored || relative === rule.base) && rule.name.test(entry!.name))) continue;
        if (entry.isDirectory()) walk(child, ignored, depth + 1);
        else {
          if (!entry.isFile()) throw new Error("future fixture file refused");
          names.push(child); nameBytes += Buffer.byteLength(child) + 1;
          if (names.length > 2045 || nameBytes > 1024 * 1024) throw new Error("future fixture file bound");
        }
      }
    } finally { directory.closeSync(); }
  }
  const rootRules = rules("", []);
  for (const relative of ["packages", "scripts"]) walk(relative, rootRules, 0);
  return names.sort();
}

/** A separately identified future SOURCE fixture. No installed-version receipt,
 * production flag or caller context can activate the bridge being reviewed. */
export function runFutureDispatchHistorySourcePair(entry: string, bridgeSourceChecks?: unknown) {
  const root=requireIsolatedProofEnvironment();
  if(DISPATCH_HISTORY_BUILD_PAIR.mode!=="rollback-bridge")throw new Error("future fixture already selected");
  const allowed=["scripts/dispatch-history-qualified-proof.ts","scripts/dispatch-history-adoption-proof.ts","scripts/dispatch-binding-proof.ts"];
  if(!allowed.includes(entry))throw new Error("future source fixture entry refused");
  const repo=fs.realpathSync(process.cwd()),future=path.join(root,"future-dispatch-source-pair");fs.mkdirSync(future,{mode:0o700});
  const names=futureDispatchHistorySourceFiles(repo);
  names.push("package.json","pnpm-lock.yaml","tsconfig.json");
  if(names.length>2048)throw new Error("future fixture file bound");
  const records:Array<{path:string;sha256:string;bytes:number}>=[];let total=0;
  for(const name of new Set(names)){
    if(path.isAbsolute(name)||name.split(path.sep).includes("..")||name.includes("node_modules/")||name.includes("/dist/"))throw new Error("future fixture path refused");
    const source=path.join(repo,name),stat=fs.lstatSync(source);
    if(!stat.isFile()||stat.isSymbolicLink()||stat.size>8*1024*1024)throw new Error("future fixture file refused");
    total+=stat.size;if(total>64*1024*1024)throw new Error("future fixture byte bound");
    const target=path.join(future,name);fs.mkdirSync(path.dirname(target),{recursive:true,mode:0o700});fs.copyFileSync(source,target,fs.constants.COPYFILE_EXCL);
    records.push({path:name,sha256:crypto.createHash("sha256").update(fs.readFileSync(target)).digest("hex"),bytes:stat.size});
  }
  fs.symlinkSync(path.join(repo,"node_modules"),path.join(future,"node_modules"),"dir");
  const policy=path.join(future,"packages/collector-cli/src/dispatch-history-build-pair.ts");
  const before=fs.readFileSync(policy,"utf8");
  const replacement=before.replace('mode: "rollback-bridge", previousSourceCommit: "34d58bcd90865679e09fcbd1ee1703de5effda97"',
    `mode: "future-source-pair", previousSourceCommit: "${DISPATCH_HISTORY_ROLLBACK_READER.sourceCommit}"`)
    .replace('previousReadsHistory: false','previousReadsHistory: true').replace('qualificationScope: "installed-pair-required",','qualificationScope: "source-fixture-only",');
  if(replacement===before)throw new Error("future fixture policy binding failed");
  fs.writeFileSync(policy,replacement);
  const manifest={schema:"dispatch-history-future-source-pair-fixture/v1",sourceFixtureOnly:true,installedQualified:false,
    adoptionApproved:false,writerBaseCommit:DISPATCH_HISTORY_ROLLBACK_READER.sourceCommit,previousSource:DISPATCH_HISTORY_ROLLBACK_READER,
    productionBridgePolicySha256:crypto.createHash("sha256").update(before).digest("hex"),
    futureFixturePolicySha256:crypto.createHash("sha256").update(replacement).digest("hex"),files:records};
  fs.writeFileSync(path.join(future,"future-pair-source-manifest.json"),JSON.stringify(manifest,null,2)+"\n",{mode:0o600});
  const child=spawnSync(process.execPath,["--import",path.join(repo,"node_modules/tsx/dist/loader.mjs"),path.join(future,entry)],
    {cwd:future,env:process.env,stdio:"inherit",timeout:600000});
  if(child.status!==0||child.error)throw new Error("future source pair proof failed: "+child.status+" "+child.error?.message);
  const receipt=JSON.parse(fs.readFileSync(process.env.PLIMSOLL_PROOF_RECEIPT!,"utf8"));
  if(!receipt.completed||receipt.status!=="passed")throw new Error("future pair completion missing");
  fs.writeFileSync(process.env.PLIMSOLL_PROOF_RECEIPT!,JSON.stringify({...receipt,futureSourcePair:manifest,...(bridgeSourceChecks?{bridgeSourceChecks}:{})},null,2)+"\n",{mode:0o600});
  const evidence=path.join(future,"evidence");if(fs.existsSync(evidence))fs.cpSync(evidence,path.join(repo,"evidence"),{recursive:true});
}
