import { AsyncLocalStorage } from "node:async_hooks";
import crypto from "node:crypto";
import { spawnSync } from "node:child_process";
import { createRequire } from "node:module";
import fs from "node:fs";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { z } from "zod";
import { DISPATCH_HISTORY_BUILD_PAIR } from "./dispatch-history-build-pair";
import { assertPrivateStateDirectory, readPrivateStateFile } from "./collector-state-io";

/** Frozen bridge consumers plus a complete source/runtime artifact witness.
 * This probe is NEVER the released/installed previous version. This build's
 * installed previous target remains released 34d58bcd, so adoption stays OFF.
 * A later reviewed build must bind its actual installed bridge pair separately. */
export const DISPATCH_HISTORY_ROLLBACK_READER = Object.freeze({
  protocol: "plimsoll.dispatch-history-bridge-reader/v2" as const,
  sourceCommit: "7f53dd8e1b08b369d95b7471692bb0d83adefd92",
  sourceTree: "b6f2df6668d56b9eddd7814b6141bd1a478afdef",
  artifactSha256: "3ce6b28122eb9cfd88de8389a798f5a951f8e77201d78cf03beb3342e956a9b8",
  completeRuntimeSha256: "bbfc9f6dc459566e7f489f86b8afec8b5c6052b3aaff266036e0f124f1cd37a3",
  consumerSourceSha256: "acf519a3dfbd9af40aa78a40f00d267f0c71badce391327870885f489b9ff6b4",
  frozenLockSha256: "5b07af7eac1bb338f36cd902a7ceb00ddfde64e722a9c2a60ac1fd69dc741a2e",
  collectorVersion: "0.7.48" as const, vanillaCompatible: false as const, readOnly: true as const,
});
export const DISPATCH_HISTORY_ADOPTION_LIMITS = Object.freeze({
  contractBytes: 64 * 1024, artifactBytes: 2 * 1024 * 1024,
  runtimeArtifactBytes: 8 * 1024 * 1024, namedUsageLedgerBytes: 32 * 1024 * 1024,
  inputBytes: 96 * 1024 * 1024, outputBytes: 64 * 1024,
  qualificationMs: 2_000, writerMs: 5_000, validityMs: 60_000,
});
const hash = (value: string | Buffer) => crypto.createHash("sha256").update(value).digest("hex");
const sha = z.string().regex(/^[a-f0-9]{64}$/);
const rootInventorySchema = z.object({
  rootDigest: sha, rootMetadataSha256: sha, inventorySha256: sha,
  hot: z.number().int().min(0).max(1000), historical: z.number().int().min(0).max(4096),
  total: z.number().int().min(0).max(5096),
}).strict();
export type DispatchRollbackRootInventory = z.infer<typeof rootInventorySchema>;
export const dispatchHistoryAdoptionContractSchema = z.object({
  schema: z.literal("dispatch-history-source-adoption/v2"), qualificationId: z.string().uuid(),
  sourceQualificationOnly: z.literal(true), rolloutAuthorized: z.literal(false),
  writerBaseCommit: z.literal(DISPATCH_HISTORY_ROLLBACK_READER.sourceCommit),
  reader: z.object({
    protocol: z.literal(DISPATCH_HISTORY_ROLLBACK_READER.protocol),
    sourceCommit: z.literal(DISPATCH_HISTORY_ROLLBACK_READER.sourceCommit),
    artifactSha256: z.literal(DISPATCH_HISTORY_ROLLBACK_READER.artifactSha256),
    sourceTree: z.literal(DISPATCH_HISTORY_ROLLBACK_READER.sourceTree),
    completeRuntimeSha256: z.literal(DISPATCH_HISTORY_ROLLBACK_READER.completeRuntimeSha256),
    consumerSourceSha256: z.literal(DISPATCH_HISTORY_ROLLBACK_READER.consumerSourceSha256),
    frozenLockSha256: z.literal(DISPATCH_HISTORY_ROLLBACK_READER.frozenLockSha256),
    collectorVersion: z.literal("0.7.48"), vanillaCompatible: z.literal(false), readOnly: z.literal(true),
  }).strict(),
  runtime: z.object({ node: z.string().min(1).max(32), abi: z.literal("127"), platform: z.string().min(1).max(32),
    arch: z.string().min(1).max(32), nodeSha256: sha, nativeDependencySha256: sha,
    nativeDependencyVersion: z.literal("12.10.0"), frozenLockSha256: z.literal(DISPATCH_HISTORY_ROLLBACK_READER.frozenLockSha256),
  }).strict(),
  namedUsage: z.object({ ledgerSha256: sha, sealedSha256: sha, now: z.iso.datetime(),
    workspaceId: z.string().uuid(), deviceId: z.string().min(1).max(128) }).strict(),
  issuedAt: z.iso.datetime(), validUntil: z.iso.datetime(),
  sourceProfileSha256: sha, nextProfileSha256: sha, imageSha256: sha,
  generation: z.string().uuid(), inventorySha256: sha, terminalSha256: sha,
  roots: z.array(rootInventorySchema).min(1).max(64),
  operatingRequirements: z.object({
    holdOlderReadersBeforeAdoption: z.literal(true), holdAllWritersDuringRollback: z.literal(true),
    originalProfileAndImagesRetained: z.literal(true),
    exactReaderAndFrozenDependenciesRetained: z.literal(true), vanillaDowngradeForbidden: z.literal(true),
  }).strict(),
}).strict();
export type DispatchHistoryAdoptionContract = z.infer<typeof dispatchHistoryAdoptionContractSchema>;
export type DispatchHistoryAdoptionRequest = Readonly<{
  sourceProfileSha256: string; nextProfileSha256: string; imageSha256: string; generation: string;
  inventorySha256: string; terminalSha256: string; roots: readonly DispatchRollbackRootInventory[];
  now: string;
}>;
export type DispatchHistoryAdoptionInput = {
  contract: unknown; readerArtifactPath: string; runtimeArtifactPath: string;
  dependencyDirectory: string; scratchDirectory: string;
  namedUsage: { ledgerArtifactPath: string; now: string; workspaceId: string; deviceId: string; sealedSha256: string };
};
type Publication = {
  sourceProfileSha256: string | null; sourcePath: string; started: number;
  normalizeRoots: (roots: readonly unknown[]) => unknown[];
  profileForRoots: (normalizedRoots: readonly unknown[]) => Buffer;
};
const adoptions = new AsyncLocalStorage<(request: DispatchHistoryAdoptionRequest) => DispatchHistoryAdoptionInput>();
const publications = new AsyncLocalStorage<Publication>();
// Capabilities are minted only after the pinned reader succeeds. Never accept
// successful qualification hashes from public context input or retained objects.
const qualifiedPublications = new WeakMap<Publication, Readonly<{
  rootsSha256: string; profileSha256: string; sourceProfileSha256: string;
  readerArtifactSha256: string; nodeAbi: string; sourceIdentitySha256: string;
  readerArtifactPath: string; runtimeArtifactPath: string; dependencyDirectory: string;
  runtimeFingerprint: string; namedUsageLedgerPath: string; namedUsageLedgerSha256: string;
}>>();

/** Explicit, call-scoped technical qualification. No CLI flag, environment default,
 * config receipt, or outbox proof enables this context. External owner integration
 * supplies the source-bound contract; it must separately obtain L2 adoption approval. */
export function withDispatchHistoryAdoption<T>(
  supply: (request: DispatchHistoryAdoptionRequest) => DispatchHistoryAdoptionInput, action: () => T,
): T { return adoptions.run(supply, action); }
/** A source receipt cannot enable this bridge or change its installed pair. */
export function assertDispatchHistoryBuildPairQualified(roots?: Parameters<typeof dispatchHistoryAdoptionRequired>[0]) {
  if (DISPATCH_HISTORY_BUILD_PAIR.mode !== "future-source-pair" ||
      !DISPATCH_HISTORY_BUILD_PAIR.previousReadsHistory ||
      DISPATCH_HISTORY_BUILD_PAIR.previousSourceCommit !== DISPATCH_HISTORY_ROLLBACK_READER.sourceCommit ||
      DISPATCH_HISTORY_BUILD_PAIR.previousCollectorVersion !== DISPATCH_HISTORY_ROLLBACK_READER.collectorVersion) {
    if(roots)dispatchHistoryAdoptionRequired(roots);
    throw new Error("dispatch_history_adoption_required_bridge: actual installed previous " +
      DISPATCH_HISTORY_BUILD_PAIR.previousSourceCommit + " cannot read dispatchHistory; " +
      "install/qualify this rollback bridge before a later adopting release. Source receipts cannot change the pair.");
  }
}
export function hasDispatchHistoryAdoption() { return Boolean(adoptions.getStore()); }
export function hasDispatchHistoryPublication() { return Boolean(publications.getStore()); }
export function withDispatchHistoryPublication<T>(publication: Publication, action: () => T): T {
  const owned: Publication = Object.freeze({
    sourceProfileSha256: publication.sourceProfileSha256, sourcePath: publication.sourcePath,
    started: publication.started, normalizeRoots: publication.normalizeRoots,
    profileForRoots: publication.profileForRoots,
  });
  return publications.run(owned, action);
}
export function assertDispatchHistoryWriterDeadline() {
  const publication = publications.getStore();
  if (publication && performance.now() - publication.started > DISPATCH_HISTORY_ADOPTION_LIMITS.writerMs)
    throw new Error("dispatch_history_writer_deadline_exceeded");
}
export function assertDispatchHistoryPublicationSource(observedSha256: string | null) {
  const publication = publications.getStore();
  if (publication && publication.sourceProfileSha256 !== observedSha256)
    throw new Error("collector_config_source_changed_reread_required");
}
export function dispatchHistoryPublicationQualified(roots: readonly unknown[], profile?: Buffer) {
  const publication = publications.getStore();
  const qualified = publication && qualifiedPublications.get(publication);
  if(qualified) {
    assertDispatchHistoryWriterDeadline();
    if(hash(readPrivateStateFile(qualified.readerArtifactPath,DISPATCH_HISTORY_ADOPTION_LIMITS.artifactBytes))!==qualified.readerArtifactSha256)
      throw new Error("dispatch_history_adoption_reader_digest_mismatch");
    if(hash(readPrivateStateFile(qualified.runtimeArtifactPath,DISPATCH_HISTORY_ADOPTION_LIMITS.runtimeArtifactBytes))!==DISPATCH_HISTORY_ROLLBACK_READER.completeRuntimeSha256)
      throw new Error("dispatch_history_adoption_complete_runtime_digest_mismatch");
    if(JSON.stringify(dispatchHistoryRuntimeIdentity(qualified.dependencyDirectory))!==qualified.runtimeFingerprint)
      throw new Error("dispatch_history_adoption_runtime_mismatch");
    if(hash(readPrivateStateFile(qualified.namedUsageLedgerPath,DISPATCH_HISTORY_ADOPTION_LIMITS.namedUsageLedgerBytes))!==qualified.namedUsageLedgerSha256)
      throw new Error("dispatch_history_adoption_named_usage_mismatch");
    assertDispatchHistoryWriterDeadline();
  }
  return Boolean(publication && qualified &&
    qualified.sourceIdentitySha256 === hash(JSON.stringify([DISPATCH_HISTORY_BUILD_PAIR,DISPATCH_HISTORY_ROLLBACK_READER])) &&
    qualified.sourceProfileSha256 === publication.sourceProfileSha256 &&
    qualified.readerArtifactSha256 === DISPATCH_HISTORY_ROLLBACK_READER.artifactSha256 &&
    qualified.nodeAbi === process.versions.modules &&
    qualified.rootsSha256 === hash(JSON.stringify(publication.normalizeRoots(roots))) &&
    (!profile || qualified.profileSha256 === hash(profile)));
}
export function dispatchHistoryAdoptionRequired(roots: readonly {
  rootId: string; dispatch?: unknown[]; dispatchHistory?: { rootRows: number };
}[]): never {
  const pressure = roots.map(root => ({ rootId: root.rootId,
    retainedBindings: (root.dispatch?.length ?? 0) + (root.dispatchHistory?.rootRows ?? 0),
    legacyLimit: 1000, availableLegacy: Math.max(0, 1000 - (root.dispatch?.length ?? 0) - (root.dispatchHistory?.rootRows ?? 0)),
  }));
  const error = new Error("dispatch_history_adoption_required: bridge adoption is OFF; actual installed previous " +
    DISPATCH_HISTORY_BUILD_PAIR.previousSourceCommit + " cannot consume history. " +
    "A later release requires the installed complete bridge/runtime/consumer pair and L2 approval. Pressure: " + JSON.stringify(pressure));
  Object.assign(error, { pressure: { state: "known", roots: pressure }, rollbackTarget: DISPATCH_HISTORY_BUILD_PAIR });
  throw error;
}

// Execute the already pinned artifact bytes, never reopen an executable pathname
// after hashing it. The reader only opens a private copy of the immutable snapshot.
// A bounded binary frame avoids profile/base64/string copies and huge JSON
// serialization. The parent has already pinned every byte; views are read-only
// until the real consumers stage their one private filesystem snapshot.
const READER_DRIVER = `try { const fs=require('node:fs'),path=require('node:path'),Module=require('node:module'),crypto=require('node:crypto');
const bytes=fs.readFileSync(0);if(bytes.length>96*1024*1024||bytes.length<24||bytes.subarray(0,4).toString()!=='DHB2')throw Error('reader_input_bound');
const lengths=[0,1,2,3,4].map(i=>bytes.readUInt32BE(4+i*4));if(lengths.reduce((a,b)=>a+b,24)!==bytes.length||lengths[0]>65536||lengths[1]>2*1024*1024||lengths.slice(2).some(n=>n>32*1024*1024))throw Error('reader_input_bound');
let at=24;const views=lengths.map(n=>{const v=bytes.subarray(at,at+n);at+=n;return v;});const meta=JSON.parse(views[0].toString());
const filename=path.join(process.cwd(),'dispatch-history-bridge-reader.cjs');const reader=new Module(filename);reader.filename=filename;reader.paths=Module._nodeModulePaths(process.cwd());reader._compile(views[1].toString('utf8'),filename);
const result=reader.exports.readRollbackSnapshot({profile:views[2],image:views[3],scratchRoot:meta.scratchRoot,namedUsage:{...meta.namedUsage,ledger:views[4]}});
const hashFile=file=>{const descriptor=fs.openSync(fs.realpathSync(file),fs.constants.O_RDONLY|fs.constants.O_NOFOLLOW);try{const stat=fs.fstatSync(descriptor);if(!stat.isFile()||stat.size>256*1024*1024)throw Error('runtime_byte_bound');const chunk=Buffer.alloc(65536),hash=crypto.createHash('sha256');let offset=0;while(offset<stat.size){const n=fs.readSync(descriptor,chunk,0,Math.min(chunk.length,stat.size-offset),offset);if(!n)throw Error('runtime_changed');hash.update(chunk.subarray(0,n));offset+=n;}return hash.digest('hex');}finally{fs.closeSync(descriptor);}};
const pkgFile=Module.createRequire(filename).resolve('better-sqlite3/package.json'),native=path.join(path.dirname(pkgFile),'build/Release/better_sqlite3.node');
result.runtime={node:process.versions.node,abi:process.versions.modules,platform:process.platform,arch:process.arch,nodeSha256:hashFile(process.execPath),nativeDependencySha256:hashFile(native),nativeDependencyVersion:JSON.parse(fs.readFileSync(pkgFile)).version,frozenLockSha256:meta.frozenLockSha256};
process.stdout.write(JSON.stringify(result));
} catch(error) {
const name=typeof error?.name==='string'?error.name.slice(0,80):'UNKNOWN';
const code=typeof error?.code==='string'?error.code.slice(0,80):null;
const message=typeof error?.message==='string'&&/^[a-z][a-z0-9_:.-]{0,160}$/.test(error.message)?error.message:null;
const issues=Array.isArray(error?.issues)?error.issues:null;
const positions=typeof error?.stack==='string'?error.stack.split('\\n').filter(line=>/^\\s+at /.test(line)).slice(0,8).map(line=>{
const location=/:([0-9]+):([0-9]+)\\)?$/.exec(line),functionName=/at (?:new )?([a-zA-Z0-9_.$]+) /.exec(line);
return {reader:line.includes('dispatch-history-bridge-reader.cjs'),functionName:functionName?.[1]?.slice(0,80)??null,
line:location?Number(location[1]):null,column:location?Number(location[2]):null};}):null;
process.stderr.write(JSON.stringify({readerError:{name,code,message,
messageSha256:typeof error?.message==='string'?require('node:crypto').createHash('sha256').update(error.message).digest('hex'):null,
issuesTotal:issues?.length??null,positions,
issues:issues?.slice(0,8).map(issue=>({code:String(issue.code).slice(0,64),
path:Array.isArray(issue.path)?issue.path.slice(0,12).map(part=>typeof part==='number'?part:String(part).slice(0,64)):null,
expected:typeof issue.expected==='string'?issue.expected.slice(0,64):null}))??null}})+'\\n');
process.exitCode=1;
}`;

/** Read only the explicitly supplied frozen dependency/source tree. These
 * observable digests are evidence, never an activation or installation grant. */
const runtimeFileDigests = new Map<string, Readonly<{ identity: string; sha256: string }>>();
export function dispatchHistoryRuntimeIdentity(dependencyDirectory: string) {
  const hashFile=(file:string) => {
    const physical=fs.realpathSync(file),descriptor=fs.openSync(physical,fs.constants.O_RDONLY|fs.constants.O_NOFOLLOW);
    try {
      // Cache only an actually read digest, scoped to this module. Every reuse
      // reopens the physical file and compares nanosecond inode metadata before
      // and after the lookup. Caller inputs cannot populate this cache.
      const identity=(value:fs.BigIntStats)=>[value.dev,value.ino,value.size,value.mtimeNs,value.ctimeNs,
        value.mode,value.uid,value.nlink].map(String).join(":");
      const before=fs.fstatSync(descriptor,{bigint:true});
      if(!before.isFile()||before.size>BigInt(256*1024*1024))throw new Error("dispatch_history_runtime_byte_bound");
      const beforeIdentity=identity(before),cached=runtimeFileDigests.get(physical);
      let value=cached?.identity===beforeIdentity?cached.sha256:undefined;
      if(!value){
        const chunk=Buffer.alloc(64*1024),digest=crypto.createHash("sha256");let offset=0;
        while(offset<Number(before.size)){const n=fs.readSync(descriptor,chunk,0,Math.min(chunk.length,Number(before.size)-offset),offset);if(!n)throw new Error("dispatch_history_runtime_changed");digest.update(chunk.subarray(0,n));offset+=n;}
        value=digest.digest("hex");
      }
      const after=fs.fstatSync(descriptor,{bigint:true}),named=fs.lstatSync(physical,{bigint:true});
      if(identity(after)!==beforeIdentity||identity(named)!==beforeIdentity)
        throw new Error("dispatch_history_runtime_changed");
      if(!cached||cached.identity!==beforeIdentity){
        if(runtimeFileDigests.size>=8)runtimeFileDigests.delete(runtimeFileDigests.keys().next().value!);
        runtimeFileDigests.set(physical,Object.freeze({identity:beforeIdentity,sha256:value}));
      }
      return value;
    } finally {fs.closeSync(descriptor);}
  };
  const require=createRequire(path.join(dependencyDirectory,"dispatch-reader-runtime.cjs"));
  const pkg=require.resolve("better-sqlite3/package.json"),native=path.join(path.dirname(pkg),"build/Release/better_sqlite3.node");
  return {node:process.versions.node,abi:process.versions.modules,platform:process.platform,arch:process.arch,
    nodeSha256:hashFile(process.execPath),nativeDependencySha256:hashFile(native),nativeDependencyVersion:JSON.parse(fs.readFileSync(pkg,"utf8")).version as string,
    frozenLockSha256:hashFile(path.resolve(dependencyDirectory,"../../pnpm-lock.yaml"))};
}

const readerResultSchema = z.object({
  protocol: z.literal(DISPATCH_HISTORY_ROLLBACK_READER.protocol),
  sourceCommit: z.literal(DISPATCH_HISTORY_ROLLBACK_READER.sourceCommit), collectorVersion: z.literal("0.7.48"),
  profileSha256: sha, imageSha256: sha, generation: z.string().uuid(),
  roots: z.array(rootInventorySchema).min(1).max(64), terminalSha256: sha, inventorySha256: sha, nodeAbi: z.string(),
  runtime: dispatchHistoryAdoptionContractSchema.shape.runtime,
  namedUsage: z.object({ sealedSha256: sha, items: z.literal(2), locallyDead: z.literal(0), leaseExpiryRespected: z.literal(true) }).strict(),
}).strict();

/** Reopen the exact planned immutable bytes BEFORE either image or profile is
 * published. A JSON receipt alone cannot qualify: the pinned reader must execute
 * successfully and agree with every root, binding digest, generation and marker. */
export function qualifyDispatchHistoryPublication(roots: readonly unknown[], image: Buffer,
  expected: { generation: string; roots: DispatchRollbackRootInventory[]; terminalSha256: string }, now: Date,
  actualProfile?: Buffer) {
  const publication = publications.getStore(), supply = adoptions.getStore();
  if (!publication || !supply) throw new Error("dispatch_history_adoption_required");
  if (!publication.sourceProfileSha256) throw new Error("dispatch_history_adoption_source_profile_missing");
  assertDispatchHistoryBuildPairQualified();
  assertDispatchHistoryWriterDeadline();
  const normalized = publication.normalizeRoots(roots), profile = actualProfile ?? publication.profileForRoots(normalized);
  if (profile.length > 32 * 1024 * 1024 || image.length > 32 * 1024 * 1024)
    throw new Error("dispatch_history_adoption_snapshot_byte_bound");
  const request: DispatchHistoryAdoptionRequest = Object.freeze({
    sourceProfileSha256: publication.sourceProfileSha256, nextProfileSha256: hash(profile),
    imageSha256: hash(image), generation: expected.generation,
    inventorySha256: hash(JSON.stringify(expected.roots)), terminalSha256: expected.terminalSha256,
    roots: Object.freeze(expected.roots.map(root => Object.freeze({ ...root }))), now: now.toISOString(),
  });
  const input = supply(request);
  assertDispatchHistoryWriterDeadline();
  if (!input || typeof input !== "object" || input.contract === undefined)
    throw new Error("dispatch_history_adoption_contract_invalid");
  const serializedContract = JSON.stringify(input.contract);
  if (serializedContract === undefined) throw new Error("dispatch_history_adoption_contract_invalid");
  if (Buffer.byteLength(serializedContract) > DISPATCH_HISTORY_ADOPTION_LIMITS.contractBytes)
    throw new Error("dispatch_history_adoption_contract_byte_bound");
  const parsed = dispatchHistoryAdoptionContractSchema.safeParse(input.contract);
  if (!parsed.success) throw new Error("dispatch_history_adoption_contract_invalid");
  const contract = parsed.data, issued = Date.parse(contract.issuedAt), until = Date.parse(contract.validUntil);
  if (!Number.isFinite(now.getTime()) || !Number.isFinite(issued) || !Number.isFinite(until) ||
      issued > now.getTime() || until <= now.getTime() || until <= issued ||
      until - issued > DISPATCH_HISTORY_ADOPTION_LIMITS.validityMs)
    throw new Error("dispatch_history_adoption_contract_clock_invalid");
  for (const field of ["sourceProfileSha256", "nextProfileSha256", "imageSha256", "generation",
    "inventorySha256", "terminalSha256"] as const)
    if (contract[field] !== request[field]) throw new Error("dispatch_history_adoption_contract_stale_or_mismatched");
  if (JSON.stringify(contract.roots) !== JSON.stringify(request.roots))
    throw new Error("dispatch_history_adoption_contract_root_scope_mismatch");
  if (typeof input.readerArtifactPath !== "string" || typeof input.dependencyDirectory !== "string" ||
      typeof input.scratchDirectory !== "string" || !path.isAbsolute(input.readerArtifactPath) || !path.isAbsolute(input.dependencyDirectory) ||
      fs.realpathSync(input.dependencyDirectory) !== input.dependencyDirectory)
    throw new Error("dispatch_history_adoption_reader_path_invalid");
  assertPrivateStateDirectory(input.scratchDirectory);
  const artifact = readPrivateStateFile(input.readerArtifactPath, DISPATCH_HISTORY_ADOPTION_LIMITS.artifactBytes);
  if (hash(artifact) !== DISPATCH_HISTORY_ROLLBACK_READER.artifactSha256)
    throw new Error("dispatch_history_adoption_reader_digest_mismatch");
  if(typeof input.runtimeArtifactPath!=="string"||!path.isAbsolute(input.runtimeArtifactPath)||!input.namedUsage||
      typeof input.namedUsage.ledgerArtifactPath!=="string"||!path.isAbsolute(input.namedUsage.ledgerArtifactPath))
    throw new Error("dispatch_history_adoption_complete_pair_missing");
  const runtimeBytes=readPrivateStateFile(input.runtimeArtifactPath,DISPATCH_HISTORY_ADOPTION_LIMITS.runtimeArtifactBytes);
  if(hash(runtimeBytes)!==DISPATCH_HISTORY_ROLLBACK_READER.completeRuntimeSha256)
    throw new Error("dispatch_history_adoption_complete_runtime_digest_mismatch");
  const runtime=dispatchHistoryRuntimeIdentity(input.dependencyDirectory);
  if(JSON.stringify(contract.runtime)!==JSON.stringify(runtime))throw new Error("dispatch_history_adoption_runtime_mismatch");
  const ledger=readPrivateStateFile(input.namedUsage.ledgerArtifactPath,DISPATCH_HISTORY_ADOPTION_LIMITS.namedUsageLedgerBytes);
  const namedUsage={ledgerSha256:hash(ledger),sealedSha256:input.namedUsage.sealedSha256,now:input.namedUsage.now,
    workspaceId:input.namedUsage.workspaceId,deviceId:input.namedUsage.deviceId};
  if(JSON.stringify(namedUsage)!==JSON.stringify(contract.namedUsage))throw new Error("dispatch_history_adoption_named_usage_mismatch");
  const meta=Buffer.from(JSON.stringify({scratchRoot:input.scratchDirectory,namedUsage:input.namedUsage,frozenLockSha256:runtime.frozenLockSha256}));
  const chunks=[meta,artifact,profile,image,ledger],header=Buffer.alloc(24);header.write("DHB2");
  chunks.forEach((chunk,index)=>header.writeUInt32BE(chunk.length,4+index*4));
  const bytes=chunks.reduce((count,chunk)=>count+chunk.length,header.length);
  if(meta.length>64*1024||bytes>DISPATCH_HISTORY_ADOPTION_LIMITS.inputBytes)
    throw new Error("dispatch_history_adoption_reader_input_bound");
  const body=Buffer.concat([header,...chunks],bytes);
  const remaining = DISPATCH_HISTORY_ADOPTION_LIMITS.writerMs - (performance.now() - publication.started);
  if (remaining <= 0) throw new Error("dispatch_history_writer_deadline_exceeded");
  const started = performance.now();
  const child = spawnSync(process.execPath, ["--eval", READER_DRIVER], {
    cwd: input.dependencyDirectory, input: body, encoding: "utf8",
    env: { LANG: "en_US.UTF-8", TZ: "UTC", HOME: input.scratchDirectory, TMPDIR: input.scratchDirectory },
    timeout: Math.max(1, Math.floor(Math.min(remaining, DISPATCH_HISTORY_ADOPTION_LIMITS.qualificationMs))),
    killSignal: "SIGKILL",
    maxBuffer: DISPATCH_HISTORY_ADOPTION_LIMITS.outputBytes,
  });
  assertDispatchHistoryWriterDeadline();
  if (child.status !== 0 || child.error || performance.now() - started > DISPATCH_HISTORY_ADOPTION_LIMITS.qualificationMs)
    throw new Error("dispatch_history_adoption_reader_unqualified");
  const opened = readerResultSchema.safeParse(JSON.parse(child.stdout));
  if (!opened.success || opened.data.profileSha256 !== request.nextProfileSha256 ||
      opened.data.imageSha256 !== request.imageSha256 || opened.data.generation !== request.generation ||
      opened.data.inventorySha256 !== request.inventorySha256 || opened.data.terminalSha256 !== request.terminalSha256 ||
      opened.data.nodeAbi !== process.versions.modules ||
      JSON.stringify(opened.data.runtime)!==JSON.stringify(runtime)||opened.data.namedUsage.sealedSha256!==namedUsage.sealedSha256|| JSON.stringify(opened.data.roots) !== JSON.stringify(request.roots))
    throw new Error("dispatch_history_adoption_reader_inventory_mismatch");
  assertDispatchHistoryPublicationSource(hash(readPrivateStateFile(publication.sourcePath)));
  qualifiedPublications.set(publication, Object.freeze({
    rootsSha256: hash(JSON.stringify(normalized)), profileSha256: request.nextProfileSha256,
    sourceProfileSha256: publication.sourceProfileSha256,
    readerArtifactSha256: DISPATCH_HISTORY_ROLLBACK_READER.artifactSha256, nodeAbi: process.versions.modules!,
    sourceIdentitySha256:hash(JSON.stringify([DISPATCH_HISTORY_BUILD_PAIR,DISPATCH_HISTORY_ROLLBACK_READER])),
    readerArtifactPath:input.readerArtifactPath,runtimeArtifactPath:input.runtimeArtifactPath,
    dependencyDirectory:input.dependencyDirectory,runtimeFingerprint:JSON.stringify(runtime),
    namedUsageLedgerPath:input.namedUsage.ledgerArtifactPath,namedUsageLedgerSha256:namedUsage.ledgerSha256,
  }));
  // Reject post-execution substitution before the caller can publish an image.
  if(!dispatchHistoryPublicationQualified(roots,profile))throw new Error("dispatch_history_adoption_reader_inventory_mismatch");
}
