import { AsyncLocalStorage } from "node:async_hooks";
import crypto from "node:crypto";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { z } from "zod";
import { assertPrivateStateDirectory, readPrivateStateFile } from "./collector-state-io";

/** An immutable read-only bundle of the exact PR453 reader, not vanilla 0.7.48.
 * The artifact and its complete source/input manifest accompany the source packet.
 * An owner must retain this reader and its frozen dependencies before adoption.
 * This technical qualification never represents release-owner installation approval. */
export const DISPATCH_HISTORY_ROLLBACK_READER = Object.freeze({
  protocol: "plimsoll.dispatch-history-rollback-reader/v1" as const,
  sourceCommit: "e7e937faa4960fa0a8cb5d5263361fc02894b7f6",
  artifactSha256: "bc08c2edf8ae459a9085851b3131e655865ed11dfa6a103e82286653c49703f4",
  collectorVersion: "0.7.48" as const,
  vanillaCompatible: false as const,
  readOnly: true as const,
});
export const DISPATCH_HISTORY_ADOPTION_LIMITS = Object.freeze({
  contractBytes: 64 * 1024, artifactBytes: 2 * 1024 * 1024,
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
  schema: z.literal("dispatch-history-source-adoption/v1"), qualificationId: z.string().uuid(),
  sourceQualificationOnly: z.literal(true), rolloutAuthorized: z.literal(false),
  writerBaseCommit: z.literal(DISPATCH_HISTORY_ROLLBACK_READER.sourceCommit),
  reader: z.object({
    protocol: z.literal(DISPATCH_HISTORY_ROLLBACK_READER.protocol),
    sourceCommit: z.literal(DISPATCH_HISTORY_ROLLBACK_READER.sourceCommit),
    artifactSha256: z.literal(DISPATCH_HISTORY_ROLLBACK_READER.artifactSha256),
    collectorVersion: z.literal("0.7.48"), vanillaCompatible: z.literal(false), readOnly: z.literal(true),
  }).strict(),
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
  contract: unknown; readerArtifactPath: string; dependencyDirectory: string; scratchDirectory: string;
};
type Publication = {
  sourceProfileSha256: string | null; sourcePath: string; started: number;
  normalizeRoots: (roots: readonly unknown[]) => unknown[];
  profileForRoots: (normalizedRoots: readonly unknown[]) => Buffer;
  qualifiedRootsSha256?: string;
  qualifiedProfileSha256?: string;
};
const adoptions = new AsyncLocalStorage<(request: DispatchHistoryAdoptionRequest) => DispatchHistoryAdoptionInput>();
const publications = new AsyncLocalStorage<Publication>();

/** Explicit, call-scoped technical qualification. No CLI flag, environment default,
 * config receipt, or outbox proof enables this context. External owner integration
 * supplies the source-bound contract; it must separately obtain L2 adoption approval. */
export function withDispatchHistoryAdoption<T>(
  supply: (request: DispatchHistoryAdoptionRequest) => DispatchHistoryAdoptionInput, action: () => T,
): T { return adoptions.run(supply, action); }
export function hasDispatchHistoryAdoption() { return Boolean(adoptions.getStore()); }
export function hasDispatchHistoryPublication() { return Boolean(publications.getStore()); }
export function withDispatchHistoryPublication<T>(publication: Publication, action: () => T): T {
  return publications.run(publication, action);
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
  return Boolean(publication?.qualifiedRootsSha256 &&
    publication.qualifiedRootsSha256 === hash(JSON.stringify(publication.normalizeRoots(roots))) &&
    (!profile || publication.qualifiedProfileSha256 === hash(profile)));
}
export function dispatchHistoryAdoptionRequired(roots: readonly {
  rootId: string; dispatch?: unknown[]; dispatchHistory?: { rootRows: number };
}[]): never {
  const pressure = roots.map(root => ({ rootId: root.rootId,
    retainedBindings: (root.dispatch?.length ?? 0) + (root.dispatchHistory?.rootRows ?? 0),
    legacyLimit: 1000, availableLegacy: Math.max(0, 1000 - (root.dispatch?.length ?? 0) - (root.dispatchHistory?.rootRows ?? 0)),
  }));
  const error = new Error("dispatch_history_adoption_required: qualify the exact retained rollback reader " +
    "and obtain L2 adoption approval; vanilla 0.7.48 cannot consume history. Pressure: " + JSON.stringify(pressure));
  Object.assign(error, { pressure: { state: "known", roots: pressure }, rollbackReader: DISPATCH_HISTORY_ROLLBACK_READER });
  throw error;
}

// Execute the already pinned artifact bytes, never reopen an executable pathname
// after hashing it. The reader only opens a private copy of the immutable snapshot.
const READER_DRIVER = `const fs=require('node:fs'),path=require('node:path'),Module=require('node:module');
const input=JSON.parse(fs.readFileSync(0,'utf8'));const code=Buffer.from(input.artifact,'base64').toString('utf8');
const filename=path.join(process.cwd(),'dispatch-history-rollback-reader.cjs');
const reader=new Module(filename);reader.filename=filename;reader.paths=Module._nodeModulePaths(process.cwd());
reader._compile(code,filename);process.stdout.write(JSON.stringify(reader.exports.readRollbackSnapshot(input.snapshot)));`;
const readerResultSchema = z.object({
  protocol: z.literal(DISPATCH_HISTORY_ROLLBACK_READER.protocol),
  sourceCommit: z.literal(DISPATCH_HISTORY_ROLLBACK_READER.sourceCommit), collectorVersion: z.literal("0.7.48"),
  profileSha256: sha, imageSha256: sha, generation: z.string().uuid(),
  roots: z.array(rootInventorySchema).min(1).max(64), terminalSha256: sha, inventorySha256: sha, nodeAbi: z.string(),
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
  const body = JSON.stringify({ artifact: artifact.toString("base64"), snapshot: {
    profileBase64: profile.toString("base64"), imageBase64: image.toString("base64"), scratchRoot: input.scratchDirectory,
  } });
  if (Buffer.byteLength(body) > DISPATCH_HISTORY_ADOPTION_LIMITS.inputBytes)
    throw new Error("dispatch_history_adoption_reader_input_bound");
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
      opened.data.nodeAbi !== process.versions.modules || JSON.stringify(opened.data.roots) !== JSON.stringify(request.roots))
    throw new Error("dispatch_history_adoption_reader_inventory_mismatch");
  assertDispatchHistoryPublicationSource(hash(readPrivateStateFile(publication.sourcePath)));
  publication.qualifiedRootsSha256 = hash(JSON.stringify(normalized));
  publication.qualifiedProfileSha256 = request.nextProfileSha256;
}
