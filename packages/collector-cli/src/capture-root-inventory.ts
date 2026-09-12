import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { z } from "zod";
import { accountAssertionContains, accountAssertionV1Schema, type AccountAssertionV1 } from "./account-assertion";
import type { CaptureBaselineFileObservation } from "./capture-baseline";
const id=z.string().regex(/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/);
const legacyAccountSchema=z.object({
  actorHash: z.string().regex(/^sha256:[a-f0-9]{64}$/),
  validFrom: z.iso.datetime(),validUntil: z.iso.datetime().nullable(),evidenceRef: id
}).strict();
const accountAssertionEpochSchema=z.object({
  actorHash: z.string().regex(/^sha256:[a-f0-9]{64}$/),
  validFrom: z.iso.datetime(),
  evidenceRef: z.string().regex(/^sha256:[a-f0-9]{64}$/),
  installationEpochId: z.string().uuid(),
}).strict();
export const captureRootSchema=z.object({
  rootId: id,profileId: id,installationEpochId: id,
  source: z.enum(["codex","claude_code"]),directory: z.string().min(1),
  /** Explicit enrollment attestation; no search of neighboring auth stores. */
  dispatch: z.array(z.object({
    sessionId: id,workItemId: id,projectKey: z.string().regex(/^sha256:[a-f0-9]{64}$/),
    companyRef: id.nullable(),attemptId: id,parentAttemptId: id.nullable(),acceptedOutcomeId: id.nullable(),
    validFrom: z.iso.datetime(),validUntil: z.iso.datetime().nullable(),evidenceRef: id,
  }).strict()).max(1000).optional(),
  /** Legacy account rows remain accepted; new enrollments use the additive V1 contract. */
  account: z.union([legacyAccountSchema,accountAssertionV1Schema]).optional(),
  /** Immutable historical account windows hydrated from the maintenance key. */
  accountAssertions: z.array(accountAssertionV1Schema).max(1024).optional(),
  /** Hash-only mapping from each assertion window to its source-binding epoch. */
  accountAssertionEpochs: z.array(accountAssertionEpochSchema).max(1024).optional(),
}).strict();
export type CaptureRoot=z.infer<typeof captureRootSchema>;
export type CaptureRootAccount=NonNullable<CaptureRoot["account"]>;
export { accountAssertionV1Schema };
export type { AccountAssertionV1 };
export type CaptureRootCoverage={
  rootId: string;
  profileId: string;
  installationEpochId: string;
  source: CaptureRoot["source"];
  state: "ready"|"missing"|"unreadable"|"unsafe"|"partial";
  observedAt: string;
  reason: string|null;
  pathDigest: string;
};
export function validateCaptureRoots(input: unknown): CaptureRoot[] {
  const roots=z.array(captureRootSchema).max(64).parse(input);
  const ids=new Set<string>();
  for(const root of roots) {
    if(!path.isAbsolute(root.directory))
      throw new Error("capture_root_requires_absolute_path");
    root.directory=path.resolve(root.directory);
    if(ids.has(root.rootId))
      throw new Error("capture_root_duplicate_id");
    ids.add(root.rootId);
    if (root.account && "schema" in root.account && root.account.schema === "account-assertion/v1" &&
        root.account.source !== root.source)
      throw new Error("capture_account_source_mismatch");
    if(root.account?.validUntil&&Date.parse(root.account.validUntil)<=Date.parse(root.account.validFrom))
      throw new Error("capture_identity_window_invalid");
    const assertions = root.accountAssertions ?? [];
    for (const assertion of assertions) {
      if (assertion.source !== root.source || (assertion.validUntil !== null && Date.parse(assertion.validUntil) <= Date.parse(assertion.validFrom)))
        throw new Error("capture_identity_window_invalid");
    }
    for (let i = 1; i < assertions.length; i += 1) {
      const previous = assertions[i - 1];
      if (Date.parse(previous.validFrom) >= Date.parse(assertions[i].validFrom) ||
          previous.validUntil === null ||
          Date.parse(previous.validUntil) > Date.parse(assertions[i].validFrom))
        throw new Error("capture_identity_window_invalid");
    }
    const epochKeys = new Set<string>();
    for (const epoch of root.accountAssertionEpochs ?? []) {
      const key = `${epoch.actorHash}\u0000${epoch.validFrom}\u0000${epoch.evidenceRef}`;
      if (epochKeys.has(key) || !assertions.some(assertion => assertion.actorHash === epoch.actorHash &&
          assertion.validFrom === epoch.validFrom && assertion.evidenceRef === epoch.evidenceRef)) {
        throw new Error("capture_identity_epoch_invalid");
      }
      epochKeys.add(key);
    }
    for(const binding of root.dispatch??[]) {
      if(binding.validUntil&&Date.parse(binding.validUntil)<=Date.parse(binding.validFrom)) {
        throw new Error("capture_dispatch_window_invalid");
      }
    }
  }
  for(let i=0;i<roots.length;i++)
    for(let j=i+1;j<roots.length;j++) {
      const a=roots[i].directory,b=roots[j].directory;
      if(a===b||a.startsWith(b+path.sep)||b.startsWith(a+path.sep))
        throw new Error("capture_roots_overlap");
    }
  return roots;
}
export function captureRootDigest(root: CaptureRoot): string {
  return crypto.createHash("sha256").update(JSON.stringify([root.source,root.rootId,root.profileId,root.installationEpochId,root.directory])).digest("hex");
}
export function inspectCaptureRoots(roots: readonly CaptureRoot[],now=new Date()): CaptureRootCoverage[] {
  return roots.map(root => {
    let state: CaptureRootCoverage["state"]="ready",reason: string|null=null;
    try {
      const stat=fs.lstatSync(root.directory);
      if(stat.isSymbolicLink()||!stat.isDirectory()||fs.realpathSync(root.directory)!==root.directory) {
        state="unsafe";
        reason="capture_root_not_physical_directory";
      }
      else
        fs.accessSync(root.directory,fs.constants.R_OK|fs.constants.X_OK);
    }
    catch(error) {
      state=(error as NodeJS.ErrnoException).code==="ENOENT"? "missing":"unreadable";
      reason=`capture_root_${state}`;
    }
    return {
      rootId: root.rootId,profileId: root.profileId,installationEpochId: root.installationEpochId,source: root.source,state,
      observedAt: now.toISOString(),reason,pathDigest: captureRootDigest(root)
    };
  });
}
export function rootForFile(roots: readonly CaptureRoot[],file: string): CaptureRoot|undefined {
  return roots.find(root => file.startsWith(root.directory+path.sep));
}
export function rootCursorKey(roots: readonly CaptureRoot[],file: string): string {
  const root=rootForFile(roots,file);
  return root? `${file}\u0000${captureRootDigest(root)}`:file;
}
export function rootEventMetadata(root: CaptureRoot|undefined,sourceEventId: string,observedAt: string,sessionId?: string,
  accountAttributionEnabled=true): Record<string, unknown> {
  if(!root)
    return {};
  const at=Date.parse(observedAt);
  const accountCandidates = accountAttributionEnabled ? [...new Map(
    [ ...(root.accountAssertions ?? []), ...(root.account ? [root.account] : []) ]
      .map(account => [JSON.stringify([account.actorHash,account.validFrom,account.validUntil,account.evidenceRef]), account] as const),
  ).values()] : [];
  const matchingAccounts = accountCandidates.filter(account => at>=Date.parse(account.validFrom)&&(!account.validUntil||at<Date.parse(account.validUntil)));
  const account = matchingAccounts.length === 1 ? matchingAccounts[0] : null;
  const accountEpochs = account ? (root.accountAssertionEpochs ?? []).filter(epoch =>
    epoch.actorHash===account.actorHash&&epoch.validFrom===account.validFrom&&epoch.evidenceRef===account.evidenceRef) : [];
  const installationEpochId = accountEpochs.length===1 ? accountEpochs[0].installationEpochId : root.installationEpochId;
  const bindings=(root.dispatch??[]).filter(binding => binding.sessionId===sessionId&&at>=Date.parse(binding.validFrom)&&(!binding.validUntil||at<Date.parse(binding.validUntil)));
  const binding=bindings.length===1? bindings[0]:null;
  return {
    ...(binding? {
      workItemId: binding.workItemId,dispatchProjectKey: binding.projectKey,
      workEvidenceRef: binding.evidenceRef,attemptId: binding.attemptId,
      ...(binding.parentAttemptId? { parentAttemptId: binding.parentAttemptId }:{}),
      ...(binding.companyRef? { companyRef: binding.companyRef }:{}),
      ...(binding.acceptedOutcomeId? { acceptedOutcomeId: binding.acceptedOutcomeId }:{}),
    }:{}),...(bindings.length>1? { workAttributionState: "conflict" }:{}),
    captureRootId: root.rootId,captureProfileId: root.profileId,installationEpochId,
    logicalSourceEventId: sourceEventId,sourceIdentityEvidenceRef: "native_runtime_event_v1",
    ...(account ? { captureAccountHash: account.actorHash,accountEvidenceRef: account.evidenceRef } : {}),
    ...(matchingAccounts.length > 1 || accountEpochs.length > 1 ? { accountAttributionState: "conflict" } : {})
  };
}
/** Reopen the existing provider baseline at its original cutoff when the inventory changes.
 * Old generation exclusions and cursors stay intact. No raw event is changed or removed. */
export function bindCaptureInventory(database: import("better-sqlite3").Database,source: CaptureRoot["source"],roots: CaptureRoot[],coverage?: CaptureRootCoverage[]) {
  if(!roots.length)
    return false;
  ensureRootObservationSchema(database);
  const inventoryDigest=crypto.createHash("sha256").update(JSON.stringify([
    roots.map(captureRootDigest).sort(),coverage?.filter(row => row.state==="ready").map(row => row.rootId).sort()??null,
  ])).digest("hex");
  database.exec(`create table if not exists capture_root_inventory_bindings (
    source text primary key, inventory_digest text not null, updated_at text not null)`);
  const prior=database.prepare("select inventory_digest as digest from capture_root_inventory_bindings where source=?").get(source) as {
    digest: string;
  }|undefined;
  if(prior?.digest===inventoryDigest)
    return false;
  database.transaction(() => {
    if(database.prepare("select 1 from sqlite_master where type='table' and name='automatic_capture_baseline_state'").get()) {
      database.prepare(`update automatic_capture_baseline_state set status='in_progress',completed_at=null
        where source=? and status='complete'`).run(source);
    }
    database.prepare(`insert into capture_root_inventory_bindings(source,inventory_digest,updated_at) values(?,?,?)
      on conflict(source) do update set inventory_digest=excluded.inventory_digest,updated_at=excluded.updated_at`)
      .run(source,inventoryDigest,new Date().toISOString());
  }).immediate();
  return true;
}
const initializedObservationDatabases=new WeakSet<object>();
const captureRootEpochCapabilities=new WeakMap<object,string>();
/** Read-only sidecar used by LocalEventBuffer; JSON cannot mint this capability. */
export function captureRootEventInstallationEpoch(event: object) {
  return captureRootEpochCapabilities.get(event);
}
function ensureRootObservationSchema(database: import("better-sqlite3").Database) {
  if(initializedObservationDatabases.has(database))
    return;
  database.exec(`create table if not exists capture_root_observations (
    root_digest text not null,event_id text not null,payload_digest text not null,
    observed_at text not null,state text not null check(state in ('admitted','duplicate','conflict')),
    primary key(root_digest,event_id));
    create index if not exists idx_capture_root_event on capture_root_observations(event_id)`);
  // A rolled-back transaction must not leave a cached schema assertion.
  if(!database.inTransaction)
    initializedObservationDatabases.add(database);
}
/** Root sightings live beside immutable events; replay/failover never changes the first receipt. */
export function appendRootObservation(buffer: import("./buffer").LocalEventBuffer,event: import("../../shared/src/schemas").AiInteractionEvent,root: CaptureRoot|undefined): boolean {
  const parsedAccount = root?.account ? accountAssertionV1Schema.safeParse(root.account) : null;
  const trustedEpoch = root && parsedAccount?.success && accountAssertionContains(parsedAccount.data, event.observedAt) &&
    event.metadata?.installationEpochId===root.installationEpochId ? root.installationEpochId : undefined;
  if (buffer.eventAdmissionReason(event.observedAt, root?.installationEpochId ?? event.metadata?.installationEpochId, trustedEpoch))
    return false;
  if(!root)
    return buffer.append(event,[]);
  const database=buffer.database;
  ensureRootObservationSchema(database);
  const nativeSignature=(value: typeof event) => crypto.createHash("sha256").update(JSON.stringify([
    value.source,value.id,value.sessionId,value.observedAt,value.model,value.inputTokens??null,value.outputTokens??null,
    value.cacheReadTokens??null,value.cacheCreationTokens??null,value.costUsd??null,
  ])).digest("hex");
  const payloadDigest=nativeSignature(event),rootDigest=captureRootDigest(root);
  const existing=database.prepare("select payload_json as payload from buffered_events where id=?").get(event.id) as {
    payload: string;
  }|undefined;
  const priorSightings=database.prepare(`select payload_digest as digest,state
    from capture_root_observations where event_id=?`).all(event.id) as Array<{
    digest: string;
    state: string;
  }>;
  // Compact source receipts survive raw retention so an epoch change cannot
  // revive already observed consumption. They contain no paths or payloads.
  const priorConflict=priorSightings.some(row => row.digest!==payloadDigest||row.state==="conflict");
  const alreadyObserved=Boolean(existing)||priorSightings.length>0;
  let same=false;
  if(existing) {
    try {
      same=nativeSignature(JSON.parse(existing.payload))===payloadDigest;
    }
    catch { /* corruption remains a conflict */ }
  }
  else
    same=priorSightings.length>0&&!priorConflict;
  let inserted=false;
  if(!alreadyObserved) {
    if(trustedEpoch) captureRootEpochCapabilities.set(event,trustedEpoch);
    try { inserted=buffer.append(event,[]); }
    finally { captureRootEpochCapabilities.delete(event); }
  }
  // Another connection may have enrolled between the first check and append.
  if (!alreadyObserved && !inserted && buffer.eventAdmissionReason(event.observedAt, root?.installationEpochId, trustedEpoch))
    return false;
  const state=priorConflict? "conflict":alreadyObserved? (same? "duplicate":"conflict"):inserted? "admitted":"conflict";
  database.prepare(`insert into capture_root_observations values(?,?,?,?,?)
    on conflict(root_digest,event_id) do update set state=case when payload_digest<>excluded.payload_digest then 'conflict' else state end`)
    .run(rootDigest,event.id,payloadDigest,event.observedAt,state);
  if(state==="conflict")
    throw new Error("capture_logical_source_conflict");
  return inserted;
}

/**
 * Additive root registration (bead eco-6hoxj.53).
 *
 * A host that later gains a native root — a new Claude seat's `projects/`, a
 * new Codex profile's `sessions/`, or the main `~/.claude/projects` an older
 * enrollment never registered — has had no product path to register it:
 * capture roots are minted at enrollment, which lives outside this repository,
 * and the gap has been repaired by host-sealed one-off scripts. The helpers
 * below carry the reviewed semantics of those scripts: the same identity
 * derivation, the same standard shapes, and the same refusal to change
 * anything that already exists.
 */

/** Directory shapes a native root takes under the operator home. */
export const CAPTURE_ROOT_SHAPES = [
  { shape: "claude_home", source: "claude_code" as const, segments: [".claude", "projects"] },
  { shape: "claude_seat", source: "claude_code" as const, parent: ".claude-seats", leaf: "projects" },
  { shape: "codex_home", source: "codex" as const, segments: [".codex", "sessions"] },
  { shape: "codex_profile", source: "codex" as const, parent: ".codex-profiles", leaf: "sessions" },
] as const;

export type CaptureRootCandidate = {
  shape: string;
  source: CaptureRoot["source"];
  /** Absolute physical directory. */
  directory: string;
  /** Directory relative to the operator home; a candidate never leaves it. */
  relativeDirectory: string;
};

export type CaptureRootDiscoveryEntry = {
  source: CaptureRoot["source"];
  state: "registered" | "candidate" | "missing";
  /** Relative to the operator home, or null for a configured root outside it. */
  directory: string | null;
  outsideHome: boolean;
  shape: string | null;
  rootId: string | null;
};

/**
 * Python `json.dumps(value, sort_keys=True, separators=(",", ":"),
 * ensure_ascii=True)` for the one shape this derivation uses — an array of
 * strings — so a root added here derives byte-identically to one the sealed
 * helpers appended.
 */
function canonicalAscii(value: readonly string[]): string {
  return JSON.stringify(value).replace(/[^ -~]/g, (character) =>
    `\\u${character.charCodeAt(0).toString(16).padStart(4, "0")}`);
}

/**
 * Root identity as the sealed enrollment helpers derive it:
 * `root-<sha256(canonical([machine, source, directory]))[:24]>`, with the same
 * digest behind `profile-`. The machine label is the fleet label, not a
 * hostname, so callers resolve it from the roots already in the config.
 */
export function deriveCaptureRootIdentity(
  machine: string,
  source: CaptureRoot["source"],
  directory: string,
): { rootId: string; profileId: string } {
  const digest = crypto.createHash("sha256")
    .update(canonicalAscii([machine, source, directory])).digest("hex").slice(0, 24);
  return { rootId: `root-${digest}`, profileId: `profile-${digest}` };
}

/** True when every configured root reproduces its own rootId under `machine`. */
export function captureRootsDeriveFrom(roots: readonly CaptureRoot[], machine: string): boolean {
  return roots.every((root) =>
    deriveCaptureRootIdentity(machine, root.source, root.directory).rootId === root.rootId);
}

/**
 * The machine label the config was enrolled under. It is stored nowhere — only
 * its digests are — so it is recovered by checking candidate labels against the
 * roots that already exist. A host with no roots yet has nothing to check
 * against, and the caller must state the label explicitly.
 */
export function resolveCaptureRootMachineLabel(
  roots: readonly CaptureRoot[],
  candidates: readonly string[],
): string | null {
  if (!roots.length) return null;
  return candidates.find((candidate) => candidate.length > 0 && captureRootsDeriveFrom(roots, candidate)) ?? null;
}

/**
 * The home a discovery runs against, resolved the way a physical root is.
 * `add` resolves the home through this too: a directory `discover` reports as
 * a candidate must never be refused `path_outside_home` merely because a
 * component of `$HOME` is a symlink.
 */
export function resolveDiscoveryHome(home: string): string {
  try { return fs.realpathSync(path.resolve(home)); }
  catch { return path.resolve(home); }
}

/** The physical directory a configured root captures from. */
export function configuredCaptureRootDirectory(root: Pick<CaptureRoot, "directory">): string {
  return physicalCaptureRootDirectory(root.directory) ?? path.resolve(root.directory);
}

/** The physical directory an entry names, following a link; undefined when it is neither. */
export function physicalCaptureRootDirectory(entry: string): string | undefined {
  try {
    const resolved = fs.realpathSync(entry);
    return fs.statSync(resolved).isDirectory() ? resolved : undefined;
  } catch {
    // A dangling link or an unreadable entry is skipped, never reported.
    return undefined;
  }
}

/**
 * Candidate directories in the standard shapes under `home`. Read-only: it
 * stats directories, never opens a file, and never leaves the home.
 */
export function discoverCaptureRootCandidates(home: string): CaptureRootCandidate[] {
  const resolvedHome = resolveDiscoveryHome(home);
  const found: CaptureRootCandidate[] = [];
  const add = (shape: string, source: CaptureRoot["source"], entry: string) => {
    const directory = physicalCaptureRootDirectory(entry);
    if (directory === undefined) return;
    const relativeDirectory = path.relative(resolvedHome, directory);
    // A seat relocated to shared storage and symlinked in resolves outside the
    // home; that is the seat tooling's root to register, not a home candidate.
    if (relativeDirectory.startsWith("..") || path.isAbsolute(relativeDirectory)) return;
    found.push({ shape, source, directory, relativeDirectory });
  };
  for (const shape of CAPTURE_ROOT_SHAPES) {
    if ("segments" in shape) {
      add(shape.shape, shape.source, path.join(resolvedHome, ...shape.segments));
      continue;
    }
    const parent = path.join(resolvedHome, shape.parent);
    let entries: fs.Dirent[];
    try { entries = fs.readdirSync(parent, { withFileTypes: true }); }
    catch { continue; }
    for (const entry of [...entries].sort((left, right) => left.name < right.name ? -1 : left.name > right.name ? 1 : 0)) {
      if (entry.name.startsWith(".")) continue;
      add(shape.shape, shape.source, path.join(parent, entry.name, shape.leaf));
    }
  }
  return found;
}

/**
 * Every configured root plus every unregistered candidate, as one list. A
 * configured root outside the home keeps its identity but not its path, and is
 * never stat-ed: discovery stays inside the home.
 */
export function discoverCaptureRoots(
  home: string,
  roots: readonly CaptureRoot[],
): CaptureRootDiscoveryEntry[] {
  const resolvedHome = resolveDiscoveryHome(home);
  const candidates = discoverCaptureRootCandidates(resolvedHome);
  const shapes = new Map(candidates.map((candidate) => [candidate.directory, candidate.shape]));
  const configured = new Set(roots.map((root) => configuredCaptureRootDirectory(root)));
  const entries: CaptureRootDiscoveryEntry[] = roots.map((root) => {
    const directory = configuredCaptureRootDirectory(root);
    const relative = path.relative(resolvedHome, directory);
    const outsideHome = relative.startsWith("..") || path.isAbsolute(relative);
    return {
      source: root.source,
      state: outsideHome || physicalCaptureRootDirectory(root.directory) !== undefined
        ? "registered" as const
        : "missing" as const,
      directory: outsideHome ? null : relative,
      outsideHome,
      shape: outsideHome ? null : shapes.get(directory) ?? null,
      rootId: root.rootId,
    };
  });
  for (const candidate of candidates) {
    if (configured.has(candidate.directory)) continue;
    entries.push({
      source: candidate.source,
      state: "candidate",
      directory: candidate.relativeDirectory,
      outsideHome: false,
      shape: candidate.shape,
      rootId: null,
    });
  }
  return entries;
}

/**
 * One entry the baseline walk could not resolve unambiguously. The count was
 * always reported; the entries are listed as well (bead eco-6hoxj.55, review
 * N5) so `--allow-scan-errors` can name exactly what it is leaving unfenced.
 */
export type CaptureRootScanError = {
  path: string;
  reason: "directory_unreadable" | "not_a_regular_file" | "stat_failed";
};

/** Every file the `source` tailer would discover under a capture root.
 *
 * Mirrors `IncrementalJsonlDiscovery`'s predicates so the set fenced at
 * registration is the set that would otherwise be replayed: a recursive walk
 * matching `*.jsonl` for Claude transcripts and `rollout-*.jsonl` for Codex
 * rollouts, never following a symlinked candidate. Anything the walk cannot
 * resolve is counted, never guessed: the caller refuses rather than publish a
 * fence it cannot prove exhaustive. */
export function captureRootBaselineFiles(
  source: CaptureRoot["source"],
  directory: string,
): { files: string[]; errors: number; errorEntries: CaptureRootScanError[] } {
  const matches = source === "codex"
    ? (name: string) => name.startsWith("rollout-") && name.endsWith(".jsonl")
    : (name: string) => name.endsWith(".jsonl");
  const files: string[] = [];
  const errorEntries: CaptureRootScanError[] = [];
  const walk = (current: string) => {
    let entries: fs.Dirent[];
    try { entries = fs.readdirSync(current, { withFileTypes: true }); }
    catch { errorEntries.push({ path: current, reason: "directory_unreadable" }); return; }
    for (const entry of entries) {
      const candidate = path.join(current, entry.name);
      if (entry.isDirectory()) { walk(candidate); continue; }
      if (!matches(entry.name)) continue;
      // A symlinked or non-regular candidate is the discovery's own error
      // class: one physical generation must never be fenced under an alias.
      if (entry.isSymbolicLink() || !entry.isFile()) {
        errorEntries.push({ path: candidate, reason: "not_a_regular_file" });
        continue;
      }
      files.push(candidate);
    }
  };
  walk(directory);
  return { files: files.sort(), errors: errorEntries.length, errorEntries };
}

/** One stat-only observation per file, as the tailers build theirs. */
export function captureRootBaselineObservations(
  files: readonly string[],
): { observations: CaptureBaselineFileObservation[]; errors: number; errorEntries: CaptureRootScanError[] } {
  const observations: CaptureBaselineFileObservation[] = [];
  const errorEntries: CaptureRootScanError[] = [];
  for (const file of files) {
    try {
      const identity = fs.lstatSync(file, { bigint: true });
      if (identity.isSymbolicLink() || !identity.isFile()) {
        errorEntries.push({ path: file, reason: "not_a_regular_file" });
        continue;
      }
      observations.push({
        path: file,
        device: identity.dev,
        inode: identity.ino,
        size: identity.size,
        birthtimeNs: identity.birthtimeNs,
      });
    } catch { errorEntries.push({ path: file, reason: "stat_failed" }); }
  }
  return { observations, errors: errorEntries.length, errorEntries };
}
