import crypto from "node:crypto";
import os from "node:os";
import path from "node:path";

import type { LocalEventBuffer } from "./buffer";
import { AUTOMATIC_CAPTURE_LIMITS } from "./capture-work-budget";
import type { CaptureRoot } from "./capture-root-inventory";
import { collectorConfigSchema, type CollectorConfig } from "./config";
import {
  ensureRepoContextLinkDispositionSchema,
  expireRepoContextLinks,
  recordRepoContextFailureDispositions,
  recoverStartedRepoContextReplayAttempts,
  reResolveExpiredRepoContextLinks,
} from "./repo-context-link-dispositions";
import {
  REPO_CONTEXT_RESOLVER_VERSION,
  resolveRepoContextRequests,
  type RepoContextRequest,
  type RepoContextResult,
} from "./repo-context";
import {
  advanceRepoContextReplayCursor,
  beginRepoContextReplayPass,
  completeRepoContextReplayPass,
  completedRepoContextReplayPasses,
  disabledRepoContextDrainReceipt,
  ensureRepoContextReplaySchema,
  recordRepoContextReplayAttempt,
  repoContextReplayCursor,
  writeRepoContextDrainReceipt,
  type RepoContextDrainReceipt,
} from "./repo-context-replay-state";
import {
  discoverRepoContextReplaySources,
  readRepoContextReplaySlice,
  type RepoContextReplayCandidate,
  type RepoContextReplaySource,
} from "./repo-context-replay";

type DrainConfig = CollectorConfig["repoContextDrain"];
type Resolver = (request: RepoContextRequest) => RepoContextResult;

export type RepoContextDrainRuntimeConfig = {
  config: DrainConfig;
  captureRoots?: readonly CaptureRoot[];
};

const DEFAULT_REPO_CONTEXT_DRAIN_CONFIG = collectorConfigSchema.parse({}).repoContextDrain;

export type RepoContextDrainStageOptions = {
  config: DrainConfig;
  captureRoots: readonly CaptureRoot[];
  captureElapsedMs: number;
  freshContextsUsed: number;
  freshDeferred: number;
  remainingJobMs: number;
  remainingLookupMs: number;
  resolve?: Resolver;
  now?: () => number;
};

type Candidate = RepoContextReplayCandidate & { source: RepoContextReplaySource };

const digest = (value: unknown) => crypto.createHash("sha256")
  .update(JSON.stringify(value)).digest("hex");

function sourceCursor(receipt: RepoContextDrainReceipt, source: RepoContextReplaySource, position: string) {
  receipt.sourceCursor = {
    sourceKey: source.sourceKey,
    sourceDigest: source.sourceDigest,
    position,
  };
}

function writeReceipt(
  database: LocalEventBuffer["database"],
  receipt: RepoContextDrainReceipt,
  now: () => number,
  started: number,
) {
  receipt.elapsedMs = Math.max(0, now() - started);
  receipt.sliceCutShort = receipt.scanBudgetExhausted || receipt.lookupBudgetExhausted ||
    receipt.contextBudgetExhausted || receipt.cwdBudgetExhausted ||
    receipt.status === "inventory_incomplete";
  writeRepoContextDrainReceipt(database, receipt);
  return receipt;
}

function activePendingContext(database: LocalEventBuffer["database"], contextId: string) {
  return Boolean(database.prepare(
    `select 1
     from repo_context_event_links l
     where l.context_id = ? and l.context_conflict = 0
       and (
         (l.fill_pending = 1 and not exists (
           select 1 from repo_context_results r where r.context_id = l.context_id
         )) or exists (
           select 1 from repo_context_link_dispositions d
           where d.event_id = l.event_id and d.context_id = l.context_id
             and d.expired_at is not null and d.re_resolved_at is null
         )
       )
       and not exists (select 1 from repo_context_suppressions s where s.context_id = l.context_id)
       and not exists (select 1 from repo_context_inflight i where i.context_id = l.context_id)
       and not exists (select 1 from repo_context_handoffs h where h.context_id = l.context_id)
     limit 1`,
  ).get(contextId));
}

function recordCandidateOutcome(
  database: LocalEventBuffer["database"],
  candidate: Candidate,
  passId: string,
  outcome: "not_pending" | "success" | "boundary_unavailable" |
    "resolution_failed" | "worker_crash" | "replay_exhausted",
) {
  recordRepoContextReplayAttempt(database, {
    sourceKey: candidate.sourceKey,
    sourceDigest: candidate.sourceDigest,
    position: candidate.position,
    contextId: candidate.request.contextId,
    passId,
    outcome,
  });
}

function reserveCandidates(
  database: LocalEventBuffer["database"],
  candidates: Candidate[],
  passId: string,
) {
  const reserve = database.prepare(
    `insert into repo_context_inflight (context_id, started_at, owner)
     select @contextId, @at, 'child'
     where not exists (select 1 from repo_context_results where context_id = @contextId)
       and not exists (select 1 from repo_context_suppressions where context_id = @contextId)
       and not exists (select 1 from repo_context_inflight where context_id = @contextId)
       and not exists (select 1 from repo_context_handoffs where context_id = @contextId)
       and exists (
         select 1 from repo_context_event_links l
         where l.context_id = @contextId and l.fill_pending = 1 and l.context_conflict = 0
       )`,
  );
  return database.transaction(() => candidates.filter((candidate) => {
    const at = new Date().toISOString();
    const inserted = reserve.run({ contextId: candidate.request.contextId, at }).changes;
    const replayingExpired = inserted === 0 && Boolean(database.prepare(
      `select 1 from repo_context_results r
       where r.context_id = ? and exists (
         select 1 from repo_context_link_dispositions d
         where d.context_id = r.context_id and d.expired_at is not null
           and d.re_resolved_at is null
       )`,
    ).get(candidate.request.contextId));
    if (inserted === 0 && !replayingExpired) return false;
    recordRepoContextReplayAttempt(database, {
      sourceKey: candidate.sourceKey,
      sourceDigest: candidate.sourceDigest,
      position: candidate.position,
      contextId: candidate.request.contextId,
      passId,
      outcome: "started",
      at,
    });
    return true;
  })).immediate();
}

function applyGroup(
  buffer: LocalEventBuffer,
  group: Candidate[],
  result: RepoContextResult,
) {
  const results = group.map((candidate) => ({
    ...result,
    contextId: candidate.request.contextId,
    resolvedAt: new Date().toISOString(),
    resolverVersion: REPO_CONTEXT_RESOLVER_VERSION,
  }));
  for (let offset = 0; offset < results.length; offset += 8) {
    buffer.applyRepoContextResults(results.slice(offset, offset + 8));
  }
  return results.reduce(
    (total, item) => total + reResolveExpiredRepoContextLinks(buffer.database, item),
    0,
  );
}

export function runRepoContextDrainStage(
  buffer: LocalEventBuffer,
  options: RepoContextDrainStageOptions,
): RepoContextDrainReceipt {
  ensureRepoContextReplaySchema(buffer.database);
  ensureRepoContextLinkDispositionSchema(buffer.database);
  if (!options.config.enabled) {
    const disabled = disabledRepoContextDrainReceipt();
    writeRepoContextDrainReceipt(buffer.database, disabled);
    return disabled;
  }
  const now = options.now ?? (() => performance.now());
  const started = now();
  const receipt: RepoContextDrainReceipt = {
    ...disabledRepoContextDrainReceipt(),
    runId: crypto.randomUUID(),
    status: "ran",
  };
  const lookupSlots = Math.max(0, Math.min(
    options.config.maxDistinctCwdsPerRun,
    8 - Math.max(0, Math.trunc(options.freshContextsUsed)),
  ));
  if (options.freshDeferred > 0 || lookupSlots === 0 || options.remainingLookupMs <= 0) {
    receipt.status = "yielded_fresh";
    receipt.lookupBudgetExhausted = lookupSlots === 0 || options.remainingLookupMs <= 0;
    return writeReceipt(buffer.database, receipt, now, started);
  }
  receipt.unresolvedContexts.worker_crash += recoverStartedRepoContextReplayAttempts(
    buffer.database,
    options.config.maxContextsPerRun,
  );
  const scanAllowance = Math.max(0, Math.min(
    options.config.scanSliceMs,
    AUTOMATIC_CAPTURE_LIMITS.maxWallMs - Math.max(0, options.captureElapsedMs),
    options.remainingJobMs,
  ));
  if (scanAllowance <= 0) {
    receipt.scanBudgetExhausted = true;
    return writeReceipt(buffer.database, receipt, now, started);
  }
  const discovery = discoverRepoContextReplaySources(options.captureRoots, {
    deadlineMs: scanAllowance,
    now,
  });
  receipt.rowsInspected = 0;
  receipt.unavailableSourceRoots = discovery.unavailableRoots;
  receipt.unavailableSourceEntries = discovery.unavailableEntries;
  if (discovery.unavailableRoots > 0 || discovery.unavailableEntries > 0) {
    receipt.status = "inventory_incomplete";
    receipt.scanBudgetExhausted = discovery.budgetExhausted;
    return writeReceipt(buffer.database, receipt, now, started);
  }
  if (discovery.sources.length === 0) {
    receipt.status = "no_sources";
    receipt.scanBudgetExhausted = !discovery.complete;
    if (discovery.complete) {
      const inventoryDigest = digest([]);
      const pass = beginRepoContextReplayPass(buffer.database, inventoryDigest);
      receipt.replayPassId = pass.passId;
      completeRepoContextReplayPass(buffer.database, pass.passId);
      if (options.config.expireEnabled) {
        receipt.expiredLinks = expireRepoContextLinks(buffer.database, {
          completedPasses: completedRepoContextReplayPasses(buffer.database, inventoryDigest),
          expireAfterCompletePasses: options.config.expireAfterCompletePasses,
          limit: options.config.expireLinksPerRun,
        });
      }
    }
    return writeReceipt(buffer.database, receipt, now, started);
  }
  if (!discovery.complete) {
    receipt.scanBudgetExhausted = true;
    return writeReceipt(buffer.database, receipt, now, started);
  }
  const inventoryDigest = digest(discovery.sources.map((source) => [source.sourceKey, source.sourceDigest]));
  const pass = beginRepoContextReplayPass(buffer.database, inventoryDigest);
  receipt.replayPassId = pass.passId;
  const candidates: Candidate[] = [];
  const groups = new Map<string, Candidate[]>();
  const slices: Array<{
    source: RepoContextReplaySource;
    nextPosition: string;
    complete: boolean;
    candidates: Candidate[];
  }> = [];
  const maxContexts = Math.min(64, options.config.maxContextsPerRun);

  scan: for (const source of discovery.sources) {
    if (now() - started >= scanAllowance) {
      receipt.scanBudgetExhausted = true;
      break;
    }
    const prior = repoContextReplayCursor(buffer.database, {
      sourceKey: source.sourceKey,
      sourceDigest: source.sourceDigest,
      passId: pass.passId,
    });
    if (prior?.status === "complete") continue;
    const slice = readRepoContextReplaySlice(buffer, source, prior?.position ?? null, {
      maxContexts: Math.max(1, maxContexts - candidates.length),
    });
    if (slice.generationChanged) {
      receipt.sourceGenerationsChanged += 1;
      receipt.scanBudgetExhausted = true;
      break;
    }
    for (const terminal of slice.terminalRecords) {
      recordRepoContextReplayAttempt(buffer.database, {
        sourceKey: source.sourceKey,
        sourceDigest: source.sourceDigest,
        position: terminal.position,
        contextId: `record:v1:${digest([
          source.sourceKey, source.sourceDigest, terminal.position,
        ])}`,
        passId: pass.passId,
        outcome: terminal.outcome,
      });
      receipt.oversizedSourceRecords += 1;
    }
    const sourceCandidates = slice.candidates.map((candidate) => ({ ...candidate, source }));
    slices.push({ source, nextPosition: slice.nextPosition, complete: slice.complete,
      candidates: sourceCandidates });
    receipt.rowsInspected += slice.rowsInspected;
    for (const candidate of sourceCandidates) {
      if (!activePendingContext(buffer.database, candidate.request.contextId)) {
        recordCandidateOutcome(buffer.database, candidate, pass.passId, "not_pending");
        continue;
      }
      const key = `${candidate.request.source}\u0000${candidate.request.cwd}`;
      if (!groups.has(key) && groups.size >= lookupSlots) {
        receipt.cwdBudgetExhausted = true;
        break scan;
      }
      if (candidates.length >= maxContexts) {
        receipt.contextBudgetExhausted = true;
        break scan;
      }
      const group = groups.get(key) ?? [];
      group.push(candidate);
      groups.set(key, group);
      candidates.push(candidate);
    }
    if (!slice.complete && slice.candidates.length === 0) {
      advanceRepoContextReplayCursor(buffer.database, {
        sourceKey: source.sourceKey,
        sourceDigest: source.sourceDigest,
        source: source.source,
        position: slice.nextPosition,
        passId: pass.passId,
        status: slice.complete ? "complete" : "active",
      });
      sourceCursor(receipt, source, slice.nextPosition);
      receipt.scanBudgetExhausted = true;
      break;
    }
  }

  receipt.candidateContexts = candidates.length;
  receipt.distinctCwdGroups = groups.size;
  const lookupStarted = now();
  const resolve = options.resolve ?? ((request) => resolveRepoContextRequests([request])[0] ?? ({
    contextId: request.contextId,
    repoHash: null,
    branchHash: null,
    headSha: null,
    resolvedAt: new Date().toISOString(),
    resolverVersion: REPO_CONTEXT_RESOLVER_VERSION,
  }));
  for (const group of groups.values()) {
    if (now() - lookupStarted >= options.remainingLookupMs || now() - started >= options.remainingJobMs) {
      receipt.lookupBudgetExhausted = true;
      break;
    }
    const reserved = reserveCandidates(buffer.database, group, pass.passId);
    if (reserved.length === 0) continue;
    let result: RepoContextResult;
    try { result = resolve(reserved[0]!.request); }
    catch {
      result = {
        contextId: reserved[0]!.request.contextId,
        repoHash: null,
        branchHash: null,
        headSha: null,
        resolvedAt: new Date().toISOString(),
        resolverVersion: REPO_CONTEXT_RESOLVER_VERSION,
      };
    }
    receipt.reResolvedExpiredLinks += applyGroup(buffer, reserved, result);
    const outcome = result.repoHash ? "success" : "resolution_failed";
    for (const candidate of reserved) {
      recordCandidateOutcome(buffer.database, candidate, pass.passId, outcome);
    }
    if (result.repoHash) receipt.successfulContexts += reserved.length;
    else {
      receipt.unresolvedContexts.resolution_failed += reserved.length;
      for (const candidate of reserved) {
        recordRepoContextFailureDispositions(buffer.database, {
          contextId: candidate.request.contextId,
          reason: "resolution_failed",
          resolverVersion: REPO_CONTEXT_RESOLVER_VERSION,
        });
      }
    }
  }
  buffer.drainRepoContextFills(256);

  for (const slice of slices) {
    let position: string | null = null;
    let allCommitted = true;
    for (const candidate of slice.candidates) {
      const attempt = buffer.database.prepare(
        `select outcome from repo_context_replay_attempts
         where source_key = ? and source_digest = ? and position = ?
           and context_id = ? and pass_id = ?`,
      ).get(candidate.sourceKey, candidate.sourceDigest, candidate.position,
        candidate.request.contextId, pass.passId) as { outcome: string } | undefined;
      if (!attempt || attempt.outcome === "started") {
        allCommitted = false;
        break;
      }
      position = candidate.nextPosition;
    }
    if (slice.candidates.length === 0) position = slice.nextPosition;
    if (!position) continue;
    const finalPosition = allCommitted ? slice.nextPosition : position;
    advanceRepoContextReplayCursor(buffer.database, {
      sourceKey: slice.source.sourceKey,
      sourceDigest: slice.source.sourceDigest,
      source: slice.source.source,
      position: finalPosition,
      passId: pass.passId,
      status: allCommitted && slice.complete ? "complete" : "active",
    });
    sourceCursor(receipt, slice.source, finalPosition);
  }

  const allComplete = discovery.sources.every((source) =>
    repoContextReplayCursor(buffer.database, {
      sourceKey: source.sourceKey,
      sourceDigest: source.sourceDigest,
      passId: pass.passId,
    })?.status === "complete");
  if (allComplete) {
    completeRepoContextReplayPass(buffer.database, pass.passId);
    if (options.config.expireEnabled) {
      receipt.expiredLinks = expireRepoContextLinks(buffer.database, {
        completedPasses: completedRepoContextReplayPasses(buffer.database, inventoryDigest),
        expireAfterCompletePasses: options.config.expireAfterCompletePasses,
        limit: options.config.expireLinksPerRun,
      });
    }
  }
  return writeReceipt(buffer.database, receipt, now, started);
}

function defaultCaptureRoots(): CaptureRoot[] {
  return [
    { rootId: "default-codex", profileId: "default", installationEpochId: "local",
      source: "codex", directory: path.join(os.homedir(), ".codex", "sessions") },
    { rootId: "default-claude", profileId: "default", installationEpochId: "local",
      source: "claude_code", directory: path.join(os.homedir(), ".claude", "projects") },
  ];
}

export function runConfiguredRepoContextDrainStage(
  buffer: LocalEventBuffer,
  runtime: Omit<RepoContextDrainStageOptions, "config" | "captureRoots" | "resolve">,
  configured?: RepoContextDrainRuntimeConfig,
) {
  return runRepoContextDrainStage(buffer, {
    ...runtime,
    config: configured?.config ?? DEFAULT_REPO_CONTEXT_DRAIN_CONFIG,
    captureRoots: configured?.captureRoots ?? defaultCaptureRoots(),
  });
}
