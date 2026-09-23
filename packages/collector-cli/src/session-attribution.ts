import type Database from "better-sqlite3";

import type { AiInteractionEvent } from "../../shared/src/index";

/**
 * Session inheritance is deliberately a small, fail-closed join.  The
 * capture path never searches by cwd or scans the whole ledger: it can only
 * use repo_hash values that repo-context resolution already persisted for the
 * same session inside this fixed time window.
 */
export const SESSION_INHERIT_WINDOW_MS = 6 * 60 * 60 * 1_000;
export const SESSION_INHERIT_MAX_CONTEXT_ROWS = 256;

const CANONICAL_LINKAGE = /^sha256:[a-f0-9]{64}$/i;
const TOKEN_FIELDS = [
  "inputTokens",
  "outputTokens",
  "cacheReadTokens",
  "cacheCreationTokens",
] as const;
type TokenField = (typeof TOKEN_FIELDS)[number];

export type ProjectAttributionBasis =
  | "explicit"
  | "repo_context"
  | "session_inherited"
  | "unallocated";

export type SessionRepoContext = {
  rowid: number;
  sessionId: string;
  observedAt: string;
  repoHash: string;
};

export type SessionRepoContextScan = {
  rows: SessionRepoContext[];
  /** True means the bounded query hit its cap; callers must fail closed. */
  truncated: boolean;
};

export type ProjectAttributionOptions = {
  repoHash?: string | null;
  branchHash?: string | null;
  sessionContexts?: readonly SessionRepoContext[];
  sessionContextsTruncated?: boolean;
};

type SessionContextEvent = Pick<AiInteractionEvent, "sessionId" | "observedAt"> &
  Partial<Pick<AiInteractionEvent, "projectKey" | TokenField>>;

export type ProjectAttributionResult = {
  event: AiInteractionEvent;
  basis: ProjectAttributionBasis;
};

function canonicalLinkage(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const candidate = value.trim().toLowerCase();
  return CANONICAL_LINKAGE.test(candidate) ? candidate : null;
}

function isTokenBearing(event: Pick<AiInteractionEvent, TokenField>) {
  return TOKEN_FIELDS.some((field) => event[field] !== undefined);
}

function eventBasis(event: AiInteractionEvent): ProjectAttributionBasis | null {
  const value = event.metadata?.projectBasis;
  return value === "explicit" || value === "repo_context" ||
      value === "session_inherited" || value === "unallocated"
    ? value
    : null;
}

function withBasis(
  event: AiInteractionEvent,
  basis: ProjectAttributionBasis,
  projectKey?: string,
): AiInteractionEvent {
  const metadata = { ...(event.metadata ?? {}), projectBasis: basis };
  if (projectKey !== undefined) {
    return { ...event, projectKey, metadata };
  }
  const { projectKey: _projectKey, ...withoutProject } = event;
  return { ...withoutProject, metadata } as AiInteractionEvent;
}

function withOwnRepoContext(
  event: AiInteractionEvent,
  repoHash: string,
  branchHash: string | null,
): AiInteractionEvent {
  const metadata: Record<string, unknown> = {
    ...(event.metadata ?? {}),
    projectBasis: "repo_context",
  };
  if (branchHash) metadata.branchHash = branchHash;
  return { ...event, projectKey: repoHash, metadata };
}

function parseObservedAt(value: string) {
  const timestamp = Date.parse(value);
  return Number.isFinite(timestamp) ? timestamp : null;
}

function sortedContexts(
  event: AiInteractionEvent,
  contexts: readonly SessionRepoContext[],
) {
  const eventAt = parseObservedAt(event.observedAt);
  if (eventAt === null || !event.sessionId) return [];
  return contexts
    .filter((context) => {
      if (context.sessionId !== event.sessionId) return false;
      const contextAt = parseObservedAt(context.observedAt);
      return contextAt !== null &&
        Math.abs(contextAt - eventAt) <= SESSION_INHERIT_WINDOW_MS &&
        canonicalLinkage(context.repoHash) !== null;
    })
    .map((context) => ({
      ...context,
      repoHash: canonicalLinkage(context.repoHash)!,
      at: parseObservedAt(context.observedAt)!,
    }))
    .sort((left, right) => left.at - right.at || left.rowid - right.rowid);
}

function inheritedRepo(
  event: AiInteractionEvent,
  contexts: readonly SessionRepoContext[],
  truncated: boolean,
) {
  if (!event.sessionId || truncated) return null;
  const sorted = sortedContexts(event, contexts);
  const distinctRepos = [...new Set(sorted.map((context) => context.repoHash))];
  if (distinctRepos.length === 1) return distinctRepos[0]!;
  if (distinctRepos.length < 2) return null;
  // Multi-repo sessions use only the nearest preceding resolved context;
  // future context is intentionally not used to guess an event's project.
  const eventAt = parseObservedAt(event.observedAt)!;
  const preceding = sorted
    .filter((context) => context.at <= eventAt)
    .sort((left, right) => right.at - left.at || right.rowid - left.rowid)[0];
  return preceding?.repoHash ?? null;
}

/**
 * Read only a bounded, indexed session slice.  A cap hit is observable and
 * causes attribution to remain unallocated instead of guessing from a partial
 * session.  `repo_hash` is the only project evidence accepted here.
 */
export function readSessionRepoContexts(
  db: Database.Database,
  event: SessionContextEvent,
  options: { windowMs?: number; maxRows?: number; repoHash?: string | null } = {},
): SessionRepoContextScan {
  // Most buffered rows are spans/tool results without tokens.  Do not even
  // issue a session query for them (or for a row that already has a project);
  // this keeps millions of ordinary `otel_span` rows out of the attribution
  // path while retaining the indexed bounded lookup for token rows.
  if (
    !event.sessionId ||
    event.projectKey ||
    canonicalLinkage(options.repoHash) ||
    !isTokenBearing(event)
  ) return { rows: [], truncated: false };
  const eventAt = parseObservedAt(event.observedAt);
  if (eventAt === null) return { rows: [], truncated: false };
  const windowMs = Math.max(1, Math.min(
    Number.isFinite(options.windowMs)
      ? Math.trunc(options.windowMs!)
      : SESSION_INHERIT_WINDOW_MS,
    SESSION_INHERIT_WINDOW_MS,
  ));
  const maxRows = Math.max(1, Math.min(
    Number.isFinite(options.maxRows)
      ? Math.trunc(options.maxRows!)
      : SESSION_INHERIT_MAX_CONTEXT_ROWS,
    SESSION_INHERIT_MAX_CONTEXT_ROWS,
  ));
  const lower = new Date(eventAt - windowMs).toISOString();
  const upper = new Date(eventAt + windowMs).toISOString();
  const rows = db
    .prepare(
      `select rowid, session_id as sessionId, observed_at as observedAt,
         repo_hash as repoHash
       from buffered_events
       where session_id = ? and repo_hash is not null
         and data_mode <> 'evidence' and privacy_disposition is null
         and observed_at >= ? and observed_at <= ?
       order by observed_at asc, rowid asc
       limit ?`,
    )
    .all(event.sessionId, lower, upper, maxRows + 1) as Array<SessionRepoContext>;
  const truncated = rows.length > maxRows;
  return {
    rows: rows.slice(0, maxRows).filter((row) => canonicalLinkage(row.repoHash) !== null),
    truncated,
  };
}

/**
 * Apply direct repo linkage first, then bounded session inheritance.  Explicit
 * payload projects always win.  A prior generated marker is reusable only when
 * the current verified evidence agrees with its key; otherwise it is treated
 * as explicit and is never overwritten.
 */
export function applyProjectAttribution(
  event: AiInteractionEvent,
  options: ProjectAttributionOptions = {},
): ProjectAttributionResult {
  const basis = eventBasis(event);
  const ownRepo = canonicalLinkage(options.repoHash);
  const replaceable = basis === "repo_context" || basis === "session_inherited";
  // An event that carries a project without one of the collector's generated
  // markers is explicit. Return before touching any supplied session slice.
  if (event.projectKey && !replaceable) {
    return { event: withBasis(event, "explicit", event.projectKey), basis: "explicit" };
  }
  const inherited = inheritedRepo(
    event,
    options.sessionContexts ?? [],
    options.sessionContextsTruncated ?? false,
  );
  if (event.projectKey) {
    // A producer-owned project always wins. A generated marker is reusable
    // only when the current verified evidence agrees with that exact key;
    // this prevents a forged `projectBasis` from authorizing an overwrite.
    const existing = canonicalLinkage(event.projectKey);
    if (ownRepo && existing === ownRepo) {
      return { event: withOwnRepoContext(event, ownRepo, canonicalLinkage(options.branchHash)), basis: "repo_context" };
    }
    if (!ownRepo && inherited && existing === inherited) {
      return { event: withBasis(event, "session_inherited", inherited), basis: "session_inherited" };
    }
    return { event: withBasis(event, "explicit", event.projectKey), basis: "explicit" };
  }

  const branchHash = canonicalLinkage(options.branchHash);
  if (ownRepo) {
    return { event: withOwnRepoContext(event, ownRepo, branchHash), basis: "repo_context" };
  }

  if (!isTokenBearing(event)) {
    // Non-token rows do not participate in inheritance and are left alone.
    return { event, basis: basis ?? "unallocated" };
  }

  if (inherited) {
    return { event: withBasis(event, "session_inherited", inherited), basis: "session_inherited" };
  }

  return { event: withBasis(event, "unallocated"), basis: "unallocated" };
}
