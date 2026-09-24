import type Database from "better-sqlite3";

import type { AiInteractionEvent } from "../../shared/src/index";
import {
  markSessionContextIndexInvalid,
  sessionContextIndexComplete,
} from "./session-context-index";

/**
 * Session inheritance is deliberately a small, fail-closed join.  The
 * upload path never searches by cwd or scans the whole ledger: it can only
 * use repo_hash values that repo-context resolution already persisted for the
 * same session inside this fixed time window.
 */
export const SESSION_INHERIT_WINDOW_MS = 6 * 60 * 60 * 1_000;
export const SESSION_INHERIT_MAX_CONTEXT_ROWS = 256;
/**
 * Rows one lookup may examine.  Once the capture-time context index covers
 * the ledger (session-context-index.ts), a lookup reads only context rows, so
 * the bound applies to those and the session's other events cost nothing.
 * Until then (0.7.36): `idx_events_session` does not hold repo_hash, so every
 * index entry a lookup walks past can cost a ledger-row read, and one busy
 * session can hold hundreds of thousands of repo-less rows inside a single
 * window.  Such a lookup counts at most this many index entries (covering
 * index, no row reads) and reads rows only for a window known to fit; a
 * larger window fails closed to `unallocated`.
 */
export const SESSION_INHERIT_MAX_SCANNED_ROWS = 4_096;
/**
 * Rows one upload batch may read across all of its session lookups: ledger
 * rows on the 0.7.36 path, context rows kept from the index (plus at most one
 * probe row per lookup) on the index path.
 */
export const SESSION_INHERIT_MAX_BATCH_ROW_READS = 65_536;

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

export type SessionAttributionInput = {
  event: AiInteractionEvent;
  repoHash?: string | null;
};

export type SessionAttributionStats = {
  /** True when lookups read the capture-time context index. */
  contextIndex: boolean;
  /** Bounded lookups issued: one per session per six hours of batch span. */
  lookups: number;
  /** Session-index entries counted from the covering index (0.7.36 path). */
  indexEntries: number;
  /** Ledger rows read to collect repo contexts (0.7.36 path). */
  rowReads: number;
  /** Context-index rows read, probe rows included (index path). */
  contextRows: number;
  /** Lookups whose window held more entries than the scan bound. */
  boundReached: number;
  /** Lookups skipped because the batch row-read budget was spent. */
  budgetExhausted: number;
  /** Indexed lookups that found a source-row point mismatch. */
  integrityFailures: number;
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

/**
 * A batch joins each of its events against the same context rows, so a row's
 * parsed time and canonical repo are computed once per row object rather than
 * once per event. Context rows are never mutated after they are read.
 */
const contextFacts = new WeakMap<SessionRepoContext, { at: number | null; repoHash: string | null }>();

function factsOf(context: SessionRepoContext) {
  let facts = contextFacts.get(context);
  if (!facts) {
    facts = { at: parseObservedAt(context.observedAt), repoHash: canonicalLinkage(context.repoHash) };
    contextFacts.set(context, facts);
  }
  return facts;
}

function sortedContexts(
  event: AiInteractionEvent,
  contexts: readonly SessionRepoContext[],
) {
  const eventAt = parseObservedAt(event.observedAt);
  if (eventAt === null || !event.sessionId) return [];
  const sorted: Array<SessionRepoContext & { at: number }> = [];
  for (const context of contexts) {
    if (context.sessionId !== event.sessionId) continue;
    const { at, repoHash } = factsOf(context);
    if (at === null || repoHash === null || Math.abs(at - eventAt) > SESSION_INHERIT_WINDOW_MS) continue;
    sorted.push({ ...context, repoHash, at });
  }
  return sorted.sort((left, right) => left.at - right.at || left.rowid - right.rowid);
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

function needsSessionLookup(event: SessionContextEvent, repoHash?: string | null) {
  // Most buffered rows are spans/tool results without tokens.  Do not even
  // issue a session query for them (or for a row that already has a project);
  // this keeps millions of ordinary `otel_span` rows out of the attribution
  // path while retaining the indexed bounded lookup for token rows.
  return Boolean(
    event.sessionId &&
    !event.projectKey &&
    !canonicalLinkage(repoHash) &&
    isTokenBearing(event),
  );
}

type SessionWindow = { at: number; lower: string; upper: string };

function sessionWindow(observedAt: string): SessionWindow | null {
  const at = parseObservedAt(observedAt);
  if (at === null) return null;
  const lower = new Date(at - SESSION_INHERIT_WINDOW_MS);
  const upper = new Date(at + SESSION_INHERIT_WINDOW_MS);
  if (Number.isNaN(lower.getTime()) || Number.isNaN(upper.getTime())) return null;
  return { at, lower: lower.toISOString(), upper: upper.toISOString() };
}

/**
 * First row whose observed_at is >= `bound` (> `bound` when `after`).  Rows
 * arrive in SQLite BINARY order; `bound` is ASCII (toISOString), and against
 * an ASCII operand UTF-16 comparison orders exactly like SQLite's UTF-8 memcmp.
 */
function searchObservedAt(
  rows: readonly SessionRepoContext[],
  bound: string,
  after: boolean,
) {
  let low = 0;
  let high = rows.length;
  while (low < high) {
    const middle = (low + high) >>> 1;
    const value = rows[middle]!.observedAt;
    if (value < bound || (after && value === bound)) low = middle + 1;
    else high = middle;
  }
  return low;
}

function boundOption(value: number | undefined, max: number) {
  return Number.isFinite(value) ? Math.max(0, Math.min(Math.trunc(value!), max)) : max;
}

// 0.7.36 path.  Both inner selects carry a hard LIMIT; repo_hash and the
// other context predicates are applied outside them.  The count touches only
// the covering index, so an oversized window costs scanLimit + 1 index
// entries and no ledger-row reads.
const COUNT_SESSION_ENTRIES =
  `select count(*) as entries from (
     select 1 from buffered_events indexed by idx_events_session
     where session_id = ? and observed_at >= ? and observed_at <= ?
     limit ?
   )`;
const READ_SESSION_CONTEXTS =
  `select rowid, session_id as sessionId, observed_at as observedAt,
     repo_hash as repoHash
   from (
     select rowid, session_id, observed_at, repo_hash, data_mode,
       privacy_disposition
     from buffered_events indexed by idx_events_session
     where session_id = ? and observed_at >= ? and observed_at <= ?
     order by observed_at asc, rowid asc
     limit ?
   )
   where repo_hash is not null
     and data_mode <> 'evidence' and privacy_disposition is null
   order by observed_at asc, rowid asc`;
// Index path.  session_repo_contexts holds exactly the rows the outer filter
// above keeps, in the same order, so one bounded range read per lookup
// returns what READ_SESSION_CONTEXTS returns for a window that fits.
const READ_INDEXED_CONTEXTS =
  `select c.source_rowid as rowid, c.session_id as sessionId,
     c.observed_at as observedAt, c.repo_hash as repoHash,
     exists (
       select 1 from buffered_events e where e.rowid = c.source_rowid
         and e.session_id is c.session_id and e.observed_at is c.observed_at
         and e.repo_hash is c.repo_hash and e.data_mode <> 'evidence'
         and e.privacy_disposition is null
     ) as sourceValid
   from session_repo_contexts c
   where c.session_id = ? and c.observed_at >= ? and c.observed_at <= ?
   order by c.observed_at asc, c.source_rowid asc
   limit ?`;

type IndexedSessionRepoContext = SessionRepoContext & { sourceValid: number };

/**
 * Rows from better-sqlite3 are several times slower to read and spread than
 * plain objects, and a batch joins every event against them, so each lookup
 * copies its rows once.
 */
function plainContexts(rows: readonly SessionRepoContext[]): SessionRepoContext[] {
  return rows.map(({ rowid, sessionId, observedAt, repoHash }) => ({ rowid, sessionId, observedAt, repoHash }));
}

type SessionLookup = {
  firstAt: number;
  lastAt: number;
  lower: string;
  upper: string;
  /** False when the lookup hit a bound; every event it serves fails closed. */
  complete: boolean;
  rows: SessionRepoContext[];
};

/**
 * Session attribution for one upload batch.  All eligible events are planned
 * up front; each session gets one bounded lookup per six hours of batch span
 * (so a lookup window is at most eighteen hours), and every event is then
 * joined in memory against exactly the rows a query over its own ±6 h window
 * would return.  Results equal the per-event rule whenever the lookup
 * completed.  A lookup whose window holds more rows than the scan bound, or
 * that would overrun the batch read budget, is not used at all and leaves its
 * events `unallocated`: a partial session is never used to guess.
 *
 * When the capture-time context index covers the ledger, a lookup reads the
 * window's context rows from it and never walks the session's other events.
 * Context rows are a subset of the window's session-index entries, so in any
 * batch where the 0.7.36 scan reaches neither bound the index path reaches
 * none either and returns identical results.  `contextIndex: false` forces
 * the 0.7.36 path.
 */
export class SessionAttributionBatch {
  private readonly lookups = new Map<string, SessionLookup[]>();
  private readonly counters: SessionAttributionStats = {
    contextIndex: false,
    lookups: 0,
    indexEntries: 0,
    rowReads: 0,
    contextRows: 0,
    boundReached: 0,
    budgetExhausted: 0,
    integrityFailures: 0,
  };

  constructor(
    db: Database.Database,
    inputs: readonly SessionAttributionInput[],
    options: { maxScannedRows?: number; maxBatchRowReads?: number; contextIndex?: boolean } = {},
  ) {
    const planned = new Map<string, SessionWindow[]>();
    for (const input of inputs) {
      if (!needsSessionLookup(input.event, input.repoHash)) continue;
      const window = sessionWindow(input.event.observedAt);
      if (!window) continue;
      const windows = planned.get(input.event.sessionId!) ?? [];
      windows.push(window);
      planned.set(input.event.sessionId!, windows);
    }
    if (planned.size === 0) return;
    const scanLimit = boundOption(options.maxScannedRows, SESSION_INHERIT_MAX_SCANNED_ROWS);
    let readBudget = boundOption(options.maxBatchRowReads, SESSION_INHERIT_MAX_BATCH_ROW_READS);
    // One read snapshot keeps every count consistent with its row read, and
    // the coverage marker consistent with the index it describes.
    db.transaction(() => {
      let readIndexed: Database.Statement | null = null;
      if (options.contextIndex !== false && sessionContextIndexComplete(db)) {
        try {
          readIndexed = db.prepare(READ_INDEXED_CONTEXTS);
        } catch {
          // The context index is auxiliary. A missing or invalid object on a
          // read-only history connection must select the bounded ledger scan,
          // never make history upload fail.
          readIndexed = null;
        }
      }
      const countEntries = readIndexed ? null : db.prepare(COUNT_SESSION_ENTRIES);
      const readContexts = readIndexed ? null : db.prepare(READ_SESSION_CONTEXTS);
      this.counters.contextIndex = readIndexed !== null;
      let indexInvalid = false;
      for (const [sessionId, windows] of planned) {
        const lookups: SessionLookup[] = [];
        for (const window of windows.sort((left, right) => left.at - right.at)) {
          const current = lookups[lookups.length - 1];
          if (current && window.at - current.firstAt <= SESSION_INHERIT_WINDOW_MS) {
            current.lastAt = window.at;
            if (window.lower < current.lower) current.lower = window.lower;
            if (window.upper > current.upper) current.upper = window.upper;
          } else {
            lookups.push({
              firstAt: window.at,
              lastAt: window.at,
              lower: window.lower,
              upper: window.upper,
              complete: false,
              rows: [],
            });
          }
        }
        for (const lookup of lookups) {
          this.counters.lookups += 1;
          if (indexInvalid) continue;
          if (readIndexed) {
            // Read one row past the cap so a full window and an oversized one
            // differ. Rows read spend the budget, so a batch reads at most
            // maxBatchRowReads rows plus one probe row per lookup.
            const cap = Math.min(scanLimit, readBudget);
            const rows = readIndexed.all(
              sessionId,
              lookup.lower,
              lookup.upper,
              cap + 1,
            ) as IndexedSessionRepoContext[];
            this.counters.contextRows += rows.length;
            readBudget -= Math.min(readBudget, rows.length);
            if (rows.length > scanLimit) {
              this.counters.boundReached += 1;
              continue;
            }
            if (rows.length > cap) {
              this.counters.budgetExhausted += 1;
              continue;
            }
            // The two O(1) control counters catch missing/orphan cardinality.
            // Point checks keep a count-preserving stale row from authorizing
            // a project. Any invalid row makes the whole lookup fail closed.
            if (rows.some((row) => row.sourceValid !== 1)) {
              // A stale point means the auxiliary row can no longer be
              // trusted. Keep this lookup (and all later lookups in this
              // batch) unallocated, record one failure, and arrange a
              // writable reopen/rebuild. Never fall back to a partial scan
              // after the index has made an integrity claim.
              this.counters.integrityFailures += 1;
              indexInvalid = true;
              markSessionContextIndexInvalid(db, "source_point_mismatch");
              continue;
            }
            lookup.rows = plainContexts(rows);
            lookup.complete = true;
            continue;
          }
          const { entries } = countEntries!.get(
            sessionId,
            lookup.lower,
            lookup.upper,
            scanLimit + 1,
          ) as { entries: number };
          this.counters.indexEntries += entries;
          if (entries > scanLimit) {
            this.counters.boundReached += 1;
            continue;
          }
          if (entries > readBudget) {
            this.counters.budgetExhausted += 1;
            continue;
          }
          readBudget -= entries;
          this.counters.rowReads += entries;
          lookup.rows = entries === 0
            ? []
            : plainContexts(readContexts!.all(sessionId, lookup.lower, lookup.upper, entries) as SessionRepoContext[]);
          lookup.complete = true;
        }
        this.lookups.set(sessionId, lookups);
      }
    })();
  }

  /**
   * Attribute one planned event.  `excludedRowids` names ledger rows the
   * caller privacy-disposed after this batch was read; a fresh query would no
   * longer return them, so they are dropped before the rule is applied.
   */
  attribute(
    event: AiInteractionEvent,
    options: {
      repoHash?: string | null;
      branchHash?: string | null;
      excludedRowids?: ReadonlySet<number>;
    } = {},
  ): ProjectAttributionResult {
    const scan = this.sessionContexts(event, options.repoHash, options.excludedRowids);
    return applyProjectAttribution(event, {
      repoHash: options.repoHash,
      branchHash: options.branchHash,
      sessionContexts: scan.rows,
      sessionContextsTruncated: scan.truncated,
    });
  }

  stats(): SessionAttributionStats {
    return { ...this.counters };
  }

  private sessionContexts(
    event: AiInteractionEvent,
    repoHash: string | null | undefined,
    excludedRowids: ReadonlySet<number> | undefined,
  ): SessionRepoContextScan {
    if (!needsSessionLookup(event, repoHash)) return { rows: [], truncated: false };
    const window = sessionWindow(event.observedAt);
    if (!window) return { rows: [], truncated: false };
    const lookup = this.lookups.get(event.sessionId!)?.find((candidate) =>
      candidate.firstAt <= window.at && window.at <= candidate.lastAt);
    // Not planned, or bound-limited: never guess from a partial session.
    if (!lookup?.complete) return { rows: [], truncated: true };
    const rows = lookup.rows
      .slice(
        searchObservedAt(lookup.rows, window.lower, false),
        searchObservedAt(lookup.rows, window.upper, true),
      )
      .filter((row) => !excludedRowids?.has(row.rowid));
    if (rows.length > SESSION_INHERIT_MAX_CONTEXT_ROWS) return { rows: [], truncated: true };
    return {
      rows: rows.filter((row) => factsOf(row).repoHash !== null),
      truncated: false,
    };
  }
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
