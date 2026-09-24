import { randomUUID } from "node:crypto";

import { fetchCollectorUrl } from "./http-transport";
import { HOOK_AUTHORITY_CONTRACT } from "./hook-authority";
import {
  blankForbiddenRawContent,
  hookSpoolEnabled,
  resolveHookSpoolHome,
  writeHookSpoolFile,
  type HookSpoolBounds,
} from "./hook-spool";
import type { LocalIngestAuth } from "./local-auth";
import { isUuid } from "./normalizer";

type CommandHookSource = "claude_code" | "codex" | "grok";

const HOOK_PATHS: Record<CommandHookSource, string> = {
  claude_code: "/hooks/claude-code",
  codex: "/hooks/codex",
  grok: "/hooks/grok",
};

const EVENT_ID_ALIASES = HOOK_AUTHORITY_CONTRACT.eventId.aliases;

/**
 * Give a hook body a UUID the ledger can dedupe, without overwriting one it
 * already carries.
 *
 * Real Claude/Codex/Grok hook payloads have no `id`/`eventId`/`event_id`, so
 * `normalizeHookPayload` used to mint a fresh UUID on every admit. A live
 * post that committed, then an ECONNRESET, then a spool replay of the same
 * body, became two `buffered_events` rows (review r1 of eco-6hoxj.61, F2).
 * Minting here, once, before the first attempt, puts the same id on the live
 * wire and in the spool file. The ledger's `insert or ignore` then keeps the
 * replay at one row. A body the JSON parser cannot turn into an object is
 * left alone — the collector still rejects it on its merits.
 */
export function ensureHookEventId(body: string): string {
  let parsed: unknown;
  try {
    parsed = JSON.parse(body);
  } catch {
    return body;
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return body;
  const record = parsed as Record<string, unknown>;
  for (const alias of EVENT_ID_ALIASES) {
    const value = record[alias];
    if (typeof value === "string" && isUuid(value.trim())) return body;
  }
  const minted = randomUUID();
  if (!Object.prototype.hasOwnProperty.call(record, "id")) record.id = minted;
  else if (!Object.prototype.hasOwnProperty.call(record, "eventId")) record.eventId = minted;
  else record.event_id = minted;
  return JSON.stringify(record);
}

/** Unknown-outcome replay is safe only when the wire body carries a UUID. */
function bodyHasStableEventId(body: string): boolean {
  let parsed: unknown;
  try {
    parsed = JSON.parse(body);
  } catch {
    return false;
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return false;
  const record = parsed as Record<string, unknown>;
  return EVENT_ID_ALIASES.some((alias) => {
    const value = record[alias];
    return typeof value === "string" && isUuid(value.trim());
  });
}

/**
 * The spool is EXACTLY-ONCE for the `forward-hook-http` client: a spooled
 * replay of a body this process already posted cannot become a second ledger
 * row, because the body carries a client-minted event id and the ledger
 * ignores a duplicate primary key.
 *
 * That is why this set can include outcomes that do not prove the collector
 * stored nothing:
 *
 *   503  the ledger stayed contended past the 750 ms retry budget, so the
 *        durable append never happened;
 *   408  `request_deadline_exceeded` — the collector gave up while reading the
 *        request body, or ran out of its 1.5 s budget before the append, so
 *        nothing was admitted;
 *   ECONNREFUSED  nothing was listening, so there was no request at all;
 *   ECONNRESET / UND_ERR_SOCKET  the socket died after the request was sent —
 *        the collector may already have committed the row (review r1, F2);
 *   request timeouts (ETIMEDOUT, undici header/body/connect timeouts,
 *        AbortError / ABORT_ERR)  the client gave up with the same ambiguity.
 *
 * A body that cannot carry a UUID stays visible on an unknown-outcome failure:
 * replaying it would make the collector mint a different id. Known no-admit
 * outcomes (408, 503, and ECONNREFUSED) can still spool such a body because
 * the first attempt cannot have committed it.
 */
const SPOOLABLE_CONNECTION_CODES = new Set([
  "ECONNREFUSED",
  "ECONNRESET",
  "UND_ERR_SOCKET",
  "ETIMEDOUT",
  "UND_ERR_HEADERS_TIMEOUT",
  "UND_ERR_BODY_TIMEOUT",
  "UND_ERR_CONNECT_TIMEOUT",
  "AbortError",
  "ABORT_ERR",
]);

/** Statuses that prove the collector admitted nothing. See the note above. */
const SPOOLABLE_STATUSES = new Set([408, 503]);

function connectionErrorCode(error: unknown): string | undefined {
  let cursor: unknown = error;
  for (let depth = 0; depth < 4 && cursor && typeof cursor === "object"; depth += 1) {
    const code = (cursor as { code?: unknown }).code;
    if (typeof code === "string") return code;
    if ((cursor as { name?: unknown }).name === "AbortError") return "AbortError";
    cursor = (cursor as { cause?: unknown }).cause;
  }
  return undefined;
}

function producerToken(auth: LocalIngestAuth, source: CommandHookSource) {
  if (source === "claude_code") return auth.claudeCodeProducer;
  if (source === "codex") return auth.codexProducer;
  return auth.grokProducer;
}

export type ForwardHookResult =
  | { accepted: true }
  | { spooled: true; path: string };

/**
 * Forward one hook body over loopback while keeping producer credentials out
 * of the managed command and process argv. Callers load auth internally.
 *
 * Bead eco-6hoxj.61 / .65: an outcome that used to lose the event — 503 (busy
 * ledger), 408 (`request_deadline_exceeded`), ECONNREFUSED (the collector is
 * restarting), or an unknown-outcome socket death (ECONNRESET, UND_ERR_SOCKET,
 * request timeout) — spools the event under the Plimsoll home instead of
 * throwing, so the hook process exits 0 and the collector applies it on its
 * next drain. The body carries a client-minted event id on the live path and
 * in the spool file, so a replay of a request the collector already committed
 * is one `buffered_events` row, not two. Every other non-202 — every other
 * 4xx and every other 5xx — still throws `hook_forward_http_rejected:<status>`:
 * a body the collector refuses on its merits must stay visible, not accumulate
 * on disk.
 */
export async function forwardHookOverLoopback(
  body: string,
  options: {
    source: CommandHookSource;
    port: number;
    auth: LocalIngestAuth;
    fetchImpl?: typeof fetch;
    /** Proof-injectable directory bounds. Production uses HOOK_SPOOL_LIMITS. */
    spoolLimits?: Partial<HookSpoolBounds>;
    env?: NodeJS.ProcessEnv;
  },
): Promise<ForwardHookResult> {
  const token = producerToken(options.auth, options.source);
  if (!token) throw new Error("hook_forward_producer_token_unavailable");
  const env = options.env ?? process.env;
  // Mint once, before the first attempt, so the live POST and a later spool
  // replay are the same row. A body that already carries a UUID is unchanged.
  const wireBody = ensureHookEventId(body);
  // The spool carries the body and nothing else. The token stays in this
  // process, exactly as it does on the live request.
  const spool = (requireStableId = false) => {
    if (!hookSpoolEnabled(env)) return null;
    if (requireStableId && !bodyHasStableEventId(wireBody)) return null;
    // Suppressed BEFORE the write: the spool is a local write, and
    // `docs/privacy-spec.md` ("Where captured data rests on disk") holds this
    // content out of every local write. The rule is the collector's own —
    // raw content, credential-like names and paths alike — minus the few keys
    // it reads before suppressing them (`SPOOL_DERIVATION_INPUT_KEYS`). The
    // keys survive, so the collector's own suppression still sees them on
    // replay and the recovered row is unchanged.
    const blanked = blankForbiddenRawContent(wireBody);
    if (!blanked) return null;
    // Resolving the home is inside the guard (review r1, F8): a
    // CollectorHomeError here must fall through to the original 503 /
    // connection error the caller is about to surface, not replace it.
    let home: string;
    try {
      home = resolveHookSpoolHome(env);
    } catch {
      return null;
    }
    return writeHookSpoolFile({
      home,
      source: options.source,
      body: blanked.text,
      blanked: blanked.blanked,
      limits: options.spoolLimits,
    });
  };

  let response: Response;
  try {
    response = await fetchCollectorUrl(
      `http://127.0.0.1:${options.port}${HOOK_PATHS[options.source]}`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-plimsoll-source": options.source,
          "x-plimsoll-token": token,
        },
        body: wireBody,
      },
      options.fetchImpl,
    );
  } catch (error) {
    const code = connectionErrorCode(error);
    if (code && SPOOLABLE_CONNECTION_CODES.has(code)) {
      const spooled = spool(code !== "ECONNREFUSED");
      if (spooled) return { spooled: true, path: spooled.path };
    }
    throw error;
  }

  if (SPOOLABLE_STATUSES.has(response.status)) {
    const spooled = spool();
    if (spooled) return { spooled: true, path: spooled.path };
  }
  if (response.status !== 202) {
    throw new Error(`hook_forward_http_rejected:${response.status}`);
  }
  return { accepted: true };
}
