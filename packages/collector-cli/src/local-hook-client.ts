import {
  blankForbiddenRawContent,
  hookSpoolEnabled,
  resolveHookSpoolHome,
  writeHookSpoolFile,
  type HookSpoolBounds,
} from "./hook-spool";
import type { LocalIngestAuth } from "./local-auth";

type CommandHookSource = "claude_code" | "codex" | "grok";

const HOOK_PATHS: Record<CommandHookSource, string> = {
  claude_code: "/hooks/claude-code",
  codex: "/hooks/codex",
  grok: "/hooks/grok",
};

/**
 * The spool is AT-MOST-ONCE: a spooled event is never a duplicate.
 *
 * That property is the whole trigger set, and it is why the set is this small.
 * Every member is an outcome in which the collector provably stored nothing:
 *
 *   503  the ledger stayed contended past the 750 ms retry budget, so the
 *        durable append never happened;
 *   408  `request_deadline_exceeded` — the collector gave up while reading the
 *        request body, or ran out of its 1.5 s budget before the append, so
 *        nothing was admitted (this is what Studio0 is losing codex hook posts
 *        to right now);
 *   ECONNREFUSED  nothing was listening, so there was no request at all.
 *
 * ECONNRESET is deliberately NOT here (review r1, F2): a reset can arrive after
 * the server has already committed the row, and there is no dedup to absorb the
 * replay — `normalizer.ts` mints a fresh UUID for any body without an id, which
 * real hook bodies never carry. Spooling it would turn a rare loss into a rare
 * double-count, and a double-counted event inflates cost projections.
 *
 * What is still lost, stated plainly: the in-flight race when the collector
 * dies mid-request. A socket reset or a graceful close after the request was
 * sent (ECONNRESET, UND_ERR_SOCKET) and a request timeout all still throw
 * `hook_forward_http_rejected`-style and lose that one event, because the
 * collector may have stored it. Closing that window needs idempotent replay —
 * client-minted event ids the drain can reuse — which the lead tracks
 * separately.
 */
const SPOOLABLE_CONNECTION_CODES = new Set(["ECONNREFUSED"]);

/** Statuses that prove the collector admitted nothing. See the note above. */
const SPOOLABLE_STATUSES = new Set([408, 503]);

function connectionErrorCode(error: unknown): string | undefined {
  let cursor: unknown = error;
  for (let depth = 0; depth < 4 && cursor && typeof cursor === "object"; depth += 1) {
    const code = (cursor as { code?: unknown }).code;
    if (typeof code === "string") return code;
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
 * Bead eco-6hoxj.61: an outcome that proves the collector stored nothing — 503
 * (busy ledger), 408 (`request_deadline_exceeded`), or ECONNREFUSED (the
 * collector is restarting) — spools the event under the Plimsoll home instead
 * of throwing, so the hook process exits 0 and the collector applies it on its
 * next drain. Every other non-202 — every other 4xx and every other 5xx — still
 * throws `hook_forward_http_rejected:<status>`: a body the collector refuses on
 * its merits must stay visible, not accumulate on disk. The spool is
 * at-most-once; see the note on SPOOLABLE_CONNECTION_CODES for what that buys
 * and what it still loses.
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
  // The spool carries the body and nothing else. The token stays in this
  // process, exactly as it does on the live request.
  const spool = () => {
    if (!hookSpoolEnabled(env)) return null;
    // Suppressed BEFORE the write: the spool is a local write, and
    // `docs/privacy-spec.md` ("Where captured data rests on disk") holds this
    // content out of every local write. The rule is the collector's own —
    // raw content, credential-like names and paths alike — minus the few keys
    // it reads before suppressing them (`SPOOL_DERIVATION_INPUT_KEYS`). The
    // keys survive, so the collector's own suppression still sees them on
    // replay and the recovered row is unchanged.
    const blanked = blankForbiddenRawContent(body);
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
    response = await (options.fetchImpl ?? fetch)(
      `http://127.0.0.1:${options.port}${HOOK_PATHS[options.source]}`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-plimsoll-source": options.source,
          "x-plimsoll-token": token,
        },
        body,
      },
    );
  } catch (error) {
    const code = connectionErrorCode(error);
    if (code && SPOOLABLE_CONNECTION_CODES.has(code)) {
      const spooled = spool();
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
