import crypto from "node:crypto";

export type TransportFailure =
  | "invalid_url" | "insecure_url" | "embedded_credentials" | "redirect_rejected"
  | "origin_mismatch" | "request_too_large" | "response_too_large"
  | "deadline_exceeded" | "network_error" | "invalid_json";

/** Only these symbolic diagnostics may leave the transport boundary. */
export class TransportError extends Error {
  constructor(readonly code: TransportFailure, readonly networkCode: string | null = null) {
    super(`Transport failed: ${code}`);
    this.name = "TransportError";
  }
}

const SAFE_NETWORK_CODES = new Set(["ECONNRESET", "ECONNREFUSED", "ETIMEDOUT", "EAI_AGAIN", "ENETUNREACH", "EHOSTUNREACH", "UND_ERR_SOCKET", "UND_ERR_CONNECT_TIMEOUT", "UND_ERR_HEADERS_TIMEOUT", "UND_ERR_BODY_TIMEOUT", "AbortError"]);
function networkCode(error: unknown): string | null {
  let current = error;
  for (let depth = 0; depth < 3 && current && typeof current === "object"; depth++) {
    const item = current as { code?: unknown; name?: unknown; cause?: unknown };
    for (const value of [item.code, item.name]) if (typeof value === "string" && SAFE_NETWORK_CODES.has(value)) return value;
    current = item.cause;
  }
  return null;
}

export function isLoopbackHostname(hostname: string) {
  const normalized = hostname.toLowerCase().replace(/^\[|\]$/g, "");
  return normalized === "localhost" || normalized === "::1" || /^127(?:\.\d{1,3}){3}$/.test(normalized);
}

export function validatedTransportUrl(raw: string, _label: string) {
  let url: URL;
  try { url = new URL(raw); } catch { throw new TransportError("invalid_url"); }
  if (url.username || url.password) throw new TransportError("embedded_credentials");
  if (url.protocol === "https:" || (url.protocol === "http:" && isLoopbackHostname(url.hostname))) return url;
  throw new TransportError("insecure_url");
}

/** The --dev-loopback-url flag only reaches this machine, written plainly as
 * http(s)://localhost, 127.x.x.x or [::1]. The host as typed must already be
 * the normalized host, so user info, look-alike or percent-encoded names,
 * numeric shortcuts (127.1) and other names that merely resolve to this
 * machine are refused. */
function assertPlainLoopbackUrl(raw: string) {
  const refused = new Error(
    "--dev-loopback-url allows only an http(s) URL on this machine, written plainly: localhost, 127.x.x.x or [::1].",
  );
  if (!/^[\x21-\x7e]+$/.test(raw) || raw.includes("\\")) throw refused;
  const url = validatedTransportUrl(raw, "Development upload URL");
  const authority = /^https?:\/\/([^/?#]*)/i.exec(raw)?.[1] ?? "";
  const typedHost = authority.startsWith("[") ? authority.slice(0, authority.indexOf("]") + 1) : authority.split(":")[0];
  if (!isLoopbackHostname(url.hostname) || typedHost.toLowerCase() !== url.hostname) throw refused;
}

/** An upload URL override (--url) may pick another path on the configured
 * workspace, never another origin: every upload carries that workspace's
 * install key and signature. Without a joined workspace an override is
 * refused, except through the per-invocation --dev-loopback-url flag, which
 * allows this machine only and announces every use on stderr and in the
 * command's output. Returns the chosen URL unchanged. */
export function pinnedUploadUrl(
  configuredUrl: string | undefined,
  overrideUrl: string | undefined,
  options: { developmentLoopback?: boolean; log?: (line: string) => void } = {},
) {
  if (overrideUrl && options.developmentLoopback) assertPlainLoopbackUrl(overrideUrl);
  if (overrideUrl && !configuredUrl) {
    if (!options.developmentLoopback) {
      throw new Error(
        "Upload URL override needs a joined workspace; run plimsoll join first (for a test server on this machine, add --dev-loopback-url).",
      );
    }
    const origin = new URL(overrideUrl).origin;
    console.warn(
      `WARNING: --dev-loopback-url is sending this collector's upload credentials to ${origin} without a joined workspace. Use it for local development only.`,
    );
    (options.log ?? console.log)(JSON.stringify({ status: "development_upload_url_used", origin, joinedWorkspace: false }));
  }
  if (overrideUrl && configuredUrl && validatedTransportUrl(overrideUrl, "Upload URL").origin !==
      validatedTransportUrl(configuredUrl, "Configured upload URL").origin) {
    throw new Error("Upload URL must use the same origin as the configured workspace audience.");
  }
  return overrideUrl ?? configuredUrl;
}

export function assertNoRedirect(response: Response, _label: string, expectedOrigin: string) {
  if (response.redirected || (response.status >= 300 && response.status < 400)) {
    throw new TransportError("redirect_rejected");
  }
  if (response.url && new URL(response.url).origin !== expectedOrigin) throw new TransportError("origin_mismatch");
}

export const MAX_POST_BYTES = 1_500_000;
export const MAX_RESPONSE_BYTES = 256 * 1024;
export const DEFAULT_POST_TIMEOUT_MS = 30_000;

export type JsonPostOptions = {
  url: string;
  body: string;
  headers?: Record<string, string>;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
  maxRequestBytes?: number;
  maxResponseBytes?: number;
};

export type JsonPostResult = { ok: boolean; status: number; headers: Headers; body: unknown };

/** One bounded deadline covers connection, headers, streaming and JSON parsing.
 * The explicit race also bounds injected fetch/streams which ignore AbortSignal.
 * Never follow a redirect carrying a key, signature, token, or request body.
 */
export async function postJson(input: JsonPostOptions): Promise<JsonPostResult> {
  const url = validatedTransportUrl(input.url, "POST URL");
  const bounded = (value: number | undefined, max: number) =>
    value !== undefined && Number.isFinite(value) && value > 0 ? Math.min(Math.ceil(value), max) : max;
  if (Buffer.byteLength(input.body) > bounded(input.maxRequestBytes, MAX_POST_BYTES)) {
    throw new TransportError("request_too_large");
  }
  const maxResponseBytes = bounded(input.maxResponseBytes, MAX_RESPONSE_BYTES);
  const controller = new AbortController();
  let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
  const timeoutMs = bounded(input.timeoutMs ?? DEFAULT_POST_TIMEOUT_MS, 120_000);
  const startedAt = performance.now();
  let timer: ReturnType<typeof setTimeout>;
  const deadline = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(() => {
      controller.abort();
      reject(new TransportError("deadline_exceeded"));
    }, timeoutMs);
  });
  const read = async (): Promise<JsonPostResult> => {
    const response = await (input.fetchImpl ?? fetch)(url.href, {
      method: "POST", redirect: "manual", headers: input.headers,
      body: input.body, signal: controller.signal,
    });
    // A late response from an injected fetch still needs to release its body.
    if (controller.signal.aborted) {
      void response.body?.cancel().catch(() => undefined);
      throw new TransportError("deadline_exceeded");
    }
    try { assertNoRedirect(response, "POST", url.origin); }
    catch (error) { void response.body?.cancel().catch(() => undefined); throw error; }
    const declaredLength = Number(response.headers.get("content-length"));
    if (declaredLength > maxResponseBytes) {
      void response.body?.cancel().catch(() => undefined);
      throw new TransportError("response_too_large");
    }
    reader = response.body?.getReader();
    const chunks: Uint8Array[] = [];
    let bytes = 0;
    if (reader) {
      while (true) {
        const chunk = await reader.read();
        if (chunk.done) break;
        bytes += chunk.value.byteLength;
        if (bytes > maxResponseBytes) throw new TransportError("response_too_large");
        chunks.push(chunk.value);
      }
    }
    let body: unknown;
    try { body = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(Buffer.concat(chunks))); }
    catch {
      if (response.ok) throw new TransportError("invalid_json");
      body = {}; // An invalid refusal never becomes an accepted response.
    }
    if (controller.signal.aborted || performance.now() - startedAt >= timeoutMs) throw new TransportError("deadline_exceeded");
    return { ok: response.ok, status: response.status, headers: response.headers, body };
  };
  try { return await Promise.race([read(), deadline]); }
  catch (error) {
    if (error instanceof TransportError) throw error;
    throw new TransportError(controller.signal.aborted ? "deadline_exceeded" : "network_error", networkCode(error));
  } finally {
    clearTimeout(timer!);
    controller.abort();
    // Do not wait for a hostile cancel implementation to settle.
    if (reader) { void reader.cancel().catch(() => undefined); try { reader.releaseLock(); } catch {} }
  }
}

export function authenticatedJsonPost(input: JsonPostOptions & {
  installKey: string; ingestKey?: string; signingSecret?: string; now?: () => Date;
}) {
  const headers: Record<string, string> = {
    ...input.headers, "content-type": "application/json", "x-plimsoll-install-key": input.installKey,
  };
  if (input.ingestKey) headers["x-plimsoll-ingest-key"] = input.ingestKey;
  if (input.signingSecret) {
    const timestamp = (input.now ?? (() => new Date()))().toISOString();
    headers["x-plimsoll-upload-timestamp"] = timestamp;
    headers["x-plimsoll-upload-signature"] = `sha256=${crypto.createHmac("sha256", input.signingSecret).update(`${timestamp}.${input.body}`).digest("hex")}`;
    // Capture watermark v1 (eco-6hoxj.163.18): bind the claim header to the
    // same timestamp and exact body; the body signature does not cover headers.
    const capture = headers["x-plimsoll-capture"];
    if (capture !== undefined) {
      headers["x-plimsoll-capture-signature"] = `sha256=${crypto.createHmac("sha256", input.signingSecret)
        .update(`plimsoll-capture-v1\n${timestamp}\n${capture}\n${input.body}`).digest("hex")}`;
    }
  }
  return postJson({ ...input, headers });
}
