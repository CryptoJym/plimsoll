import type http from "node:http";
import zlib from "node:zlib";

export const LOCAL_HTTP_LIMITS = Object.freeze({
  compressedBodyBytes: 256 * 1024,
  decodedBodyBytes: 2 * 1024 * 1024,
  compressionRatio: 32,
  jsonDepth: 32,
  jsonNodes: 100_000,
  otlpResources: 64,
  otlpScopes: 256,
  otlpRecords: 2_048,
  otlpAttributesPerContainer: 128,
  otlpAttributesTotal: 16_384,
  requestDeadlineMs: 1_500,
});

export type LocalProducerSource = "claude_code" | "codex";

export type HttpBoundaryReason =
  | "browser_origin_not_allowed"
  | "compressed_body_too_large"
  | "compression_ratio_too_large"
  | "decoded_body_too_large"
  | "host_not_allowed"
  | "internal_rejection"
  | "invalid_compressed_body"
  | "invalid_json"
  | "json_depth_exceeded"
  | "json_node_limit_exceeded"
  | "otlp_attribute_limit_exceeded"
  | "otlp_record_limit_exceeded"
  | "otlp_resource_limit_exceeded"
  | "otlp_scope_limit_exceeded"
  | "request_deadline_exceeded"
  | "request_stream_error"
  | "source_mismatch"
  | "source_not_allowed"
  | "source_required"
  | "storage_busy_retry"
  | "unsupported_content_encoding";

export class HttpBoundaryRejection extends Error {
  constructor(
    readonly reason: HttpBoundaryReason,
    readonly status: number,
  ) {
    super(reason);
    this.name = "HttpBoundaryRejection";
  }
}

export function asHttpBoundaryRejection(error: unknown) {
  if (error instanceof HttpBoundaryRejection) return error;
  const code = error && typeof error === "object" && "code" in error
    ? String((error as { code?: unknown }).code ?? "")
    : "";
  return code === "SQLITE_BUSY" || code === "SQLITE_LOCKED"
    ? new HttpBoundaryRejection("storage_busy_retry", 503)
    : new HttpBoundaryRejection("internal_rejection", 400);
}

function firstHeader(value: string | string[] | undefined) {
  return Array.isArray(value) ? value[0] : value;
}

const LOCAL_HOST_GRAMMAR = /^(?:localhost|127\.0\.0\.1|\[::1\])(?::([1-9][0-9]{0,4}))?$/i;

/**
 * Validate the semantic Host field without URL parsing or normalization.
 * Hostnames are case-insensitive, but every other byte must already be in the
 * one accepted canonical form. In particular: no numeric/octal aliases,
 * userinfo, path/query/fragment, whitespace, empty port, leading-zero port,
 * or out-of-range port can be normalized into the allowlist.
 */
export function isAllowedLocalHostValue(host: string) {
  if (host !== host.trim()) return false;
  const match = LOCAL_HOST_GRAMMAR.exec(host);
  if (!match) return false;
  const port = match[1];
  return port === undefined || Number(port) <= 65_535;
}

export function assertAllowedHost(request: http.IncomingMessage) {
  const host = firstHeader(request.headers.host);
  const hostHeaderCount = request.rawHeaders.reduce(
    (count, header, index) =>
      index % 2 === 0 && header.toLowerCase() === "host" ? count + 1 : count,
    0,
  );
  if (!host || hostHeaderCount !== 1 || !isAllowedLocalHostValue(host)) {
    throw new HttpBoundaryRejection("host_not_allowed", 421);
  }
}

export function assertNoBrowserOrigin(request: http.IncomingMessage) {
  if (request.headers.origin !== undefined) {
    throw new HttpBoundaryRejection("browser_origin_not_allowed", 403);
  }
}

function producerSourceHeader(request: http.IncomingMessage) {
  return firstHeader(request.headers["x-plimsoll-source"]);
}

function parsedProducerSource(value: string | undefined): LocalProducerSource | undefined {
  if (value === "claude_code" || value === "codex") return value;
  return undefined;
}

export function requireOtlpSource(request: http.IncomingMessage): LocalProducerSource {
  const claimed = producerSourceHeader(request);
  if (!claimed) throw new HttpBoundaryRejection("source_required", 401);
  const parsed = parsedProducerSource(claimed);
  if (!parsed) throw new HttpBoundaryRejection("source_not_allowed", 401);
  return parsed;
}

export function assertHookSource(
  request: http.IncomingMessage,
  pathSource: LocalProducerSource,
) {
  const claimed = producerSourceHeader(request);
  if (!claimed) return;
  const parsed = parsedProducerSource(claimed);
  if (!parsed) throw new HttpBoundaryRejection("source_not_allowed", 401);
  if (parsed !== pathSource) throw new HttpBoundaryRejection("source_mismatch", 401);
}

export type RequestBudget = {
  checkpoint: () => void;
  remainingMs: () => number;
};

export function createRequestBudget(): RequestBudget {
  const deadlineAt = performance.now() + LOCAL_HTTP_LIMITS.requestDeadlineMs;
  const remainingMs = () => Math.max(0, deadlineAt - performance.now());
  return {
    remainingMs,
    checkpoint() {
      if (remainingMs() <= 0) {
        throw new HttpBoundaryRejection("request_deadline_exceeded", 408);
      }
    },
  };
}

export function readBoundedRequestBody(
  request: http.IncomingMessage,
  budget: RequestBudget,
) {
  return new Promise<Buffer>((resolve, reject) => {
    const contentLength = firstHeader(request.headers["content-length"]);
    if (contentLength && /^\d+$/.test(contentLength)) {
      const declaredBytes = Number(contentLength);
      if (
        !Number.isSafeInteger(declaredBytes) ||
        declaredBytes > LOCAL_HTTP_LIMITS.compressedBodyBytes
      ) {
        reject(new HttpBoundaryRejection("compressed_body_too_large", 413));
        request.resume();
        return;
      }
    }

    const chunks: Buffer[] = [];
    let bodyBytes = 0;
    let settled = false;

    const cleanup = () => {
      clearTimeout(timer);
      request.off("data", onData);
      request.off("end", onEnd);
      request.off("error", onError);
      request.off("aborted", onAborted);
    };
    const fail = (error: HttpBoundaryRejection) => {
      if (settled) return;
      settled = true;
      cleanup();
      chunks.length = 0;
      reject(error);
      request.resume();
    };
    const onData = (chunk: Buffer | string) => {
      const bytes = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
      bodyBytes += bytes.length;
      if (bodyBytes > LOCAL_HTTP_LIMITS.compressedBodyBytes) {
        fail(new HttpBoundaryRejection("compressed_body_too_large", 413));
        return;
      }
      chunks.push(bytes);
    };
    const onEnd = () => {
      if (settled) return;
      settled = true;
      cleanup();
      try {
        budget.checkpoint();
        resolve(Buffer.concat(chunks, bodyBytes));
      } catch (error) {
        reject(error);
      }
    };
    const onError = () => fail(new HttpBoundaryRejection("request_stream_error", 400));
    const onAborted = () => fail(new HttpBoundaryRejection("request_stream_error", 400));
    const timer = setTimeout(
      () => fail(new HttpBoundaryRejection("request_deadline_exceeded", 408)),
      Math.max(1, Math.ceil(budget.remainingMs())),
    );
    timer.unref();

    request.on("data", onData);
    request.on("end", onEnd);
    request.on("error", onError);
    request.on("aborted", onAborted);
  });
}

function zlibDecode(contentEncoding: string, body: Buffer) {
  const options = { maxOutputLength: LOCAL_HTTP_LIMITS.decodedBodyBytes + 1 };
  try {
    if (contentEncoding === "gzip") return zlib.gunzipSync(body, options);
    if (contentEncoding === "deflate") return zlib.inflateSync(body, options);
    if (contentEncoding === "br") return zlib.brotliDecompressSync(body, options);
  } catch (error) {
    if (
      error &&
      typeof error === "object" &&
      "code" in error &&
      error.code === "ERR_BUFFER_TOO_LARGE"
    ) {
      throw new HttpBoundaryRejection("decoded_body_too_large", 413);
    }
    throw new HttpBoundaryRejection("invalid_compressed_body", 400);
  }
  throw new HttpBoundaryRejection("unsupported_content_encoding", 415);
}

export function decodeBoundedRequestBody(request: http.IncomingMessage, body: Buffer) {
  const contentEncoding =
    firstHeader(request.headers["content-encoding"])?.trim().toLowerCase() ?? "identity";
  if (!["identity", "gzip", "deflate", "br"].includes(contentEncoding)) {
    throw new HttpBoundaryRejection("unsupported_content_encoding", 415);
  }

  const decoded = contentEncoding === "identity" ? body : zlibDecode(contentEncoding, body);
  if (decoded.length > LOCAL_HTTP_LIMITS.decodedBodyBytes) {
    throw new HttpBoundaryRejection("decoded_body_too_large", 413);
  }
  if (
    contentEncoding !== "identity" &&
    (body.length === 0 || decoded.length > body.length * LOCAL_HTTP_LIMITS.compressionRatio)
  ) {
    throw new HttpBoundaryRejection("compression_ratio_too_large", 413);
  }

  return {
    bodyBytes: body.length,
    contentEncoding,
    decodedBytes: decoded.length,
    text: decoded.toString("utf8"),
  };
}

function assertJsonDepth(text: string) {
  let depth = 0;
  let inString = false;
  let escaped = false;

  for (const character of text) {
    if (inString) {
      if (escaped) escaped = false;
      else if (character === "\\") escaped = true;
      else if (character === '"') inString = false;
      continue;
    }
    if (character === '"') {
      inString = true;
      continue;
    }
    if (character === "{" || character === "[") {
      depth += 1;
      if (depth > LOCAL_HTTP_LIMITS.jsonDepth) {
        throw new HttpBoundaryRejection("json_depth_exceeded", 413);
      }
    } else if (character === "}" || character === "]") {
      depth = Math.max(0, depth - 1);
    }
  }
}

export function parseBoundedJson(text: string) {
  assertJsonDepth(text);
  try {
    return JSON.parse(text || "{}");
  } catch {
    throw new HttpBoundaryRejection("invalid_json", 400);
  }
}

const RESOURCE_ARRAYS = new Set(["resourceLogs", "resourceSpans", "resourceMetrics"]);
const SCOPE_ARRAYS = new Set(["scopeLogs", "scopeSpans", "scopeMetrics"]);
const RECORD_ARRAYS = new Set([
  "logRecords",
  "spans",
  "metrics",
  "dataPoints",
  "events",
  "links",
  "exemplars",
]);

export function assertBoundedJsonNodes(root: unknown) {
  const stack: unknown[] = [root];
  let nodes = 0;
  while (stack.length > 0) {
    const value = stack.pop();
    nodes += 1;
    if (nodes > LOCAL_HTTP_LIMITS.jsonNodes) {
      throw new HttpBoundaryRejection("json_node_limit_exceeded", 413);
    }
    if (!value || typeof value !== "object") continue;
    if (Array.isArray(value)) {
      for (const entry of value) stack.push(entry);
    } else {
      for (const entry of Object.values(value)) stack.push(entry);
    }
  }
}

export function assertBoundedOtlpCardinality(root: unknown) {
  assertBoundedJsonNodes(root);
  const stack: unknown[] = [root];
  let resources = 0;
  let scopes = 0;
  let records = 0;
  let attributes = 0;

  while (stack.length > 0) {
    const value = stack.pop();
    if (!value || typeof value !== "object") continue;
    if (Array.isArray(value)) {
      for (const entry of value) stack.push(entry);
      continue;
    }

    for (const [key, entry] of Object.entries(value)) {
      if (Array.isArray(entry)) {
        if (RESOURCE_ARRAYS.has(key)) {
          resources += entry.length;
          if (resources > LOCAL_HTTP_LIMITS.otlpResources) {
            throw new HttpBoundaryRejection("otlp_resource_limit_exceeded", 413);
          }
        }
        if (SCOPE_ARRAYS.has(key)) {
          scopes += entry.length;
          if (scopes > LOCAL_HTTP_LIMITS.otlpScopes) {
            throw new HttpBoundaryRejection("otlp_scope_limit_exceeded", 413);
          }
        }
        if (RECORD_ARRAYS.has(key)) {
          records += entry.length;
          if (records > LOCAL_HTTP_LIMITS.otlpRecords) {
            throw new HttpBoundaryRejection("otlp_record_limit_exceeded", 413);
          }
        }
        if (key === "attributes") {
          if (entry.length > LOCAL_HTTP_LIMITS.otlpAttributesPerContainer) {
            throw new HttpBoundaryRejection("otlp_attribute_limit_exceeded", 413);
          }
          attributes += entry.length;
          if (attributes > LOCAL_HTTP_LIMITS.otlpAttributesTotal) {
            throw new HttpBoundaryRejection("otlp_attribute_limit_exceeded", 413);
          }
        }
      }
      stack.push(entry);
    }
  }
}
