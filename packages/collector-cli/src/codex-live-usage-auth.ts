import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import type http from "node:http";
import { isIP } from "node:net";
import type { LocalEventBuffer } from "./buffer";
import type { CollectorConfig } from "./config";
import { captureRootDigest, captureRootSchema, type CaptureRoot } from "./capture-root-inventory";
import { HttpBoundaryRejection, type createRequestBudget } from "./http-boundary";
import { readLocalIngestAuth, type LocalIngestAuth } from "./local-auth";
import { canonicalJson, exactKeys, LIVE_BODY_BYTES, LIVE_DIGEST, LIVE_ID, liveSha256, liveTimestamp } from "./codex-live-usage-protocol";
import { registerLiveCredential, revokeLiveProducer } from "./codex-live-usage-ledger";

export const LIVE_BINDINGS_FILE = "live-producer-bindings.json";
const REGISTRY_BYTES = 64 * 1024;
const TOKEN = /^[A-Za-z0-9_-]{43}$/;
export type LiveProducerBinding = { producerId: string; credentialId: string; tokenSha256: string;
  source: "codex"; captureRootId: string; profileId: string; captureRootDigest: string;
  installationEpochId: string; enabled: boolean; enrolledAt: string };
export type LiveSourceContext = { producerId: string; source: "codex"; captureRootId: string;
  profileId: string; captureRootDigest: string; installationEpochId: string; workspaceId: string; deviceId: string };
export type LiveAuthenticatedBinding = { binding: LiveProducerBinding; context: LiveSourceContext;
  contextDigest: string; scopeDigest: string; root: CaptureRoot };
type Registry = { schema: "plimsoll.live-producer-bindings.v1"; bindings: LiveProducerBinding[] };
const authenticatedBindings = new WeakSet<LiveAuthenticatedBinding>();
export const isAuthenticatedLiveBinding = (value: LiveAuthenticatedBinding) => authenticatedBindings.has(value);
function freezeBinding(auth: LiveAuthenticatedBinding) {
  const pending: object[] = [auth];
  while (pending.length) {
    const value = pending.pop()!;
    for (const child of Object.values(value)) if (child && typeof child === "object") pending.push(child);
    Object.freeze(value);
  }
  authenticatedBindings.add(auth);
  return auth;
}

export function rawLiveHeaders(request: http.IncomingMessage, name: string) {
  const values: string[] = [];
  for (let i = 0; i < request.rawHeaders.length; i += 2) {
    if (request.rawHeaders[i].toLowerCase() === name) values.push(request.rawHeaders[i + 1]);
  }
  return values;
}
export const selectsLiveUsage = (request: http.IncomingMessage) => rawLiveHeaders(request, "x-plimsoll-producer-id").length > 0;
export function assertLiveRoute(request: http.IncomingMessage) {
  const remote = request.socket.remoteAddress ?? "";
  const ipv4 = remote.startsWith("::ffff:") ? remote.slice(7) : remote;
  if (!(remote === "::1" || (isIP(ipv4) === 4 && ipv4.startsWith("127."))))
    throw new HttpBoundaryRejection("host_not_allowed", 403);
  if (request.method !== "POST" || request.url !== "/hooks/codex")
    throw new HttpBoundaryRejection("source_not_allowed", 400);
  const producer = rawLiveHeaders(request, "x-plimsoll-producer-id");
  const token = rawLiveHeaders(request, "x-plimsoll-token");
  const contentType = rawLiveHeaders(request, "content-type");
  const source = rawLiveHeaders(request, "x-plimsoll-source");
  const encoding = rawLiveHeaders(request, "content-encoding");
  const fetchSite = rawLiveHeaders(request, "sec-fetch-site");
  if (rawLiveHeaders(request, "origin").length || fetchSite.length > 1 ||
      (fetchSite.length && !["none", "same-origin"].includes(fetchSite[0])))
    throw new HttpBoundaryRejection("browser_origin_not_allowed", 403);
  if (producer.length !== 1 || token.length !== 1 || !LIVE_ID.test(producer[0]) || !TOKEN.test(token[0]))
    throw new HttpBoundaryRejection("producer_token_invalid", 401);
  if (contentType.length !== 1 || contentType[0] !== "application/json" || encoding.length > 1 ||
      (encoding.length && encoding[0] !== "identity")) throw new HttpBoundaryRejection("unsupported_content_encoding", 415);
  if (source.length > 1 || (source.length && source[0] !== "codex")) throw new HttpBoundaryRejection("source_mismatch", 403);
  return { producerId: producer[0], token: token[0] };
}

function privateStat(file: string, directory = false) {
  const s = fs.lstatSync(file);
  if (s.isSymbolicLink() || (directory ? !s.isDirectory() : !s.isFile()) ||
      (s.mode & 0o7077) !== 0 || (typeof process.getuid === "function" && s.uid !== process.getuid()))
    throw new Error("live_private_file_unsafe");
  return s;
}
function readPrivate(file: string, max: number): Buffer {
  privateStat(path.dirname(file), true);
  const before = privateStat(file);
  if (before.size > max) throw new Error("live_registry_too_large");
  const fd = fs.openSync(file, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
  try {
    const opened = fs.fstatSync(fd);
    if (opened.ino !== before.ino || opened.dev !== before.dev || opened.size !== before.size ||
        (opened.mode & 0o7077) !== 0 || opened.uid !== before.uid) throw new Error("live_private_file_changed");
    const bytes = Buffer.alloc(max + 1); const length = fs.readSync(fd, bytes, 0, bytes.length, 0);
    const after = fs.fstatSync(fd); const current = privateStat(file);
    if (length > max || after.size !== opened.size || after.mtimeMs !== opened.mtimeMs ||
        after.ctimeMs !== opened.ctimeMs || current.ino !== opened.ino || current.dev !== opened.dev)
      throw new Error("live_private_file_changed");
    return bytes.subarray(0, length);
  } finally { fs.closeSync(fd); }
}
export function readLiveProducerBindings(home: string): Registry {
  const value: unknown = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(readPrivate(path.join(home, LIVE_BINDINGS_FILE), REGISTRY_BYTES)));
  if (!exactKeys(value, ["schema", "bindings"]) || value.schema !== "plimsoll.live-producer-bindings.v1" || !Array.isArray(value.bindings))
    throw new Error("live_registry_invalid");
  const enabled = new Set<string>(); const ids = new Set<string>();
  for (const b of value.bindings) {
    if (!exactKeys(b, ["producerId", "credentialId", "tokenSha256", "source", "captureRootId", "profileId", "captureRootDigest", "installationEpochId", "enabled", "enrolledAt"]) ||
        ![b.producerId, b.credentialId, b.captureRootId, b.profileId, b.installationEpochId].every(v => typeof v === "string" && LIVE_ID.test(v)) ||
        b.source !== "codex" || typeof b.enabled !== "boolean" || !liveTimestamp(b.enrolledAt) ||
        ![b.tokenSha256, b.captureRootDigest].every(v => typeof v === "string" && LIVE_DIGEST.test(v))) throw new Error("live_registry_invalid");
    const id = canonicalJson([b.producerId, b.credentialId]);
    if (ids.has(id) || (b.enabled && enabled.has(b.producerId as string))) throw new Error("live_registry_invalid");
    ids.add(id); if (b.enabled) enabled.add(b.producerId as string);
  }
  if (enabled.size > 64) throw new Error("live_registry_capacity");
  return value as Registry;
}
export function currentLiveContext(buffer: LocalEventBuffer, config: CollectorConfig, binding: LiveProducerBinding): LiveAuthenticatedBinding {
  const roots = (config.captureRoots ?? []).filter(r => r.rootId === binding.captureRootId);
  const root = roots.length === 1 ? roots[0] : undefined;
  const current = buffer.workspaceBinding();
  if (!binding.enabled || !root || (config.captureRoots?.length ?? 0) > 64 || !captureRootSchema.safeParse(root).success ||
      root.source !== "codex" || root.profileId !== binding.profileId ||
      captureRootDigest(root) !== binding.captureRootDigest || root.installationEpochId !== binding.installationEpochId ||
      !current?.currentInstallationEpochId || !current.currentDeviceId ||
      current.currentInstallationEpochId !== binding.installationEpochId ||
      current.currentWorkspaceId !== config.tenantId || current.currentDeviceId !== config.deviceId ||
      buffer.currentDeviceId !== current.currentDeviceId || buffer.eventAdmissionReason(binding.enrolledAt, binding.installationEpochId))
    throw new HttpBoundaryRejection("source_not_allowed", 403);
  const context: LiveSourceContext = { producerId: binding.producerId, source: "codex", captureRootId: root.rootId,
    profileId: root.profileId, captureRootDigest: binding.captureRootDigest, installationEpochId: binding.installationEpochId,
    workspaceId: current.currentWorkspaceId, deviceId: current.currentDeviceId };
  const contextDigest = liveSha256(canonicalJson(context));
  return { binding, context, contextDigest,
    scopeDigest: liveSha256(canonicalJson([context, binding.credentialId])), root: structuredClone(root) };
}
export function authenticateLiveProducer(home: string, buffer: LocalEventBuffer, config: CollectorConfig,
  producerId: string, token: string, ordinaryAuth?: LocalIngestAuth | null): LiveAuthenticatedBinding {
  let registry: Registry;
  try { registry = readLiveProducerBindings(home); } catch { throw new HttpBoundaryRejection("producer_token_invalid", 401); }
  const binding = registry.bindings.find(b => b.producerId === producerId && b.enabled);
  const suppliedDigest = liveSha256(token);
  const expected = binding?.tokenSha256 ?? "0".repeat(64);
  const matches = crypto.timingSafeEqual(Buffer.from(suppliedDigest, "hex"), Buffer.from(expected, "hex"));
  if (!binding || !TOKEN.test(token) || !matches) throw new HttpBoundaryRejection("producer_token_invalid", 401);
  if (ordinaryAuth && Object.values(ordinaryAuth).some(v => typeof v === "string" && liveSha256(v) === suppliedDigest))
    throw new HttpBoundaryRejection("producer_token_invalid", 401);
  const authenticated = currentLiveContext(buffer, config, binding);
  const row = buffer.database.prepare(`select b.token_sha256, b.context_digest, b.enrolled_at, b.revoked, p.credential_id, p.enabled
    from codex_live_bindings b join codex_live_producers p using(producer_id)
    where b.producer_id=? and b.credential_id=?`).get(producerId, binding.credentialId) as
    { token_sha256: string; context_digest: string; enrolled_at: string; revoked: number; credential_id: string; enabled: number } | undefined;
  if (!row || row.revoked || !row.enabled || row.credential_id !== binding.credentialId ||
      row.context_digest !== authenticated.contextDigest || row.token_sha256 !== expected || row.enrolled_at !== binding.enrolledAt)
    throw new HttpBoundaryRejection("source_not_allowed", 403);
  return freezeBinding(authenticated);
}

function atomicPrivateWrite(file: string, bytes: string) {
  const home = path.dirname(file); privateStat(home, true);
  if (fs.existsSync(file)) privateStat(file);
  const tmp = `${file}.${crypto.randomBytes(8).toString("hex")}.tmp`;
  let fd: number | undefined;
  try {
    fd = fs.openSync(tmp, "wx", 0o600); fs.writeFileSync(fd, bytes); fs.fsyncSync(fd); fs.closeSync(fd); fd = undefined;
    fs.renameSync(tmp, file);
    const directory = fs.openSync(home, "r"); try { fs.fsyncSync(directory); } finally { fs.closeSync(directory); }
  } finally { if (fd !== undefined) fs.closeSync(fd); if (fs.existsSync(tmp)) fs.unlinkSync(tmp); }
}
/** Same-user owner API. No HTTP provisioning route; returns no token and never prints. */
export function provisionLiveProducer(options: { home: string; buffer: LocalEventBuffer; config: CollectorConfig;
  producerId: string; credentialId: string; captureRootId: string; enrolledAt?: string }) {
  const { home, buffer, config, producerId, credentialId } = options;
  privateStat(home, true);
  if (![producerId, credentialId, options.captureRootId].every(v => LIVE_ID.test(v))) throw new Error("live_binding_invalid");
  const root = config.captureRoots?.find(r => r.rootId === options.captureRootId && r.source === "codex");
  if (!root) throw new Error("live_root_not_enrolled");
  const token = crypto.randomBytes(32).toString("base64url");
  const ordinary = readLocalIngestAuth(home);
  if (!ordinary || Object.values(ordinary).includes(token)) throw new Error("live_ordinary_auth_required");
  const binding: LiveProducerBinding = { producerId, credentialId, source: "codex", captureRootId: root.rootId,
    profileId: root.profileId, captureRootDigest: captureRootDigest(root), installationEpochId: root.installationEpochId,
    enabled: true, enrolledAt: options.enrolledAt ?? new Date().toISOString(), tokenSha256: liveSha256(token) };
  const context = currentLiveContext(buffer, config, binding);
  const file = path.join(home, LIVE_BINDINGS_FILE);
  const prior = fs.existsSync(file) ? readLiveProducerBindings(home) : { schema: "plimsoll.live-producer-bindings.v1" as const, bindings: [] };
  const bindings = [...prior.bindings.filter(b => b.enabled && b.producerId !== producerId), binding];
  const registry: Registry = { schema: "plimsoll.live-producer-bindings.v1", bindings };
  if (bindings.length > 64 || Buffer.byteLength(canonicalJson(registry)) > REGISTRY_BYTES) throw new Error("live_registry_capacity");
  const credentialFile = path.join(home, `live-producer-${liveSha256(producerId).slice(0, 32)}.token`);
  // Commit invalidation first. A failed file publication disables intake, never
  // re-authorizes the old credential; recover by provisioning a fresh credential ID.
  registerLiveCredential(buffer.database, context);
  atomicPrivateWrite(credentialFile, token);
  atomicPrivateWrite(file, canonicalJson(registry));
  return { producerId, credentialId, credentialFile };
}
export function disableLiveProducer(home: string, buffer: LocalEventBuffer, producerId: string) {
  const registry = readLiveProducerBindings(home);
  revokeLiveProducer(buffer.database, producerId);
  atomicPrivateWrite(path.join(home, LIVE_BINDINGS_FILE), canonicalJson({ ...registry,
    bindings: registry.bindings.filter(b => b.producerId !== producerId) }));
}

/** Separate 4 KiB reader: never accumulates the ordinary 2 MiB admission cap. */
export function readLiveBody(request: http.IncomingMessage, budget: ReturnType<typeof createRequestBudget>): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const declared = request.headers["content-length"];
    if (declared !== undefined && (!/^\d+$/.test(String(declared)) || Number(declared) > LIVE_BODY_BYTES)) {
      request.resume(); reject(new HttpBoundaryRejection("decoded_body_too_large", 413)); return;
    }
    const chunks: Buffer[] = []; let length = 0, done = false;
    const cleanup = () => { clearTimeout(timer); request.off("data", data); request.off("end", end);
      request.off("error", error); request.off("aborted", error); };
    const fail = (failure: HttpBoundaryRejection) => { if (done) return; done = true; cleanup(); chunks.length = 0; request.resume(); reject(failure); };
    const data = (chunk: Buffer) => { length += chunk.length; if (length > LIVE_BODY_BYTES) fail(new HttpBoundaryRejection("decoded_body_too_large", 413)); else chunks.push(chunk); };
    const error = () => fail(new HttpBoundaryRejection("request_stream_error", 400));
    const end = () => { if (done) return; done = true; cleanup(); try { budget.checkpoint(); resolve(Buffer.concat(chunks, length)); } catch (e) { reject(e); } };
    const timer = setTimeout(() => fail(new HttpBoundaryRejection("request_deadline_exceeded", 408)), Math.max(1, budget.remainingMs())); timer.unref();
    request.on("data", data); request.on("end", end); request.on("error", error); request.on("aborted", error);
  });
}
