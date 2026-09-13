import { DeliveryUploadError } from "./upload";
import { SyncStorageBusyError, isSqliteContentionError } from "./sqlite-contention";

const MAX_DELAY_MS = 60 * 60 * 1_000;
const NETWORK_CODES = new Set(["ECONNRESET", "ECONNREFUSED", "ETIMEDOUT", "EAI_AGAIN", "ENETUNREACH", "EHOSTUNREACH", "UND_ERR_SOCKET", "UND_ERR_CONNECT_TIMEOUT", "UND_ERR_HEADERS_TIMEOUT", "UND_ERR_BODY_TIMEOUT", "AbortError", "deadline_exceeded"]);
const STATUS_CLASSES = new Set(["network", "remote_transient", "remote_auth", "remote_contract", "remote_validation", "local_request_budget"]);

/**
 * A server-directed wait is honoured, never trusted without a bound: a single
 * malformed-but-parsable `Retry-After` must not park the uploader for years
 * (review r1, F2). The durable floor is clamped separately, by the outbox, to
 * its own configured `maxBackoffSeconds`.
 */
function serverDelayMs(retryAfterMs: number) {
  return Number.isFinite(retryAfterMs) ? Math.min(Math.max(0, retryAfterMs), MAX_DELAY_MS) : 0;
}

/** Process-local scheduling only. Durable retry identities and floors live in the outbox. */
export class SyncBackoff {
  private streak = 0;
  private blockedUntil = 0;
  private nextTickAt: number | null = null;
  private lastError: { at: string; code: string; failureClass: string } | null = null;
  private uploaded = 0;
  constructor(readonly intervalMs: number) {
    if (!Number.isFinite(intervalMs) || intervalMs <= 0) throw new Error("invalid_sync_interval");
  }
  arm(now = Date.now()) { this.nextTickAt = now + this.intervalMs; }
  tick(now = Date.now()) { this.nextTickAt = now + this.intervalMs; }
  ready(now = Date.now()) { return now >= this.blockedUntil; }
  success(uploaded: number, retryAfterMs = 0, now = Date.now()) {
    this.uploaded = uploaded;
    this.streak = 0;
    this.blockedUntil = now + serverDelayMs(retryAfterMs);
    // A cycle that acknowledged work is not a failure. The server-directed
    // wait is reported through `notBefore` alone (review r1, F6).
    this.lastError = null;
  }
  failure(error: unknown, uploaded: number, now = Date.now(), maintenanceCircuitOpen = false) {
    // The maintenance circuit is learned from the 4th argument, not from an
    // error message: `maintenance_circuit_open` is thrown and caught inside the
    // maintenance cadence and never reaches runSync's catch (review r1, F5).
    const storageBusy = error instanceof SyncStorageBusyError || isSqliteContentionError(error);
    const delivery = error instanceof DeliveryUploadError ? error : null;
    const local = storageBusy || (maintenanceCircuitOpen && delivery?.httpStatusClass === "network");
    const serverDelay = delivery ? serverDelayMs(delivery.retryAfterMs) : 0;
    // Actual acknowledged progress and local storage pressure do not justify a
    // host-wide exponential pause. Per-item retry policy is left authoritative.
    this.streak = uploaded > 0 || local ? 0 : Math.min(this.streak + 1, 32);
    const delay = this.streak === 0 ? 0 : Math.min(this.intervalMs * 2 ** Math.min(this.streak, 4), MAX_DELAY_MS);
    this.blockedUntil = now + Math.max(delay, serverDelay);
    this.uploaded = uploaded;
    const code = storageBusy ? "local_storage_busy" : delivery?.networkCode && NETWORK_CODES.has(delivery.networkCode)
      ? delivery.networkCode : delivery && STATUS_CLASSES.has(delivery.httpStatusClass) ? delivery.httpStatusClass : "unclassified";
    this.lastError = { at: new Date(now).toISOString(), code, failureClass: storageBusy ? "local_storage_busy" : delivery?.failureClass ?? "unclassified" };
    return { failureStreak: this.streak, localPressure: local, backoffMs: Math.max(delay, serverDelay), uploadedEvents: uploaded, timestamp: new Date(now).toISOString(), error: this.lastError };
  }
  status(inFlight = false, now = Date.now()) {
    const tick = this.nextTickAt === null ? null : Math.max(now, this.nextTickAt);
    const eligible = tick === null ? null : tick + Math.max(0, Math.ceil((this.blockedUntil - tick) / this.intervalMs)) * this.intervalMs;
    return { inFlight, failureStreak: this.streak,
      nextAttemptAt: inFlight || eligible === null ? null : new Date(eligible).toISOString(),
      notBefore: this.blockedUntil > now ? new Date(this.blockedUntil).toISOString() : null,
      lastError: this.lastError, lastCycleUploadedEvents: this.uploaded,
      scheduling: "earliest_eligible_cadence_tick" };
  }
}
