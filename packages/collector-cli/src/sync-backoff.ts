import { DeliveryUploadError } from "./upload";
import { SyncStorageBusyError, isSqliteContentionError } from "./sqlite-contention";

const MAX_DELAY_MS = 60 * 60 * 1_000;
const NETWORK_CODES = new Set(["ECONNRESET", "ECONNREFUSED", "ETIMEDOUT", "EAI_AGAIN", "ENETUNREACH", "EHOSTUNREACH", "UND_ERR_SOCKET", "UND_ERR_CONNECT_TIMEOUT", "UND_ERR_HEADERS_TIMEOUT", "UND_ERR_BODY_TIMEOUT", "AbortError", "deadline_exceeded"]);
const STATUS_CLASSES = new Set(["network", "remote_transient", "remote_auth", "remote_contract", "remote_validation", "local_request_budget"]);

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
    this.blockedUntil = now + Math.max(0, retryAfterMs);
    this.lastError = retryAfterMs > 0 ? { at: new Date(now).toISOString(), code: "retry_after", failureClass: "remote_transient" } : null;
  }
  failure(error: unknown, uploaded: number, now = Date.now(), maintenanceCircuitOpen = false) {
    const storageBusy = error instanceof SyncStorageBusyError || isSqliteContentionError(error) ||
      (error instanceof Error && error.message === "maintenance_circuit_open");
    const delivery = error instanceof DeliveryUploadError ? error : null;
    const local = storageBusy || (maintenanceCircuitOpen && delivery?.httpStatusClass === "network");
    const serverDelay = delivery && Number.isFinite(delivery.retryAfterMs) ? Math.max(0, delivery.retryAfterMs) : 0;
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
