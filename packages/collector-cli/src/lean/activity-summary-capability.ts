/** The most recent authenticated cloud advertisement for one install. A
 * missing field on a later response withdraws the capability immediately. */
import { createHash } from "node:crypto";
import Database from "better-sqlite3";

export type ActivitySummaryAdvertisement = {
  enabled: boolean;
  version: number;
  lastResponseAtMs: number | null;
  lastV2AdvertisedAtMs: number | null;
  actorBindingVersion: number | null;
  actorBindingInstallHeard: string | null;
};

const absent = (): ActivitySummaryAdvertisement => ({enabled:false,version:0,
  lastResponseAtMs:null,lastV2AdvertisedAtMs:null,actorBindingVersion:null,
  actorBindingInstallHeard:null});
const keyFor = (installKey: string) => `activity_summary_contract_v2:${createHash("sha256")
  .update(installKey).digest("hex")}`;
const uuid = (value: unknown): value is string => typeof value === "string" &&
  /^[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}$/i.test(value);
const record = (value: unknown): Record<string, unknown> =>
  value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};

export function readActivitySummaryAdvertisement(db: Database.Database,
  installKey: string): ActivitySummaryAdvertisement {
  const row = db.prepare(`select value from maintenance_state where key=?`)
    .get(keyFor(installKey)) as {value:string}|undefined;
  if (!row) return absent();
  try {
    const state = record(JSON.parse(row.value));
    const version = Number.isSafeInteger(state.version) && Number(state.version) >= 0
      ? Number(state.version) : 0;
    const at = (value: unknown) => Number.isSafeInteger(value) && Number(value) >= 0
      ? Number(value) : null;
    const bindingVersion = Number.isSafeInteger(state.actorBindingVersion) &&
      Number(state.actorBindingVersion) >= 0 ? Number(state.actorBindingVersion) : null;
    return {enabled:version >= 2,version,
      lastResponseAtMs:at(state.lastResponseAtMs),
      lastV2AdvertisedAtMs:at(state.lastV2AdvertisedAtMs),
      actorBindingVersion:bindingVersion,
      actorBindingInstallHeard:uuid(state.actorBindingInstallHeard)
        ? state.actorBindingInstallHeard : null};
  } catch { return absent(); }
}

/** Call only after ingest acknowledgement validation or a join grant. The
 * ledger owns this state so process restarts cannot re-enable a withdrawn v2. */
export function observeActivitySummaryAdvertisement(db: Database.Database,
  installKey: string, body: unknown, atMs = Date.now()): ActivitySummaryAdvertisement {
  if (!installKey || !Number.isSafeInteger(atMs) || atMs < 0) {
    throw new Error("invalid_activity_summary_advertisement");
  }
  const previous = readActivitySummaryAdvertisement(db, installKey);
  const response = record(body);
  const version = Number.isSafeInteger(response.activitySummaryContractVersion) &&
    Number(response.activitySummaryContractVersion) >= 0
      ? Number(response.activitySummaryContractVersion) : 0;
  const next: ActivitySummaryAdvertisement = {
    enabled:version >= 2,version,lastResponseAtMs:atMs,
    lastV2AdvertisedAtMs:version >= 2 ? atMs : previous.lastV2AdvertisedAtMs,
    actorBindingVersion:Number.isSafeInteger(response.actorBindingVersion) &&
      Number(response.actorBindingVersion) >= 0 ? Number(response.actorBindingVersion)
        : previous.actorBindingVersion,
    actorBindingInstallHeard:uuid(response.deviceId) ? response.deviceId
      : previous.actorBindingInstallHeard,
  };
  db.prepare(`insert into maintenance_state(key,value,updated_at) values(?,?,?)
    on conflict(key) do update set value=excluded.value,updated_at=excluded.updated_at`)
    .run(keyFor(installKey), JSON.stringify(next), new Date(atMs).toISOString());
  return next;
}

/** History and session tools read the live ledger through a read-only handle.
 * Use one short, separate writer turn for their authenticated acknowledgements. */
export function observeActivitySummaryAdvertisementAtPath(ledgerPath: string,
  installKey: string, body: unknown, atMs = Date.now()): ActivitySummaryAdvertisement {
  const db = new Database(ledgerPath, {fileMustExist:true,timeout:5000});
  try { return observeActivitySummaryAdvertisement(db,installKey,body,atMs); }
  finally { db.close(); }
}
