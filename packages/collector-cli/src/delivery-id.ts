import type Database from "better-sqlite3";

import { deterministicEventId } from "./normalizer";

/** PostgreSQL UUID syntax used by both the uploader and retention readers. */
export const POSTGRES_UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export function ensureUuidEventId(rawId: string): { id: string; derived: boolean } {
  if (POSTGRES_UUID_RE.test(rawId)) return { id: rawId, derived: false };
  return { id: deterministicEventId(["workspace-backfill", rawId]), derived: true };
}

const registered = new WeakSet<Database.Database>();

/** The privacy predicate is also used by independent read/export connections. */
export function registerRetentionDeliveryId(db: Database.Database) {
  if (registered.has(db)) return;
  db.function("retention_delivery_id", { deterministic: true },
    (rawId: string) => ensureUuidEventId(rawId).id);
  registered.add(db);
}

/** Inject the same delivery-ID function into eval workers' SQLite readers. */
export const RETENTION_DELIVERY_ID_WORKER_SOURCE = `
  (() => {
    const crypto = require('node:crypto');
    const postgresUuid = workerData.postgresUuid;
    db.function('retention_delivery_id', { deterministic: true }, (rawId) => {
      if (postgresUuid.test(rawId)) return rawId;
      const digest = crypto.createHash('sha256').update('workspace-backfill|' + rawId).digest('hex');
      return [digest.slice(0, 8), digest.slice(8, 12),
        '5' + digest.slice(13, 16), '9' + digest.slice(17, 20), digest.slice(20, 32)].join('-');
    });
  })();
`;
