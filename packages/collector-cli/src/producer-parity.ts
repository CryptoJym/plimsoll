import fs from "node:fs";
import path from "node:path";

import { isUuid } from "./normalizer";

export const PRODUCER_PARITY_DIRECTORY = "producer-parity";
export const PRODUCER_PARITY_LOG = "hooks.jsonl";
export const PRODUCER_PARITY_SCHEMA = "plimsoll.producer-parity/v1";
export const PRODUCER_EVENT_ID_HEADER = "x-plimsoll-event-id";

export type ProducerHookSource = "claude_code" | "codex" | "grok";
export type ProducerObserver = "producer" | "collector";
export type ProducerOutcome = "accepted" | "busy" | "timeout" | "retry" | "drop";

export type ProducerObservation = {
  v: 1;
  id: string;
  src: ProducerHookSource | string;
  http: string;
  exit?: number;
  ts: string;
  observer?: ProducerObserver;
  outcome?: ProducerOutcome;
};

export type ProducerParityCounters = {
  accepted202: number;
  busy503: number;
  timeout: number;
  retry: number;
  drop: number;
};

export type ProducerParityReport = {
  schema: typeof PRODUCER_PARITY_SCHEMA;
  windowHours: number;
  since: string;
  until: string;
  counters: ProducerParityCounters;
  producerAccepted: number;
  ledgerDurable: number;
  pendingSpool: number;
  dropped: number;
  unmatchedProducerIds: string[];
  unmatchedLedgerIds: string[];
  retryIds: string[];
  parity: boolean;
};

const HOOK_SOURCES = new Set<string>(["claude_code", "codex", "grok"]);

export function producerParityDirectory(home: string) {
  return path.join(home, PRODUCER_PARITY_DIRECTORY);
}

export function producerParityLogPath(home: string) {
  return path.join(producerParityDirectory(home), PRODUCER_PARITY_LOG);
}

export function readProducerEventIdHeader(value: string | string[] | undefined) {
  const first = Array.isArray(value) ? value[0] : value;
  if (typeof first !== "string") return undefined;
  const trimmed = first.trim().toLowerCase();
  return isUuid(trimmed) ? trimmed : undefined;
}

export function classifyProducerOutcome(http: string, priorCount = 0): ProducerOutcome {
  if (priorCount > 0) return "retry";
  if (http === "202") return "accepted";
  if (http === "503") return "busy";
  if (http === "408" || http === "000") return "timeout";
  return "drop";
}

function parseObservation(line: string): ProducerObservation | null {
  try {
    const parsed = JSON.parse(line) as ProducerObservation;
    if (parsed?.v !== 1 || typeof parsed.id !== "string" || !isUuid(parsed.id)) return null;
    if (typeof parsed.http !== "string" || typeof parsed.ts !== "string") return null;
    return parsed;
  } catch {
    return null;
  }
}

export function appendProducerObservation(
  home: string,
  observation: Omit<ProducerObservation, "v">,
) {
  const directory = producerParityDirectory(home);
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  try {
    fs.chmodSync(directory, 0o700);
  } catch {
    /* directory mode is best-effort */
  }
  const record: ProducerObservation = { v: 1, ...observation, id: observation.id.toLowerCase() };
  fs.appendFileSync(producerParityLogPath(home), `${JSON.stringify(record)}\n`, { mode: 0o600 });
  return record;
}

export function readProducerObservations(home: string, sinceMs?: number): ProducerObservation[] {
  const file = producerParityLogPath(home);
  if (!fs.existsSync(file)) return [];
  const since = sinceMs === undefined ? undefined : new Date(sinceMs).toISOString();
  return fs.readFileSync(file, "utf8").split("\n").flatMap((line) => {
    if (!line.trim()) return [];
    const parsed = parseObservation(line);
    if (!parsed) return [];
    if (since && parsed.ts < since) return [];
    return [parsed];
  });
}

export function tallyProducerCounters(observations: ProducerObservation[]): ProducerParityCounters {
  const collectorById = new Map<string, number>();
  const counters: ProducerParityCounters = {
    accepted202: 0,
    busy503: 0,
    timeout: 0,
    retry: 0,
    drop: 0,
  };
  for (const observation of observations) {
    const collector = observation.observer !== "producer";
    const prior = collector ? collectorById.get(observation.id) ?? 0 : 0;
    if (collector) collectorById.set(observation.id, prior + 1);
    const outcome = observation.outcome ?? classifyProducerOutcome(observation.http, prior);
    if (observation.http === "202") counters.accepted202 += 1;
    if (observation.http === "503") counters.busy503 += 1;
    if (observation.http === "408" || observation.http === "000") counters.timeout += 1;
    if (prior > 0 || outcome === "retry") counters.retry += 1;
    if (outcome === "drop" && observation.http !== "202") counters.drop += 1;
  }
  return counters;
}

export function listPendingSpoolIds(home: string): string[] {
  const directory = path.join(home, "hook-spool");
  if (!fs.existsSync(directory)) return [];
  const ids: string[] = [];
  for (const name of fs.readdirSync(directory)) {
    if (!name.endsWith(".json") || name.startsWith(".")) continue;
    try {
      const body = JSON.parse(fs.readFileSync(path.join(directory, name), "utf8")) as {
        body?: string;
        id?: string;
      };
      if (typeof body.id === "string" && isUuid(body.id)) {
        ids.push(body.id.toLowerCase());
        continue;
      }
      if (typeof body.body === "string") {
        const inner = JSON.parse(body.body) as { id?: string };
        if (typeof inner.id === "string" && isUuid(inner.id)) ids.push(inner.id.toLowerCase());
      }
    } catch {
      /* unreadable spool files are not producer ids */
    }
  }
  return ids;
}

export function buildProducerParityReport(options: {
  home: string;
  windowHours: number;
  nowMs?: number;
  ledger: Array<{ id: string; createdAt: string; source: string }>;
}): ProducerParityReport {
  const nowMs = options.nowMs ?? Date.now();
  const sinceMs = nowMs - Math.max(1, options.windowHours) * 60 * 60 * 1000;
  const since = new Date(sinceMs).toISOString();
  const until = new Date(nowMs).toISOString();
  const observations = readProducerObservations(options.home, sinceMs);
  const counters = tallyProducerCounters(observations);
  const acceptedIds = new Set(
    observations.filter((row) => row.http === "202").map((row) => row.id.toLowerCase()),
  );
  const collectorCounts = new Map<string, number>();
  for (const row of observations) {
    if (row.observer === "producer") continue;
    collectorCounts.set(row.id, (collectorCounts.get(row.id) ?? 0) + 1);
  }
  const retryIds = [...collectorCounts.entries()].filter(([, count]) => count > 1).map(([id]) => id).sort();
  const ledgerIds = new Set(
    options.ledger
      .filter((row) => HOOK_SOURCES.has(row.source) && row.createdAt >= since)
      .map((row) => row.id.toLowerCase()),
  );
  const pendingSpool = new Set(listPendingSpoolIds(options.home));
  const unmatchedProducerIds = [...acceptedIds].filter((id) => !ledgerIds.has(id) && !pendingSpool.has(id)).sort();
  const unmatchedLedgerIds = [...ledgerIds].filter((id) => !acceptedIds.has(id)).sort();
  const pendingAccepted = [...acceptedIds].filter((id) => pendingSpool.has(id) && !ledgerIds.has(id)).length;
  const ledgerDurable = [...acceptedIds].filter((id) => ledgerIds.has(id)).length;
  const dropped = unmatchedProducerIds.length;
  return {
    schema: PRODUCER_PARITY_SCHEMA,
    windowHours: options.windowHours,
    since,
    until,
    counters,
    producerAccepted: acceptedIds.size,
    ledgerDurable,
    pendingSpool: pendingAccepted,
    dropped,
    unmatchedProducerIds,
    unmatchedLedgerIds,
    retryIds,
    parity: dropped === 0 && acceptedIds.size === ledgerDurable + pendingAccepted,
  };
}
