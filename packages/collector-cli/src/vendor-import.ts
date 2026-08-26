import type { LocalEventBuffer } from "./buffer";
import { deterministicEventId } from "./normalizer";
import { aiInteractionEventSchema, type AiInteractionEvent } from "../../shared/src/index";

export const VENDOR_IMPORT_SOURCE = "vendor_import" as const;
export const VENDOR_IMPORT_EVENT_TYPE = "usage_vendor_import" as const;

export type VendorImportVendor = "anthropic" | "openai";

type VendorMetric =
  | "inputTokens"
  | "outputTokens"
  | "cacheReadTokens"
  | "cacheCreationTokens"
  | "costUsd";

type ParsedDailyUsage = {
  day: string;
  inputTokens?: number;
  outputTokens?: number;
  cacheReadTokens?: number;
  cacheCreationTokens?: number;
  costUsd?: number;
};

export type VendorImportParseResult = {
  vendor: VendorImportVendor;
  days: ParsedDailyUsage[];
  rowsRead: number;
  rowsIgnored: number;
};

export type VendorImportReceipt = {
  vendor: VendorImportVendor;
  rowsRead: number;
  rowsIgnored: number;
  candidateDays: number;
  importedDays: number;
  importedDaysByDay: string[];
  deduplicatedDays: number;
  skippedOverlapDays: number;
  skippedLocalFloorDays: number;
  earliestVendorDay: string | null;
  allHistoryExtended: boolean;
};

const MAX_IMPORT_BYTES = 50 * 1024 * 1024;
const MAX_IMPORT_ROWS = 250_000;
const MAX_IMPORT_COLUMNS = 64;
const MAX_CELL_BYTES = 16 * 1024;

const HEADER_ALIASES: Record<VendorMetric | "day", string[]> = {
  day: [
    "date",
    "day",
    "usagedate",
    "dateutc",
    "timestamp",
    "startdate",
  ],
  inputTokens: [
    "inputtokens",
    "inputtoken",
    "inputtokensuncached",
    "inputtokenscount",
    "inputtokensused",
  ],
  outputTokens: ["outputtokens", "outputtoken", "outputtokenscount", "outputtokensused"],
  cacheReadTokens: [
    "cachereadinputtokens",
    "cachereadtokens",
    "cachedinputtokens",
    "cacheinputtokensread",
  ],
  cacheCreationTokens: [
    "cachecreationinputtokens",
    "cachecreationtokens",
    "cachewritetokens",
    "cachecreationinputtoken",
  ],
  costUsd: [
    "cost",
    "costusd",
    "costusdollars",
    "totalcost",
    "totalcostusd",
    "amount",
    "amountusd",
    "spend",
    "spendusd",
  ],
};

const VENDOR_LOCAL_SOURCES: Record<VendorImportVendor, readonly string[]> = {
  anthropic: ["claude_code"],
  openai: ["codex"],
};

function normalizeHeader(value: string) {
  return value.trim().toLowerCase().replace(/[^a-z0-9]/g, "");
}

function parseCsv(text: string) {
  if (Buffer.byteLength(text, "utf8") > MAX_IMPORT_BYTES) {
    throw new Error("vendor_import_file_too_large");
  }

  const rows: string[][] = [];
  let row: string[] = [];
  let cell = "";
  let quoted = false;
  let justClosedQuote = false;

  const pushCell = () => {
    if (Buffer.byteLength(cell, "utf8") > MAX_CELL_BYTES) {
      throw new Error("vendor_import_cell_too_large");
    }
    row.push(cell);
    cell = "";
    justClosedQuote = false;
  };
  const pushRow = () => {
    if (row.length > MAX_IMPORT_COLUMNS) throw new Error("vendor_import_too_many_columns");
    if (row.some((value) => value.length > 0) || row.length > 1) rows.push(row);
    row = [];
    if (rows.length > MAX_IMPORT_ROWS) throw new Error("vendor_import_too_many_rows");
  };

  for (let index = text.charCodeAt(0) === 0xfeff ? 1 : 0; index < text.length; index += 1) {
    const character = text[index]!;
    if (quoted) {
      if (character === '"') {
        if (text[index + 1] === '"') {
          cell += '"';
          index += 1;
        } else {
          quoted = false;
          justClosedQuote = true;
        }
      } else {
        cell += character;
      }
      continue;
    }
    if (character === '"' && cell.length === 0) {
      quoted = true;
      continue;
    }
    if (justClosedQuote && character !== "," && character !== "\r" && character !== "\n") {
      throw new Error("vendor_import_malformed_csv");
    }
    if (character === ",") {
      pushCell();
    } else if (character === "\n" || character === "\r") {
      pushCell();
      pushRow();
      if (character === "\r" && text[index + 1] === "\n") index += 1;
    } else {
      cell += character;
    }
  }
  if (quoted) throw new Error("vendor_import_unclosed_quote");
  if (cell.length > 0 || row.length > 0) {
    pushCell();
    pushRow();
  }
  return rows;
}

function invalidRow(rowNumber: number, reason: string): never {
  throw new Error(`vendor_import_invalid_row_${rowNumber}_${reason}`);
}

function parseDay(value: string, rowNumber: number) {
  const trimmed = value.trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) {
    const parsed = new Date(`${trimmed}T00:00:00.000Z`);
    if (parsed.toISOString().slice(0, 10) === trimmed) return trimmed;
  }
  const timestamp = Date.parse(trimmed);
  if (!Number.isFinite(timestamp)) invalidRow(rowNumber, "date");
  return new Date(timestamp).toISOString().slice(0, 10);
}

function parseNumber(value: string | undefined, rowNumber: number, metric: VendorMetric) {
  if (value === undefined || value.trim() === "") return undefined;
  const normalized = value.trim().replace(/[,$\s]/g, "");
  if (!/^(?:\d+(?:\.\d*)?|\.\d+)$/.test(normalized)) {
    invalidRow(rowNumber, metric);
  }
  const parsed = Number(normalized);
  if (!Number.isFinite(parsed) || parsed < 0) invalidRow(rowNumber, metric);
  if (metric !== "costUsd" && !Number.isSafeInteger(parsed)) invalidRow(rowNumber, metric);
  return parsed;
}

function headerIndex(headers: string[], aliases: readonly string[]) {
  return headers.findIndex((header) => aliases.includes(header));
}

function parseVendor(value: string): VendorImportVendor {
  const normalized = value.trim().toLowerCase();
  if (normalized === "anthropic") return "anthropic";
  if (normalized === "openai") return "openai";
  throw new Error("vendor_import_vendor_unsupported");
}

export function parseVendorCsv(vendorInput: string, text: string): VendorImportParseResult {
  const vendor = parseVendor(vendorInput);
  const rows = parseCsv(text);
  if (rows.length < 2) throw new Error("vendor_import_header_or_rows_missing");
  const headers = rows[0]!.map(normalizeHeader);
  const dateColumn = headerIndex(headers, HEADER_ALIASES.day);
  if (dateColumn < 0) throw new Error("vendor_import_date_column_missing");
  const metricColumns = (Object.keys(HEADER_ALIASES) as Array<VendorMetric | "day">)
    .filter((metric): metric is VendorMetric => metric !== "day")
    .map((metric) => [metric, headerIndex(headers, HEADER_ALIASES[metric])] as const)
    .filter((entry): entry is readonly [VendorMetric, number] => entry[1] >= 0);
  if (metricColumns.length === 0) throw new Error("vendor_import_metric_column_missing");

  const byDay = new Map<string, ParsedDailyUsage>();
  let rowsIgnored = 0;
  for (let index = 1; index < rows.length; index += 1) {
    const sourceRow = rows[index]!;
    if (sourceRow.every((value) => value.trim() === "")) {
      rowsIgnored += 1;
      continue;
    }
    const rowNumber = index + 1;
    const day = parseDay(sourceRow[dateColumn] ?? "", rowNumber);
    const values: Partial<Record<VendorMetric, number>> = {};
    for (const [metric, column] of metricColumns) {
      const parsed = parseNumber(sourceRow[column], rowNumber, metric);
      if (parsed !== undefined) values[metric] = parsed;
    }
    if (Object.keys(values).length === 0) {
      rowsIgnored += 1;
      continue;
    }
    const aggregate = byDay.get(day) ?? { day };
    for (const [metric, value] of Object.entries(values) as Array<[VendorMetric, number]>) {
      const next = (aggregate[metric] ?? 0) + value;
      if (metric === "costUsd") {
        aggregate[metric] = Math.round(next * 1_000_000_000) / 1_000_000_000;
      } else {
        aggregate[metric] = next;
      }
      if (!Number.isFinite(aggregate[metric]) ||
        (metric !== "costUsd" && !Number.isSafeInteger(aggregate[metric]))) {
        invalidRow(rowNumber, metric);
      }
    }
    byDay.set(day, aggregate);
  }
  const days = [...byDay.values()].sort((left, right) => left.day.localeCompare(right.day));
  if (days.length === 0) throw new Error("vendor_import_no_usage_rows");
  return { vendor, days, rowsRead: rows.length - 1, rowsIgnored };
}

function eventId(vendor: VendorImportVendor, day: string) {
  return deterministicEventId(["vendor-import", vendor, day]);
}

function eventFor(vendor: VendorImportVendor, usage: ParsedDailyUsage): AiInteractionEvent {
  return aiInteractionEventSchema.parse({
    id: eventId(vendor, usage.day),
    source: VENDOR_IMPORT_SOURCE,
    eventType: VENDOR_IMPORT_EVENT_TYPE,
    dataMode: "metadata",
    observedAt: `${usage.day}T00:00:00.000Z`,
    actionClass: "other",
    ...(usage.inputTokens !== undefined ? { inputTokens: usage.inputTokens } : {}),
    ...(usage.outputTokens !== undefined ? { outputTokens: usage.outputTokens } : {}),
    ...(usage.cacheReadTokens !== undefined ? { cacheReadTokens: usage.cacheReadTokens } : {}),
    ...(usage.cacheCreationTokens !== undefined ? { cacheCreationTokens: usage.cacheCreationTokens } : {}),
    ...(usage.costUsd !== undefined ? { costUsd: usage.costUsd } : {}),
    metadata: {
      usageSource: vendor,
      vendor,
      provenance: "vendor_reported",
      grain: "day",
      importVersion: 1,
    },
  });
}

function localCoverage(buffer: LocalEventBuffer, vendor: VendorImportVendor) {
  const sources = VENDOR_LOCAL_SOURCES[vendor];
  const placeholders = sources.map(() => "?").join(",");
  const rows = buffer.database.prepare(
    `select distinct substr(observed_at, 1, 10) as day
       from buffered_events where source in (${placeholders})`,
  ).all(...sources) as Array<{ day: string }>;
  return {
    days: new Set(rows.map((row) => row.day)),
    floor: rows.map((row) => row.day).sort()[0] ?? null,
  };
}

function assertNoConflictingRows(buffer: LocalEventBuffer, events: AiInteractionEvent[]) {
  const existing = buffer.database.prepare(
    `select payload_json as payloadJson from buffered_events where id = ?`,
  );
  for (const event of events) {
    const row = existing.get(event.id) as { payloadJson: string } | undefined;
    if (row && row.payloadJson !== JSON.stringify(event)) {
      throw new Error("vendor_import_conflicting_day");
    }
  }
}

function existingEventIds(buffer: LocalEventBuffer, events: AiInteractionEvent[]) {
  const existing = buffer.database.prepare(`select 1 from buffered_events where id = ?`);
  return new Set(events
    .filter((event) => existing.get(event.id))
    .map((event) => event.id));
}

/** Import a vendor export as immutable, sessionless daily facts. */
export function importVendorCsv(
  buffer: LocalEventBuffer,
  vendorInput: string,
  text: string,
): VendorImportReceipt {
  const parsed = parseVendorCsv(vendorInput, text);
  return buffer.database.transaction(() => {
    const coverage = localCoverage(buffer, parsed.vendor);
    const candidates = parsed.days.filter((usage) => !coverage.days.has(usage.day));
    const skippedOverlapDays = parsed.days.filter((usage) => coverage.days.has(usage.day)).length;
    const preFloor = coverage.floor === null
      ? candidates
      : candidates.filter((usage) => usage.day < coverage.floor!);
    const skippedLocalFloorDays = candidates.length - preFloor.length;
    const events = preFloor.map((usage) => eventFor(parsed.vendor, usage));
    assertNoConflictingRows(buffer, events);
    const existingIds = existingEventIds(buffer, events);

    const allHistoryExtended = parsed.days.length > 0
      ? buffer.projection.expandAllHistoryTo(`${parsed.days[0]!.day}T00:00:00.000Z`)
      : false;
    const append = buffer.appendMany(events.map((event) => ({ event, suppressedFields: [] })));
    const importedEvents = events.filter((event) => !existingIds.has(event.id));
    return {
      vendor: parsed.vendor,
      rowsRead: parsed.rowsRead,
      rowsIgnored: parsed.rowsIgnored,
      candidateDays: candidates.length,
      importedDays: importedEvents.length,
      importedDaysByDay: importedEvents.map((event) => event.observedAt.slice(0, 10)),
      deduplicatedDays: append.deduplicatedCount,
      skippedOverlapDays,
      skippedLocalFloorDays,
      earliestVendorDay: parsed.days[0]?.day ?? null,
      allHistoryExtended,
    };
  })();
}
