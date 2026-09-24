/**
 * Generated Grok Build `usage.json` documents for proofs (bead eco-6hoxj.163.20).
 *
 * Key names and JSON types follow the value-blind description of real Grok
 * usage files (four shapes in a 400-file sample: key names and types only, no
 * values). Every number below is synthetic. `GROK_USAGE_DOCUMENTED_SHAPES`
 * restates those four shapes so a proof can hold the generator to them.
 */

export type FixtureModelUsage = {
  model: string;
  input: number;
  cachedRead: number;
  cacheCreation: number;
  output: number;
  reasoning: number;
  modelCalls: number;
  costTicks?: number;
  /** Grok's own usageIsIncomplete on this model row. */
  incomplete?: boolean;
};

export type FixtureTurn = {
  turnNumber: number;
  endedAt: string;
  models: FixtureModelUsage[];
  primaryModelId?: string;
  /** Grok's own usageIsIncomplete on the turn record. */
  incomplete?: boolean;
  /** Replace the turn's own totals (to make per-model rows disagree). */
  totalsOverride?: Partial<Omit<FixtureModelUsage, "model">>;
};

export type FixtureSession = {
  sessionId: string;
  updatedAt: string;
  /** `legacy`: the six-file shape without modelUsage, primaryModelId or cost ticks. */
  shape: "modern" | "legacy";
  turns: FixtureTurn[];
  /** Usage Grok's session totals carry that no turn record carries. */
  sessionOnly?: FixtureModelUsage[];
  sessionIncomplete?: boolean;
  /** Extra top-level keys (for planted-content proofs). */
  extra?: Record<string, unknown>;
};

type Totals = {
  inputTokens: number;
  outputTokens: number;
  reasoningTokens: number;
  cachedReadTokens: number;
  cacheCreationTokens: number;
  totalTokens: number;
  modelCalls: number;
  costUsdTicks?: number;
};

function totals(rows: Array<Omit<FixtureModelUsage, "model">>, withCost: boolean): Totals {
  const sum = (read: (row: Omit<FixtureModelUsage, "model">) => number) =>
    rows.reduce((total, row) => total + read(row), 0);
  const input = sum((row) => row.input);
  const output = sum((row) => row.output);
  const result: Totals = {
    inputTokens: input,
    outputTokens: output,
    reasoningTokens: sum((row) => row.reasoning),
    cachedReadTokens: sum((row) => row.cachedRead),
    cacheCreationTokens: sum((row) => row.cacheCreation),
    // Grok's convention: cached reads sit inside input, reasoning inside output.
    totalTokens: input + output,
    modelCalls: sum((row) => row.modelCalls),
  };
  if (withCost && rows.every((row) => row.costTicks !== undefined)) {
    result.costUsdTicks = sum((row) => row.costTicks ?? 0);
  }
  return result;
}

function modelUsage(rows: FixtureModelUsage[], incompleteFlag: boolean) {
  const byModel = new Map<string, FixtureModelUsage[]>();
  for (const row of rows) byModel.set(row.model, [...(byModel.get(row.model) ?? []), row]);
  return Object.fromEntries([...byModel].map(([model, group]) => [model, {
    ...totals(group, true),
    ...(incompleteFlag || group.some((row) => row.incomplete) ? { usageIsIncomplete: true } : {}),
  }]));
}

function primaryModel(rows: FixtureModelUsage[]) {
  const volume = new Map<string, number>();
  for (const row of rows) volume.set(row.model, (volume.get(row.model) ?? 0) + row.input + row.output);
  return [...volume].sort((left, right) => right[1] - left[1] || left[0].localeCompare(right[0]))[0]?.[0];
}

export function grokUsageDocument(session: FixtureSession): Record<string, unknown> {
  const modern = session.shape === "modern";
  const turns = session.turns.map((turn) => {
    const own = { ...totals(turn.models, modern), ...(turn.totalsOverride
      ? Object.fromEntries(Object.entries({
          inputTokens: turn.totalsOverride.input,
          outputTokens: turn.totalsOverride.output,
          reasoningTokens: turn.totalsOverride.reasoning,
          cachedReadTokens: turn.totalsOverride.cachedRead,
          cacheCreationTokens: turn.totalsOverride.cacheCreation,
          costUsdTicks: turn.totalsOverride.costTicks,
        }).filter(([, value]) => value !== undefined))
      : {}) };
    if (turn.totalsOverride && (turn.totalsOverride.input !== undefined || turn.totalsOverride.output !== undefined)) {
      own.totalTokens = own.inputTokens + own.outputTokens;
    }
    return {
      ...own,
      endedAt: turn.endedAt,
      ...(modern ? {
        modelUsage: modelUsage(turn.models, turn.incomplete === true),
        primaryModelId: turn.primaryModelId ?? primaryModel(turn.models),
      } : {}),
      ...(turn.incomplete ? { usageIsIncomplete: true } : {}),
      turnCount: 1,
      turnNumber: turn.turnNumber,
    };
  });
  const allRows = [...session.turns.flatMap((turn) => turn.models), ...(session.sessionOnly ?? [])];
  // Session totals add up the turn records (as written, overrides included)
  // plus any usage Grok kept only at the session level.
  const sessionOnly = totals(session.sessionOnly ?? [], modern);
  const summed = (key: keyof Totals) => turns.reduce((total, turn) =>
    total + Number((turn as Record<string, unknown>)[key] ?? 0), 0) + Number(sessionOnly[key] ?? 0);
  const turnTicks = turns.every((turn) => (turn as Record<string, unknown>).costUsdTicks !== undefined) &&
    (session.sessionOnly ?? []).every((row) => row.costTicks !== undefined);
  const sessionTotals = {
    inputTokens: summed("inputTokens"),
    outputTokens: summed("outputTokens"),
    reasoningTokens: summed("reasoningTokens"),
    cachedReadTokens: summed("cachedReadTokens"),
    cacheCreationTokens: summed("cacheCreationTokens"),
    totalTokens: summed("inputTokens") + summed("outputTokens"),
    modelCalls: summed("modelCalls"),
    ...(modern && turnTicks ? { costUsdTicks: summed("costUsdTicks") } : {}),
    ...(modern ? {
      modelUsage: modelUsage(allRows, session.sessionIncomplete === true),
      primaryModelId: primaryModel(allRows),
    } : {}),
    turnCount: session.turns.length,
    ...(session.sessionIncomplete ? { usageIsIncomplete: true } : {}),
  };
  return {
    ...(session.extra ?? {}),
    session: sessionTotals,
    sessionId: session.sessionId,
    turns,
    updatedAt: session.updatedAt,
  };
}

/**
 * The value-blind shape of a JSON value: object keys with JSON types, arrays
 * as the shape of their elements. Two documents with the same keys and types
 * have the same shape whatever their numbers.
 */
export function valueBlindShape(value: unknown): unknown {
  if (Array.isArray(value)) {
    const shapes = value.map(valueBlindShape);
    const distinct = [...new Map(shapes.map((shape) => [JSON.stringify(shape), shape])).values()];
    return distinct;
  }
  if (value && typeof value === "object") {
    return Object.fromEntries(Object.keys(value).sort().map((key) =>
      [key, valueBlindShape((value as Record<string, unknown>)[key])]));
  }
  if (typeof value === "number") return Number.isInteger(value) ? "int" : "float";
  if (typeof value === "string") return "str";
  if (typeof value === "boolean") return "bool";
  return value === null ? "null" : typeof value;
}

/** Key-sorted copy, so two shape descriptions compare as JSON text. */
export function canonicalShape(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonicalShape);
  if (value && typeof value === "object") {
    return Object.fromEntries(Object.keys(value).sort().map((key) =>
      [key, canonicalShape((value as Record<string, unknown>)[key])]));
  }
  return value;
}

const modelRow = (incomplete = false) => ({
  cacheCreationTokens: "int",
  cachedReadTokens: "int",
  costUsdTicks: "int",
  inputTokens: "int",
  modelCalls: "int",
  outputTokens: "int",
  reasoningTokens: "int",
  totalTokens: "int",
  ...(incomplete ? { usageIsIncomplete: "bool" } : {}),
});

const aggregate = {
  cacheCreationTokens: "int",
  cachedReadTokens: "int",
  costUsdTicks: "int",
  inputTokens: "int",
  modelCalls: "int",
  outputTokens: "int",
  primaryModelId: "str",
  reasoningTokens: "int",
  totalTokens: "int",
  turnCount: "int",
};

const legacyAggregate = {
  cacheCreationTokens: "int",
  cachedReadTokens: "int",
  inputTokens: "int",
  modelCalls: "int",
  outputTokens: "int",
  reasoningTokens: "int",
  totalTokens: "int",
  turnCount: "int",
};

function documented(model: string) {
  return {
    session: { ...aggregate, modelUsage: { [model]: modelRow() } },
    sessionId: "str",
    turns: [{ ...aggregate, endedAt: "str", modelUsage: { [model]: modelRow() }, turnNumber: "int" }],
    updatedAt: "str",
  };
}

/** The four documented shapes (key names and JSON types only). */
export const GROK_USAGE_DOCUMENTED_SHAPES = [
  documented("grok-4.6-build"),
  documented("grok-4.7-build"),
  {
    session: legacyAggregate,
    sessionId: "str",
    turns: [{ ...legacyAggregate, endedAt: "str", turnNumber: "int" }],
    updatedAt: "str",
  },
  {
    session: {
      ...aggregate,
      modelUsage: { "grok-4.6-build": modelRow(true), "grok-4.7-build": modelRow(true) },
      usageIsIncomplete: "bool",
    },
    sessionId: "str",
    turns: [{ ...aggregate, endedAt: "str", modelUsage: { "grok-4.7-build": modelRow() }, turnNumber: "int" }],
    updatedAt: "str",
  },
].map(canonicalShape);

/** Content files that sit beside usage.json in a real session directory. */
export const GROK_SESSION_CONTENT_FILES = [
  "chat_history.jsonl",
  "chat_history.jsonl.lock",
  "events.jsonl",
  "prompt_context.json",
  "rewind_points.jsonl",
  "rewind_points.jsonl.lock",
  "signals.json",
  "summary.json",
  "summary.json.lock",
  "system_prompt.txt",
  "title_refresh_idx",
  "tool_definitions.json",
  "updates.jsonl",
  "updates.jsonl.lock",
  "announcement_state.json",
  "resources_state.json",
] as const;
