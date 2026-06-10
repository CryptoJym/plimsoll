/**
 * Model pricing for cost estimation where telemetry carries no vendor cost.
 * Claude Code reports cost_usd directly, so this table is primarily for
 * OpenAI/Codex. Estimated costs are always flagged — the dashboard renders
 * them as computed, never as vendor-reported truth.
 *
 * Prices are USD per 1M tokens. Source: OpenAI pricing page, fetched
 * 2026-06-10 (https://platform.openai.com/docs/pricing). OpenAI semantics:
 * input_tokens INCLUDES cached tokens; cached portion bills at cachedInput.
 */
export type ModelPrice = {
  input: number;
  cachedInput: number;
  output: number;
  vendor: "openai" | "anthropic";
  asOf: string;
};

export const MODEL_PRICING: Record<string, ModelPrice> = {
  "gpt-5.5": { input: 5.0, cachedInput: 0.5, output: 30.0, vendor: "openai", asOf: "2026-06-10" },
  "gpt-5.4": { input: 2.5, cachedInput: 0.25, output: 15.0, vendor: "openai", asOf: "2026-06-10" },
  "gpt-5.3-codex": { input: 1.75, cachedInput: 0.175, output: 14.0, vendor: "openai", asOf: "2026-06-10" },
  // Superseded frontier model; rate from its own model page (snapshot
  // gpt-5.2-2025-12-11), fetched 2026-06-10. The rollout backfill surfaced
  // 12.5M unpriced tokens on it (issue 0025 / GH #32).
  "gpt-5.2": { input: 1.75, cachedInput: 0.175, output: 14.0, vendor: "openai", asOf: "2026-06-10" },
  // Anthropic rates (platform.claude.com/docs pricing, fetched 2026-06-10).
  // cachedInput = cache HIT rate (0.1x input). Anthropic semantics differ
  // from OpenAI: usage.input_tokens EXCLUDES cache reads. Cache WRITES bill
  // 1.25x input but have no column yet (issue 0024) — estimates exclude
  // them and are therefore a floor. Long-context (1m) tiers bill standard.
  "claude-fable-5": { input: 10.0, cachedInput: 1.0, output: 50.0, vendor: "anthropic", asOf: "2026-06-10" },
  "claude-opus-4-8": { input: 5.0, cachedInput: 0.5, output: 25.0, vendor: "anthropic", asOf: "2026-06-10" },
  "claude-opus-4-7": { input: 5.0, cachedInput: 0.5, output: 25.0, vendor: "anthropic", asOf: "2026-06-10" },
  "claude-opus-4-6": { input: 5.0, cachedInput: 0.5, output: 25.0, vendor: "anthropic", asOf: "2026-06-10" },
  "claude-opus-4-5": { input: 5.0, cachedInput: 0.5, output: 25.0, vendor: "anthropic", asOf: "2026-06-10" },
  "claude-sonnet-4-6": { input: 3.0, cachedInput: 0.3, output: 15.0, vendor: "anthropic", asOf: "2026-06-10" },
  "claude-sonnet-4-5": { input: 3.0, cachedInput: 0.3, output: 15.0, vendor: "anthropic", asOf: "2026-06-10" },
  "claude-haiku-4-5": { input: 1.0, cachedInput: 0.1, output: 5.0, vendor: "anthropic", asOf: "2026-06-10" },
};

export function priceForModel(model: string | undefined): ModelPrice | undefined {
  if (!model) return undefined;
  const key = model.toLowerCase().trim();
  if (MODEL_PRICING[key]) return MODEL_PRICING[key];
  // Longest-prefix match handles dated/suffixed ids (gpt-5.5-2026-01-01).
  let best: { length: number; price: ModelPrice } | undefined;
  for (const [candidate, price] of Object.entries(MODEL_PRICING)) {
    if (key.startsWith(candidate) && (!best || candidate.length > best.length)) {
      best = { length: candidate.length, price };
    }
  }
  return best?.price;
}

export function estimateCostUsd(options: {
  model?: string;
  inputTokens?: number;
  outputTokens?: number;
  cacheReadTokens?: number;
}): { costUsd: number; model: string; estimated: true } | undefined {
  const price = priceForModel(options.model);
  if (!price) return undefined;
  const input = options.inputTokens ?? 0;
  // OpenAI reports cached as a subset of input; Anthropic reports input
  // EXCLUSIVE of cache reads (reads can be 1000x input — never clamp them).
  const cached =
    price.vendor === "openai"
      ? Math.min(options.cacheReadTokens ?? 0, input)
      : options.cacheReadTokens ?? 0;
  const output = options.outputTokens ?? 0;
  if (input === 0 && output === 0 && cached === 0) return undefined;
  const billableInput = price.vendor === "openai" ? input - cached : input;
  const costUsd =
    (billableInput * price.input + cached * price.cachedInput + output * price.output) / 1_000_000;
  return { costUsd: Number(costUsd.toFixed(6)), model: options.model as string, estimated: true };
}
