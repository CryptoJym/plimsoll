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
  const cached = Math.min(options.cacheReadTokens ?? 0, input);
  const output = options.outputTokens ?? 0;
  if (input === 0 && output === 0) return undefined;
  const billableInput = price.vendor === "openai" ? input - cached : input;
  const costUsd =
    (billableInput * price.input + cached * price.cachedInput + output * price.output) / 1_000_000;
  return { costUsd: Number(costUsd.toFixed(6)), model: options.model as string, estimated: true };
}
