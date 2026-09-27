import type { AiInteractionEvent } from "../../shared/src/index";

export const STOP_WINDOW_PROBE_HEADER = "x-plimsoll-stop-window-probe";

/** A probe remains observable, but carries no spend inputs to any projection. */
export function markStopWindowProbe(event: AiInteractionEvent): AiInteractionEvent {
  return {
    ...event,
    model: undefined,
    inputTokens: undefined,
    outputTokens: undefined,
    cacheReadTokens: undefined,
    cacheCreationTokens: undefined,
    costUsd: undefined,
    costKind: undefined,
    metadata: { stopWindowProbe: true },
  };
}
