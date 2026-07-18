import { createHash } from "node:crypto";

import {
  techniqueExposureFactSchema,
  techniqueExposureInputSchema,
  type TechniqueExposureFact,
  type TechniqueExposureInput,
} from "./schemas";

function uuidV5Shape(digest: string): string {
  return [
    digest.slice(0, 8),
    digest.slice(8, 12),
    `5${digest.slice(13, 16)}`,
    `9${digest.slice(17, 20)}`,
    digest.slice(20, 32),
  ].join("-");
}

/** Hash length-prefixed parts so raw producer IDs never enter promoted facts. */
export function deterministicLearningFactId(parts: readonly string[]): string {
  const framed = parts
    .map((part) => `${Buffer.byteLength(part, "utf8")}:${part}`)
    .join("|");
  return uuidV5Shape(createHash("sha256").update(framed).digest("hex"));
}

/** One shared semantic identity function for producers, stores, and analysis. */
export function deriveTechniqueExposureId(
  input: TechniqueExposureInput | TechniqueExposureFact,
): string {
  const canonical = techniqueExposureInputSchema.parse({
    episodeId: input.episodeId,
    techniqueId: input.techniqueId,
    techniqueVersion: input.techniqueVersion,
    contentDigest: input.contentDigest,
    assignmentId: input.assignmentId,
    workClass: input.workClass,
    complexityBand: input.complexityBand,
    exposedAt: input.exposedAt,
    mode: input.mode,
  });
  return deterministicLearningFactId([
    "technique-exposure-v1",
    canonical.episodeId,
    canonical.techniqueId,
    canonical.techniqueVersion ?? "",
    canonical.contentDigest ?? "",
    canonical.assignmentId,
    canonical.exposedAt,
    canonical.mode,
  ]);
}

/** Parse a closed exposure fact and prove its ID binds every semantic field. */
export function validateTechniqueExposureFactIdentity(input: unknown): TechniqueExposureFact {
  const fact = techniqueExposureFactSchema.parse(input);
  if (fact.exposureId !== deriveTechniqueExposureId(fact)) {
    throw new Error("TechniqueExposureDerivedIdMismatch");
  }
  return fact;
}
