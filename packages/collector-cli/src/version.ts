import packageMetadata from "../package.json" with { type: "json" };

/**
 * The collector package manifest is the lifecycle version source. Runtime
 * paths, receipts, doctor/fleet adapters, and release tooling must consume
 * this value instead of maintaining their own constants.
 */
export const PLIMSOLL_VERSION = packageMetadata.version;
