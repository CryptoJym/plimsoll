const spawnNonce = process.argv[3] ?? "";
process.on("SIGTERM", () => {});
process.on("message", (request) => {
  if (request?.type !== "run") return;
  process.send?.({
    schema: 1,
    type: "enrichment_job_progress",
    generation: request.generation,
    nonce: request.nonce,
    sequence: 1,
    rows: 1,
    ms: 1,
    remaining: 99,
  });
});
process.send?.({ schema: 1, type: "ready", spawnNonce });
