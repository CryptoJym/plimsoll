const schema = 2;
const spawnNonce = process.env.PLIMSOLL_MAINTENANCE_SPAWN_NONCE ?? "";

process.on("message", (message) => {
  if (!message || message.schema !== schema || message.type !== "run") return;
  process.disconnect?.();
  process.exit(17);
});

process.send?.({ schema, type: "ready", spawnNonce });
