import fs from "node:fs";
import path from "node:path";

const schema = 1;
const spawnNonce = process.env.PLIMSOLL_MAINTENANCE_SPAWN_NONCE ?? "";
const root = process.env.PLIMSOLL_HOME ?? process.env.TMPDIR ?? "/tmp";
const fifoPath = path.join(root, "maintenance-boundary-block.fifo");
const markerPath = path.join(root, "maintenance-boundary-blocked.marker");

// Keep SIGTERM from turning the fixture into a cooperative shutdown. The
// proof needs a child stuck in a real synchronous filesystem operation so the
// parent's TERM -> KILL -> close/reap path is exercised end to end.
process.on("SIGTERM", () => undefined);

process.on("message", (message) => {
  if (!message || message.schema !== schema) return;
  if (message.type === "shutdown") {
    process.send?.({ schema, type: "closed", nonce: message.nonce }, () => {
      process.disconnect?.();
    });
    return;
  }
  if (message.type !== "run") return;

  fs.writeFileSync(markerPath, "blocked\n", { mode: 0o600 });
  // No writer is opened by the proof. This blocks until the supervisor reaps
  // the child, reproducing the macOS filesystem stall that motivated #150.
  fs.openSync(fifoPath, fs.constants.O_RDONLY);
});

process.send?.({ schema, type: "ready", spawnNonce });
