// Proof-only barrier for the real start, forward-hook and status commands.
// Each process reaches the WAL switch before any is released.
const fs = require("node:fs");
const path = require("node:path");
const net = require("node:net");
const Database = require(path.join(process.cwd(), "node_modules/better-sqlite3"));

const root = process.env.STARTUP_SCHEMA_MIXED_ROOT;
const role = process.env.STARTUP_SCHEMA_MIXED_ROLE;
if (!root || !role) throw new Error("startup_schema_fixture_missing");
const ledger = path.join(process.env.PLIMSOLL_HOME, "work-ledger.sqlite");
const originalPragma = Database.prototype.pragma;
let gated = false;
Database.prototype.pragma = function (sql, ...args) {
  if (!gated && this.name === ledger && sql === "journal_mode = WAL") {
    gated = true;
    fs.writeFileSync(path.join(root, `ready-${role}`), String(process.pid));
    const wait = new Int32Array(new SharedArrayBuffer(4));
    const deadline = Date.now() + 15_000;
    while (!fs.existsSync(path.join(root, "go"))) {
      if (Date.now() >= deadline) throw new Error("startup_schema_cli_barrier_timeout");
      Atomics.wait(wait, 0, 0, 2);
    }
  }
  return Reflect.apply(originalPragma, this, [sql, ...args]);
};

// The proof may only listen on its fixture's Unix sockets and ports 49100-49199.
const leases = path.join(process.env.TMPDIR, "startup-schema-port-leases");
fs.mkdirSync(leases, { recursive: true });
const originalListen = net.Server.prototype.listen;
net.Server.prototype.listen = function (...args) {
  const options = typeof args[0] === "object" && args[0] !== null ? args[0] : null;
  const requested = options ? options.port : args[0];
  if (typeof requested !== "number") {
    const socket = options?.path || args[0];
    if (typeof socket !== "string" ||
        !path.resolve(socket).startsWith(path.resolve(process.env.TMPDIR) + path.sep))
      throw new Error("startup_schema_refuses_nonfixture_listener");
    return Reflect.apply(originalListen, this, args);
  }
  let chosen = requested;
  let lease;
  if (requested === 0) {
    for (let i = 0; i < 50; i++) {
      chosen = 49150 + ((process.pid + i) % 50);
      const file = path.join(leases, String(chosen));
      try { fs.writeFileSync(file, String(process.pid), { flag: "wx" }); lease = file; break; }
      catch (error) { if (error.code !== "EEXIST") throw error; }
    }
    if (!lease) throw new Error("startup_schema_fixture_ports_exhausted");
  }
  if (chosen < 49100 || chosen > 49199) throw new Error(`startup_schema_refuses_port_${chosen}`);
  if (options) args[0] = { ...options, port: chosen, host: options.host || "127.0.0.1" };
  else args[0] = chosen;
  const cleanup = () => { if (lease) { try { fs.unlinkSync(lease); } catch {} lease = undefined; } };
  this.once("close", cleanup);
  process.once("exit", cleanup);
  if (role === "daemon") this.once("listening", () => {
    fs.writeFileSync(path.join(root, "listening-daemon"), JSON.stringify(this.address()));
  });
  return Reflect.apply(originalListen, this, args);
};

const childProcess = require("node:child_process");
for (const method of ["spawn", "spawnSync", "execFile", "execFileSync"]) {
  const invoke = childProcess[method];
  childProcess[method] = function (command, ...args) {
    if (/(^|\/)launchctl$/.test(String(command))) throw new Error("startup_schema_refuses_launchctl");
    const index = Array.isArray(args[0]) ? 1 : 0;
    const options = args[index] && typeof args[index] === "object" ? args[index] : {};
    const env = { ...(options.env || process.env), NEXT_TELEMETRY_DISABLED: "1" };
    if (/(^|\/)(node|tsx)$/.test(String(command)))
      env.NODE_OPTIONS = `${env.NODE_OPTIONS || ""} --require=${__filename}`.trim();
    args[index] = { ...options, env };
    return Reflect.apply(invoke, this, [command, ...args]);
  };
}
