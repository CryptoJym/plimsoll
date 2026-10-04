import { spawn as nodeSpawn, spawnSync as nodeSpawnSync, type SpawnOptions, type SpawnSyncOptions } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

export * from "node:child_process";

/** Proof children run files, without tsx's CLI IPC listener or extra process. */
function invocation(command: string, args: readonly string[], cwd?: string | URL) {
  const directory = cwd instanceof URL ? fileURLToPath(cwd) : cwd ?? process.cwd();
  function loaderFor(cli: string) {
    const absolute = path.resolve(directory, cli);
    if (absolute.endsWith(`${path.sep}node_modules${path.sep}tsx${path.sep}dist${path.sep}cli.mjs`)) {
      return path.join(path.dirname(absolute), "loader.mjs");
    }
    if (absolute.endsWith(`${path.sep}node_modules${path.sep}.bin${path.sep}tsx`)) {
      return path.join(path.dirname(path.dirname(absolute)), "tsx/dist/loader.mjs");
    }
    return null;
  }
  if (path.basename(command) === "node" && args[0]) {
    const loader = loaderFor(args[0]);
    if (loader) return { command, args: ["--import", loader, ...args.slice(1)] };
  }
  const loader = loaderFor(command);
  if (loader) return { command: process.execPath, args: ["--import", loader, ...args] };
  if (command === "pnpm" && args[0] === "exec" && args[1] === "tsx") {
    return { command: process.execPath, args: ["--import", path.resolve(directory, "node_modules/tsx/dist/loader.mjs"), ...args.slice(2)] };
  }
  return { command, args: [...args] };
}

// Retain Node's overloads, including encoding-dependent spawnSync result types.
export const spawn: typeof nodeSpawn = ((command: string, argsOrOptions?: readonly string[] | SpawnOptions, options?: SpawnOptions) => {
  const args = Array.isArray(argsOrOptions) ? argsOrOptions : [];
  const opts = options ?? (Array.isArray(argsOrOptions) ? undefined : argsOrOptions as SpawnOptions | undefined);
  const child = invocation(command, args, opts?.cwd);
  return nodeSpawn(child.command, child.args, opts ?? {});
}) as typeof nodeSpawn;

export const spawnSync: typeof nodeSpawnSync = ((command: string, argsOrOptions?: readonly string[] | SpawnSyncOptions, options?: SpawnSyncOptions) => {
  const args = Array.isArray(argsOrOptions) ? argsOrOptions : [];
  const opts = options ?? (Array.isArray(argsOrOptions) ? undefined : argsOrOptions as SpawnSyncOptions | undefined);
  const child = invocation(command, args, opts?.cwd);
  return nodeSpawnSync(child.command, child.args, opts);
}) as typeof nodeSpawnSync;
