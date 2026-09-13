import fs from "node:fs";
import path from "node:path";
import { listHookSpoolFiles, readHookSpoolFile, writeHookSpoolFile } from "../../packages/collector-cli/src/hook-spool";

type Fault = "file_sync" | "parent_sync" | "rename" | "directory_sync";
type Check = { name: string; passed: boolean; detail: unknown };

/** Synchronous fault injection touches only one disposable fixture directory. */
function observeWrite(home: string, fault?: Fault) {
  const directory = path.join(home, "hook-spool");
  const calls: string[] = [];
  const descriptors = new Map<number, string>();
  const original = { open: fs.openSync, close: fs.closeSync, sync: fs.fsyncSync, rename: fs.renameSync };
  let injected = false;
  let privateAtSync = false;
  const fail = (stage: Fault) => {
    calls.push(stage);
    if (fault === stage && !injected) {
      injected = true;
      throw Object.assign(new Error("fixture_sync_failure"), { code: "EIO" });
    }
  };
  fs.openSync = ((...args: Parameters<typeof fs.openSync>) => {
    const fd = original.open(...args);
    descriptors.set(fd, String(args[0]));
    return fd;
  }) as typeof fs.openSync;
  fs.closeSync = (fd: number) => {
    original.close(fd);
    descriptors.delete(fd);
  };
  fs.fsyncSync = (fd: number) => {
    const file = descriptors.get(fd);
    if (file === home) fail("parent_sync");
    else if (file === directory) fail("directory_sync");
    else if (file?.startsWith(directory + path.sep) && file.endsWith(".json.tmp")) {
      privateAtSync = (fs.fstatSync(fd).mode & 0o777) === 0o600;
      fail("file_sync");
    }
    original.sync(fd);
  };
  fs.renameSync = (from, to) => {
    if (String(from).startsWith(directory + path.sep) && String(from).endsWith(".json.tmp") && String(to).endsWith(".json")) fail("rename");
    else calls.push("other_rename");
    original.rename(from, to);
  };
  const body = JSON.stringify({ event_type: "session_stop", session_id: "fixture-only-session" });
  let result: ReturnType<typeof writeHookSpoolFile>;
  let leaked: number;
  try {
    result = writeHookSpoolFile({ home, source: "codex", body, blanked: 3 });
    calls.push("returned");
    leaked = descriptors.size;
  } finally {
    fs.openSync = original.open; fs.closeSync = original.close;
    fs.fsyncSync = original.sync; fs.renameSync = original.rename;
    // Report implementation leaks, but never leak a descriptor from the proof itself.
    for (const fd of descriptors.keys()) { try { original.close(fd); } catch { /* already closed */ } }
  }
  return { result, calls, injected, leaked: leaked!, privateAtSync, body };
}

export function hookSpoolDurabilityChecks(fixtureHome: (label: string) => { home: string }): Check[] {
  const checks: Check[] = [];
  const check = (name: string, passed: boolean, detail: unknown) => checks.push({ name, passed, detail });
  const { home } = fixtureHome("durable-order");
  const successful = observeWrite(home);
  const file = successful.calls.indexOf("file_sync");
  const rename = successful.calls.indexOf("rename");
  const directory = successful.calls.lastIndexOf("directory_sync");
  const parent = successful.calls.indexOf("parent_sync");
  const returned = successful.calls.indexOf("returned");
  check("spool_file_flushed_before_publication", file >= 0 && file < rename, successful.calls);
  check("spool_new_directory_parent_flushed_before_publication", parent >= 0 && parent < rename, successful.calls);
  check("spool_directory_flushed_before_success", rename >= 0 && directory > rename && directory < returned && successful.result !== null, successful.calls);
  check("spool_private_at_flush_and_descriptors_closed", successful.privateAtSync && successful.leaked === 0, { privateAtSync: successful.privateAtSync, openDescriptors: successful.leaked });
  const read = successful.result ? readHookSpoolFile(successful.result.path) : null;
  check("spool_durability_preserves_envelope", read?.ok === true && read.envelope.body === successful.body && read.envelope.source === "codex" && read.envelope.blanked === 3, { trusted: read?.ok, fileCount: listHookSpoolFiles(home).length });
  for (const fault of ["file_sync", "parent_sync", "rename", "directory_sync"] as const) {
    const { home: failedHome } = fixtureHome(`durable-${fault}`);
    const result = observeWrite(failedHome, fault);
    check(`spool_${fault}_failure_is_not_success`, result.injected && result.result === null, { injected: result.injected, returnedSuccess: result.result !== null, calls: result.calls });
    check(`spool_${fault}_failure_has_no_visible_pending_file`, listHookSpoolFiles(failedHome).length === 0 && result.leaked === 0, { pending: listHookSpoolFiles(failedHome).length, openDescriptors: result.leaked });
  }
  return checks;
}
