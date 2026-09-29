/** A second writer must not replace an obligation published during the final commit. */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { joinRestartObligationPath, readJoinRestartObligation, writeJoinRestartObligation } from
  "../packages/collector-cli/src/join-setup-journal";

const root = fs.mkdtempSync(path.join(process.cwd(), "pr428-r4-obligation-race-"));
const file = joinRestartObligationPath(root);
const originalRename = fs.renameSync;
const originalLink = fs.linkSync;
let injected = false;
let rejected = false;
let concurrentOperationId: string | null = null;
try {
  const configBeforeRoots = `${JSON.stringify(collectorConfigSchema.parse({ port: 49390 }))}\n`;
  const input = {
    port: 49390, configPath: path.join(root, "collector.config.json"), configBeforeRoots,
    priorManifestDigest: null, priorContent: null, priorOwnedTemplateContent: null,
    priorOwnedTemplateIdentityContent: null,
    replacementManifestDigest: `sha256:${"0".repeat(64)}`,
  };
  const interleave = (from: fs.PathLike, to: fs.PathLike) => {
    if (!injected && String(to) === file && String(from).includes(".prepared-")) {
      fs.renameSync = originalRename;
      fs.linkSync = originalLink;
      try {
        const concurrent = writeJoinRestartObligation(root, {
          ...input, replacementManifestDigest: `sha256:${"1".repeat(64)}`,
        });
        concurrentOperationId = concurrent.operationId;
      } finally {
        fs.renameSync = interleavedRename as typeof fs.renameSync;
        fs.linkSync = interleavedLink as typeof fs.linkSync;
      }
      injected = true;
    }
  };
  const interleavedRename = ((from: fs.PathLike, to: fs.PathLike) => {
    interleave(from, to);
    return originalRename(from, to);
  }) as typeof fs.renameSync;
  const interleavedLink = ((from: fs.PathLike, to: fs.PathLike) => {
    interleave(from, to);
    return originalLink(from, to);
  }) as typeof fs.linkSync;
  fs.renameSync = interleavedRename;
  fs.linkSync = interleavedLink;
  try {
    writeJoinRestartObligation(root, input);
  } catch { rejected = true; }
  const final = readJoinRestartObligation(root);
  console.log(JSON.stringify({ injected, rejected,
    concurrentOperationId, finalOperationId: final?.operationId ?? null,
    concurrentObligationPreserved: final?.operationId === concurrentOperationId }));
  assert.ok(injected, "the competing writer did not interleave at publication");
  assert.ok(rejected && final?.operationId === concurrentOperationId,
    "a valid concurrent obligation was overwritten by the prepared-file rename");
} finally {
  fs.renameSync = originalRename;
  fs.linkSync = originalLink;
  fs.rmSync(root, { recursive: true, force: true });
}
