/** A foreign writer's same-name link must not be removed as our crash residue. */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { collectorConfigSchema } from "../../packages/collector-cli/src/config";
import { joinRestartObligationPath, readJoinRestartObligation,
  writeJoinRestartObligation } from "../../packages/collector-cli/src/join-setup-journal";

const home = fs.mkdtempSync(path.join(process.cwd(), "pr428-r5-prepared-link-"));
try {
  const configBeforeRoots = `${JSON.stringify(collectorConfigSchema.parse({ port: 49390 }))}\n`;
  const obligation = writeJoinRestartObligation(home, {
    port: 49390, configPath: path.join(home, "collector.config.json"), configBeforeRoots,
    priorManifestDigest: null, priorContent: null, priorOwnedTemplateContent: null,
    priorOwnedTemplateIdentityContent: null,
    replacementManifestDigest: `sha256:${"0".repeat(64)}`,
  });
  const final = joinRestartObligationPath(home);
  const foreignLink = `${final}.prepared-${obligation.operationId}`;
  // The first writer finished. A separate owner process now creates the same
  // name from the readable operation ID in the final obligation.
  fs.linkSync(final, foreignLink);
  let threw = false;
  try { readJoinRestartObligation(home); } catch { threw = true; }
  const observed = { threw, foreignLinkPreserved: fs.existsSync(foreignLink),
    finalLinks: fs.statSync(final).nlink };
  console.log(JSON.stringify(observed));
  assert.ok(threw || observed.foreignLinkPreserved,
    "recovery removed a hard link created after publication by another process");
} finally { fs.rmSync(home, { recursive: true, force: true }); }
