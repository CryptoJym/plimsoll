/** A foreign writer's same-name link must not be removed as our crash residue. */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { collectorConfigSchema } from "../../packages/collector-cli/src/config";
import { joinRestartObligationPath, readJoinRestartObligation,
  writeJoinRestartObligation } from "../../packages/collector-cli/src/join-setup-journal";

const home = fs.mkdtempSync(path.join(process.cwd(), "pr428-r5-prepared-link-"));
const final = joinRestartObligationPath(home);
const nativeLink = fs.linkSync;
let recordedBeforePublication = false;
try {
  fs.linkSync = ((from: fs.PathLike, to: fs.PathLike) => {
    if (String(to) === final) {
      const journalName = fs.readdirSync(home).find((name) =>
        name.startsWith("join.restart-obligation.json.prepared-journal-"));
      if (journalName) {
        const journal = JSON.parse(fs.readFileSync(path.join(home, journalName), "utf8")) as {
          preparedName?: string; device?: string; inode?: string;
        };
        const created = fs.statSync(from, { bigint: true });
        recordedBeforePublication = journal.preparedName === path.basename(String(from)) &&
          journal.device === String(created.dev) && journal.inode === String(created.ino) &&
          /\.prepared-[0-9a-f-]{36}-[0-9a-f-]{36}$/i.test(journal.preparedName ?? "");
      }
    }
    return nativeLink(from, to);
  }) as typeof fs.linkSync;
  const configBeforeRoots = `${JSON.stringify(collectorConfigSchema.parse({ port: 49390 }))}\n`;
  const obligation = writeJoinRestartObligation(home, {
    port: 49390, configPath: path.join(home, "collector.config.json"), configBeforeRoots,
    priorManifestDigest: null, priorContent: null, priorOwnedTemplateContent: null,
    priorOwnedTemplateIdentityContent: null,
    replacementManifestDigest: `sha256:${"0".repeat(64)}`,
  });
  fs.linkSync = nativeLink;
  const foreignLink = `${final}.prepared-${obligation.operationId}`;
  // The first writer finished. A separate owner process now creates the same
  // name from the readable operation ID in the final obligation.
  fs.linkSync(final, foreignLink);
  let threw = false;
  try { readJoinRestartObligation(home); } catch { threw = true; }
  const observed = { recordedBeforePublication, threw, foreignLinkPreserved: fs.existsSync(foreignLink),
    finalLinks: fs.statSync(final).nlink };
  console.log(JSON.stringify(observed));
  assert.ok(recordedBeforePublication,
    "the random prepared name and created inode were not journaled before publication");
  assert.ok(threw || observed.foreignLinkPreserved,
    "recovery removed a hard link created after publication by another process");
} finally {
  fs.linkSync = nativeLink;
  fs.rmSync(home, { recursive: true, force: true });
}
