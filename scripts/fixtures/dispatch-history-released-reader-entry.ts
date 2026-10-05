import fs from "node:fs";import path from "node:path";import crypto from "node:crypto";
import {readCollectorConfig,collectorConfigPath} from "../../packages/collector-cli/src/config";
import {currentDispatchCaptureRoots} from "../../packages/collector-cli/src/capture-root-inventory";
export function readReleasedSnapshot(profile:Buffer,scratchRoot:string) {
  const home=fs.mkdtempSync(path.join(scratchRoot,"released-reader-")),old=process.env.PLIMSOLL_HOME;
  try {process.env.PLIMSOLL_HOME=home;fs.writeFileSync(collectorConfigPath(),profile,{mode:0o600,flag:"wx"});
    const read=readCollectorConfig(),roots=currentDispatchCaptureRoots();
    return {sourceCommit:"34d58bcd90865679e09fcbd1ee1703de5effda97",collectorVersion:"0.7.48",status:read.status,
      profileSha256:crypto.createHash("sha256").update(profile).digest("hex"),roots};
  } finally {process.env.PLIMSOLL_HOME=old;fs.rmSync(home,{recursive:true,force:true});}
}
