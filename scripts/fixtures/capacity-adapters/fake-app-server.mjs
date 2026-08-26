#!/usr/bin/env node
/**
 * Fake `codex app-server` for the provider-capacity-adapters proof (issue
 * #169). Speaks the stable JSONL wire shape WITHOUT the "jsonrpc" member and
 * records every received line so the proof can assert the EXACT outbound
 * sequence. Behavior is selected by FAKE_BEHAVIOR; never touches a network.
 *
 * Env:
 *   FAKE_BEHAVIOR  happy|secondary|provider_error|garbage_line|flood|
 *                  hang_rate_limits|exit_early|slow_init
 *   FAKE_LOG       file the received outbound lines are appended to (JSONL)
 *   FAKE_RELEASE   file whose existence unblocks slow_init
 */
import fs from "node:fs";

const behavior = process.env.FAKE_BEHAVIOR ?? "happy";
const logPath = process.env.FAKE_LOG ?? "";
const releasePath = process.env.FAKE_RELEASE ?? "";

function record(line) {
  if (!logPath) return;
  try {
    fs.appendFileSync(logPath, `${JSON.stringify(JSON.parse(line))}\n`, "utf8");
  } catch {
    fs.appendFileSync(logPath, `${JSON.stringify({ unparsable: line })}\n`, "utf8");
  }
}

let buffer = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk) => {
  buffer += chunk;
  let index = buffer.indexOf("\n");
  while (index !== -1) {
    const line = buffer.slice(0, index).trim();
    buffer = buffer.slice(index + 1);
    index = buffer.indexOf("\n");
    if (line.length > 0) handle(line);
  }
});
process.stdin.on("end", () => {
  process.exit(0);
});

function send(message) {
  process.stdout.write(`${JSON.stringify(message)}\n`);
}

const RATE_LIMITS_RESULT = {
  id: 2,
  result: {
    rateLimits: {
      primary: { usedPercent: 25, windowDurationMins: 15, resetsAt: 1730947200 },
      secondary: null,
      rateLimitReachedType: null,
    },
    rateLimitResetCredits: null,
  },
};

async function waitForRelease() {
  while (!fs.existsSync(releasePath)) {
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

async function handle(line) {
  record(line);
  let message;
  try {
    message = JSON.parse(line);
  } catch {
    return;
  }
  if (message.method === "initialize") {
    if (behavior === "slow_init") await waitForRelease();
    if (behavior === "exit_early") process.exit(3);
    if (behavior === "garbage_line") {
      process.stdout.write("this is not json at all\n");
      return;
    }
    send({ id: message.id, result: { userAgent: "fake-codex/1.0" } });
    return;
  }
  // The initialized notification carries no id and expects no response.
  if (message.method === "initialized") return;
  if (message.method === "account/rateLimits/read") {
    if (behavior === "hang_rate_limits") {
      setInterval(() => {}, 60_000); // hang until killed
      return;
    }
    if (behavior === "flood") {
      const blob = "x".repeat(4096);
      const flush = () => {
        while (true) {
          const ok = process.stdout.write(`{"noise":"${blob}"}\n`);
          if (!ok) break;
        }
      };
      setInterval(flush, 5);
      return;
    }
    if (behavior === "provider_error") {
      send({
        id: message.id,
        error: { code: -32000, message: "SECRET-AUTH-DETAILS-hostile-provider-payload" },
      });
      setTimeout(() => process.exit(0), 20);
      return;
    }
    const base = structuredClone(RATE_LIMITS_RESULT);
    base.id = message.id;
    if (behavior === "secondary") {
      base.result.rateLimits.secondary = {
        usedPercent: 77.5,
        windowDurationMins: 10080,
        resetsAt: 1731552000,
      };
    }
    send(base);
    setTimeout(() => process.exit(0), 20);
    return;
  }
}
