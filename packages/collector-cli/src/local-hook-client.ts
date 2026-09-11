import type { LocalIngestAuth } from "./local-auth";

type CommandHookSource = "claude_code" | "codex" | "grok";

const HOOK_PATHS: Record<CommandHookSource, string> = {
  claude_code: "/hooks/claude-code",
  codex: "/hooks/codex",
  grok: "/hooks/grok",
};

function producerToken(auth: LocalIngestAuth, source: CommandHookSource) {
  if (source === "claude_code") return auth.claudeCodeProducer;
  if (source === "codex") return auth.codexProducer;
  return auth.grokProducer;
}

/**
 * Forward one hook body over loopback while keeping producer credentials out
 * of the managed command and process argv. Callers load auth internally.
 */
export async function forwardHookOverLoopback(
  body: string,
  options: {
    source: CommandHookSource;
    port: number;
    auth: LocalIngestAuth;
    fetchImpl?: typeof fetch;
  },
) {
  const token = producerToken(options.auth, options.source);
  if (!token) throw new Error("hook_forward_producer_token_unavailable");
  const response = await (options.fetchImpl ?? fetch)(
    `http://127.0.0.1:${options.port}${HOOK_PATHS[options.source]}`,
    {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-plimsoll-source": options.source,
        "x-plimsoll-token": token,
      },
      body,
    },
  );
  if (response.status !== 202) {
    throw new Error(`hook_forward_http_rejected:${response.status}`);
  }
}
