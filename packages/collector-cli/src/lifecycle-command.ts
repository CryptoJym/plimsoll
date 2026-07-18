import {
  PURGE_CONFIRMATION,
  LifecycleManager,
  lifecycleBoundaryStatement,
  type LifecycleAdapter,
  type RuntimeArtifact,
} from "./lifecycle";

export type LifecycleArtifactResolver = (reference: string) => Promise<RuntimeArtifact>;

function option(argv: readonly string[], name: string) {
  const index = argv.indexOf(name);
  return index >= 0 ? argv[index + 1] : undefined;
}

/** Injectable command boundary used by the packaged installer. */
export async function runLifecycleCommand(input: {
  argv: readonly string[];
  adapter: LifecycleAdapter;
  resolveArtifact: LifecycleArtifactResolver;
}) {
  const [action] = input.argv;
  const operationId = option(input.argv, "--operation-id") ?? "";
  const manager = new LifecycleManager(input.adapter);
  if (action === "update" || action === "rollback") {
    const reference = option(input.argv, "--artifact");
    if (!reference) throw new Error(`${action} requires --artifact`);
    const artifact = await input.resolveArtifact(reference);
    const receipt = action === "update"
      ? await manager.update({ operationId, artifact })
      : await manager.rollback({ operationId, artifact });
    return { receipt, boundary: lifecycleBoundaryStatement() };
  }
  if (action === "uninstall") {
    const receipt = await manager.uninstall({ operationId, apply: input.argv.includes("--apply") });
    return { receipt, boundary: lifecycleBoundaryStatement() };
  }
  if (action === "purge") {
    const confirmation = option(input.argv, "--confirm-exact") ?? "";
    if (confirmation !== PURGE_CONFIRMATION) throw new Error("purge exact confirmation missing");
    const receipt = await manager.purge({ operationId, confirmation });
    return { receipt, boundary: lifecycleBoundaryStatement() };
  }
  if (action === "support-bundle") {
    const result = await manager.supportBundle(operationId);
    return { ...result, boundary: lifecycleBoundaryStatement() };
  }
  throw new Error("Expected lifecycle update|rollback|uninstall|purge|support-bundle");
}
