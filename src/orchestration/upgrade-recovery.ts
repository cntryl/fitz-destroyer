import type { RunConfig } from "../config.js";
import { totalDurableEntries, type WorkloadShape } from "../workloads/model.js";
import type { Artifacts } from "./artifacts.js";
import type { ComposeStack } from "./compose.js";

export async function runUpgradeRecoveryScenario(
  stack: ComposeStack,
  config: RunConfig,
  shape: WorkloadShape,
  artifacts: Artifacts,
): Promise<void> {
  const startedAt = performance.now();
  await stack.runRecoveryJob("load", shape);
  const replacementStartedAt = performance.now();
  const replacement = await stack.replaceFitzForUpgrade();
  const replacementMs = Math.round(performance.now() - replacementStartedAt);
  await stack.runRecoveryJob("verify", shape);
  await artifacts.event("upgrade_recovery_complete", {
    loaded: totalDurableEntries(shape),
    verified: totalDurableEntries(shape),
    replacementMs,
    crossVersion: replacement.crossVersion,
    crossVersionRequested: config.upgradeFromImage !== undefined,
    sourceImageId: replacement.sourceImageId,
    targetImageId: replacement.targetImageId,
    elapsedMs: Math.round(performance.now() - startedAt),
  });
}
