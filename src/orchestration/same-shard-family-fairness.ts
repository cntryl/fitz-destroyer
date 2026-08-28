import { createDestroyerToken } from "../auth-token.js";
import type { RunConfig } from "../config.js";
import type { WorkloadShape } from "../workloads/model.js";
import { assertSameShardFamilyFairnessEvidence } from "../workloads/same-shard-family-fairness.js";
import type { Artifacts } from "./artifacts.js";
import type { ComposeStack } from "./compose.js";
import { requiredEvent } from "./workload-log.js";

export async function runSameShardFamilyFairnessScenario(stack: ComposeStack, config: RunConfig, shape: WorkloadShape, artifacts: Artifacts): Promise<void> {
  const startedAt = performance.now();
  const permissions = [`notice://${shape.namespace}/**#*`];
  const role = await stack.startRoleContainers("same-shard-family-fairness", 1, shape, {
    DESTROYER_JWT: createDestroyerToken("identity-a", permissions),
    DESTROYER_SAME_SHARD_URL: "ws://fitz:4090/ws",
  });
  const logs = await stack.finishRoleContainers(role, "same-shard-family-fairness");
  const log = logs.get("0");
  if (log === undefined) throw new Error("same-shard fairness worker log was missing");
  const evidence = requiredEvent(log, "same_shard_family_fairness_worker_complete");
  assertSameShardFamilyFairnessEvidence(evidence);
  await artifacts.writeJson("same-shard-family-fairness-evidence.json", evidence);
  await artifacts.event("same_shard_family_fairness_complete", { ...evidence, elapsedMs: Math.round(performance.now() - startedAt), configuredTimeoutMs: config.requestTimeoutMs });
}
