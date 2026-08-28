import { createDestroyerToken } from "../auth-token.js";
import type { RunConfig } from "../config.js";
import type { WorkloadShape } from "../workloads/model.js";
import { assertSameShardFamilyFailureEvidence } from "../workloads/same-shard-family-failure-isolation.js";
import type { Artifacts } from "./artifacts.js";
import type { ComposeStack } from "./compose.js";
import { requiredEvent } from "./workload-log.js";

export async function runSameShardFamilyFailureIsolationScenario(
  stack: ComposeStack,
  config: RunConfig,
  shape: WorkloadShape,
  artifacts: Artifacts,
): Promise<void> {
  const startedAt = performance.now();
  const permissions = [
    `stream://${shape.namespace}/**#*`,
    `rpc://${shape.namespace}/**#*`,
  ];
  const role = await stack.startRoleContainers("same-shard-family-failure-isolation", 1, shape, {
    DESTROYER_JWT: createDestroyerToken("identity-a", permissions),
    DESTROYER_SAME_SHARD_FAILURE_URL: "ws://fitz:4090/ws",
    DESTROYER_SAME_SHARD_FAILURE_HTTP_URL: "http://fitz:4090",
  });
  const logs = await stack.finishRoleContainers(role, "same-shard-family-failure-isolation");
  const log = logs.get("0");
  if (log === undefined) throw new Error("same-shard family failure worker log was missing");
  const evidence = requiredEvent(log, "same_shard_family_failure_isolation_worker_complete");
  assertSameShardFamilyFailureEvidence(evidence);
  await artifacts.writeJson("same-shard-family-failure-isolation-evidence.json", evidence);
  await artifacts.event("same_shard_family_failure_isolation_complete", {
    ...evidence,
    elapsedMs: Math.round(performance.now() - startedAt),
    configuredTimeoutMs: config.requestTimeoutMs,
  });
}
