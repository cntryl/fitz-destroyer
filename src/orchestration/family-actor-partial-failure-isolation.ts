import { createDestroyerToken } from "../auth-token.js";
import type { RunConfig } from "../config.js";
import { assertFamilyActorPartialFailureEvidence } from "../workloads/family-actor-partial-failure-isolation.js";
import type { WorkloadShape } from "../workloads/model.js";
import type { Artifacts } from "./artifacts.js";
import type { ComposeStack } from "./compose.js";
import { requiredEvent } from "./workload-log.js";

export async function runFamilyActorPartialFailureIsolationScenario(
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
  const role = await stack.startRoleContainers("family-actor-partial-failure-isolation", 1, shape, {
    DESTROYER_JWT: createDestroyerToken("identity-a", permissions),
    DESTROYER_FAMILY_FAILURE_URL: "ws://fitz:4090/ws",
    DESTROYER_FAMILY_FAILURE_HTTP_URL: "http://fitz:4090",
  });
  const logs = await stack.finishRoleContainers(role, "family-actor-partial-failure-isolation");
  const log = logs.get("0");
  if (log === undefined) throw new Error("family actor partial failure worker log was missing");
  const evidence = requiredEvent(log, "family_actor_partial_failure_isolation_worker_complete");
  assertFamilyActorPartialFailureEvidence(evidence);
  await artifacts.writeJson("family-actor-partial-failure-isolation-evidence.json", evidence);
  await artifacts.event("family_actor_partial_failure_isolation_complete", {
    ...evidence,
    elapsedMs: Math.round(performance.now() - startedAt),
    configuredTimeoutMs: config.requestTimeoutMs,
  });
}
