import type { RunConfig } from "../config.js";
import { ALL_DOMAINS, type WorkloadShape } from "../workloads/model.js";
import type { Artifacts } from "./artifacts.js";
import type { ComposeStack } from "./compose.js";
import { numericField, requiredEvent } from "./workload-log.js";

export async function runRouteCardinalityChurnScenario(
  stack: ComposeStack,
  _config: RunConfig,
  shape: WorkloadShape,
  artifacts: Artifacts,
): Promise<void> {
  const startedAt = performance.now();
  const roles = await stack.startRoleContainers("route-cardinality-churn", 1, shape);
  const logs = await stack.finishRoleContainers(roles, "route-cardinality-churn");
  const completion = requiredEvent(logs.get("0") ?? "", "route_cardinality_churn_complete");
  const routes = numericField(completion, "routes");
  const expected = shape.entriesPerResource * ALL_DOMAINS.length;
  if (routes !== expected) throw new Error(`Route cardinality completed ${routes}/${expected} routes`);

  await stack.gracefulRestartFitz();
  const probeShape = { ...shape, namespace: `${shape.namespace}-probe`, entriesPerResource: 1 };
  const probes = await stack.startRoleContainers("canary", 1, probeShape);
  const probeLogs = await stack.finishRoleContainers(probes, "route-cardinality-probe");
  const probe = requiredEvent(probeLogs.get("0") ?? "", "canary_complete");
  const operationsPerDomain = numericField(probe, "operationsPerDomain");
  if (operationsPerDomain !== 1) throw new Error("Route-cardinality recovery probe was incomplete");
  await stack.waitForPressureQuiescence();
  await artifacts.event("route_cardinality_churn_scenario_complete", {
    routes,
    domains: ALL_DOMAINS.length,
    recoveryProbeOperations: ALL_DOMAINS.length,
    elapsedMs: Math.round(performance.now() - startedAt),
  });
}
