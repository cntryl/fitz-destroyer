import { createDestroyerToken } from "../auth-token.js";
import type { RunConfig } from "../config.js";
import type { WorkloadShape } from "../workloads/model.js";
import type { Artifacts } from "./artifacts.js";
import type { ComposeStack } from "./compose.js";
import { numericField, requiredEvent } from "./workload-log.js";

export async function runAuthorizationIsolationScenario(
  stack: ComposeStack,
  _config: RunConfig,
  shape: WorkloadShape,
  artifacts: Artifacts,
): Promise<void> {
  const startedAt = performance.now();
  const permissions = [
    `kv://${shape.namespace}/**#read`,
    `kv://${shape.namespace}/**#write`,
  ];
  let ownRouteOperations = 0;
  let deniedOperations = 0;

  for (const identity of ["identity-a", "identity-b"] as const) {
    const token = createDestroyerToken(identity, permissions);
    for (const action of ["write", "verify"] as const) {
      const containers = await stack.startRoleContainers(
        "authorization-isolation",
        1,
        shape,
        {
          DESTROYER_WORKER_ID: identity.slice(-1),
          DESTROYER_AUTH_ACTION: action,
          DESTROYER_JWT: token,
        },
      );
      const logs = await stack.finishRoleContainers(
        containers,
        `authorization-${identity}-${action}`,
      );
      const complete = requiredEvent([...logs.values()][0] ?? "", "authorization_isolation_role_complete");
      ownRouteOperations += numericField(complete, "ownRouteOperations");
      deniedOperations += numericField(complete, "deniedOperations");
    }
  }

  assertAuthorizationIsolation(ownRouteOperations, deniedOperations);
  await artifacts.event("authorization_isolation_complete", {
    identities: 2,
    ownRouteOperations,
    deniedOperations,
    elapsedMs: Math.round(performance.now() - startedAt),
  });
}

export function assertAuthorizationIsolation(
  ownRouteOperations: number,
  deniedOperations: number,
): void {
  if (ownRouteOperations !== 4 || deniedOperations !== 4) {
    throw new Error(
      `Authorization isolation counts mismatch: own=${ownRouteOperations}, denied=${deniedOperations}`,
    );
  }
}
