import { createDestroyerToken } from "../auth-token.js";
import type { RunConfig } from "../config.js";
import type { WorkloadShape } from "../workloads/model.js";
import type { Artifacts } from "./artifacts.js";
import type { ComposeStack } from "./compose.js";
import { requiredEvent } from "./workload-log.js";

export function assertFamilyActorExhaustionEvidence(record: Readonly<Record<string, unknown>>): void {
  exact(record, "domainsExhausted", 2, "exhausted domains");
  exact(record, "partialReadinessChecks", 2, "partial readiness checks");
  exact(record, "totalReadinessWithdrawals", 2, "total readiness withdrawals");
  exact(record, "failedFamilyRejections", 4, "failed-family rejections");
  exact(record, "restartRecoveries", 2, "restart recoveries");
}

export async function runFamilyActorExhaustionReadinessScenario(
  stack: ComposeStack,
  config: RunConfig,
  shape: WorkloadShape,
  artifacts: Artifacts,
): Promise<void> {
  const startedAt = performance.now();
  let partialReadinessChecks = 0;
  let totalReadinessWithdrawals = 0;
  let failedFamilyRejections = 0;
  let restartRecoveries = 0;
  const canaryShape = { ...shape, entriesPerResource: 1 };
  const allPermissions = ["queue://**#*", "kv://**#*", "stream://**#*", "schedule://**#*", "notice://**#*", "lease://**#*", "rpc://**#*"];
  for (const domain of ["stream", "rpc"] as const) {
    const permissions = [`${domain}://${shape.namespace}/**#*`];
    const role = await stack.startRoleContainers("family-actor-exhaustion-readiness", 1, shape, {
      DESTROYER_JWT: createDestroyerToken("identity-a", permissions),
      DESTROYER_FAMILY_EXHAUSTION_DOMAIN: domain,
      DESTROYER_FAMILY_EXHAUSTION_URL: "ws://fitz:4090/ws",
      DESTROYER_FAMILY_EXHAUSTION_HTTP_URL: "http://fitz:4090",
    });
    const logs = await stack.finishRoleContainers(role, `family-actor-exhaustion-${domain}`);
    const complete = requiredEvent(onlyLog(logs), "family_actor_exhaustion_worker_complete");
    partialReadinessChecks += numberField(complete, "partialReadinessChecks");
    totalReadinessWithdrawals += numberField(complete, "totalReadinessWithdrawals");
    failedFamilyRejections += numberField(complete, "failedFamilyRejections");
    await stack.stopFitz();
    await stack.restartFitz();
    const canary = await stack.startRoleContainers("canary", 1, {
      ...canaryShape,
      namespace: `${shape.namespace}-${domain}-exhaustion-recovery`,
    }, {
      DESTROYER_JWT: createDestroyerToken("identity-a", allPermissions),
    });
    const canaryComplete = requiredEvent(onlyLog(await stack.finishRoleContainers(canary, `family-actor-exhaustion-${domain}-recovery`)), "canary_complete");
    if (Array.isArray(canaryComplete.domains) && canaryComplete.domains.includes(domain) && canaryComplete.operationsPerDomain === 1) restartRecoveries += 1;
  }
  const evidence = { domainsExhausted: 2, partialReadinessChecks, totalReadinessWithdrawals, failedFamilyRejections, restartRecoveries };
  assertFamilyActorExhaustionEvidence(evidence);
  await artifacts.writeJson("family-actor-exhaustion-readiness-evidence.json", evidence);
  await artifacts.event("family_actor_exhaustion_readiness_complete", {
    ...evidence,
    elapsedMs: Math.round(performance.now() - startedAt),
    configuredTimeoutMs: config.requestTimeoutMs,
  });
}

function onlyLog(logs: ReadonlyMap<string, string>): string {
  const log = [...logs.values()][0];
  if (logs.size !== 1 || log === undefined) throw new Error(`expected one worker log, found ${logs.size}`);
  return log;
}

function numberField(record: Readonly<Record<string, unknown>>, field: string): number {
  const value = record[field];
  if (typeof value !== "number" || !Number.isFinite(value)) throw new Error(`${field} is unavailable`);
  return value;
}

function exact(record: Readonly<Record<string, unknown>>, field: string, expected: number, label: string): void {
  if (record[field] !== expected) throw new Error(`${label} ${String(record[field])}/${expected}`);
}
