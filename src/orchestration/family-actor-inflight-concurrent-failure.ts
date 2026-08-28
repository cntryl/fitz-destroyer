import { createDestroyerToken } from "../auth-token.js";
import type { RunConfig } from "../config.js";
import type { WorkloadShape } from "../workloads/model.js";
import type { Artifacts } from "./artifacts.js";
import type { ComposeStack } from "./compose.js";
import { requiredEvent } from "./workload-log.js";

const CYCLES = 3;

export function assertFamilyActorInflightFailureEvidence(record: Readonly<Record<string, unknown>>): void {
  exact(record, "cycles", CYCLES, "cycles");
  exact(record, "concurrentFailures", 12, "concurrent failures");
  exact(record, "streamInflightRejections", 6, "Stream in-flight rejections");
  exact(record, "rpcInflightTerminations", 6, "RPC in-flight terminations");
  exact(record, "siblingOperations", 6, "sibling operations");
  exact(record, "readinessChecks", 6, "readiness checks");
  exact(record, "metricIncrements", 12, "metric increments");
  exact(record, "restartRecoveries", CYCLES, "restart recoveries");
  exact(record, "crossFamilyDeliveries", 0, "cross-family deliveries");
}

export async function runFamilyActorInflightConcurrentFailureScenario(
  stack: ComposeStack,
  config: RunConfig,
  shape: WorkloadShape,
  artifacts: Artifacts,
): Promise<void> {
  const startedAt = performance.now();
  const allPermissions = ["queue://**#*", "kv://**#*", "stream://**#*", "schedule://**#*", "notice://**#*", "lease://**#*", "rpc://**#*"];
  let concurrentFailures = 0;
  let streamInflightRejections = 0;
  let rpcInflightTerminations = 0;
  let siblingOperations = 0;
  let readinessChecks = 0;
  let metricIncrements = 0;
  let restartRecoveries = 0;
  let crossFamilyDeliveries = 0;
  for (let cycle = 0; cycle < CYCLES; cycle += 1) {
    const cycleShape = { ...shape, namespace: `${shape.namespace}-cycle-${cycle}` };
    const permissions = [`stream://${cycleShape.namespace}/**#*`, `rpc://${cycleShape.namespace}/**#*`];
    const streamBaseline = await stack.prometheusMetricValue("fitz_stream_family_failed_closed_total");
    const rpcBaseline = await stack.prometheusMetricValue("fitz_rpc_family_failed_closed_total");
    const role = await stack.startRoleContainers("family-actor-inflight-concurrent-failure", 1, cycleShape, {
      DESTROYER_JWT: createDestroyerToken("identity-a", permissions),
      DESTROYER_FAMILY_INFLIGHT_URL: "ws://fitz:4090/ws",
      DESTROYER_FAMILY_INFLIGHT_HTTP_URL: "http://fitz:4090",
    });
    const complete = requiredEvent(onlyLog(await stack.finishRoleContainers(role, `family-actor-inflight-cycle-${cycle}`)), "family_actor_inflight_concurrent_failure_worker_complete");
    concurrentFailures += numberField(complete, "concurrentFailures");
    streamInflightRejections += numberField(complete, "streamInflightRejections");
    rpcInflightTerminations += numberField(complete, "rpcInflightTerminations");
    siblingOperations += numberField(complete, "siblingOperations");
    readinessChecks += numberField(complete, "readinessChecks");
    crossFamilyDeliveries += numberField(complete, "crossFamilyDeliveries");
    metricIncrements += await waitForMetricDelta(stack, "fitz_stream_family_failed_closed_total", streamBaseline, 2, config.requestTimeoutMs);
    metricIncrements += await waitForMetricDelta(stack, "fitz_rpc_family_failed_closed_total", rpcBaseline, 2, config.requestTimeoutMs);
    await stack.stopFitz();
    await stack.restartFitz();
    const canary = await stack.startRoleContainers("canary", 1, {
      ...shape,
      namespace: `${shape.namespace}-cycle-${cycle}-recovery`,
      entriesPerResource: 1,
    }, { DESTROYER_JWT: createDestroyerToken("identity-c", allPermissions) });
    const recovered = requiredEvent(onlyLog(await stack.finishRoleContainers(canary, `family-actor-inflight-cycle-${cycle}-recovery`)), "canary_complete");
    if (Array.isArray(recovered.domains) && recovered.domains.includes("stream") && recovered.domains.includes("rpc") && recovered.operationsPerDomain === 1) restartRecoveries += 1;
  }
  const evidence = { cycles: CYCLES, concurrentFailures, streamInflightRejections, rpcInflightTerminations, siblingOperations, readinessChecks, metricIncrements, restartRecoveries, crossFamilyDeliveries };
  assertFamilyActorInflightFailureEvidence(evidence);
  await artifacts.writeJson("family-actor-inflight-concurrent-failure-evidence.json", evidence);
  await artifacts.event("family_actor_inflight_concurrent_failure_complete", {
    ...evidence,
    elapsedMs: Math.round(performance.now() - startedAt),
    configuredTimeoutMs: config.requestTimeoutMs,
  });
}

async function waitForMetricDelta(stack: ComposeStack, metric: string, baseline: number, expected: number, timeoutMs: number): Promise<number> {
  const deadline = Date.now() + timeoutMs;
  let delta = 0;
  while (Date.now() < deadline) {
    delta = await stack.prometheusMetricValue(metric) - baseline;
    if (delta === expected) return delta;
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  throw new Error(`${metric} incremented ${delta}/${expected}`);
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
