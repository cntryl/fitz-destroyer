import { createDestroyerToken } from "../auth-token.js";
import type { RunConfig } from "../config.js";
import type { WorkloadShape } from "../workloads/model.js";
import type { Artifacts } from "./artifacts.js";
import type { ComposeStack } from "./compose.js";
import { requiredEvent } from "./workload-log.js";

const DOMAINS = [
  { domain: "stream", metric: "fitz_stream_family_failed_closed_total" },
  { domain: "rpc", metric: "fitz_rpc_family_failed_closed_total" },
] as const;

export function assertFamilyActorDegradationEvidence(record: Readonly<Record<string, unknown>>): void {
  exact(record, "domainsObserved", 2, "observed domains");
  exact(record, "metricIncrements", 2, "metric increments");
  exact(record, "duplicateMetricIncrements", 0, "duplicate metric increments");
  exact(record, "siblingCanaries", 2, "sibling canaries");
  exact(record, "readinessChecks", 2, "readiness checks");
  exact(record, "restartMetricResets", 2, "restart metric resets");
}

export async function runFamilyActorDegradationObservabilityScenario(
  stack: ComposeStack,
  config: RunConfig,
  shape: WorkloadShape,
  artifacts: Artifacts,
): Promise<void> {
  const startedAt = performance.now();
  let metricIncrements = 0;
  let duplicateMetricIncrements = 0;
  let siblingCanaries = 0;
  let readinessChecks = 0;
  let restartMetricResets = 0;
  const permissions = ["queue://**#*", "kv://**#*", "stream://**#*", "schedule://**#*", "notice://**#*", "lease://**#*", "rpc://**#*"];
  for (const { domain, metric } of DOMAINS) {
    const baseline = await stack.prometheusMetricValue(metric);
    await inject(config, domain);
    const first = await waitForMetric(stack, metric, baseline + 1, config.requestTimeoutMs);
    metricIncrements += first - baseline;
    await inject(config, domain);
    await new Promise((resolve) => setTimeout(resolve, 250));
    const repeated = await stack.prometheusMetricValue(metric);
    duplicateMetricIncrements += repeated - first;
    const ready = await fetch(`http://127.0.0.1:${config.port}/readyz`, { signal: AbortSignal.timeout(config.requestTimeoutMs) });
    if (ready.status !== 200) throw new Error(`${domain} partial family failure withdrew readiness`);
    readinessChecks += 1;
    const canary = await stack.startRoleContainers("canary", 1, {
      ...shape,
      namespace: `${shape.namespace}-${domain}-degradation-canary`,
      entriesPerResource: 1,
    }, {
      DESTROYER_JWT: createDestroyerToken("identity-b", permissions),
    });
    const complete = requiredEvent(onlyLog(await stack.finishRoleContainers(canary, `family-degradation-${domain}-canary`)), "canary_complete");
    if (Array.isArray(complete.domains) && complete.domains.includes(domain) && complete.operationsPerDomain === 1) siblingCanaries += 1;
    await stack.stopFitz();
    await stack.restartFitz();
    if (await waitForMetric(stack, metric, 0, config.requestTimeoutMs) === 0) restartMetricResets += 1;
  }
  const evidence = { domainsObserved: 2, metricIncrements, duplicateMetricIncrements, siblingCanaries, readinessChecks, restartMetricResets };
  assertFamilyActorDegradationEvidence(evidence);
  await artifacts.writeJson("family-actor-degradation-observability-evidence.json", evidence);
  await artifacts.event("family_actor_degradation_observability_complete", {
    ...evidence,
    elapsedMs: Math.round(performance.now() - startedAt),
    configuredTimeoutMs: config.requestTimeoutMs,
  });
}

async function inject(config: RunConfig, domain: "stream" | "rpc"): Promise<void> {
  const response = await fetch(`http://127.0.0.1:${config.port}/destroyer/failpoints/${domain}-family-1-actor-panic`, {
    method: "POST",
    signal: AbortSignal.timeout(config.requestTimeoutMs),
  });
  if (!response.ok) throw new Error(`${domain} family failpoint returned HTTP ${response.status}`);
}

async function waitForMetric(stack: ComposeStack, metric: string, expected: number, timeoutMs: number): Promise<number> {
  const deadline = Date.now() + timeoutMs;
  let observed: number | undefined;
  let lastError: unknown;
  while (Date.now() < deadline) {
    try {
      observed = await stack.prometheusMetricValue(metric);
      if (observed === expected) return observed;
    } catch (error) {
      lastError = error;
    }
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  throw new Error(`${metric} observed ${String(observed)}, expected ${expected}; last fetch error=${String(lastError)}`);
}

function onlyLog(logs: ReadonlyMap<string, string>): string {
  const log = [...logs.values()][0];
  if (logs.size !== 1 || log === undefined) throw new Error(`expected one canary log, found ${logs.size}`);
  return log;
}

function exact(record: Readonly<Record<string, unknown>>, field: string, expected: number, label: string): void {
  if (record[field] !== expected) throw new Error(`${label} ${String(record[field])}/${expected}`);
}
