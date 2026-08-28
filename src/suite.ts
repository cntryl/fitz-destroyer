import { randomBytes } from "node:crypto";
import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
import type { RunConfig } from "./config.js";
import type { ConcreteScenario, ScenarioResult } from "./scenario.js";

export const ALL_SCENARIOS: readonly ConcreteScenario[] = [
  "clean-restart",
  "cache-loss",
  "durability-crash-cuts",
  "queue-overload-recovery",
  "response-loss",
  "active-graceful-shutdown",
  "half-open-session",
  "authorization-isolation",
  "stream-global-recovery",
  "queue-dead-letter-fencing",
  "cold-boot-provider-outage",
  "hostile-rpc-worker",
  "upgrade-recovery",
  "cross-transport-recovery",
  "outbound-blackhole",
  "broker-pause",
  "route-cardinality-churn",
  "cache-and-disk-exhaustion",
  "queue-redelivery",
  "lease-contention",
  "hot-route-canary",
  "protocol-abuse",
  "notice-fanout",
  "schedule-delivery",
  "session-boundaries",
  "rpc-pressure",
  "rpc-stream-hose",
  "connection-storm",
  "domain-pressure",
  "chaos",
  "soak",
  "storage-faults",
  "queue-lifecycle",
  "schedule-outage",
  "transaction-contention",
  "stream-replay",
  "live-churn",
  "lease-route-aliasing",
  "tcp-preauth-framing-slowloris",
  "connect-pipeline-family-rebind",
  "ephemeral-reply-loss-cleanup",
  "saturated-slow-recipient-isolation",
  "shutdown-reconnect-cleanup-storm",
  "control-lane-cleanup-under-saturation",
  "route-family-isolation-matrix",
  "rpc-response-state-conformance",
  "response-envelope-boundaries",
  "lease-waiter-disconnect-races",
  "wildcard-registration-quota-reclamation",
  "stream-selector-cursor-conformance",
  "same-shard-family-fairness",
  "actor-supervision-failpoint",
  "family-actor-partial-failure-isolation",
  "same-shard-family-failure-isolation",
  "family-actor-exhaustion-readiness",
  "family-actor-degradation-observability",
  "family-actor-inflight-concurrent-failure",
];

export type SuiteSummary = {
  suiteId: string;
  startedAt: string;
  completedAt: string;
  durationMs: number;
  totals: { passed: number; failed: number; scenarios: number };
  results: readonly ScenarioResult[];
};

type ScenarioExecutor = (
  config: RunConfig,
  scenario: ConcreteScenario,
  options: { preserveFailure: boolean },
) => Promise<ScenarioResult>;

export function allocateScenarioConfigs(
  config: RunConfig,
  scenarios: readonly ConcreteScenario[],
): readonly { scenario: ConcreteScenario; config: RunConfig }[] {
  return scenarios.map((scenario, index) => {
    const port = config.port + index;
    if (port > 65_535) {
      throw new Error(
        `Suite port allocation exceeds 65535 at scenario ${scenario}; choose --port ${65_536 - scenarios.length} or lower`,
      );
    }
    return { scenario, config: { ...config, scenario, port, keep: false } };
  });
}

export function aggregateSuiteResults(
  suiteId: string,
  startedAt: string,
  completedAt: string,
  durationMs: number,
  results: readonly ScenarioResult[],
): SuiteSummary {
  const passed = results.filter(({ verdict }) => verdict === "passed").length;
  return {
    suiteId,
    startedAt,
    completedAt,
    durationMs,
    totals: { passed, failed: results.length - passed, scenarios: results.length },
    results: [...results],
  };
}

export async function runSuite(
  config: RunConfig,
  execute: ScenarioExecutor,
  scenarios: readonly ConcreteScenario[] = ALL_SCENARIOS,
): Promise<SuiteSummary> {
  const suiteId = createSuiteId();
  const directory = join(config.rootDir, "artifacts", "suites", suiteId);
  await mkdir(directory, { recursive: true });
  const startedAt = new Date().toISOString();
  const started = performance.now();
  const results: ScenarioResult[] = [];

  for (const entry of allocateScenarioConfigs(config, scenarios)) {
    results.push(await execute(entry.config, entry.scenario, { preserveFailure: false }));
  }

  const summary = aggregateSuiteResults(
    suiteId,
    startedAt,
    new Date().toISOString(),
    Math.round(performance.now() - started),
    results,
  );
  await writeFile(join(directory, "summary.json"), `${JSON.stringify(summary, null, 2)}\n`, "utf8");
  process.stdout.write(`${JSON.stringify({ event: "suite_complete", artifactPath: directory, ...summary.totals })}\n`);
  return summary;
}

function createSuiteId(): string {
  const timestamp = new Date().toISOString().replace(/[-:]/gu, "").replace(/\.\d{3}Z$/u, "z");
  return `${timestamp}-all-${randomBytes(3).toString("hex")}`;
}
