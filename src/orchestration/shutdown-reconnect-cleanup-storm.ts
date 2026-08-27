import type { RunConfig } from "../config.js";
import type { WorkloadShape } from "../workloads/model.js";
import { RELIABILITY_RPC_PROBE_CALLS } from "../workloads/reliability-session-state.js";
import type { Artifacts } from "./artifacts.js";
import type { LiveDomain } from "./live-observability.js";
import { numericField, requiredEvent } from "./workload-log.js";
import {
  nonNegativeInteger,
  observeReadinessDuring,
  type ReliabilityComposeStack,
} from "./reliability-cleanup-helpers.js";

const LIVE_DOMAINS: readonly LiveDomain[] = [
  "kv",
  "stream",
  "notice",
  "rpc",
  "lease",
  "schedule",
];

export type ShutdownReconnectCleanupCycle = {
  cycle: number;
  clients: number;
  readinessDropped: boolean;
  finalReadinessStatus: number;
  restartElapsedMs: number;
  reconnects: number;
  staleHandleRejections: number;
  queueRedelivered: number;
  kvUncommittedValues: number;
  streamUncommittedRecords: number;
  leaseHeld: number;
  leasePendingWaiters: number;
  rpcProbeCalls: number;
  rpcProbeFailures: number;
  cleanupPending: number;
  noticeSubscriptions: number;
  rpcWorkers: number;
  scheduleSubscriptions: number;
};

export type ShutdownReconnectCleanupLedger = {
  expectedCycles: number;
  restartBudgetMs: number;
  cycles: readonly ShutdownReconnectCleanupCycle[];
};

type ShutdownReconnectRole = "shutdown-reconnect-cleanup-storm";

export async function runShutdownReconnectCleanupStormScenario(
  stack: ReliabilityComposeStack<ShutdownReconnectRole>,
  config: RunConfig,
  shape: WorkloadShape,
  artifacts: Artifacts,
): Promise<void> {
  const startedAt = performance.now();
  const expectedCycles = Math.min(shape.resources, 10);
  const restartBudgetMs = config.startupTimeoutMs + 25_000;
  const cycles: ShutdownReconnectCleanupCycle[] = [];
  await artifacts.event("shutdown_reconnect_cleanup_storm_started", {
    cycles: expectedCycles,
    clientsPerCycle: config.clientReplicas,
    restartBudgetMs,
  });

  for (let index = 0; index < expectedCycles; index += 1) {
    const cycle = index + 1;
    const cycleLabel = `shutdown-reconnect-cycle-${cycle.toString().padStart(3, "0")}`;
    const cycleShape = {
      ...shape,
      namespace: `${shape.namespace}-shutdown-${cycle}`,
      entriesPerResource: 1,
    };
    const workers = await stack.startRoleContainers(
      "shutdown-reconnect-cleanup-storm",
      config.clientReplicas,
      cycleShape,
      {
        DESTROYER_RECONNECT_TIMEOUT_MS: String(config.startupTimeoutMs),
      },
    );
    await stack.waitForRoleEvent(workers, "shutdown_reconnect_cleanup_armed");

    const restartStartedAt = performance.now();
    const readiness = await observeReadinessDuring(
      config.port,
      () => stack.gracefulRestartFitz(),
      25,
    );
    const restartElapsedMs = Math.round(performance.now() - restartStartedAt);
    await artifacts.writeJson(`${cycleLabel}-readiness.json`, readiness);

    const postRestartBaselines = new Map(
      await Promise.all(
        LIVE_DOMAINS.map(async (domain) => [domain, await stack.liveDomainSnapshot(domain)] as const),
      ),
    );
    const logs = await stack.finishRoleContainers(workers, cycleLabel);
    const workerEvidence = aggregateWorkerEvidence(logs);
    const finalSnapshots = new Map(
      await Promise.all(
        LIVE_DOMAINS.map(async (domain) => {
          const baseline = postRestartBaselines.get(domain);
          if (baseline === undefined) throw new Error(`Missing ${domain} post-restart baseline`);
          return [
            domain,
            await stack.waitForLiveDomainQuiescence(
              domain,
              baseline,
              `${cycleLabel}-${domain}`,
              true,
            ),
          ] as const;
        }),
      ),
    );
    await stack.waitForPressureQuiescence();

    const notice = requiredSnapshot(finalSnapshots, "notice");
    const rpc = requiredSnapshot(finalSnapshots, "rpc");
    const schedule = requiredSnapshot(finalSnapshots, "schedule");
    const cycleEvidence: ShutdownReconnectCleanupCycle = {
      cycle,
      clients: config.clientReplicas,
      readinessDropped: readiness.readinessDropped,
      finalReadinessStatus: readiness.finalReadinessStatus,
      restartElapsedMs,
      ...workerEvidence,
      cleanupPending: Math.max(
        ...[...finalSnapshots.values()].map((snapshot) => snapshot.cleanup.pending),
      ),
      noticeSubscriptions: nonNegativeInteger(notice.domain, "subscriptions_active"),
      rpcWorkers: nonNegativeInteger(rpc.domain, "workers_registered"),
      scheduleSubscriptions: nonNegativeInteger(schedule.domain, "subscriptions_active"),
    };
    cycles.push(cycleEvidence);
    await artifacts.event("shutdown_reconnect_cleanup_cycle_complete", cycleEvidence);
  }

  const ledger: ShutdownReconnectCleanupLedger = {
    expectedCycles,
    restartBudgetMs,
    cycles,
  };
  assertShutdownReconnectCleanupStorm(ledger);
  await artifacts.writeJson("shutdown-reconnect-cleanup-storm-ledger.json", ledger);
  await artifacts.event("shutdown_reconnect_cleanup_storm_complete", {
    cycles: cycles.length,
    reconnects: cycles.reduce((sum, cycle) => sum + cycle.reconnects, 0),
    staleHandleRejections: cycles.reduce(
      (sum, cycle) => sum + cycle.staleHandleRejections,
      0,
    ),
    elapsedMs: Math.round(performance.now() - startedAt),
  });
}

export function assertShutdownReconnectCleanupStorm(
  ledger: ShutdownReconnectCleanupLedger,
): void {
  if (ledger.cycles.length !== ledger.expectedCycles) {
    throw new Error(
      `Shutdown cleanup storm completed ${ledger.cycles.length}/${ledger.expectedCycles} cycles`,
    );
  }
  for (const [index, cycle] of ledger.cycles.entries()) {
    if (cycle.cycle !== index + 1) throw new Error(`Shutdown cleanup cycle ${cycle.cycle} is out of order`);
    if (!cycle.readinessDropped) {
      throw new Error(`Shutdown cleanup cycle ${cycle.cycle} readiness never became unavailable`);
    }
    if (cycle.finalReadinessStatus !== 200) {
      throw new Error(
        `Shutdown cleanup cycle ${cycle.cycle} ended at readiness HTTP ${cycle.finalReadinessStatus}`,
      );
    }
    if (cycle.restartElapsedMs > ledger.restartBudgetMs) {
      throw new Error(
        `Shutdown cleanup cycle ${cycle.cycle} restart took ${cycle.restartElapsedMs}ms beyond ${ledger.restartBudgetMs}ms`,
      );
    }
    assertEqual(cycle, "reconnects", cycle.clients);
    assertEqual(cycle, "staleHandleRejections", cycle.clients * 4);
    assertEqual(cycle, "queueRedelivered", cycle.clients);
    assertEqual(cycle, "kvUncommittedValues", 0);
    assertEqual(cycle, "streamUncommittedRecords", 0);
    assertEqual(cycle, "leaseHeld", 0);
    assertEqual(cycle, "leasePendingWaiters", 0);
    assertEqual(cycle, "rpcProbeCalls", cycle.clients * RELIABILITY_RPC_PROBE_CALLS);
    assertEqual(cycle, "rpcProbeFailures", 0);
    assertEqual(cycle, "cleanupPending", 0);
    if (cycle.noticeSubscriptions !== 0) {
      throw new Error(`Notice subscriptions remained after cycle ${cycle.cycle}`);
    }
    if (cycle.rpcWorkers !== 0) throw new Error(`RPC workers remained after cycle ${cycle.cycle}`);
    if (cycle.scheduleSubscriptions !== 0) {
      throw new Error(`Schedule subscriptions remained after cycle ${cycle.cycle}`);
    }
  }
}

function aggregateWorkerEvidence(
  logs: ReadonlyMap<string, string>,
): Pick<
  ShutdownReconnectCleanupCycle,
  | "reconnects"
  | "staleHandleRejections"
  | "queueRedelivered"
  | "kvUncommittedValues"
  | "streamUncommittedRecords"
  | "leaseHeld"
  | "leasePendingWaiters"
  | "rpcProbeCalls"
  | "rpcProbeFailures"
> {
  const totals = {
    reconnects: 0,
    staleHandleRejections: 0,
    queueRedelivered: 0,
    kvUncommittedValues: 0,
    streamUncommittedRecords: 0,
    leaseHeld: 0,
    leasePendingWaiters: 0,
    rpcProbeCalls: 0,
    rpcProbeFailures: 0,
  };
  for (const log of logs.values()) {
    const complete = requiredEvent(log, "shutdown_reconnect_cleanup_complete");
    for (const field of Object.keys(totals) as Array<keyof typeof totals>) {
      totals[field] += numericField(complete, field);
    }
  }
  return totals;
}

function requiredSnapshot(
  snapshots: ReadonlyMap<LiveDomain, Awaited<ReturnType<ReliabilityComposeStack<string>["liveDomainSnapshot"]>>>,
  domain: LiveDomain,
): Awaited<ReturnType<ReliabilityComposeStack<string>["liveDomainSnapshot"]>> {
  const snapshot = snapshots.get(domain);
  if (snapshot === undefined) throw new Error(`Missing final ${domain} snapshot`);
  return snapshot;
}

function assertEqual(
  cycle: ShutdownReconnectCleanupCycle,
  field: keyof ShutdownReconnectCleanupCycle,
  expected: number,
): void {
  const actual = cycle[field];
  if (actual !== expected) {
    throw new Error(`Shutdown cleanup cycle ${cycle.cycle} ${field}=${String(actual)}/${expected}`);
  }
}
