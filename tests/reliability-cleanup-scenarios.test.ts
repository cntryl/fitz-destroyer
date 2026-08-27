import assert from "node:assert/strict";
import test from "node:test";
import {
  assertShutdownReconnectCleanupStorm,
  type ShutdownReconnectCleanupLedger,
} from "../src/orchestration/shutdown-reconnect-cleanup-storm.js";
import {
  assertControlLaneCleanupUnderSaturation,
  saturationProgressDelta,
  type ControlLaneCleanupLedger,
} from "../src/orchestration/control-lane-cleanup-under-saturation.js";
import { reliabilityRoutes } from "../src/workloads/reliability-session-state.js";
import { ALL_DOMAINS } from "../src/workloads/model.js";

test("should_require_every_shutdown_cycle_to_drop_readiness_reconnect_and_clear_session_state", () => {
  // Arrange
  const ledger: ShutdownReconnectCleanupLedger = {
    expectedCycles: 1,
    restartBudgetMs: 30_000,
    cycles: [
      {
        cycle: 1,
        clients: 2,
        readinessDropped: true,
        finalReadinessStatus: 200,
        restartElapsedMs: 1_250,
        reconnects: 2,
        staleHandleRejections: 8,
        queueRedelivered: 2,
        kvUncommittedValues: 0,
        streamUncommittedRecords: 0,
        leaseHeld: 0,
        leasePendingWaiters: 0,
        rpcProbeCalls: 8,
        rpcProbeFailures: 0,
        cleanupPending: 0,
        noticeSubscriptions: 0,
        rpcWorkers: 0,
        scheduleSubscriptions: 0,
      },
    ],
  };

  // Act and Assert
  assert.doesNotThrow(() => assertShutdownReconnectCleanupStorm(ledger));
});

test("should_reject_a_shutdown_cycle_without_a_readiness_drop_or_with_ghost_state", () => {
  // Arrange
  const cycle: ShutdownReconnectCleanupLedger["cycles"][number] = {
    cycle: 1,
    clients: 1,
    readinessDropped: false,
    finalReadinessStatus: 200,
    restartElapsedMs: 500,
    reconnects: 1,
    staleHandleRejections: 4,
    queueRedelivered: 1,
    kvUncommittedValues: 0,
    streamUncommittedRecords: 0,
    leaseHeld: 0,
    leasePendingWaiters: 0,
    rpcProbeCalls: 4,
    rpcProbeFailures: 0,
    cleanupPending: 0,
    noticeSubscriptions: 0,
    rpcWorkers: 0,
    scheduleSubscriptions: 1,
  };
  const ledger: ShutdownReconnectCleanupLedger = {
    expectedCycles: 1,
    restartBudgetMs: 1_000,
    cycles: [cycle],
  };

  // Act and Assert
  assert.throws(
    () => assertShutdownReconnectCleanupStorm(ledger),
    /never became unavailable|Schedule subscriptions remained/u,
  );
});

test("should_measure_saturation_progress_only_after_the_cleanup_cut", () => {
  // Arrange
  const before = new Map([
    ["0", `${JSON.stringify({ event: "control_lane_saturation_progress", totals: totals(2) })}\n`],
  ]);
  const after = new Map([
    ["0", `${JSON.stringify({ event: "control_lane_saturation_progress", totals: totals(5) })}\n`],
  ]);

  // Act
  const delta = saturationProgressDelta(before, after);

  // Assert
  assert.deepEqual(delta, Object.fromEntries(ALL_DOMAINS.map((domain) => [domain, 3])));
});

test("should_require_cleanup_completion_and_a_sibling_canary_during_saturation", () => {
  // Arrange
  const ledger: ControlLaneCleanupLedger = {
    targets: 2,
    cleanupCompletedWhileSaturated: true,
    cleanupPending: 0,
    noticeSubscriptionsDelta: 0,
    rpcWorkersDelta: 0,
    scheduleSubscriptionsDelta: 0,
    queueRedelivered: 2,
    kvUncommittedValues: 0,
    streamUncommittedRecords: 0,
    leaseHeld: 0,
    leasePendingWaiters: 0,
    rpcProbeCalls: 8,
    rpcProbeFailures: 0,
    saturationProgress: Object.fromEntries(
      ALL_DOMAINS.map((domain) => [domain, 1]),
    ) as Record<(typeof ALL_DOMAINS)[number], number>,
    canaryDomains: [...ALL_DOMAINS],
    canaryOperationsPerDomain: 1,
  };

  // Act and Assert
  assert.doesNotThrow(() => assertControlLaneCleanupUnderSaturation(ledger));
});

test("should_reject_control_cleanup_that_waits_for_saturation_to_stop", () => {
  // Arrange
  const ledger: ControlLaneCleanupLedger = {
    targets: 1,
    cleanupCompletedWhileSaturated: false,
    cleanupPending: 0,
    noticeSubscriptionsDelta: 0,
    rpcWorkersDelta: 0,
    scheduleSubscriptionsDelta: 0,
    queueRedelivered: 1,
    kvUncommittedValues: 0,
    streamUncommittedRecords: 0,
    leaseHeld: 0,
    leasePendingWaiters: 0,
    rpcProbeCalls: 4,
    rpcProbeFailures: 0,
    saturationProgress: Object.fromEntries(
      ALL_DOMAINS.map((domain) => [domain, 1]),
    ) as Record<(typeof ALL_DOMAINS)[number], number>,
    canaryDomains: [...ALL_DOMAINS],
    canaryOperationsPerDomain: 1,
  };

  // Act and Assert
  assert.throws(
    () => assertControlLaneCleanupUnderSaturation(ledger),
    /did not complete while normal-lane saturation was active/u,
  );
});

test("should_keep_each_reliability_worker_on_independent_concrete_routes", () => {
  // Arrange
  const first = reliabilityRoutes("run", "0");
  const second = reliabilityRoutes("run", "1");

  // Act
  const routes = [...Object.values(first), ...Object.values(second)];

  // Assert
  assert.equal(new Set(routes).size, routes.length);
  assert.ok(routes.every((route) => !route.includes("*")));
});

function totals(value: number): Record<string, { success: number; error: number }> {
  return Object.fromEntries(
    ALL_DOMAINS.map((domain) => [domain, { success: value, error: 0 }]),
  );
}
