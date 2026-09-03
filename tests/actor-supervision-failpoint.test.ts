import assert from "node:assert/strict";
import test from "node:test";
import {
  activeFaultObservationTimeoutMs,
  assertActorSupervisionEvidence,
  recoveryCanaryNamespace,
} from "../src/orchestration/actor-supervision-failpoint.js";
import { parseWindowErrors } from "../src/orchestration/compose-evidence.js";

test("should_require_fail_closed_actor_supervision_and_restart_recovery", () => {
  // Arrange
  const evidence = { domainsInjected: 7, correlatedDomainsInjected: 7, activeFaultClients: 4, expectedActiveFaultClients: 4, activeFaultErrors: 4, readinessWithdrawals: 8, restartsRecovered: 8, canaryDeliveries: 4, expectedCanaryDeliveries: 4, queueRecovered: 1, expectedQueueRecovered: 1, kvRecovered: 1, leaseRecovered: 1, scheduleRecovered: 1, streamRecovered: 1, rpcRecovered: 1, correlatedRecoveryOperations: 7 };

  // Act
  // Assert
  assert.doesNotThrow(() => assertActorSupervisionEvidence(evidence));
  assert.throws(() => assertActorSupervisionEvidence({ ...evidence, readinessWithdrawals: 2 }), /readiness withdrawals/u);
  assert.throws(() => assertActorSupervisionEvidence({ ...evidence, canaryDeliveries: 3 }), /recovery canary/u);
  assert.throws(() => assertActorSupervisionEvidence({ ...evidence, queueRecovered: 0 }), /Queue recovery/u);
  assert.throws(() => assertActorSupervisionEvidence({ ...evidence, kvRecovered: 0 }), /KV recovery/u);
  assert.throws(() => assertActorSupervisionEvidence({ ...evidence, leaseRecovered: 0 }), /Lease recovery/u);
  assert.throws(() => assertActorSupervisionEvidence({ ...evidence, scheduleRecovered: 0 }), /Schedule recovery/u);
  assert.throws(() => assertActorSupervisionEvidence({ ...evidence, streamRecovered: 0 }), /Stream recovery/u);
  assert.throws(() => assertActorSupervisionEvidence({ ...evidence, rpcRecovered: 0 }), /RPC recovery/u);
  assert.throws(() => assertActorSupervisionEvidence({ ...evidence, correlatedDomainsInjected: 6 }), /correlated actor failpoint/u);
  assert.throws(() => assertActorSupervisionEvidence({ ...evidence, correlatedRecoveryOperations: 6 }), /Correlated recovery/u);
  assert.throws(() => assertActorSupervisionEvidence({ ...evidence, activeFaultClients: 3 }), /Active-fault clients/u);
  assert.throws(() => assertActorSupervisionEvidence({ ...evidence, activeFaultErrors: 3 }), /Active-fault errors/u);
});

test("should_count_only_completed_fault_window_errors", () => {
  // Arrange
  const log = [
    "npm prelude",
    JSON.stringify({ event: "progress", window: { queue: { success: 1, error: 2 }, rpc: { success: 0, error: 1 } } }),
    JSON.stringify({ event: "progress", window: { kv: { success: 0, error: 3 } } }),
    JSON.stringify({ event: "other", window: { queue: { error: 99 } } }),
  ].join("\n");

  // Act
  const errors = parseWindowErrors(log);

  // Assert
  assert.equal(errors, 6);
});

test("should_isolate_each_post_restart_canary_route_namespace", () => {
  // Arrange
  const namespace = "campaign";

  // Act
  const kv = recoveryCanaryNamespace(namespace, "kv");
  const lease = recoveryCanaryNamespace(namespace, "lease");
  const schedule = recoveryCanaryNamespace(namespace, "schedule");
  const stream = recoveryCanaryNamespace(namespace, "stream");
  const rpc = recoveryCanaryNamespace(namespace, "rpc");

  // Assert
  assert.equal(kv, "campaign-kv-recovery");
  assert.equal(lease, "campaign-lease-recovery");
  assert.equal(schedule, "campaign-schedule-recovery");
  assert.equal(stream, "campaign-stream-recovery");
  assert.equal(rpc, "campaign-rpc-recovery");
  assert.notEqual(kv, lease);
  assert.notEqual(lease, schedule);
  assert.notEqual(schedule, stream);
  assert.notEqual(stream, rpc);
});

test("should_allow_active clients to finish an in-flight request after a correlated fault", () => {
  // Arrange
  const requestTimeoutMs = 10_000;

  // Act
  const hostedBudget = activeFaultObservationTimeoutMs(requestTimeoutMs, 180_000);
  const boundedBudget = activeFaultObservationTimeoutMs(requestTimeoutMs, 20_000);

  // Assert
  assert.equal(hostedBudget, 30_000);
  assert.equal(boundedBudget, 20_000);
});
