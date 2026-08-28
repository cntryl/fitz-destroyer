import assert from "node:assert/strict";
import test from "node:test";
import { assertActorSupervisionEvidence, recoveryCanaryNamespace } from "../src/orchestration/actor-supervision-failpoint.js";

test("should_require_fail_closed_actor_supervision_and_restart_recovery", () => {
  // Arrange
  const evidence = { domainsInjected: 7, readinessWithdrawals: 7, restartsRecovered: 7, canaryDeliveries: 4, expectedCanaryDeliveries: 4, queueRecovered: 1, expectedQueueRecovered: 1, kvRecovered: 1, leaseRecovered: 1, scheduleRecovered: 1, streamRecovered: 1, rpcRecovered: 1 };

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
