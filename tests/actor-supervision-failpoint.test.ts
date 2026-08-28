import assert from "node:assert/strict";
import test from "node:test";
import { assertActorSupervisionEvidence } from "../src/orchestration/actor-supervision-failpoint.js";

test("should_require_fail_closed_actor_supervision_and_restart_recovery", () => {
  // Arrange
  const evidence = { domainsInjected: 3, readinessWithdrawals: 3, restartsRecovered: 3, canaryDeliveries: 4, expectedCanaryDeliveries: 4, queueRecovered: 1, expectedQueueRecovered: 1, kvRecovered: 1 };

  // Act
  // Assert
  assert.doesNotThrow(() => assertActorSupervisionEvidence(evidence));
  assert.throws(() => assertActorSupervisionEvidence({ ...evidence, readinessWithdrawals: 2 }), /readiness withdrawals/u);
  assert.throws(() => assertActorSupervisionEvidence({ ...evidence, canaryDeliveries: 3 }), /recovery canary/u);
  assert.throws(() => assertActorSupervisionEvidence({ ...evidence, queueRecovered: 0 }), /Queue recovery/u);
  assert.throws(() => assertActorSupervisionEvidence({ ...evidence, kvRecovered: 0 }), /KV recovery/u);
});
