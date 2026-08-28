import assert from "node:assert/strict";
import test from "node:test";
import { assertActorSupervisionEvidence } from "../src/orchestration/actor-supervision-failpoint.js";

test("should_require_fail_closed_actor_supervision_and_restart_recovery", () => {
  // Arrange
  const evidence = { domainsInjected: 2, readinessWithdrawals: 2, restartsRecovered: 2, canaryDeliveries: 4, expectedCanaryDeliveries: 4, queueRecovered: 1, expectedQueueRecovered: 1 };

  // Act
  // Assert
  assert.doesNotThrow(() => assertActorSupervisionEvidence(evidence));
  assert.throws(() => assertActorSupervisionEvidence({ ...evidence, readinessWithdrawals: 1 }), /readiness withdrawals/u);
  assert.throws(() => assertActorSupervisionEvidence({ ...evidence, canaryDeliveries: 3 }), /recovery canary/u);
  assert.throws(() => assertActorSupervisionEvidence({ ...evidence, queueRecovered: 0 }), /Queue recovery/u);
});
