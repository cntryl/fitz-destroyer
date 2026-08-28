import assert from "node:assert/strict";
import test from "node:test";
import { assertActorSupervisionEvidence } from "../src/orchestration/actor-supervision-failpoint.js";

test("should_require_fail_closed_actor_supervision_and_restart_recovery", () => {
  // Arrange
  const evidence = { injected: true, readinessWithdrawn: true, restartRecovered: true, canaryDeliveries: 4, expectedCanaryDeliveries: 4 };

  // Act
  // Assert
  assert.doesNotThrow(() => assertActorSupervisionEvidence(evidence));
  assert.throws(() => assertActorSupervisionEvidence({ ...evidence, readinessWithdrawn: false }), /withdraw readiness/u);
  assert.throws(() => assertActorSupervisionEvidence({ ...evidence, canaryDeliveries: 3 }), /recovery canary/u);
});
