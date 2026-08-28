import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { assertFamilyActorInflightFailureEvidence } from "../src/orchestration/family-actor-inflight-concurrent-failure.js";

describe("family actor in-flight concurrent failure evidence", () => {
  it("should_accept_three_bounded_failure_and_recovery_cycles", () => {
    // Arrange
    const evidence = {
      cycles: 3,
      concurrentFailures: 12,
      streamInflightRejections: 6,
      rpcInflightTerminations: 6,
      siblingOperations: 6,
      readinessChecks: 6,
      metricIncrements: 12,
      restartRecoveries: 3,
      crossFamilyDeliveries: 0,
    };

    // Act / Assert
    assert.doesNotThrow(() => assertFamilyActorInflightFailureEvidence(evidence));
  });

  it("should_reject_an_in_flight_operation_that_never_terminates", () => {
    // Arrange
    const evidence = {
      cycles: 3,
      concurrentFailures: 12,
      streamInflightRejections: 6,
      rpcInflightTerminations: 5,
      siblingOperations: 6,
      readinessChecks: 6,
      metricIncrements: 12,
      restartRecoveries: 3,
      crossFamilyDeliveries: 0,
    };

    // Act / Assert
    assert.throws(() => assertFamilyActorInflightFailureEvidence(evidence), /RPC in-flight terminations/u);
  });
});
