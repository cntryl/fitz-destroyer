import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { assertFamilyActorExhaustionEvidence } from "../src/orchestration/family-actor-exhaustion-readiness.js";

describe("family actor exhaustion readiness evidence", () => {
  it("should_accept_partial_health_total_withdrawal_and_restart_recovery", () => {
    // Arrange
    const evidence = {
      domainsExhausted: 2,
      partialReadinessChecks: 2,
      totalReadinessWithdrawals: 2,
      failedFamilyRejections: 4,
      restartRecoveries: 2,
    };

    // Act / Assert
    assert.doesNotThrow(() => assertFamilyActorExhaustionEvidence(evidence));
  });

  it("should_reject_readiness_that_withdraws_after_only_one_family_fails", () => {
    // Arrange
    const evidence = {
      domainsExhausted: 2,
      partialReadinessChecks: 1,
      totalReadinessWithdrawals: 2,
      failedFamilyRejections: 4,
      restartRecoveries: 2,
    };

    // Act / Assert
    assert.throws(() => assertFamilyActorExhaustionEvidence(evidence), /partial readiness/u);
  });
});
