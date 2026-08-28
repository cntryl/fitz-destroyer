import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { assertFamilyActorDegradationEvidence } from "../src/orchestration/family-actor-degradation-observability.js";

describe("family actor degradation observability evidence", () => {
  it("should_accept_exact_once_failure_metrics_and_sibling_progress", () => {
    // Arrange
    const evidence = {
      domainsObserved: 2,
      metricIncrements: 2,
      duplicateMetricIncrements: 0,
      siblingCanaries: 2,
      readinessChecks: 2,
      restartMetricResets: 2,
    };

    // Act / Assert
    assert.doesNotThrow(() => assertFamilyActorDegradationEvidence(evidence));
  });

  it("should_reject_duplicate_failure_counter_increments", () => {
    // Arrange
    const evidence = {
      domainsObserved: 2,
      metricIncrements: 2,
      duplicateMetricIncrements: 1,
      siblingCanaries: 2,
      readinessChecks: 2,
      restartMetricResets: 2,
    };

    // Act / Assert
    assert.throws(() => assertFamilyActorDegradationEvidence(evidence), /duplicate metric increments/u);
  });
});
