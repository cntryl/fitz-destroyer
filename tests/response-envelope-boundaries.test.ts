import assert from "node:assert/strict";
import test from "node:test";
import { boundarySizes } from "../src/workloads/response-envelope-boundaries.js";
import { assertResponseEnvelopeEvidence } from "../src/orchestration/response-envelope-boundaries.js";

test("should_calculate_exact_and_one_over_response_boundaries", () => {
  // Arrange / Act
  const sizes = boundarySizes(65_506);
  // Assert
  assert.deepEqual(sizes, { exact: 65_506, oneOver: 65_507 });
});

test("should_reject_invalid_response_boundary_limits", () => {
  assert.throws(() => boundarySizes(0), /invalid response limit/u);
});

test("should_require_every_response_boundary_domain_and_recovery_canary", () => {
  // Arrange
  const evidence = { domains: 7, exactFit: 7, oneOverRejected: 3, boundedAggregates: 2, canaryOperations: 7 };
  // Act / Assert
  assert.doesNotThrow(() => assertResponseEnvelopeEvidence(evidence));
  assert.throws(() => assertResponseEnvelopeEvidence({ ...evidence, boundedAggregates: 1 }), /boundedAggregates=1\/2/u);
});
