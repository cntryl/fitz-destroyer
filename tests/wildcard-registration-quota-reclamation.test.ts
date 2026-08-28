import assert from "node:assert/strict";
import test from "node:test";
import {
  assertWildcardQuotaEvidence,
  wildcardQuotaPattern,
} from "../src/workloads/wildcard-registration-quota-reclamation.js";

test("should_build_unique_canonical_wildcard_patterns_for_every_domain", () => {
  assert.equal(wildcardQuotaPattern("notice", "run-1", 7), "notice://destroyer/run-1-quota-007/*");
  assert.equal(wildcardQuotaPattern("schedule", "run-1", 7), "schedule://destroyer/run-1-quota-007/*/*");
});

test("should_require_quota_rejection_and_reclamation_for_every_domain", () => {
  // Arrange
  const evidence = { domains: 6, registrations: 1536, limitRejections: 12, unsubscribeReclaims: 6, disconnectReclaims: 6, canaryFailures: 0 };
  // Act / Assert
  assert.doesNotThrow(() => assertWildcardQuotaEvidence(evidence));
  assert.throws(() => assertWildcardQuotaEvidence({ ...evidence, disconnectReclaims: 5 }), /disconnectReclaims=5\/6/u);
});
