import assert from "node:assert/strict";
import test from "node:test";
import {
  assertSameShardFamilyFairnessEvidence,
  sameShardFamilyConfig,
} from "../src/workloads/same-shard-family-fairness.js";

test("should_provision_a_guaranteed_same_shard_family_pair", () => {
  // Arrange
  // Act
  const config = sameShardFamilyConfig(8);

  // Assert
  assert.equal(config.families.length, 9);
  assert.equal(config.noisyFamily, 1);
  assert.equal(config.canaryFamily, 9);
  assert.equal((config.noisyFamily - 1) % 8, (config.canaryFamily - 1) % 8);
});

test("should_require_bounded_sibling_progress_during_same_shard_pressure", () => {
  // Arrange
  const evidence = {
    noisyCompleted: 1_000,
    canariesAttempted: 32,
    canariesCompleted: 32,
    canaryErrors: 0,
    longestCanaryMs: 40,
    requestTimeoutMs: 1_000,
  };

  // Act
  // Assert
  assert.doesNotThrow(() => assertSameShardFamilyFairnessEvidence(evidence));
  assert.throws(
    () => assertSameShardFamilyFairnessEvidence({ ...evidence, canariesCompleted: 31 }),
    /canaries completed/u,
  );
  assert.throws(
    () => assertSameShardFamilyFairnessEvidence({ ...evidence, longestCanaryMs: 1_000 }),
    /canary latency/u,
  );
});
