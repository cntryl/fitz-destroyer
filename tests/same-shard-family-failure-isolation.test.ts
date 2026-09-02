import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  DESTROYER_FAMILY_ACTOR_SHARD_COUNT,
  DESTROYER_PRIMARY_FAMILY,
  DESTROYER_SAME_SHARD_FAMILY,
} from "../src/family-shard-topology.js";
import { assertSameShardFamilyFailureEvidence } from "../src/workloads/same-shard-family-failure-isolation.js";

describe("same-shard family failure isolation evidence", () => {
  it("should_accept_isolated_stream_and_rpc_failures_on_one_shared_shard", () => {
    // Arrange
    const evidence = {
      shardCount: DESTROYER_FAMILY_ACTOR_SHARD_COUNT,
      failedFamily: DESTROYER_PRIMARY_FAMILY,
      siblingFamily: DESTROYER_SAME_SHARD_FAMILY,
      failedFamilyRejections: 2,
      siblingOperations: 2,
      readinessChecks: 2,
      crossFamilyDeliveries: 0,
    };

    // Act / Assert
    assert.doesNotThrow(() => assertSameShardFamilyFailureEvidence(evidence));
  });

  it("should_reject_a_sibling_that_does_not_share_the_failed_family_shard", () => {
    // Arrange
    const evidence = {
      shardCount: DESTROYER_FAMILY_ACTOR_SHARD_COUNT,
      failedFamily: DESTROYER_PRIMARY_FAMILY,
      siblingFamily: 2,
      failedFamilyRejections: 2,
      siblingOperations: 2,
      readinessChecks: 2,
      crossFamilyDeliveries: 0,
    };

    // Act / Assert
    assert.throws(
      () => assertSameShardFamilyFailureEvidence(evidence),
      /same shard/u,
    );
  });
});
