import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { assertFamilyActorPartialFailureEvidence } from "../src/workloads/family-actor-partial-failure-isolation.js";

describe("family actor partial failure isolation evidence", () => {
  it("should accept isolated Stream and RPC family failures", () => {
    assert.doesNotThrow(() => assertFamilyActorPartialFailureEvidence({
      targetedFamilies: 2,
      failedFamilyRejections: 2,
      siblingOperations: 2,
      readinessChecks: 2,
      crossFamilyDeliveries: 0,
    }));
  });

  it("should reject a sibling progress failure", () => {
    assert.throws(() => assertFamilyActorPartialFailureEvidence({
      targetedFamilies: 2,
      failedFamilyRejections: 2,
      siblingOperations: 1,
      readinessChecks: 2,
      crossFamilyDeliveries: 0,
    }), /sibling operations/u);
  });
});
