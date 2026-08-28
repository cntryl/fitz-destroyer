import assert from "node:assert/strict";
import test from "node:test";
import {
  assertScheduleDueStormEvidence,
  scheduleDueStormDefinitionCount,
  scheduleDueStormPermissions,
} from "../src/workloads/schedule-due-storm-isolation.js";
import { usesAuthenticatedRouteFamilies } from "../src/orchestration/compose.js";

test("should_scale_schedule_due_storm_beyond_existing_delivery_coverage", () => {
  assert.equal(scheduleDueStormDefinitionCount("smoke", 20), 512);
  assert.equal(scheduleDueStormDefinitionCount("standard", 1_000), 2_000);
  assert.equal(scheduleDueStormDefinitionCount("large", 5_000), 5_000);
  assert.equal(scheduleDueStormDefinitionCount("smoke", 3_000), 3_000);
});

test("should_assign_the_storm_clients_to_distinct_authenticated_families", () => {
  assert.equal(usesAuthenticatedRouteFamilies("schedule-due-storm-isolation"), true);
  assert.deepEqual(scheduleDueStormPermissions("run-1"), [
    "schedule://destroyer/run-1/**#*",
    "schedule://**#read",
  ]);
});

test("should_require_exact_storm_delivery_and_sibling_progress", () => {
  const valid = {
    definitionsCreated: 512,
    definitionsCancelled: 512,
    remainingDefinitions: 0,
    deliveries: 512,
    duplicates: 0,
    invalidDeliveries: 0,
    maxLatenessMs: 900,
    canariesAttempted: 24,
    canariesCompleted: 24,
    canaryErrors: 0,
    longestCanaryMs: 900,
    readinessChecks: 20,
    readinessFailures: 0,
    postStormCanaries: 1,
    requestTimeoutMs: 1_000,
  };

  assert.doesNotThrow(() => assertScheduleDueStormEvidence(valid));
  assert.throws(
    () => assertScheduleDueStormEvidence({ ...valid, deliveries: 511 }),
    /deliveries 511\/512/u,
  );
  assert.throws(
    () => assertScheduleDueStormEvidence({ ...valid, longestCanaryMs: 1_000 }),
    /reached request timeout/u,
  );
  assert.throws(
    () => assertScheduleDueStormEvidence({ ...valid, readinessFailures: 1 }),
    /readiness failures=1/u,
  );
});
