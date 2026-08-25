import assert from "node:assert/strict";
import test from "node:test";
import {
  STORAGE_FAULT_IDENTITIES,
  classifyStorageFailure,
  storageFaultPlan,
} from "../src/orchestration/storage-faults.js";

test("should_generate_seeded_storage_fault_ledger_iterations", () => {
  const plan = storageFaultPlan(7, 1234);

  assert.deepEqual(plan.map(({ sequence }) => sequence), [1, 2, 3, 4, 5, 6, 7]);
  assert.deepEqual(plan.slice(0, 5).map(({ fault }) => fault), STORAGE_FAULT_IDENTITIES);
  assert.ok(plan.every(({ seed }) => seed === 1234));
});

test("should_classify_storage_fault_layers_from_observed_errors", () => {
  assert.equal(classifyStorageFailure("queue capacity exhausted"), "admission");
  assert.equal(classifyStorageFailure("WebSocket connection closed"), "routing");
  assert.equal(classifyStorageFailure("S3 provider persistence failed"), "persistence");
  assert.equal(classifyStorageFailure("hydrate recovery failed"), "recovery");
});
