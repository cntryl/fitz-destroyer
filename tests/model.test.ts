import assert from "node:assert/strict";
import test from "node:test";
import {
  deterministicPayload,
  parseDomainSelection,
  resourceRoute,
  scheduleRoute,
  scheduleSelector,
  totalDurableEntries,
  type WorkloadShape,
} from "../src/workloads/model.js";

const shape: WorkloadShape = {
  namespace: "test-run",
  seed: 42,
  resources: 10,
  entriesPerResource: 1_000,
  payloadBytes: 128,
};

test("should_generate_repeatable_exact_size_payloads", () => {
  // Arrange
  const first = deterministicPayload(shape, "queue", 2, 17);

  // Act
  const second = deterministicPayload(shape, "queue", 2, 17);

  // Assert
  assert.equal(first.length, 128);
  assert.deepEqual(first, second);
});

test("should_change_payload_when_identity_changes", () => {
  // Arrange
  const first = deterministicPayload(shape, "stream", 2, 17);

  // Act
  const second = deterministicPayload(shape, "stream", 2, 18);

  // Assert
  assert.notDeepEqual(first, second);
});

test("should_generate_domain_routes_and_total_entry_count", () => {
  // Arrange
  const queue = resourceRoute("queue", shape, 3);

  // Act
  const schedule = scheduleRoute(shape, 3, 27);

  // Assert
  assert.equal(queue, "queue://destroyer/test-run/queue-0003");
  assert.equal(schedule, "schedule://destroyer/test-run/schedule-0003/job-00000027");
  assert.equal(scheduleSelector(shape), "schedule://destroyer/test-run/*");
  assert.equal(totalDurableEntries(shape), 40_000);
});

test("should_parse_a_canonical_bombard_domain_subset", () => {
  // Arrange
  const selection = "rpc, queue,rpc";

  // Act
  const domains = parseDomainSelection(selection);

  // Assert
  assert.deepEqual(domains, ["queue", "rpc"]);
});

test("should_reject_an_unknown_bombard_domain", () => {
  // Arrange
  const parse = () => parseDomainSelection("queue,banana");

  // Act
  // Assert
  assert.throws(parse, /banana/);
});
