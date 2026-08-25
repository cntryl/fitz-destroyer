import assert from "node:assert/strict";
import test from "node:test";
import { assertQueueRedelivery } from "../src/orchestration/queue-redelivery.js";
import { queueRedeliveryRoute } from "../src/workloads/queue-redelivery.js";

test("should_accept_exact_redelivery_after_consumer_death", () => {
  // Arrange
  const killed = [1, 3];
  const recovered = [0, 1, 2, 3];

  // Act and Assert
  assert.doesNotThrow(() => assertQueueRedelivery(4, killed, recovered));
});

test("should_reject_a_reservation_lost_with_its_consumer", () => {
  // Arrange
  const killed = [1, 3];
  const recovered = [0, 1, 2];

  // Act and Assert
  assert.throws(() => assertQueueRedelivery(4, killed, recovered), /completed 3\/4/);
});

test("should_build_one_shared_redelivery_route", () => {
  assert.equal(queueRedeliveryRoute("run-1"), "queue://destroyer/run-1/redelivery");
});
