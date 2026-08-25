import assert from "node:assert/strict";
import test from "node:test";
import { canaryRoute } from "../src/workloads/canary.js";
import { protocolAttackNames } from "../src/workloads/protocol-abuse.js";

test("should_keep_canary_routes_outside_the_hot_route", () => {
  // Arrange
  const namespace = "run-1";

  // Act
  const routes = [
    canaryRoute("queue", namespace),
    canaryRoute("stream", namespace, 4),
    canaryRoute("schedule", namespace, 4),
  ];

  // Assert
  assert.deepEqual(routes, [
    "queue://destroyer/run-1/canary",
    "stream://destroyer/run-1/canary-4",
    "schedule://destroyer/run-1/canary/job-4",
  ]);
});

test("should_cover_transport_and_tlv_protocol_abuse", () => {
  // Arrange
  const attacks = protocolAttackNames();

  // Act and Assert
  assert.ok(attacks.includes("text-before-connect"));
  assert.ok(attacks.includes("duplicate-tag-after-connect"));
  assert.ok(attacks.includes("oversize-frame"));
});
