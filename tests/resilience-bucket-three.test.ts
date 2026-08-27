import assert from "node:assert/strict";
import test from "node:test";
import { assertSessionBoundaryEvidence } from "../src/orchestration/half-open-session.js";
import { routeCardinalityRoute } from "../src/workloads/route-cardinality-churn.js";
import { ALL_DOMAINS } from "../src/workloads/model.js";

test("should_generate_a_unique_concrete_route_for_every_domain_and_sequence", () => {
  // Arrange
  const routes = ALL_DOMAINS.flatMap((domain) =>
    [0, 1].map((sequence) => routeCardinalityRoute(domain, "namespace", sequence))
  );

  // Act
  const unique = new Set(routes);

  // Assert
  assert.equal(unique.size, ALL_DOMAINS.length * 2);
  assert.ok(routes.every((route) => !route.includes("*")));
});

test("should_require_full_session_cleanup_after_directional_or_pause_faults", () => {
  // Arrange
  const valid = JSON.stringify({
    event: "session_boundaries_complete",
    staleRejections: 4,
    queueRedelivered: 1,
    queueCompleted: 1,
    kvUncommittedValues: 0,
    streamUncommittedRecords: 0,
    leaseReacquired: 1,
    leaseHeldAfterRestart: false,
  });
  const invalid = valid.replace('"staleRejections":4', '"staleRejections":3');

  // Act
  const complete = assertSessionBoundaryEvidence(valid);

  // Assert
  assert.equal(complete.staleRejections, 4);
  assert.throws(() => assertSessionBoundaryEvidence(invalid), /staleRejections=3\/4/u);
});
