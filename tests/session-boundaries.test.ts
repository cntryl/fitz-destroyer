import assert from "node:assert/strict";
import test from "node:test";
import { boundaryRoute } from "../src/workloads/session-boundaries.js";

test("should_generate_domain_correct_session_boundary_routes", () => {
  // Arrange
  const namespace = "run-123";

  // Act
  const routes = ["queue", "kv", "stream", "lease"].map((domain) =>
    boundaryRoute(domain as "queue" | "kv" | "stream" | "lease", namespace),
  );

  // Assert
  assert.deepEqual(routes, [
    "queue://destroyer/run-123/session-boundary",
    "kv://destroyer/run-123/session-boundary",
    "stream://destroyer/run-123/session-boundary",
    "lease://destroyer/run-123/session-boundary",
  ]);
});
