import assert from "node:assert/strict";
import test from "node:test";
import { boundarySizes } from "../src/workloads/response-envelope-boundaries.js";

test("should_calculate_exact_and_one_over_response_boundaries", () => {
  // Arrange / Act
  const sizes = boundarySizes(65_506);
  // Assert
  assert.deepEqual(sizes, { exact: 65_506, oneOver: 65_507 });
});

test("should_reject_invalid_response_boundary_limits", () => {
  assert.throws(() => boundarySizes(0), /invalid response limit/u);
});
