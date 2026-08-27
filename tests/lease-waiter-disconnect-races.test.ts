import assert from "node:assert/strict";
import test from "node:test";
import { assertLeaseWaiterRaceEvidence, leaseWaiterRoute } from "../src/workloads/lease-waiter-disconnect-races.js";

test("should_build_one_canonical_route_for_lease_waiter_races", () => {
  assert.equal(leaseWaiterRoute("run-1"), "lease://destroyer/run-1/waiter-race");
});

test("should_require_disconnected_waiters_to_quiesce_without_ghost_ownership", () => {
  // Arrange
  const evidence = { rounds: 8, waitersQueued: 32, waitersDisconnected: 32, ghostAcquisitions: 0, pendingWaiters: 0, replacementAcquisitions: 8, fencingRegressions: 0 };
  // Act / Assert
  assert.doesNotThrow(() => assertLeaseWaiterRaceEvidence(evidence));
  assert.throws(() => assertLeaseWaiterRaceEvidence({ ...evidence, ghostAcquisitions: 1 }), /ghostAcquisitions=1\/0/u);
});
