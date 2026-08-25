import assert from "node:assert/strict";
import test from "node:test";
import {
  assertLeaseAdmissions,
  type LeaseAdmission,
} from "../src/orchestration/lease-contention.js";
import { leaseContentionRoute } from "../src/workloads/lease-contention.js";

test("should_accept_unique_fencing_for_every_contender", () => {
  // Arrange
  const admissions: LeaseAdmission[] = [
    { participant: "a", sequence: 0, fencingToken: 1n },
    { participant: "b", sequence: 0, fencingToken: 2n },
    { participant: "a", sequence: 1, fencingToken: 3n },
    { participant: "b", sequence: 1, fencingToken: 4n },
  ];

  // Act and Assert
  assert.doesNotThrow(() => assertLeaseAdmissions(admissions, 4, 2));
});

test("should_reject_reused_lease_fencing", () => {
  // Arrange
  const admissions: LeaseAdmission[] = [
    { participant: "a", sequence: 0, fencingToken: 1n },
    { participant: "b", sequence: 0, fencingToken: 1n },
  ];

  // Act and Assert
  assert.throws(() => assertLeaseAdmissions(admissions, 2, 2), /reused a fencing token/);
});

test("should_build_one_shared_contention_route", () => {
  assert.equal(leaseContentionRoute("run-1"), "lease://destroyer/run-1/contended");
});
