import assert from "node:assert/strict";
import test from "node:test";
import {
  assertNoCrossFamilyNotice,
  assertRouteFamilyArmedEvidence,
  assertRouteFamilyHolderEvidence,
  assertRouteFamilyProbeEvidence,
} from "../src/orchestration/route-family-isolation-matrix.js";
import {
  decodeRouteFamilyPayload,
  routeFamilyIsolationPermissions,
  routeFamilyIsolationRoutes,
} from "../src/workloads/route-family-isolation-matrix.js";
import { ALL_DOMAINS } from "../src/workloads/model.js";

test("should_use_the_same_logical_routes_for_every_authenticated_family", () => {
  // Arrange
  const first = routeFamilyIsolationRoutes("matrix-realm");
  const second = routeFamilyIsolationRoutes("matrix-realm");

  // Act
  const domains = Object.keys(first);

  // Assert
  assert.deepEqual(domains, ALL_DOMAINS);
  assert.deepEqual(first, second);
  assert.equal(first.kv, "kv://matrix-realm/isolation/shared");
  assert.equal(first.schedule, "schedule://matrix-realm/isolation/shared/job");
});

test("should_retain_the_route_family_identity_in_every_workload_payload", () => {
  // Arrange
  const payload = new TextEncoder().encode("route-family:identity-b:survivor");

  // Act
  const decoded = decodeRouteFamilyPayload(payload);

  // Assert
  assert.deepEqual(decoded, { identity: "identity-b", marker: "survivor" });
  assert.throws(
    () => decodeRouteFamilyPayload(new TextEncoder().encode("route-family:identity-c:survivor")),
    /invalid route-family payload/u,
  );
});

test("should_keep_mutations_scoped_while_allowing_schedule_list_authorization", () => {
  // Arrange
  const namespace = "matrix-realm";

  // Act
  const permissions = routeFamilyIsolationPermissions(namespace);

  // Assert
  assert.ok(permissions.includes("schedule://**#read"));
  assert.ok(permissions.includes(`schedule://${namespace}/**#*`));
  assert.ok(!permissions.includes("schedule://**#write"));
});

test("should_reject_holder_evidence_with_cross_family_delivery", () => {
  // Arrange
  const valid = {
    event: "route_family_isolation_holder_complete",
    identity: "identity-a",
    verifiedDomains: 7,
    ownNotices: 1,
    foreignNotices: 0,
    rpcResponses: 1,
    rpcRequestsHandled: 1,
  };

  // Act
  const evidence = assertRouteFamilyHolderEvidence(valid, "identity-a", 1);

  // Assert
  assert.equal(evidence.verifiedDomains, 7);
  assert.throws(
    () => assertRouteFamilyHolderEvidence({ ...valid, foreignNotices: 1 }, "identity-a", 1),
    /cross-family Notice/u,
  );
  assert.equal(
    assertRouteFamilyArmedEvidence(
      { event: "route_family_isolation_holder_armed", identity: "identity-a", verifiedDomains: 7 },
      "identity-a",
    ).verifiedDomains,
    7,
  );
  assert.doesNotThrow(() => assertNoCrossFamilyNotice(JSON.stringify(valid), "identity-a"));
  assert.throws(
    () => assertNoCrossFamilyNotice(
      JSON.stringify({ event: "route_family_isolation_foreign_notice" }),
      "identity-a",
    ),
    /cross-family Notice/u,
  );
});

test("should_require_every_domain_and_the_expected_cleanup_state", () => {
  // Arrange
  const survivor = {
    event: "route_family_isolation_probe_complete",
    identity: "identity-b",
    action: "verify-survivor",
    verifiedDomains: 7,
    leaseHeldBeforeProbe: true,
    rpcUnavailableBeforeProbe: false,
  };
  const closed = {
    ...survivor,
    identity: "identity-a",
    action: "verify-closed",
    leaseHeldBeforeProbe: false,
    rpcUnavailableBeforeProbe: true,
  };

  // Act
  const survivorEvidence = assertRouteFamilyProbeEvidence(survivor, "identity-b", "verify-survivor");
  const closedEvidence = assertRouteFamilyProbeEvidence(closed, "identity-a", "verify-closed");

  // Assert
  assert.equal(survivorEvidence.verifiedDomains, ALL_DOMAINS.length);
  assert.equal(closedEvidence.rpcUnavailableBeforeProbe, true);
  assert.throws(
    () => assertRouteFamilyProbeEvidence({ ...closed, leaseHeldBeforeProbe: true }, "identity-a", "verify-closed"),
    /closed family retained Lease ownership/u,
  );
});
