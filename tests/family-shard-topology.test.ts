import assert from "node:assert/strict";
import test from "node:test";
import {
  DESTROYER_FAMILY_ACTOR_FAMILIES,
  DESTROYER_FAMILY_ACTOR_SHARD_COUNT,
  DESTROYER_PRIMARY_FAMILY,
  DESTROYER_SAME_SHARD_FAMILY,
} from "../src/family-shard-topology.js";

test("should_fit_the_same_shard_topology_on_hosted_runners", () => {
  assert.deepEqual(DESTROYER_FAMILY_ACTOR_FAMILIES, [1, 2, 3, 4, 5]);
  assert.equal(DESTROYER_FAMILY_ACTOR_SHARD_COUNT, 4);
  assert.equal(
    (DESTROYER_PRIMARY_FAMILY - 1) % DESTROYER_FAMILY_ACTOR_SHARD_COUNT,
    (DESTROYER_SAME_SHARD_FAMILY - 1) % DESTROYER_FAMILY_ACTOR_SHARD_COUNT,
  );
});
