import assert from "node:assert/strict";
import test from "node:test";
import { runResourceOperationsSequentially } from "../src/workloads/recovery.js";

test("should_keep_same_domain_recovery_operations_sequential", async () => {
  let active = 0;
  let peak = 0;
  const completed: number[] = [];

  await runResourceOperationsSequentially(4, async (resource) => {
    active += 1;
    peak = Math.max(peak, active);
    await new Promise<void>((resolve) => setImmediate(resolve));
    completed.push(resource);
    active -= 1;
  });

  assert.equal(peak, 1);
  assert.deepEqual(completed, [0, 1, 2, 3]);
});
