import assert from "node:assert/strict";
import test from "node:test";
import { assertQueueOverloadReconciled } from "../src/orchestration/queue-overload.js";

test("should_reconcile_acknowledged_and_ambiguous_overload_outcomes", () => {
  assert.doesNotThrow(() => assertQueueOverloadReconciled(
    {
      "worker-a": { started: 4, acknowledged: [0, 2], failed: [1, 3] },
    },
    { "worker-a": [0, 1, 2] },
  ));
});

test("should_reject_a_missing_acknowledged_overload_outcome", () => {
  assert.throws(
    () => assertQueueOverloadReconciled(
      { "worker-a": { started: 2, acknowledged: [1], failed: [0] } },
      { "worker-a": [0] },
    ),
    /Acknowledged Queue sequence worker-a\/1 disappeared/u,
  );
});

test("should_reject_an_unstarted_overload_outcome", () => {
  assert.throws(
    () => assertQueueOverloadReconciled(
      { "worker-a": { started: 2, acknowledged: [], failed: [0, 1] } },
      { "worker-a": [2] },
    ),
    /Queue exposed unstarted sequence worker-a\/2/u,
  );
});
