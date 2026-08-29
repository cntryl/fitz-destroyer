import assert from "node:assert/strict";
import test from "node:test";
import { exhaustionPhaseShape } from "../src/orchestration/cache-and-disk-exhaustion.js";
import { executeStorageFaultRecovery } from "../src/orchestration/compose-jobs.js";
import type { WorkloadShape } from "../src/workloads/model.js";

test("should_isolate_each_exhaustion_phase_with_its_own_durable_baseline", () => {
  const shape: WorkloadShape = {
    namespace: "run-1",
    resources: 2,
    entriesPerResource: 20,
    payloadBytes: 256,
    seed: 424242,
  };

  assert.deepEqual(exhaustionPhaseShape(shape, "cache"), {
    ...shape,
    namespace: "run-1-cache",
  });
  assert.deepEqual(exhaustionPhaseShape(shape, "storage"), {
    ...shape,
    namespace: "run-1-storage",
  });
});

test("should_stop_fitz_before_restarting_exhausted_storage", async () => {
  const calls: string[] = [];

  await executeStorageFaultRecovery({
    stopFitz: async () => { calls.push("stop-fitz"); },
    restartStorage: async () => { calls.push("restart-storage"); },
    startFitz: async () => { calls.push("start-fitz"); },
  });

  assert.deepEqual(calls, ["stop-fitz", "restart-storage", "start-fitz"]);
});
