import assert from "node:assert/strict";
import test from "node:test";
import {
  STORAGE_FAULT_IDENTITIES,
  classifyStorageFailure,
  runStorageFaultIteration,
  storageFaultPlan,
} from "../src/orchestration/storage-faults.js";
import type { ComposeStack } from "../src/orchestration/compose.js";
import type { WorkloadShape } from "../src/workloads/model.js";

test("should_generate_seeded_storage_fault_ledger_iterations", () => {
  const plan = storageFaultPlan(7, 1234);

  assert.deepEqual(plan.map(({ sequence }) => sequence), [1, 2, 3, 4, 5, 6, 7]);
  assert.deepEqual(plan.slice(0, 5).map(({ fault }) => fault), STORAGE_FAULT_IDENTITIES);
  assert.ok(plan.every(({ seed }) => seed === 1234));
});

test("should_classify_storage_fault_layers_from_observed_errors", () => {
  assert.equal(classifyStorageFailure("queue capacity exhausted"), "admission");
  assert.equal(classifyStorageFailure("WebSocket connection closed"), "routing");
  assert.equal(classifyStorageFailure("S3 provider persistence failed"), "persistence");
  assert.equal(classifyStorageFailure("hydrate recovery failed"), "recovery");
});

test("should_connect_the_writer_before_cutting_its_storage_path", async () => {
  const calls: string[] = [];
  const stack = {
    setFaultProxy: async (_service: string, fault: { mode: string }) => {
      calls.push(`proxy-${fault.mode}`);
    },
    startRoleContainers: async () => {
      calls.push("writer-started");
      return [];
    },
    waitForRoleEvent: async (_role: unknown, event: string) => {
      calls.push(event);
    },
    signalRoleContainers: async () => {
      calls.push("writer-signalled");
    },
    finishRoleContainers: async () => {
      calls.push("writer-finished");
      return new Map<string, string>();
    },
    ensureReady: async () => {
      calls.push("broker-ready");
    },
  } as unknown as ComposeStack;
  const shape: WorkloadShape = {
    namespace: "storage-fault",
    resources: 1,
    entriesPerResource: 1,
    payloadBytes: 1,
    seed: 42,
  };

  await runStorageFaultIteration(
    stack,
    shape,
    {},
    { iteration: 1, sequence: 1, seed: 42, fault: "connection-reset" },
    async () => undefined,
  );

  assert.deepEqual(calls, [
    "proxy-healthy",
    "writer-started",
    "live_producer_ready",
    "proxy-reset",
    "writer-signalled",
    "durability_operations_dispatched",
    "proxy-healthy",
    "writer-finished",
    "proxy-healthy",
    "broker-ready",
  ]);
});
