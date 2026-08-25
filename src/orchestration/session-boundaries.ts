import type { RunConfig } from "../config.js";
import type { WorkloadShape } from "../workloads/model.js";
import type { Artifacts } from "./artifacts.js";
import type { ComposeStack } from "./compose.js";
import { numericField, requiredEvent } from "./workload-log.js";

export async function runSessionBoundariesScenario(
  stack: ComposeStack,
  config: RunConfig,
  shape: WorkloadShape,
  artifacts: Artifacts,
): Promise<void> {
  const startedAt = performance.now();
  const runLabel = "session-boundaries";
  await artifacts.event("session_boundaries_started", {
    fault: "fitz-sigkill",
    assertions: [
      "stale handles reject",
      "Queue inflight redelivers",
      "KV uncommitted writes roll back",
      "Stream uncommitted appends abort",
      "Lease ownership is released",
    ],
  });
  const holders = await stack.startRoleContainers("session-boundaries", 1, shape, {
    DESTROYER_SEED: String(shape.seed),
  });
  await stack.waitForRoleEvent(holders, "session_boundaries_armed");
  await stack.killFitz();
  await stack.restartFitz();
  const logs = await stack.finishRoleContainers(holders, runLabel);
  const log = logs.get("0");
  if (log === undefined) throw new Error("Session-boundary worker log is missing");
  const complete = requiredEvent(log, "session_boundaries_complete");
  assertCount(complete, "staleRejections", 4);
  assertCount(complete, "queueRedelivered", 1);
  assertCount(complete, "queueCompleted", 1);
  assertCount(complete, "kvCommittedValues", 1);
  assertCount(complete, "kvUncommittedValues", 0);
  assertCount(complete, "streamCommittedRecords", 1);
  assertCount(complete, "streamUncommittedRecords", 0);
  assertCount(complete, "leaseReacquired", 1);
  if (complete.leaseHeldAfterRestart !== false) {
    throw new Error("Lease was still held after broker restart");
  }
  await artifacts.event("session_boundaries_complete", {
    staleRejections: 4,
    queueRedelivered: 1,
    queueCompleted: 1,
    kvUncommittedValues: 0,
    streamUncommittedRecords: 0,
    leaseReacquired: 1,
    elapsedMs: Math.round(performance.now() - startedAt),
  });
}

function assertCount(
  record: Readonly<Record<string, unknown>>,
  field: string,
  expected: number,
): void {
  const actual = numericField(record, field);
  if (actual !== expected) throw new Error(`${field}=${actual}/${expected}`);
}
