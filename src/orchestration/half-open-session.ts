import type { RunConfig } from "../config.js";
import type { WorkloadShape } from "../workloads/model.js";
import type { Artifacts } from "./artifacts.js";
import type { ComposeStack } from "./compose.js";
import { numericField, requiredEvent } from "./workload-log.js";

export async function runHalfOpenSessionScenario(
  stack: ComposeStack,
  _config: RunConfig,
  shape: WorkloadShape,
  artifacts: Artifacts,
): Promise<void> {
  const startedAt = performance.now();
  await artifacts.event("half_open_session_started", {
    fault: "bidirectional-silent-blackhole",
    detection: "client-heartbeat",
    assertions: [
      "stale handles reject",
      "Queue inflight redelivers",
      "KV and Stream uncommitted state is discarded",
      "Lease ownership is released",
    ],
  });
  const holder = await stack.startRoleContainers("session-boundaries", 1, shape, {
    FITZ_URL: "ws://client-proxy:4090/ws",
    DESTROYER_SEED: String(shape.seed),
  });
  await stack.waitForRoleEvent(holder, "session_boundaries_armed");
  try {
    await stack.setFaultProxy("client-proxy", { mode: "blackhole" });
    await stack.waitForRoleEvent(holder, "session_boundaries_disconnect_observed");
  } finally {
    await stack.setFaultProxy("client-proxy", { mode: "healthy" }).catch(() => undefined);
  }
  const logs = await stack.finishRoleContainers(holder, "half-open-session");
  const log = logs.get("0");
  if (log === undefined) throw new Error("Half-open session worker log was missing");
  const complete = assertSessionBoundaryEvidence(log);
  await artifacts.event("half_open_session_complete", {
    staleRejections: numericField(complete, "staleRejections"),
    queueRedelivered: numericField(complete, "queueRedelivered"),
    leaseReacquired: numericField(complete, "leaseReacquired"),
    elapsedMs: Math.round(performance.now() - startedAt),
  });
}

export function assertSessionBoundaryEvidence(
  log: string,
): Readonly<Record<string, unknown>> {
  const complete = requiredEvent(log, "session_boundaries_complete");
  assertCount(complete, "staleRejections", 4);
  assertCount(complete, "queueRedelivered", 1);
  assertCount(complete, "queueCompleted", 1);
  assertCount(complete, "kvUncommittedValues", 0);
  assertCount(complete, "streamUncommittedRecords", 0);
  assertCount(complete, "leaseReacquired", 1);
  if (complete.leaseHeldAfterRestart !== false) {
    throw new Error("Lease remained held after the half-open session was reaped");
  }
  return complete;
}

function assertCount(
  record: Readonly<Record<string, unknown>>,
  field: string,
  expected: number,
): void {
  const actual = numericField(record, field);
  if (actual !== expected) throw new Error(`${field}=${actual}/${expected}`);
}
