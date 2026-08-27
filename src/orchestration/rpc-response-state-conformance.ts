import type { RunConfig } from "../config.js";
import type { WorkloadShape } from "../workloads/model.js";
import type { Artifacts } from "./artifacts.js";
import type { ComposeStack } from "./compose.js";
import { numericField, requiredEvent } from "./workload-log.js";

type RpcResponseStateEvidence = {
  cases: number;
  callersTerminated: number;
  duplicateCallerTerminals: number;
  unknownCorrelationRejected: number;
  duplicateTerminalRejected: number;
  postCancelResponsesObserved: number;
  postDisconnectRejected: number;
  healthyCalls: number;
  healthyFailures: number;
};

export function assertRpcResponseStateEvidence(
  record: Readonly<Record<string, unknown>>,
): RpcResponseStateEvidence {
  const evidence: RpcResponseStateEvidence = {
    cases: numericField(record, "cases"),
    callersTerminated: numericField(record, "callersTerminated"),
    duplicateCallerTerminals: numericField(record, "duplicateCallerTerminals"),
    unknownCorrelationRejected: numericField(record, "unknownCorrelationRejected"),
    duplicateTerminalRejected: numericField(record, "duplicateTerminalRejected"),
    postCancelResponsesObserved: numericField(record, "postCancelResponsesObserved"),
    postDisconnectRejected: numericField(record, "postDisconnectRejected"),
    healthyCalls: numericField(record, "healthyCalls"),
    healthyFailures: numericField(record, "healthyFailures"),
  };
  const expected: Readonly<Partial<RpcResponseStateEvidence>> = {
    cases: 5,
    callersTerminated: 4,
    duplicateCallerTerminals: 0,
    unknownCorrelationRejected: 1,
    duplicateTerminalRejected: 1,
    postCancelResponsesObserved: 1,
    postDisconnectRejected: 1,
    healthyCalls: 4,
    healthyFailures: 0,
  };
  for (const [field, value] of Object.entries(expected)) {
    const actual = evidence[field as keyof RpcResponseStateEvidence];
    if (actual !== value) throw new Error(`${field}=${actual}/${value}`);
  }
  return evidence;
}

export async function runRpcResponseStateConformanceScenario(
  stack: ComposeStack,
  _config: RunConfig,
  shape: WorkloadShape,
  artifacts: Artifacts,
): Promise<void> {
  const startedAt = performance.now();
  await artifacts.event("rpc_response_state_conformance_started", { cases: 5, workerCredit: 1 });
  const role = await stack.startRoleContainers("rpc-response-state-conformance", 1, shape, {
    DESTROYER_RPC_STATE_URL: "ws://fitz:4090/ws",
  });
  const logs = await stack.finishRoleContainers(role, "rpc-response-state-conformance");
  const log = logs.get("0");
  if (log === undefined) throw new Error("RPC response-state worker log was missing");
  const evidence = assertRpcResponseStateEvidence(
    requiredEvent(log, "rpc_response_state_conformance_worker_complete"),
  );
  await artifacts.writeJson("rpc-response-state-conformance-evidence.json", evidence);
  await artifacts.event("rpc_response_state_conformance_complete", {
    ...evidence,
    elapsedMs: Math.round(performance.now() - startedAt),
  });
}
