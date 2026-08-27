import type { RunConfig } from "../config.js";
import type { HostileRpcBehavior } from "../workloads/hostile-rpc-worker.js";
import type { WorkloadShape } from "../workloads/model.js";
import type { Artifacts } from "./artifacts.js";
import type { ComposeStack } from "./compose.js";
import { numericField, requiredEvent } from "./workload-log.js";

export async function runHostileRpcWorkerScenario(
  stack: ComposeStack,
  _config: RunConfig,
  shape: WorkloadShape,
  artifacts: Artifacts,
): Promise<void> {
  const startedAt = performance.now();
  const baseline = await stack.liveDomainSnapshot("rpc");
  const returned = await runHostilePhase(stack, shape, "return-without-terminal");
  const threw = await runHostilePhase(stack, shape, "throw");
  const probeShape = { ...shape, namespace: `${shape.namespace}-probe`, entriesPerResource: 1 };
  const workers = await stack.startRoleContainers("rpc-worker", 1, probeShape);
  await stack.waitForRoleEvent(workers, "rpc_worker_ready");
  const callers = await stack.startRoleContainers("rpc-caller", 1, probeShape);
  const callerLogs = await stack.finishRoleContainers(callers, "hostile-rpc-probe-caller");
  await stack.signalRoleContainers(workers, "SIGTERM");
  await stack.finishRoleContainers(workers, "hostile-rpc-probe-worker");
  const probe = requiredEvent([...callerLogs.values()][0] ?? "", "rpc_caller_complete");
  const probeFrames = numericField(probe, "responseFrames");
  assertHostileRpcWorker(returned.failures, returned.frames, threw.failures, threw.frames, probeFrames);
  await stack.waitForLiveDomainQuiescence("rpc", baseline, "hostile-rpc-worker", true);
  await artifacts.event("hostile_rpc_worker_complete", {
    returnWithoutTerminalFailures: returned.failures,
    returnWithoutTerminalFrames: returned.frames,
    thrownHandlerFailures: threw.failures,
    thrownHandlerFrames: threw.frames,
    probeFrames,
    elapsedMs: Math.round(performance.now() - startedAt),
  });
}

export function assertHostileRpcWorker(
  returnedFailures: number,
  returnedFrames: number,
  thrownFailures: number,
  thrownFrames: number,
  probeFrames: number,
): void {
  if (
    returnedFailures !== 1 ||
    returnedFrames !== 0 ||
    thrownFailures !== 0 ||
    thrownFrames !== 1 ||
    probeFrames !== 2
  ) {
    throw new Error(
      `Hostile RPC isolation failed: returned=${returnedFailures}/${returnedFrames}, ` +
        `thrown=${thrownFailures}/${thrownFrames}, probeFrames=${probeFrames}`,
    );
  }
}

async function runHostilePhase(
  stack: ComposeStack,
  shape: WorkloadShape,
  behavior: HostileRpcBehavior,
): Promise<{ failures: number; frames: number }> {
  const phaseShape = { ...shape, namespace: `${shape.namespace}-${behavior}`, entriesPerResource: 1 };
  const workers = await stack.startRoleContainers(
    "hostile-rpc-worker",
    1,
    phaseShape,
    {
      DESTROYER_HOSTILE_RPC_BEHAVIOR: behavior,
      DESTROYER_REQUEST_TIMEOUT_MS: "1500",
    },
  );
  await stack.waitForRoleEvent(workers, "hostile_rpc_worker_ready");
  const callers = await stack.startRoleContainers(
    "hostile-rpc-caller",
    1,
    phaseShape,
    {
      DESTROYER_HOSTILE_RPC_BEHAVIOR: behavior,
      DESTROYER_REQUEST_TIMEOUT_MS: "1500",
    },
  );
  const callerLogs = await stack.finishRoleContainers(callers, `hostile-rpc-${behavior}-caller`);
  await stack.signalRoleContainers(workers, "SIGTERM");
  await stack.finishRoleContainers(workers, `hostile-rpc-${behavior}-worker`);
  const completion = requiredEvent(
    [...callerLogs.values()][0] ?? "",
    "hostile_rpc_caller_complete",
  );
  return {
    failures: numericField(completion, "failures"),
    frames: numericField(completion, "frames"),
  };
}
