import type { RunConfig } from "../config.js";
import type { WorkloadShape } from "../workloads/model.js";
import type { Artifacts } from "./artifacts.js";
import type { ComposeStack, RoleContainer } from "./compose.js";
import {
  analyzeDurabilityLedger,
  assertDurabilityLedger,
} from "./durability-crash-cuts.js";
import { numericField, requiredEvent } from "./workload-log.js";

export async function runActiveGracefulShutdownScenario(
  stack: ComposeStack,
  config: RunConfig,
  shape: WorkloadShape,
  artifacts: Artifacts,
): Promise<void> {
  const startedAt = performance.now();
  const durabilityEnvironment = {
    DESTROYER_SEED: String(shape.seed),
    DESTROYER_DURABILITY_ITERATIONS: "1",
  };
  await artifacts.event("active_graceful_shutdown_started", {
    durableDomains: ["queue", "kv", "stream", "schedule"],
    liveDomain: "rpc",
  });

  const baseline = await stack.startRoleContainers("durability-writer", 1, shape, {
    ...durabilityEnvironment,
    DESTROYER_DURABILITY_ACTION: "baseline",
    DESTROYER_DURABILITY_SEQUENCE: "0",
  });
  const baselineLogs = await stack.finishRoleContainers(
    baseline,
    "active-graceful-shutdown-baseline",
  );

  const rpcShape = { ...shape, namespace: `${shape.namespace}-active-rpc`, entriesPerResource: 1 };
  const rpcEnvironment = streamEnvironment(config, 1_000, 5, "failure");
  const rpcWorker = await stack.startRoleContainers("rpc-stream-worker", 1, rpcShape, rpcEnvironment);
  await stack.waitForRoleEvent(rpcWorker, "rpc_stream_worker_ready");
  const rpcCaller = await stack.startRoleContainers("rpc-stream-caller", 1, rpcShape, {
    ...rpcEnvironment,
    DESTROYER_WAIT_FOR_START_SIGNAL: "true",
  });
  await stack.waitForRoleEvent(rpcCaller, "live_producer_ready");
  await stack.signalRoleContainers(rpcCaller, "SIGUSR1");
  await stack.waitForRoleEvent(rpcWorker, "rpc_stream_worker_progress");

  const cut = await stack.startRoleContainers("durability-writer", 1, shape, {
    ...durabilityEnvironment,
    DESTROYER_DURABILITY_ACTION: "cut",
    DESTROYER_DURABILITY_SEQUENCE: "1",
    DESTROYER_WAIT_FOR_START_SIGNAL: "true",
  });
  await stack.waitForRoleEvent(cut, "live_producer_ready");
  await stack.signalRoleContainers(cut, "SIGUSR1");
  await stack.waitForRoleEvent(cut, "durability_operations_dispatched");
  await stack.gracefulRestartFitz();

  const callerLogs = await stack.finishRoleContainers(
    rpcCaller,
    "active-graceful-shutdown-rpc-caller",
  );
  assertInterruptedCaller(callerLogs);
  await stopWorkers(stack, rpcWorker, "active-graceful-shutdown-rpc-worker");
  const cutLogs = await stack.finishRoleContainers(cut, "active-graceful-shutdown-cut");
  const verifier = await stack.startRoleContainers("durability-verifier", 1, shape, {
    ...durabilityEnvironment,
    DESTROYER_DURABILITY_SEQUENCE: "0",
  });
  const verifyLogs = await stack.finishRoleContainers(
    verifier,
    "active-graceful-shutdown-verify",
  );
  const ledger = analyzeDurabilityLedger(baselineLogs, cutLogs, verifyLogs);
  assertDurabilityLedger(ledger, 1);

  const probeFrames = 16;
  const probeShape = { ...rpcShape, namespace: `${rpcShape.namespace}-probe` };
  const probeEnvironment = streamEnvironment(config, probeFrames, 1, "complete");
  const probeWorker = await stack.startRoleContainers(
    "rpc-stream-worker",
    1,
    probeShape,
    probeEnvironment,
  );
  await stack.waitForRoleEvent(probeWorker, "rpc_stream_worker_ready");
  const probeCaller = await stack.startRoleContainers(
    "rpc-stream-caller",
    1,
    probeShape,
    probeEnvironment,
  );
  const probeLogs = await stack.finishRoleContainers(
    probeCaller,
    "active-graceful-shutdown-probe-caller",
  );
  const probeComplete = onlyCompletion(probeLogs);
  if (
    numericField(probeComplete, "completed") !== 1 ||
    numericField(probeComplete, "responseFrames") !== probeFrames
  ) {
    throw new Error(`Post-shutdown RPC probe was incomplete: ${JSON.stringify(probeComplete)}`);
  }
  await stopWorkers(stack, probeWorker, "active-graceful-shutdown-probe-worker");

  await artifacts.writeJson("active-graceful-shutdown-ledger.json", ledger);
  await artifacts.event("active_graceful_shutdown_complete", {
    durableOperationsStarted: 4,
    rpcCallsInterrupted: 1,
    probeFrames,
    elapsedMs: Math.round(performance.now() - startedAt),
  });
}

function streamEnvironment(
  config: RunConfig,
  frames: number,
  progressAfterFrames: number,
  expectedOutcome: "complete" | "failure",
): Record<string, string> {
  return {
    DESTROYER_RPC_STREAM_FRAMES: String(frames),
    DESTROYER_RPC_STREAM_FRAME_BYTES: String(Math.min(1_024, config.rpcStreamFrameBytes)),
    DESTROYER_RPC_STREAM_READER_DELAY_MS: "1",
    DESTROYER_RPC_STREAM_PROGRESS_AFTER_FRAMES: String(progressAfterFrames),
    DESTROYER_HANDLER_DELAY_MS: "5",
    DESTROYER_REQUEST_TIMEOUT_MS: "30000",
    DESTROYER_RPC_STREAM_EXPECTED_OUTCOME: expectedOutcome,
  };
}

async function stopWorkers(
  stack: ComposeStack,
  workers: readonly RoleContainer[],
  label: string,
): Promise<void> {
  await stack.signalRoleContainers(workers, "SIGTERM");
  await stack.finishRoleContainers(workers, label);
}

function assertInterruptedCaller(logs: ReadonlyMap<string, string>): void {
  const complete = onlyCompletion(logs);
  if (
    complete.expectedOutcome !== "failure" ||
    numericField(complete, "completed") !== 0 ||
    numericField(complete, "interrupted") !== 1
  ) {
    throw new Error(`Active RPC call did not terminate during shutdown: ${JSON.stringify(complete)}`);
  }
}

function onlyCompletion(logs: ReadonlyMap<string, string>): Record<string, unknown> {
  const log = [...logs.values()][0];
  if (logs.size !== 1 || log === undefined) throw new Error("Expected exactly one RPC caller log");
  return requiredEvent(log, "rpc_stream_caller_complete");
}
