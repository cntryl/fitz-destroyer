import type { RunConfig } from "../config.js";
import type { WorkloadShape } from "../workloads/model.js";
import type { Artifacts } from "./artifacts.js";
import { type ComposeStack, type RoleContainer } from "./compose.js";
import { numericField, requiredEvent } from "./workload-log.js";

type StreamShape = {
  namespace: string;
  callers: number;
  callsPerCaller: number;
  framesPerCall: number;
  frameBytes: number;
  readerDelayMs: number;
  handlerDelayMs: number;
};

export async function runRpcStreamHose(
  stack: ComposeStack,
  config: RunConfig,
  shape: WorkloadShape,
  artifacts: Artifacts,
): Promise<void> {
  const startedAt = performance.now();
  const full: StreamShape = {
    namespace: `${shape.namespace}-full`,
    callers: config.clientReplicas,
    callsPerCaller: config.rpcStreamCalls,
    framesPerCall: config.rpcStreamFrames,
    frameBytes: config.rpcStreamFrameBytes,
    readerDelayMs: config.rpcStreamReaderDelayMs,
    handlerDelayMs: config.handlerDelayMs,
  };
  const expectedCalls = safeProduct(full.callers, full.callsPerCaller, "RPC stream calls");
  const expectedFrames = safeProduct(expectedCalls, full.framesPerCall, "RPC stream frames");
  const expectedBytes = safeProduct(expectedFrames, full.frameBytes, "RPC stream bytes");

  await artifacts.event("rpc_stream_hose_started", {
    callers: full.callers,
    workers: full.callers,
    callsPerCaller: full.callsPerCaller,
    framesPerCall: full.framesPerCall,
    frameBytes: full.frameBytes,
    expectedCalls,
    expectedFrames,
    expectedBytes,
    readerDelayMs: full.readerDelayMs,
    handlerDelayMs: full.handlerDelayMs,
  });

  await runSuccessfulFleet(stack, artifacts, config, "rpc-stream-full", full);

  const faultFrames = Math.min(config.rpcStreamFrames, 1_000);
  const progressAfterFrames = Math.max(1, Math.min(10, Math.floor(faultFrames / 4)));
  const fault: StreamShape = {
    namespace: `${shape.namespace}-fault`,
    callers: 1,
    callsPerCaller: 1,
    framesPerCall: faultFrames,
    frameBytes: config.rpcStreamFrameBytes,
    readerDelayMs: config.rpcStreamReaderDelayMs,
    handlerDelayMs: Math.max(5, config.handlerDelayMs),
  };

  await runCancellationPhase(stack, artifacts, config, {
    ...fault,
    namespace: `${fault.namespace}-cancel`,
  }, progressAfterFrames);
  await runWorkerKillPhase(stack, artifacts, config, {
    ...fault,
    namespace: `${fault.namespace}-worker-kill`,
  }, progressAfterFrames);
  await runFitzRestartPhase(stack, artifacts, config, {
    ...fault,
    namespace: `${fault.namespace}-fitz-restart`,
  }, progressAfterFrames);

  await artifacts.event("rpc_stream_hose_complete", {
    expectedCalls,
    expectedFrames,
    expectedBytes,
    destructivePhases: ["caller-cancel", "worker-kill", "fitz-restart"],
    elapsedMs: elapsedMs(startedAt),
  });
}

async function runSuccessfulFleet(
  stack: ComposeStack,
  artifacts: Artifacts,
  config: RunConfig,
  label: string,
  stream: StreamShape,
): Promise<void> {
  const startedAt = performance.now();
  const baseline = await stack.liveDomainSnapshot("rpc");
  await artifacts.writeJson(`${label}-stats-before.json`, baseline);
  const roleShape = workloadShape(stream);
  const progressAfterFrames = Math.max(1, Math.min(10, Math.floor(stream.framesPerCall / 4)));
  const workers = await stack.startRoleContainers(
    "rpc-stream-worker",
    stream.callers,
    roleShape,
    streamEnvironment(config, stream, progressAfterFrames),
  );
  await stack.waitForRoleEvent(workers, "rpc_stream_worker_ready");

  let callerLogs: Map<string, string> | undefined;
  let callerFailure: unknown;
  try {
    const callers = await stack.startRoleContainers(
      "rpc-stream-caller",
      stream.callers,
      roleShape,
      {
        ...streamEnvironment(config, stream, progressAfterFrames),
        DESTROYER_WAIT_FOR_START_SIGNAL: "true",
        DESTROYER_RPC_STREAM_EXPECTED_OUTCOME: "complete",
      },
    );
    await stack.waitForRoleEvent(callers, "live_producer_ready");
    await stack.signalRoleContainers(callers, "SIGUSR1");
    callerLogs = await stack.finishRoleContainers(callers, `${label}-caller`);
  } catch (error) {
    callerFailure = error;
  }

  await stack.signalRoleContainers(workers, "SIGTERM");
  const workerLogs = await stack.finishRoleContainers(workers, `${label}-worker`);
  if (callerFailure !== undefined) throw callerFailure;
  if (callerLogs === undefined) throw new Error(`${label} caller logs were not captured`);

  const expectedCalls = safeProduct(stream.callers, stream.callsPerCaller, `${label} calls`);
  const expectedFrames = safeProduct(expectedCalls, stream.framesPerCall, `${label} frames`);
  const expectedBytes = safeProduct(expectedFrames, stream.frameBytes, `${label} bytes`);
  const callerTotals = sumCompletionLogs(callerLogs, "rpc_stream_caller_complete");
  const workerTotals = sumWorkerLogs(workerLogs);
  if (
    callerTotals.completed !== expectedCalls ||
    callerTotals.interrupted !== 0 ||
    callerTotals.responseFrames !== expectedFrames ||
    callerTotals.responseBytes !== expectedBytes ||
    workerTotals.handled !== expectedCalls ||
    workerTotals.failures !== 0 ||
    workerTotals.framesSent !== expectedFrames
  ) {
    throw new Error(
      `${label} counts do not match: callers=${JSON.stringify(callerTotals)}, workers=${JSON.stringify(workerTotals)}, expectedCalls=${expectedCalls}, expectedFrames=${expectedFrames}, expectedBytes=${expectedBytes}`,
    );
  }

  const quiescence = await stack.waitForLiveDomainQuiescence("rpc", baseline, label);
  await artifacts.event("rpc_stream_success_phase_complete", {
    label,
    ...callerTotals,
    workerMaxActive: workerTotals.maxActive,
    cleanup: quiescence.cleanup,
    elapsedMs: elapsedMs(startedAt),
  });
}

async function runCancellationPhase(
  stack: ComposeStack,
  artifacts: Artifacts,
  config: RunConfig,
  stream: StreamShape,
  cancelAfterFrames: number,
): Promise<void> {
  const startedAt = performance.now();
  const label = "rpc-stream-caller-cancel";
  const baseline = await stack.liveDomainSnapshot("rpc");
  await artifacts.writeJson(`${label}-stats-before.json`, baseline);
  const workers = await startFaultWorker(stack, config, stream, cancelAfterFrames);
  const callers = await startFaultCaller(
    stack,
    config,
    stream,
    "cancel",
    cancelAfterFrames,
  );
  const callerLogs = await stack.finishRoleContainers(callers, `${label}-caller`);
  assertObservedFault(callerLogs, "cancel");
  await stopWorkers(stack, workers, `${label}-worker`);
  const quiescence = await stack.waitForLiveDomainQuiescence("rpc", baseline, label, true);
  await artifacts.event("rpc_stream_fault_phase_complete", {
    label,
    expectedOutcome: "cancel",
    cancelAfterFrames,
    cleanup: quiescence.cleanup,
    elapsedMs: elapsedMs(startedAt),
  });
  await runProbe(stack, artifacts, config, `${label}-probe`, stream);
}

async function runWorkerKillPhase(
  stack: ComposeStack,
  artifacts: Artifacts,
  config: RunConfig,
  stream: StreamShape,
  progressAfterFrames: number,
): Promise<void> {
  const startedAt = performance.now();
  const label = "rpc-stream-worker-kill";
  const baseline = await stack.liveDomainSnapshot("rpc");
  await artifacts.writeJson(`${label}-stats-before.json`, baseline);
  const workers = await startFaultWorker(stack, config, stream, progressAfterFrames);
  const callers = await startFaultCaller(stack, config, stream, "failure", 1);
  await stack.waitForRoleEvent(workers, "rpc_stream_worker_progress");
  await stack.killRoleContainers(workers, `${label}-worker`);
  const callerLogs = await stack.finishRoleContainers(callers, `${label}-caller`);
  assertObservedFault(callerLogs, "failure");
  const quiescence = await stack.waitForLiveDomainQuiescence("rpc", baseline, label, true);
  await artifacts.event("rpc_stream_fault_phase_complete", {
    label,
    expectedOutcome: "failure",
    progressAfterFrames,
    cleanup: quiescence.cleanup,
    elapsedMs: elapsedMs(startedAt),
  });
  await runProbe(stack, artifacts, config, `${label}-probe`, stream);
}

async function runFitzRestartPhase(
  stack: ComposeStack,
  artifacts: Artifacts,
  config: RunConfig,
  stream: StreamShape,
  progressAfterFrames: number,
): Promise<void> {
  const startedAt = performance.now();
  const label = "rpc-stream-fitz-restart";
  const baseline = await stack.liveDomainSnapshot("rpc");
  await artifacts.writeJson(`${label}-stats-before.json`, baseline);
  const workers = await startFaultWorker(stack, config, stream, progressAfterFrames);
  const callers = await startFaultCaller(stack, config, stream, "failure", 1);
  await stack.waitForRoleEvent(workers, "rpc_stream_worker_progress");
  await stack.killFitz();
  await stack.restartFitz();
  const postRestartBaseline = await stack.liveDomainSnapshot("rpc");
  await artifacts.writeJson(`${label}-stats-after-restart.json`, postRestartBaseline);
  const callerLogs = await stack.finishRoleContainers(callers, `${label}-caller`);
  assertObservedFault(callerLogs, "failure");
  await stopWorkers(stack, workers, `${label}-worker`);
  const quiescence = await stack.waitForLiveDomainQuiescence(
    "rpc",
    postRestartBaseline,
    label,
    true,
  );
  await artifacts.event("rpc_stream_fault_phase_complete", {
    label,
    expectedOutcome: "failure",
    progressAfterFrames,
    cleanup: quiescence.cleanup,
    elapsedMs: elapsedMs(startedAt),
  });
  await runProbe(stack, artifacts, config, `${label}-probe`, stream);
}

async function runProbe(
  stack: ComposeStack,
  artifacts: Artifacts,
  config: RunConfig,
  label: string,
  source: StreamShape,
): Promise<void> {
  await runSuccessfulFleet(stack, artifacts, config, label, {
    ...source,
    namespace: `${source.namespace}-probe`,
    callers: 1,
    callsPerCaller: 1,
    framesPerCall: Math.min(source.framesPerCall, 32),
    frameBytes: Math.min(source.frameBytes, 4_096),
    readerDelayMs: 0,
    handlerDelayMs: 0,
  });
}

async function startFaultWorker(
  stack: ComposeStack,
  config: RunConfig,
  stream: StreamShape,
  progressAfterFrames: number,
): Promise<RoleContainer[]> {
  const workers = await stack.startRoleContainers(
    "rpc-stream-worker",
    1,
    workloadShape(stream),
    streamEnvironment(config, stream, progressAfterFrames, 15_000),
  );
  await stack.waitForRoleEvent(workers, "rpc_stream_worker_ready");
  return workers;
}

async function startFaultCaller(
  stack: ComposeStack,
  config: RunConfig,
  stream: StreamShape,
  expectedOutcome: "cancel" | "failure",
  cancelAfterFrames: number,
): Promise<RoleContainer[]> {
  const callers = await stack.startRoleContainers(
    "rpc-stream-caller",
    1,
    workloadShape(stream),
    {
      ...streamEnvironment(config, stream, cancelAfterFrames, 15_000),
      DESTROYER_WAIT_FOR_START_SIGNAL: "true",
      DESTROYER_RPC_STREAM_EXPECTED_OUTCOME: expectedOutcome,
      DESTROYER_RPC_STREAM_CANCEL_AFTER_FRAMES: String(cancelAfterFrames),
    },
  );
  await stack.waitForRoleEvent(callers, "live_producer_ready");
  await stack.signalRoleContainers(callers, "SIGUSR1");
  return callers;
}

async function stopWorkers(
  stack: ComposeStack,
  workers: readonly RoleContainer[],
  label: string,
): Promise<void> {
  await stack.signalRoleContainers(workers, "SIGTERM");
  await stack.finishRoleContainers(workers, label);
}

function assertObservedFault(
  logs: ReadonlyMap<string, string>,
  expectedOutcome: "cancel" | "failure",
): void {
  if (logs.size !== 1) throw new Error(`Expected one RPC stream fault caller, found ${logs.size}`);
  const log = [...logs.values()][0];
  if (log === undefined) throw new Error("RPC stream fault caller log was not captured");
  const complete = requiredEvent(log, "rpc_stream_caller_complete");
  if (
    complete.expectedOutcome !== expectedOutcome ||
    numericField(complete, "completed") !== 0 ||
    numericField(complete, "interrupted") !== 1
  ) {
    throw new Error(`RPC stream did not observe ${expectedOutcome}: ${JSON.stringify(complete)}`);
  }
}

function sumCompletionLogs(
  logs: ReadonlyMap<string, string>,
  event: string,
): { completed: number; interrupted: number; responseFrames: number; responseBytes: number } {
  const totals = { completed: 0, interrupted: 0, responseFrames: 0, responseBytes: 0 };
  for (const log of logs.values()) {
    const complete = requiredEvent(log, event);
    totals.completed += numericField(complete, "completed");
    totals.interrupted += numericField(complete, "interrupted");
    totals.responseFrames += numericField(complete, "responseFrames");
    totals.responseBytes += numericField(complete, "responseBytes");
  }
  return totals;
}

function sumWorkerLogs(
  logs: ReadonlyMap<string, string>,
): { handled: number; failures: number; framesSent: number; maxActive: number } {
  const totals = { handled: 0, failures: 0, framesSent: 0, maxActive: 0 };
  for (const log of logs.values()) {
    const complete = requiredEvent(log, "rpc_stream_worker_complete");
    totals.handled += numericField(complete, "handled");
    totals.failures += numericField(complete, "failures");
    totals.framesSent += numericField(complete, "framesSent");
    totals.maxActive = Math.max(totals.maxActive, numericField(complete, "maxActive"));
  }
  return totals;
}

function workloadShape(stream: StreamShape): WorkloadShape {
  return {
    namespace: stream.namespace,
    seed: 0,
    resources: 1,
    entriesPerResource: stream.callsPerCaller,
    payloadBytes: stream.frameBytes,
  };
}

function streamEnvironment(
  config: RunConfig,
  stream: StreamShape,
  progressAfterFrames: number,
  requestTimeoutMs = config.startupTimeoutMs,
): Record<string, string> {
  return {
    DESTROYER_RPC_STREAM_FRAMES: String(stream.framesPerCall),
    DESTROYER_RPC_STREAM_FRAME_BYTES: String(stream.frameBytes),
    DESTROYER_RPC_STREAM_READER_DELAY_MS: String(stream.readerDelayMs),
    DESTROYER_RPC_STREAM_PROGRESS_AFTER_FRAMES: String(progressAfterFrames),
    DESTROYER_HANDLER_DELAY_MS: String(stream.handlerDelayMs),
    DESTROYER_REQUEST_TIMEOUT_MS: String(requestTimeoutMs),
  };
}

function safeProduct(...valuesAndLabel: [...number[], string]): number {
  const label = valuesAndLabel.at(-1);
  const values = valuesAndLabel.slice(0, -1) as number[];
  const result = values.reduce((product, value) => product * value, 1);
  if (!Number.isSafeInteger(result)) {
    throw new Error(`${String(label)} exceeds JavaScript's safe integer range`);
  }
  return result;
}

function elapsedMs(startedAt: number): number {
  return Math.round(performance.now() - startedAt);
}
