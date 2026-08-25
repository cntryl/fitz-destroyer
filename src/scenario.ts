import { randomBytes } from "node:crypto";
import type { RunConfig, ScenarioName } from "./config.js";
import { Artifacts } from "./orchestration/artifacts.js";
import { ComposeStack } from "./orchestration/compose.js";
import { runDurabilityCrashCutsScenario } from "./orchestration/durability-crash-cuts.js";
import { runLeaseContentionScenario } from "./orchestration/lease-contention.js";
import { runQueueRedeliveryScenario } from "./orchestration/queue-redelivery.js";
import {
  runHotRouteCanaryScenario,
  runProtocolAbuseScenario,
} from "./orchestration/interference.js";
import { runRpcStreamHose } from "./orchestration/rpc-stream-hose.js";
import { runScheduleDelivery } from "./orchestration/schedule-delivery.js";
import { runSessionBoundariesScenario } from "./orchestration/session-boundaries.js";
import { runPressureScenario } from "./orchestration/pressure.js";
import { runStorageFaultsScenario } from "./orchestration/storage-faults.js";
import { runQueueLifecycleScenario } from "./orchestration/queue-lifecycle.js";
import { runTransactionContentionScenario } from "./orchestration/transaction-contention.js";
import { runStreamReplayScenario } from "./orchestration/stream-replay.js";
import { runScheduleOutageScenario } from "./orchestration/schedule-outage.js";
import { runLiveChurnScenario } from "./orchestration/live-churn.js";
import { totalDurableEntries, type WorkloadShape } from "./workloads/model.js";

export type ConcreteScenario = Exclude<ScenarioName, "all">;

export type FailureClassification = "setup" | "workload" | "assertion" | "timeout" | "cleanup";
export type CleanupState = "removed" | "preserved" | "failed";
export type ScenarioResult = {
  scenario: ConcreteScenario;
  verdict: "passed" | "failed";
  durationMs: number;
  artifactPath: string;
  failureClassification: FailureClassification | null;
  cleanupState: CleanupState;
  runId: string;
  project: string;
  error?: string;
  cleanupError?: string;
  cleanupCommand?: string;
};

type ScenarioRunOptions = { preserveFailure?: boolean };

export async function runScenario(
  config: RunConfig,
  scenario: ConcreteScenario,
  options: ScenarioRunOptions = {},
): Promise<ScenarioResult> {
  const runId = createRunId(scenario);
  const namespace = routeSegment(runId);
  const project = `fitz-destroyer-${namespace}`.slice(0, 63).replace(/-$/, "");
  const artifacts = await Artifacts.create(config.rootDir, runId);
  const stack = new ComposeStack(config, project, namespace, artifacts);
  const shape: WorkloadShape = {
    namespace,
    seed: config.seed,
    resources: config.resources,
    entriesPerResource: config.entriesPerResource,
    payloadBytes: config.payloadBytes,
  };
  const startedAt = performance.now();
  let verdict: ScenarioResult["verdict"] = "failed";
  let failureClassification: FailureClassification | null = null;
  let failureMessage: string | undefined;
  let cleanupError: string | undefined;
  let cleanupState: CleanupState = "preserved";
  let phase: "setup" | "workload" = "setup";

  await artifacts.event("scenario_started", {
    scenario,
    runId,
    project,
    config: {
      scale: config.scale,
      ...shape,
      durableEntries: totalDurableEntries(shape),
      clientReplicas: config.clientReplicas,
      phaseMs: config.phaseMs,
      liveConcurrency: config.liveConcurrency,
      handlerDelayMs: config.handlerDelayMs,
      scheduleLeadMs: config.scheduleLeadMs,
      bombardDomains: config.bombardDomains,
      rpcStreamCalls: config.rpcStreamCalls,
      rpcStreamFrames: config.rpcStreamFrames,
      rpcStreamFrameBytes: config.rpcStreamFrameBytes,
      rpcStreamReaderDelayMs: config.rpcStreamReaderDelayMs,
      clientProfile: config.clientProfile,
      durationMs: config.durationMs,
      sampleMs: config.sampleMs,
      iterations: config.iterations,
      port: config.port,
      ...(config.fitzImage === undefined
        ? { fitzSourceDir: config.fitzSourceDir }
        : { fitzImage: config.fitzImage }),
    },
  });

  try {
    await stack.preflight();
    await stack.reset();
    await stack.build();
    await stack.startCore();
    phase = "workload";

    if (scenario === "clean-restart") {
      await stack.runRecoveryJob("load", shape);
      await stack.gracefulRestartFitz();
      await stack.runRecoveryJob("verify", shape);
      await stack.gracefulRestartFitz();
      await stack.runRecoveryJob("verify", shape);
      await stack.stopFitz();
    } else if (scenario === "cache-loss") {
      await stack.runRecoveryJob("load", shape);
      await stack.discardFitzCacheAndRestart();
      await stack.runRecoveryJob("verify", shape);
      await stack.gracefulRestartFitz();
      await stack.runRecoveryJob("verify", shape);
      await stack.stopFitz();
    } else if (scenario === "durability-crash-cuts") {
      await runDurabilityCrashCutsScenario(stack, config, shape, artifacts);
      await stack.stopFitz();
    } else if (scenario === "notice-fanout") {
      await stack.runNoticeFanout(shape);
      await stack.stopFitz();
    } else if (scenario === "queue-redelivery") {
      await runQueueRedeliveryScenario(stack, config, shape, artifacts);
      await stack.stopFitz();
    } else if (scenario === "lease-contention") {
      await runLeaseContentionScenario(stack, config, shape, artifacts);
      await stack.stopFitz();
    } else if (scenario === "hot-route-canary") {
      await runHotRouteCanaryScenario(stack, config, shape, artifacts);
      await stack.stopFitz();
    } else if (scenario === "protocol-abuse") {
      await runProtocolAbuseScenario(stack, config, shape, artifacts);
      await stack.stopFitz();
    } else if (scenario === "schedule-delivery") {
      await runScheduleDelivery(stack, config, shape, artifacts);
      await stack.stopFitz();
    } else if (scenario === "session-boundaries") {
      await runSessionBoundariesScenario(stack, config, shape, artifacts);
      await stack.stopFitz();
    } else if (scenario === "rpc-pressure") {
      await stack.runRpcPressure(shape);
      await stack.stopFitz();
    } else if (scenario === "rpc-stream-hose") {
      await runRpcStreamHose(stack, config, shape, artifacts);
      await stack.stopFitz();
    } else if (scenario === "connection-storm") {
      await stack.runConnectionStorm(shape);
      await stack.stopFitz();
    } else if (scenario === "domain-pressure" || scenario === "soak") {
      await runPressureScenario(stack, config, shape, artifacts, scenario);
      await stack.stopFitz();
    } else if (scenario === "storage-faults") {
      await runStorageFaultsScenario(stack, config, shape, artifacts);
      await stack.stopFitz();
    } else if (scenario === "queue-lifecycle") {
      await runQueueLifecycleScenario(stack, config, shape, artifacts);
      await stack.stopFitz();
    } else if (scenario === "transaction-contention") {
      await runTransactionContentionScenario(stack, config, shape, artifacts);
      await stack.stopFitz();
    } else if (scenario === "stream-replay") {
      await runStreamReplayScenario(stack, config, shape, artifacts);
      await stack.stopFitz();
    } else if (scenario === "schedule-outage") {
      await runScheduleOutageScenario(stack, config, shape, artifacts);
      await stack.stopFitz();
    } else if (scenario === "live-churn") {
      await runLiveChurnScenario(stack, config, shape, artifacts);
      await stack.stopFitz();
    } else if (scenario === "chaos") {
      await runChaos(stack, config);
    } else {
      throw new Error(`Scenario ${scenario} is not implemented`);
    }
    verdict = "passed";
  } catch (error) {
    failureMessage = errorMessage(error);
    failureClassification = classifyFailure(error, phase);
    await artifacts.event("scenario_failed", {
      error: failureMessage,
      failureClassification,
    });
  } finally {
    await stack.collect().catch(async (error: unknown) => {
      const message = errorMessage(error);
      await artifacts.event("artifact_collection_failed", { error: message });
      if (verdict === "passed") {
        verdict = "failed";
        failureMessage = message;
        failureClassification = "setup";
      }
    });
    const shouldCleanup = verdict === "passed" ? !config.keep : options.preserveFailure === false;
    if (shouldCleanup) {
      try {
        await stack.cleanup();
        cleanupState = "removed";
      } catch (error) {
        cleanupError = errorMessage(error);
        cleanupState = "failed";
        await artifacts.event("cleanup_failed", { error: cleanupError, project });
        if (verdict === "passed") {
          verdict = "failed";
          failureMessage = cleanupError;
          failureClassification = "cleanup";
        }
      }
    }
    const summary: ScenarioResult = {
      scenario,
      runId,
      project,
      verdict,
      durationMs: Math.round(performance.now() - startedAt),
      artifactPath: artifacts.directory,
      failureClassification,
      cleanupState,
      ...(failureMessage === undefined ? {} : { error: failureMessage }),
      ...(cleanupError === undefined ? {} : { cleanupError }),
      ...(cleanupState === "removed" ? {} : { cleanupCommand: stack.cleanupCommand() }),
    };
    await artifacts.writeJson("summary.json", summary);
    await artifacts.event("scenario_complete", summary);
    if (cleanupState !== "removed") {
      process.stderr.write(`Stack preserved. Cleanup when finished:\n${stack.cleanupCommand()}\n`);
    }
    return summary;
  }
}

async function runChaos(stack: ComposeStack, config: RunConfig): Promise<void> {
  const replicas = config.clientReplicas;

  let since = new Date();
  await stack.startClients(replicas);
  await stack.waitForAllClientDomains(since, replicas);
  await sleep(config.phaseMs);

  await stack.killFitz();
  await stack.restartFitz();
  await stack.startClients(replicas);
  since = await freshProgressMarker();
  await stack.waitForAllClientDomains(since, replicas);
  await sleep(config.phaseMs);

  await stack.killOneClientAndRestore(replicas);
  since = await freshProgressMarker();
  await stack.waitForAllClientDomains(since, replicas);
  await sleep(config.phaseMs);

  await stack.killSqrzlAndRestore();
  await stack.replaceClients(replicas);
  since = await freshProgressMarker();
  await stack.waitForAllClientDomains(since, replicas);

  await stack.stopClients();
  await stack.stopFitz();
}

function createRunId(scenario: ConcreteScenario): string {
  const timestamp = new Date().toISOString().replace(/[-:]/g, "").replace(/\.\d{3}Z$/, "z");
  return `${timestamp}-${scenario}-${randomBytes(3).toString("hex")}`;
}

function routeSegment(value: string): string {
  return value.toLowerCase().replace(/[^a-z0-9-]/g, "-").replace(/-+/g, "-").replace(/^-|-$/g, "");
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? `${error.name}: ${error.message}` : String(error);
}

export function classifyFailure(
  error: unknown,
  phase: "setup" | "workload" = "workload",
): FailureClassification {
  if (phase === "setup") return "setup";
  const name = error instanceof Error ? error.name : "";
  const message = errorMessage(error);
  if (name === "TimeoutError" || /\b(?:timed? out|timeout|deadline)\b/iu.test(message)) {
    return "timeout";
  }
  if (
    name === "AssertionError" ||
    /\b(?:expected|unexpected|missing|mismatch|duplicate|disappeared|observed|omitted|did not|does not|quiesce|reused)\b/iu.test(
      message,
    )
  ) {
    return "assertion";
  }
  return "workload";
}

function sleep(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

async function freshProgressMarker(): Promise<Date> {
  // Drop the first one-second progress window because it can straddle the
  // readiness transition and contain operations completed before the fault.
  await sleep(1_100);
  return new Date();
}
