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
import { runQueueOverloadRecoveryScenario } from "./orchestration/queue-overload.js";
import { runResponseLossScenario } from "./orchestration/response-loss.js";
import { runActiveGracefulShutdownScenario } from "./orchestration/active-graceful-shutdown.js";
import { runHalfOpenSessionScenario } from "./orchestration/half-open-session.js";
import { runAuthorizationIsolationScenario } from "./orchestration/authorization-isolation.js";
import { runStreamGlobalRecoveryScenario } from "./orchestration/stream-global-recovery.js";
import { runQueueDeadLetterFencingScenario } from "./orchestration/queue-dead-letter-fencing.js";
import { runColdBootProviderOutageScenario } from "./orchestration/cold-boot-provider-outage.js";
import { runHostileRpcWorkerScenario } from "./orchestration/hostile-rpc-worker.js";
import { runUpgradeRecoveryScenario } from "./orchestration/upgrade-recovery.js";
import { runCrossTransportRecoveryScenario } from "./orchestration/cross-transport-recovery.js";
import { runOutboundBlackholeScenario } from "./orchestration/outbound-blackhole.js";
import { runBrokerPauseScenario } from "./orchestration/broker-pause.js";
import { runRouteCardinalityChurnScenario } from "./orchestration/route-cardinality-churn.js";
import { runCacheAndDiskExhaustionScenario } from "./orchestration/cache-and-disk-exhaustion.js";
import {
  runConnectPipelineFamilyRebindScenario,
  runLeaseRouteAliasingScenario,
  runTcpPreauthFramingSlowlorisScenario,
} from "./orchestration/wire-conformance.js";
import { runEphemeralReplyLossCleanupScenario } from "./orchestration/ephemeral-reply-loss-cleanup.js";
import { runSaturatedSlowRecipientIsolationScenario } from "./orchestration/saturated-slow-recipient-isolation.js";
import { runShutdownReconnectCleanupStormScenario } from "./orchestration/shutdown-reconnect-cleanup-storm.js";
import { runControlLaneCleanupUnderSaturationScenario } from "./orchestration/control-lane-cleanup-under-saturation.js";
import { runRouteFamilyIsolationMatrixScenario } from "./orchestration/route-family-isolation-matrix.js";
import { runSameShardFamilyFairnessScenario } from "./orchestration/same-shard-family-fairness.js";
import { runActorSupervisionFailpointScenario } from "./orchestration/actor-supervision-failpoint.js";
import { runFamilyActorPartialFailureIsolationScenario } from "./orchestration/family-actor-partial-failure-isolation.js";
import { runSameShardFamilyFailureIsolationScenario } from "./orchestration/same-shard-family-failure-isolation.js";
import { runFamilyActorExhaustionReadinessScenario } from "./orchestration/family-actor-exhaustion-readiness.js";
import { runFamilyActorDegradationObservabilityScenario } from "./orchestration/family-actor-degradation-observability.js";
import { runFamilyActorInflightConcurrentFailureScenario } from "./orchestration/family-actor-inflight-concurrent-failure.js";
import { runRpcResponseStateConformanceScenario } from "./orchestration/rpc-response-state-conformance.js";
import { runResponseEnvelopeBoundariesScenario } from "./orchestration/response-envelope-boundaries.js";
import { runLeaseWaiterDisconnectRacesScenario } from "./orchestration/lease-waiter-disconnect-races.js";
import { runWildcardRegistrationQuotaReclamationScenario } from "./orchestration/wildcard-registration-quota-reclamation.js";
import { runStreamSelectorCursorConformanceScenario } from "./orchestration/stream-selector-cursor-conformance.js";
import { runScheduleDueStormIsolationScenario } from "./orchestration/schedule-due-storm-isolation.js";
import { totalDurableEntries, type WorkloadShape } from "./workloads/model.js";

export type ConcreteScenario = Exclude<ScenarioName, "all">;

export type FailureClassification = "setup" | "workload" | "assertion" | "timeout" | "cleanup";
export type CleanupState = "removed" | "preserved" | "failed";
export type ScenarioResult = {
  scenario: ConcreteScenario;
  verdict: "passed" | "failed";
  durationMs: number;
  workloadStartedAt?: string | null;
  workloadCompletedAt?: string | null;
  workloadDurationMs?: number | null;
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
  let workloadStartedAt: string | null = null;
  let workloadCompletedAt: string | null = null;
  let workloadDurationMs: number | null = null;
  let workloadStarted = 0;

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
      ...(config.upgradeFromImage === undefined
        ? {}
        : { upgradeFromImage: config.upgradeFromImage }),
      ...(config.destroyerImage === undefined
        ? { fitzSourceDir: config.fitzSourceDir }
        : {
            fitzImage: config.fitzImage ?? "ghcr.io/cntryl/fitz:latest",
            destroyerImage: config.destroyerImage,
          }),
    },
  });

  try {
    await stack.preflight();
    await stack.reset();
    await stack.prepareImages();
    await stack.startCore();
    phase = "workload";
    workloadStartedAt = new Date().toISOString();
    workloadStarted = performance.now();
    await artifacts.event("workload_started", { scenario, startedAt: workloadStartedAt });
    await executeWorkload(scenario, stack, config, shape, artifacts);
    workloadDurationMs = Math.round(performance.now() - workloadStarted);
    workloadCompletedAt = new Date().toISOString();
    await artifacts.event("workload_complete", {
      scenario,
      outcome: "passed",
      startedAt: workloadStartedAt,
      completedAt: workloadCompletedAt,
      elapsedMs: workloadDurationMs,
    });
    await stack.stopFitz();
    verdict = "passed";
  } catch (error) {
    if (workloadStartedAt !== null && workloadCompletedAt === null) {
      workloadDurationMs = Math.round(performance.now() - workloadStarted);
      workloadCompletedAt = new Date().toISOString();
      await artifacts.event("workload_complete", {
        scenario,
        outcome: "failed",
        startedAt: workloadStartedAt,
        completedAt: workloadCompletedAt,
        elapsedMs: workloadDurationMs,
      });
    }
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
      workloadStartedAt,
      workloadCompletedAt,
      workloadDurationMs,
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

async function executeWorkload(
  scenario: ConcreteScenario,
  stack: ComposeStack,
  config: RunConfig,
  shape: WorkloadShape,
  artifacts: Artifacts,
): Promise<void> {
  if (scenario === "clean-restart") {
    await stack.runRecoveryJob("load", shape);
    await stack.gracefulRestartFitz();
    await stack.runRecoveryJob("verify", shape);
    await stack.gracefulRestartFitz();
    await stack.runRecoveryJob("verify", shape);
  } else if (scenario === "cache-loss") {
    await stack.runRecoveryJob("load", shape);
    await stack.discardFitzCacheAndRestart();
    await stack.runRecoveryJob("verify", shape);
    await stack.gracefulRestartFitz();
    await stack.runRecoveryJob("verify", shape);
  } else if (scenario === "durability-crash-cuts") {
    await runDurabilityCrashCutsScenario(stack, config, shape, artifacts);
  } else if (scenario === "queue-overload-recovery") {
    await runQueueOverloadRecoveryScenario(stack, config, shape, artifacts);
  } else if (scenario === "response-loss") {
    await runResponseLossScenario(stack, config, shape, artifacts);
  } else if (scenario === "active-graceful-shutdown") {
    await runActiveGracefulShutdownScenario(stack, config, shape, artifacts);
  } else if (scenario === "half-open-session") {
    await runHalfOpenSessionScenario(stack, config, shape, artifacts);
  } else if (scenario === "authorization-isolation") {
    await runAuthorizationIsolationScenario(stack, config, shape, artifacts);
  } else if (scenario === "stream-global-recovery") {
    await runStreamGlobalRecoveryScenario(stack, config, shape, artifacts);
  } else if (scenario === "queue-dead-letter-fencing") {
    await runQueueDeadLetterFencingScenario(stack, config, shape, artifacts);
  } else if (scenario === "cold-boot-provider-outage") {
    await runColdBootProviderOutageScenario(stack, config, shape, artifacts);
  } else if (scenario === "hostile-rpc-worker") {
    await runHostileRpcWorkerScenario(stack, config, shape, artifacts);
  } else if (scenario === "upgrade-recovery") {
    await runUpgradeRecoveryScenario(stack, config, shape, artifacts);
  } else if (scenario === "cross-transport-recovery") {
    await runCrossTransportRecoveryScenario(stack, config, shape, artifacts);
  } else if (scenario === "outbound-blackhole") {
    await runOutboundBlackholeScenario(stack, config, shape, artifacts);
  } else if (scenario === "broker-pause") {
    await runBrokerPauseScenario(stack, config, shape, artifacts);
  } else if (scenario === "route-cardinality-churn") {
    await runRouteCardinalityChurnScenario(stack, config, shape, artifacts);
  } else if (scenario === "cache-and-disk-exhaustion") {
    await runCacheAndDiskExhaustionScenario(stack, config, shape, artifacts);
  } else if (scenario === "notice-fanout") {
    await stack.runNoticeFanout(shape);
  } else if (scenario === "queue-redelivery") {
    await runQueueRedeliveryScenario(stack, config, shape, artifacts);
  } else if (scenario === "lease-contention") {
    await runLeaseContentionScenario(stack, config, shape, artifacts);
  } else if (scenario === "hot-route-canary") {
    await runHotRouteCanaryScenario(stack, config, shape, artifacts);
  } else if (scenario === "protocol-abuse") {
    await runProtocolAbuseScenario(stack, config, shape, artifacts);
  } else if (scenario === "schedule-delivery") {
    await runScheduleDelivery(stack, config, shape, artifacts);
  } else if (scenario === "session-boundaries") {
    await runSessionBoundariesScenario(stack, config, shape, artifacts);
  } else if (scenario === "rpc-pressure") {
    await stack.runRpcPressure(shape);
  } else if (scenario === "rpc-stream-hose") {
    await runRpcStreamHose(stack, config, shape, artifacts);
  } else if (scenario === "connection-storm") {
    await stack.runConnectionStorm(shape);
  } else if (scenario === "domain-pressure" || scenario === "soak") {
    await runPressureScenario(stack, config, shape, artifacts, scenario);
  } else if (scenario === "storage-faults") {
    await runStorageFaultsScenario(stack, config, shape, artifacts);
  } else if (scenario === "queue-lifecycle") {
    await runQueueLifecycleScenario(stack, config, shape, artifacts);
  } else if (scenario === "transaction-contention") {
    await runTransactionContentionScenario(stack, config, shape, artifacts);
  } else if (scenario === "stream-replay") {
    await runStreamReplayScenario(stack, config, shape, artifacts);
  } else if (scenario === "schedule-outage") {
    await runScheduleOutageScenario(stack, config, shape, artifacts);
  } else if (scenario === "live-churn") {
    await runLiveChurnScenario(stack, config, shape, artifacts);
  } else if (scenario === "chaos") {
    await runChaos(stack, config, artifacts);
  } else if (scenario === "lease-route-aliasing") {
    await runLeaseRouteAliasingScenario(stack, config, shape, artifacts);
  } else if (scenario === "tcp-preauth-framing-slowloris") {
    await runTcpPreauthFramingSlowlorisScenario(stack, config, shape, artifacts);
  } else if (scenario === "connect-pipeline-family-rebind") {
    await runConnectPipelineFamilyRebindScenario(stack, config, shape, artifacts);
  } else if (scenario === "ephemeral-reply-loss-cleanup") {
    await runEphemeralReplyLossCleanupScenario(stack, config, shape, artifacts);
  } else if (scenario === "saturated-slow-recipient-isolation") {
    await runSaturatedSlowRecipientIsolationScenario(stack, config, shape, artifacts);
  } else if (scenario === "shutdown-reconnect-cleanup-storm") {
    await runShutdownReconnectCleanupStormScenario(stack, config, shape, artifacts);
  } else if (scenario === "control-lane-cleanup-under-saturation") {
    await runControlLaneCleanupUnderSaturationScenario(stack, config, shape, artifacts);
  } else if (scenario === "route-family-isolation-matrix") {
    await runRouteFamilyIsolationMatrixScenario(stack, config, shape, artifacts);
  } else if (scenario === "rpc-response-state-conformance") {
    await runRpcResponseStateConformanceScenario(stack, config, shape, artifacts);
  } else if (scenario === "response-envelope-boundaries") {
    await runResponseEnvelopeBoundariesScenario(stack, config, shape, artifacts);
  } else if (scenario === "lease-waiter-disconnect-races") {
    await runLeaseWaiterDisconnectRacesScenario(stack, config, shape, artifacts);
  } else if (scenario === "wildcard-registration-quota-reclamation") {
    await runWildcardRegistrationQuotaReclamationScenario(stack, config, shape, artifacts);
  } else if (scenario === "stream-selector-cursor-conformance") {
    await runStreamSelectorCursorConformanceScenario(stack, config, shape, artifacts);
  } else if (scenario === "schedule-due-storm-isolation") {
    await runScheduleDueStormIsolationScenario(stack, config, shape, artifacts);
  } else if (scenario === "same-shard-family-fairness") {
    await runSameShardFamilyFairnessScenario(stack, config, shape, artifacts);
  } else if (scenario === "actor-supervision-failpoint") {
    await runActorSupervisionFailpointScenario(stack, config, shape, artifacts);
  } else if (scenario === "family-actor-partial-failure-isolation") {
    await runFamilyActorPartialFailureIsolationScenario(stack, config, shape, artifacts);
  } else if (scenario === "same-shard-family-failure-isolation") {
    await runSameShardFamilyFailureIsolationScenario(stack, config, shape, artifacts);
  } else if (scenario === "family-actor-exhaustion-readiness") {
    await runFamilyActorExhaustionReadinessScenario(stack, config, shape, artifacts);
  } else if (scenario === "family-actor-degradation-observability") {
    await runFamilyActorDegradationObservabilityScenario(stack, config, shape, artifacts);
  } else if (scenario === "family-actor-inflight-concurrent-failure") {
    await runFamilyActorInflightConcurrentFailureScenario(stack, config, shape, artifacts);
  } else {
    throw new Error(`Scenario ${scenario} is not implemented`);
  }
}

async function runChaos(stack: ComposeStack, config: RunConfig, artifacts: Artifacts): Promise<void> {
  const replicas = config.clientReplicas;
  const recoveries: Array<{ fault: string; elapsedMs: number }> = [];
  const startedAt = performance.now();

  let since = new Date();
  await stack.startClients(replicas);
  await stack.waitForAllClientDomains(since, replicas);
  await sleep(config.phaseMs);

  let faultStarted = performance.now();
  await stack.killFitz();
  await stack.restartFitz();
  await stack.startClients(replicas);
  since = await freshProgressMarker();
  await stack.waitForAllClientDomains(since, replicas);
  recoveries.push(await recordChaosRecovery(artifacts, "fitz-sigkill", faultStarted));
  await sleep(config.phaseMs);

  faultStarted = performance.now();
  await stack.killOneClientAndRestore(replicas);
  since = await freshProgressMarker();
  await stack.waitForAllClientDomains(since, replicas);
  recoveries.push(await recordChaosRecovery(artifacts, "client-sigkill", faultStarted));
  await sleep(config.phaseMs);

  faultStarted = performance.now();
  await stack.killSqrzlAndRestore();
  await stack.replaceClients(replicas);
  since = await freshProgressMarker();
  await stack.waitForAllClientDomains(since, replicas);
  recoveries.push(await recordChaosRecovery(artifacts, "sqrzl-sigkill", faultStarted));

  await stack.stopClients();
  await artifacts.event("chaos_complete", {
    recoveries,
    iterations: recoveries.length,
    elapsedMs: Math.round(performance.now() - startedAt),
  });
}

async function recordChaosRecovery(
  artifacts: Artifacts,
  fault: string,
  startedAt: number,
): Promise<{ fault: string; elapsedMs: number }> {
  const recovery = { fault, elapsedMs: Math.round(performance.now() - startedAt) };
  await artifacts.event("chaos_fault_recovery_complete", recovery);
  return recovery;
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
