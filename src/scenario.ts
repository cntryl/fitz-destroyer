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
import { totalDurableEntries, type WorkloadShape } from "./workloads/model.js";

export type ConcreteScenario = Exclude<ScenarioName, "all">;

export async function runScenario(config: RunConfig, scenario: ConcreteScenario): Promise<void> {
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
  let passed = false;

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
      reuseImages: config.reuseImages,
      port: config.port,
      fitzSourceDir: config.fitzSourceDir,
    },
  });

  try {
    await stack.preflight();
    await stack.reset();
    if (config.reuseImages) {
      await artifacts.event("build_skipped", { reason: "reuse-images" });
    } else {
      await stack.build();
    }
    await stack.startCore();

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
    } else if (scenario === "domain-pressure") {
      await stack.runDomainPressure(config.clientReplicas, config.phaseMs);
      await stack.stopFitz();
    } else {
      await runChaos(stack, config);
    }
    passed = true;
  } catch (error) {
    await artifacts.event("scenario_failed", { error: errorMessage(error) });
    throw error;
  } finally {
    await stack.collect().catch(async (error: unknown) => {
      await artifacts.event("artifact_collection_failed", { error: errorMessage(error) });
    });
    if (passed && !config.keep) {
      await stack.cleanup();
    }
    const summary = {
      scenario,
      runId,
      project,
      passed,
      kept: config.keep || !passed,
      elapsedMs: Math.round(performance.now() - startedAt),
      artifacts: artifacts.directory,
      cleanupCommand: config.keep || !passed ? stack.cleanupCommand() : undefined,
    };
    await artifacts.writeJson("summary.json", summary);
    await artifacts.event("scenario_complete", summary);
    if (!passed || config.keep) {
      process.stderr.write(`Stack preserved. Cleanup when finished:\n${stack.cleanupCommand()}\n`);
    }
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

function sleep(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

async function freshProgressMarker(): Promise<Date> {
  // Drop the first one-second progress window because it can straddle the
  // readiness transition and contain operations completed before the fault.
  await sleep(1_100);
  return new Date();
}
