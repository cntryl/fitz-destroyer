import { createDestroyerToken } from "../auth-token.js";
import type { RunConfig } from "../config.js";
import {
  assertScheduleDueStormEvidence,
  scheduleDueStormDefinitionCount,
  scheduleDueStormPermissions,
} from "../workloads/schedule-due-storm-isolation.js";
import type { WorkloadShape } from "../workloads/model.js";
import type { Artifacts } from "./artifacts.js";
import type { ComposeStack } from "./compose.js";
import { requiredEvent } from "./workload-log.js";
import { nextWholeMinute } from "./schedule-outage.js";

export async function runScheduleDueStormIsolationScenario(
  stack: ComposeStack,
  config: RunConfig,
  shape: WorkloadShape,
  artifacts: Artifacts,
): Promise<void> {
  const startedAt = performance.now();
  const definitions = scheduleDueStormDefinitionCount(config.scale, shape.entriesPerResource);
  const stormShape = { ...shape, entriesPerResource: definitions };
  const fireAtMs = nextWholeMinute(Date.now() + config.scheduleLeadMs);
  const permissions = scheduleDueStormPermissions(shape.namespace);
  const baseline = await stack.liveDomainSnapshot("schedule");
  await artifacts.event("schedule_due_storm_isolation_started", {
    definitions,
    noisyFamily: 1,
    siblingFamily: 2,
    fireAtMs,
    fireAt: new Date(fireAtMs).toISOString(),
    requestTimeoutMs: config.requestTimeoutMs,
  });

  const role = await stack.startRoleContainers("schedule-due-storm-isolation", 1, stormShape, {
    DESTROYER_JWT: createDestroyerToken("identity-a", permissions),
    DESTROYER_SCHEDULE_STORM_FIRE_AT_MS: String(fireAtMs),
    DESTROYER_SCHEDULE_STORM_OBSERVATION_MS: String(config.phaseMs),
    DESTROYER_SCHEDULE_STORM_URL: "ws://fitz:4090/ws",
  });
  await stack.waitForRoleEvent(role, "schedule_due_storm_armed");
  await sleepUntil(fireAtMs + config.phaseMs);
  const logs = await stack.finishRoleContainers(role, "schedule-due-storm-isolation");
  const log = logs.get("0");
  if (log === undefined) throw new Error("schedule due storm worker log was missing");
  const evidence = requiredEvent(log, "schedule_due_storm_worker_complete");
  assertScheduleDueStormEvidence(evidence);
  const quiescence = await stack.waitForLiveDomainQuiescence(
    "schedule",
    baseline,
    "schedule-due-storm-isolation",
  );
  await artifacts.writeJson("schedule-due-storm-isolation-evidence.json", {
    ...evidence,
    cleanup: quiescence.cleanup,
  });
  await artifacts.event("schedule_due_storm_isolation_complete", {
    ...evidence,
    cleanup: quiescence.cleanup,
    elapsedMs: Math.round(performance.now() - startedAt),
  });
}

function sleepUntil(timestampMs: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, Math.max(0, timestampMs - Date.now())));
}
