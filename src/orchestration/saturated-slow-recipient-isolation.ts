import type { RunConfig } from "../config.js";
import type { WorkloadShape } from "../workloads/model.js";
import { slowRecipientSaturationShape } from "../workloads/slow-recipient-isolation.js";
import type { Artifacts } from "./artifacts.js";
import type { ComposeStack } from "./compose.js";
import { numericField, requiredEvent } from "./workload-log.js";

type StackRole = Parameters<ComposeStack["startRoleContainers"]>[0];

export type SlowRecipientEvidence = {
  published: number;
  received: number;
  duplicates: number;
  invalid: number;
};

export function assertSlowRecipientEvidence(
  publisherLog: string,
  observerLog: string,
): SlowRecipientEvidence {
  const publisher = requiredEvent(publisherLog, "slow_recipient_publisher_complete");
  const observer = requiredEvent(observerLog, "slow_recipient_observer_complete");
  const evidence = {
    published: numericField(publisher, "published"),
    received: numericField(observer, "received"),
    duplicates: numericField(observer, "duplicates"),
    invalid: numericField(observer, "invalid"),
  };
  if (evidence.received !== evidence.published) {
    throw new Error(`received=${evidence.received}/${evidence.published}`);
  }
  if (evidence.duplicates !== 0 || evidence.invalid !== 0) {
    throw new Error(
      `Healthy observer saw duplicates=${evidence.duplicates}, invalid=${evidence.invalid}`,
    );
  }
  return evidence;
}

export async function runSaturatedSlowRecipientIsolationScenario(
  stack: ComposeStack,
  _config: RunConfig,
  shape: WorkloadShape,
  artifacts: Artifacts,
): Promise<void> {
  const startedAt = performance.now();
  const pressure = slowRecipientSaturationShape(shape.entriesPerResource, shape.payloadBytes);
  const pressureShape = {
    ...shape,
    entriesPerResource: pressure.operations,
    payloadBytes: pressure.payloadBytes,
  };
  const baseline = await stack.liveDomainSnapshot("notice");
  await artifacts.event("saturated_slow_recipient_isolation_started", {
    fault: "downstream-read-paused",
    operations: pressure.operations,
    payloadBytes: pressure.payloadBytes,
    attemptedBytes: pressure.operations * pressure.payloadBytes,
    completionSemantics: "healthy-recipient-delivery",
  });

  const slow = await stack.startRoleContainers("slow-recipient" as StackRole, 1, pressureShape, {
    FITZ_URL: "ws://client-proxy:4090/ws",
    DESTROYER_HEARTBEAT_ENABLED: "false",
  });
  await stack.waitForRoleEvent(slow, "slow_recipient_ready");
  const observer = await stack.startRoleContainers("slow-recipient-observer" as StackRole, 1, pressureShape);
  await stack.waitForRoleEvent(observer, "slow_recipient_observer_ready");

  let slowLogs: ReadonlyMap<string, string> | undefined;
  let evidence: SlowRecipientEvidence | undefined;
  try {
    await stack.setFaultProxy("client-proxy", { mode: "downstream-pause" });
    const publishers = await stack.startRoleContainers("slow-recipient-publisher" as StackRole, 1, pressureShape);
    const publisherLogs = await stack.finishRoleContainers(
      publishers,
      "slow-recipient-publisher",
    );
    const observerLogs = await stack.finishRoleContainers(observer, "slow-recipient-observer");
    const publisherLog = publisherLogs.get("0");
    const observerLog = observerLogs.get("0");
    if (publisherLog === undefined || observerLog === undefined) {
      throw new Error("Slow-recipient publisher or healthy observer log was missing");
    }
    evidence = assertSlowRecipientEvidence(publisherLog, observerLog);

    const canaryShape = {
      ...shape,
      namespace: `${shape.namespace}-sibling-canary`,
      entriesPerResource: 1,
      payloadBytes: Math.min(shape.payloadBytes, 1_024),
    };
    const canary = await stack.startRoleContainers("canary", 1, canaryShape);
    await stack.finishRoleContainers(canary, "slow-recipient-sibling-canary");
    slowLogs = await stack.killRoleContainers(slow, "slow-recipient-paused");
  } finally {
    await stack.setFaultProxy("client-proxy", { mode: "healthy" }).catch(() => undefined);
  }
  if (slowLogs?.get("0") === undefined) throw new Error("Paused slow-recipient log was missing");
  await stack.waitForLiveDomainQuiescence(
    "notice",
    baseline,
    "saturated-slow-recipient-isolation",
    true,
  );
  if (evidence === undefined) throw new Error("Slow-recipient evidence was not captured");
  await artifacts.event("saturated_slow_recipient_isolation_complete", {
    ...evidence,
    bytesPublished: evidence.published * pressure.payloadBytes,
    siblingCanaryDomains: 7,
    elapsedMs: Math.round(performance.now() - startedAt),
  });
}
