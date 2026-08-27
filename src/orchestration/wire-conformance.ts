import type { RunConfig } from "../config.js";
import type { WorkloadShape } from "../workloads/model.js";
import type { WireConformanceCase } from "../workloads/wire-conformance.js";
import type { Artifacts } from "./artifacts.js";
import type { ComposeStack } from "./compose.js";
import { numericField, parseJsonRecords, requiredEvent } from "./workload-log.js";

type StackRole = Parameters<ComposeStack["startRoleContainers"]>[0];

export async function runLeaseRouteAliasingScenario(
  stack: ComposeStack,
  config: RunConfig,
  shape: WorkloadShape,
  artifacts: Artifacts,
): Promise<void> {
  await runWireCase(stack, config, shape, artifacts, "lease-route-aliasing");
}

export async function runTcpPreauthFramingSlowlorisScenario(
  stack: ComposeStack,
  config: RunConfig,
  shape: WorkloadShape,
  artifacts: Artifacts,
): Promise<void> {
  await runWireCase(stack, config, shape, artifacts, "tcp-preauth-framing-slowloris");
}

export async function runConnectPipelineFamilyRebindScenario(
  stack: ComposeStack,
  config: RunConfig,
  shape: WorkloadShape,
  artifacts: Artifacts,
): Promise<void> {
  await runWireCase(stack, config, shape, artifacts, "connect-pipeline-family-rebind");
}

export async function runWireCase(
  stack: ComposeStack,
  config: RunConfig,
  shape: WorkloadShape,
  artifacts: Artifacts,
  wireCase: WireConformanceCase,
): Promise<void> {
  const startedAt = performance.now();
  await artifacts.event("wire_conformance_started", { wireCase });
  const role = await stack.startRoleContainers("wire-conformance" as StackRole, 1, shape, {
    DESTROYER_WIRE_CASE: wireCase,
    DESTROYER_WIRE_URL: "ws://fitz:4090/ws",
    DESTROYER_REQUEST_TIMEOUT_MS: String(config.requestTimeoutMs),
    DESTROYER_CLIENT_REPLICAS: String(config.clientReplicas),
  });
  const logs = await stack.finishRoleContainers(role, `wire-${wireCase}`);
  const log = logs.get("0");
  if (log === undefined) throw new Error(`wire conformance log was missing for ${wireCase}`);
  const event = requiredEvent(log, `${eventPrefix(wireCase)}_complete`);
  assertWireEvidence(wireCase, event, log);
  const evidence = Object.fromEntries(
    Object.entries(event).filter(([key]) => key !== "event"),
  );
  await artifacts.writeJson(`${wireCase}-evidence.json`, evidence);
  await artifacts.event(`${eventPrefix(wireCase)}_observed`, {
    ...evidence,
    elapsedMs: Math.round(performance.now() - startedAt),
  });
}

export function eventPrefix(wireCase: WireConformanceCase): string {
  return wireCase.replaceAll("-", "_");
}

export function assertWireEvidence(
  wireCase: WireConformanceCase,
  event: Readonly<Record<string, unknown>>,
  log: string,
): void {
  if (wireCase === "lease-route-aliasing") {
    if (numericField(event, "operations") !== 6 || numericField(event, "rejected") !== 6) {
      throw new Error("Lease alias evidence did not reject all six operations");
    }
    if (numericField(event, "canonicalPreserved") !== 6) {
      throw new Error("Lease alias evidence did not preserve all canonical leases");
    }
    return;
  }
  if (wireCase === "tcp-preauth-framing-slowloris") {
    if (numericField(event, "socketsOpened") !== numericField(event, "socketsClosed")) {
      throw new Error("pre-auth evidence did not close every held TCP socket");
    }
    if (numericField(event, "tcpCanary") !== 1 || numericField(event, "websocketCanary") !== 1) {
      throw new Error("pre-auth evidence is missing a transport canary");
    }
    return;
  }
  if (numericField(event, "transports") !== 2) throw new Error("pipeline did not exercise WS and TCP");
  if (numericField(event, "accepted") + numericField(event, "rejected") !== 2) {
    throw new Error("pipeline evidence has an incomplete outcome count");
  }
  const cases = parseJsonRecords(log).filter((record) => record.event === "connect_pipeline_family_rebind_case");
  if (cases.length !== 2) throw new Error(`pipeline emitted ${cases.length}/2 transport cases`);
}
