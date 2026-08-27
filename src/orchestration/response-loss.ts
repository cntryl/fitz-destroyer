import type { RunConfig } from "../config.js";
import { DURABLE_DOMAINS, type WorkloadShape } from "../workloads/model.js";
import type { Artifacts } from "./artifacts.js";
import type { ComposeStack } from "./compose.js";
import {
  analyzeDurabilityLedger,
  assertDurabilityLedger,
} from "./durability-crash-cuts.js";

export async function runResponseLossScenario(
  stack: ComposeStack,
  _config: RunConfig,
  shape: WorkloadShape,
  artifacts: Artifacts,
): Promise<void> {
  const startedAt = performance.now();
  const environment = {
    DESTROYER_SEED: String(shape.seed),
    DESTROYER_DURABILITY_ITERATIONS: "1",
  };
  await artifacts.event("response_loss_started", {
    domains: DURABLE_DOMAINS,
    fault: "broker-to-client-response-drop",
  });

  const baseline = await stack.startRoleContainers("durability-writer", 1, shape, {
    ...environment,
    DESTROYER_DURABILITY_ACTION: "baseline",
    DESTROYER_DURABILITY_SEQUENCE: "0",
  });
  const baselineLogs = await stack.finishRoleContainers(baseline, "response-loss-baseline");

  const cut = await stack.startRoleContainers("durability-writer", 1, shape, {
    ...environment,
    FITZ_URL: "ws://client-proxy:4090/ws",
    DESTROYER_DURABILITY_ACTION: "cut",
    DESTROYER_DURABILITY_SEQUENCE: "1",
    DESTROYER_REQUEST_TIMEOUT_MS: "1000",
    DESTROYER_WAIT_FOR_START_SIGNAL: "true",
  });
  let cutLogs: ReadonlyMap<string, string>;
  await stack.waitForRoleEvent(cut, "live_producer_ready");
  try {
    await stack.setFaultProxy("client-proxy", { mode: "downstream-drop" });
    await stack.signalRoleContainers(cut, "SIGUSR1");
    await stack.waitForRoleEvent(cut, "durability_operations_dispatched");
    cutLogs = await stack.finishRoleContainers(cut, "response-loss-cut");
  } finally {
    await stack.setFaultProxy("client-proxy", { mode: "healthy" }).catch(() => undefined);
  }

  const verifier = await stack.startRoleContainers("durability-verifier", 1, shape, {
    ...environment,
    DESTROYER_DURABILITY_SEQUENCE: "0",
  });
  const verifyLogs = await stack.finishRoleContainers(verifier, "response-loss-verify");
  const ledger = analyzeDurabilityLedger(baselineLogs, cutLogs, verifyLogs);
  assertDurabilityLedger(ledger, 1);
  const acknowledgedAfterDrop = DURABLE_DOMAINS.reduce(
    (total, domain) => total + Number(ledger[domain].acknowledged.includes(1)),
    0,
  );
  const observedAfterDrop = DURABLE_DOMAINS.reduce(
    (total, domain) => total + Number(ledger[domain].observed.includes(1)),
    0,
  );
  if (acknowledgedAfterDrop !== 0) {
    throw new Error(`${acknowledgedAfterDrop} operations received replies during response loss`);
  }
  if (observedAfterDrop === 0) {
    throw new Error("Response loss did not produce an ambiguous durable outcome to reconcile");
  }
  await artifacts.writeJson("response-loss-ledger.json", ledger);
  await artifacts.event("response_loss_complete", {
    attempted: DURABLE_DOMAINS.length,
    acknowledgedAfterDrop,
    observedAfterDrop,
    elapsedMs: Math.round(performance.now() - startedAt),
  });
}
