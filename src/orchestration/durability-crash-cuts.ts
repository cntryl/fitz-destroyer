import type { RunConfig } from "../config.js";
import { DURABLE_DOMAINS, type DurableDomain, type WorkloadShape } from "../workloads/model.js";
import type { Artifacts } from "./artifacts.js";
import type { ComposeStack } from "./compose.js";
import { numericValue, parseJsonRecords, recordField, requiredEvent } from "./workload-log.js";

type DomainLedger = Record<
  DurableDomain,
  { started: number[]; acknowledged: number[]; failed: number[]; observed: number[] }
>;

export async function runDurabilityCrashCutsScenario(
  stack: ComposeStack,
  config: RunConfig,
  shape: WorkloadShape,
  artifacts: Artifacts,
): Promise<void> {
  const startedAt = performance.now();
  const runLabel = "durability-crash-cuts";
  const environment = { DESTROYER_SEED: String(shape.seed) };
  await artifacts.event("durability_crash_cuts_started", {
    domains: DURABLE_DOMAINS,
    cut: "provider request blocked by paused Sqrzl, then Fitz SIGKILL",
  });

  const baseline = await stack.startRoleContainers("durability-writer", 1, shape, {
    ...environment,
    DESTROYER_DURABILITY_ACTION: "baseline",
  });
  const baselineLogs = await stack.finishRoleContainers(baseline, `${runLabel}-baseline`);

  let sqrzlPaused = false;
  let cutLogs: ReadonlyMap<string, string>;
  try {
    await stack.pauseSqrzl();
    sqrzlPaused = true;
    const cut = await stack.startRoleContainers("durability-writer", 1, shape, {
      ...environment,
      DESTROYER_DURABILITY_ACTION: "cut",
      DESTROYER_WAIT_FOR_START_SIGNAL: "true",
    });
    await stack.waitForRoleEvent(cut, "live_producer_ready");
    await stack.signalRoleContainers(cut, "SIGUSR1");
    await stack.waitForRoleEvent(cut, "durability_operations_dispatched");
    await stack.killFitz();
    await stack.unpauseSqrzl();
    sqrzlPaused = false;
    await stack.restartFitz();
    cutLogs = await stack.finishRoleContainers(cut, `${runLabel}-cut`);
  } finally {
    if (sqrzlPaused) await stack.unpauseSqrzl().catch(() => undefined);
  }

  const verifier = await stack.startRoleContainers("durability-verifier", 1, shape, environment);
  const verifyLogs = await stack.finishRoleContainers(verifier, `${runLabel}-verify`);
  const ledger = analyzeDurabilityLedger(baselineLogs, cutLogs, verifyLogs);
  await artifacts.writeJson(`${runLabel}-ledger.json`, ledger);
  assertDurabilityLedger(ledger);
  await artifacts.event("durability_crash_cuts_complete", {
    ledger,
    elapsedMs: Math.round(performance.now() - startedAt),
  });
}

export function analyzeDurabilityLedger(
  baselineLogs: ReadonlyMap<string, string>,
  cutLogs: ReadonlyMap<string, string>,
  verifyLogs: ReadonlyMap<string, string>,
): DomainLedger {
  const ledger = emptyLedger();
  for (const log of [...baselineLogs.values(), ...cutLogs.values()]) {
    for (const record of parseJsonRecords(log)) {
      const domain = durableDomain(record.domain);
      if (domain === undefined) continue;
      const sequence = numericValue(record.sequence, "durability sequence");
      if (record.event === "durability_operation_started") ledger[domain].started.push(sequence);
      if (record.event === "durability_operation_acknowledged") {
        ledger[domain].acknowledged.push(sequence);
      }
      if (record.event === "durability_operation_failed") ledger[domain].failed.push(sequence);
    }
  }
  const verifyLog = [...verifyLogs.values()][0];
  if (verifyLogs.size !== 1 || verifyLog === undefined) {
    throw new Error(`Expected one durability verifier log, found ${verifyLogs.size}`);
  }
  const observed = recordField(requiredEvent(verifyLog, "durability_verify_complete"), "observed");
  for (const domain of DURABLE_DOMAINS) {
    const values = observed[domain];
    if (!Array.isArray(values)) throw new Error(`Durability verifier omitted ${domain}`);
    ledger[domain].observed = values.map((value) => numericValue(value, `${domain} observed`));
    for (const field of ["started", "acknowledged", "failed", "observed"] as const) {
      ledger[domain][field] = [...new Set(ledger[domain][field])].sort((left, right) => left - right);
    }
  }
  return ledger;
}

export function assertDurabilityLedger(ledger: DomainLedger): void {
  for (const domain of DURABLE_DOMAINS) {
    const entry = ledger[domain];
    if (!entry.started.includes(0) || !entry.started.includes(1)) {
      throw new Error(`${domain} did not start both baseline and crash-cut operations`);
    }
    if (!entry.acknowledged.includes(0)) {
      throw new Error(`${domain} baseline was not acknowledged`);
    }
    for (const sequence of entry.acknowledged) {
      if (!entry.observed.includes(sequence)) {
        throw new Error(`${domain} acknowledged sequence ${sequence} disappeared after restart`);
      }
    }
    for (const sequence of entry.observed) {
      if (!entry.started.includes(sequence)) {
        throw new Error(`${domain} exposed unstarted sequence ${sequence}`);
      }
    }
  }
}

function emptyLedger(): DomainLedger {
  return {
    queue: { started: [], acknowledged: [], failed: [], observed: [] },
    kv: { started: [], acknowledged: [], failed: [], observed: [] },
    stream: { started: [], acknowledged: [], failed: [], observed: [] },
    schedule: { started: [], acknowledged: [], failed: [], observed: [] },
  };
}

function durableDomain(value: unknown): DurableDomain | undefined {
  return typeof value === "string" && (DURABLE_DOMAINS as readonly string[]).includes(value)
    ? (value as DurableDomain)
    : undefined;
}
