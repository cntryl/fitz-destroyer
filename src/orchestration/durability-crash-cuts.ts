import type { RunConfig } from "../config.js";
import { DURABLE_DOMAINS, type DurableDomain, type WorkloadShape } from "../workloads/model.js";
import type { Artifacts } from "./artifacts.js";
import type { ComposeStack } from "./compose.js";
import { numericValue, parseJsonRecords, recordField, requiredEvent } from "./workload-log.js";

export type DomainLedger = Record<
  DurableDomain,
  { started: number[]; acknowledged: number[]; failed: number[]; observed: number[] }
>;

export const CRASH_CUT_IDENTITIES = [
  "request-dispatch",
  "blocked-provider-access",
  "provider-recovery",
  "broker-kill",
  "acknowledgement",
  "restart-with-storage-inflight",
] as const;
export type CrashCutIdentity = (typeof CRASH_CUT_IDENTITIES)[number];

export type CrashCutIteration = {
  iteration: number;
  sequence: number;
  seed: number;
  cut: CrashCutIdentity;
};

export async function runDurabilityCrashCutsScenario(
  stack: ComposeStack,
  config: RunConfig,
  shape: WorkloadShape,
  artifacts: Artifacts,
): Promise<void> {
  const startedAt = performance.now();
  const runLabel = "durability-crash-cuts";
  const environment = {
    DESTROYER_SEED: String(shape.seed),
    DESTROYER_DURABILITY_ITERATIONS: String(config.iterations),
  };
  const iterations = crashCutPlan(config.iterations, shape.seed);
  await artifacts.event("durability_crash_cuts_started", {
    domains: DURABLE_DOMAINS,
    iterations: config.iterations,
    cuts: iterations,
  });

  const baseline = await stack.startRoleContainers("durability-writer", 1, shape, {
    ...environment,
    DESTROYER_DURABILITY_ACTION: "baseline",
    DESTROYER_DURABILITY_SEQUENCE: "0",
  });
  const baselineLogs = await stack.finishRoleContainers(baseline, `${runLabel}-baseline`);

  const cutLogs = new Map<string, string>();
  const iterationEvidence: Array<CrashCutIteration & { outcomes: ReturnType<typeof cutOutcomes> }> = [];
  for (const iteration of iterations) {
    const iterationStartedAt = performance.now();
    const logs = await runCrashCutIteration(stack, shape, environment, iteration, runLabel);
    for (const [worker, log] of logs) cutLogs.set(`${iteration.iteration}-${worker}`, log);
    const outcomes = cutOutcomes(logs, iteration.sequence);
    iterationEvidence.push({ ...iteration, outcomes });
    await artifacts.event("durability_crash_cut_iteration_complete", {
      ...iteration,
      outcomes,
      elapsedMs: Math.round(performance.now() - iterationStartedAt),
    });
  }

  const verifier = await stack.startRoleContainers("durability-verifier", 1, shape, {
    ...environment,
    DESTROYER_DURABILITY_SEQUENCE: "0",
  });
  const verifyLogs = await stack.finishRoleContainers(verifier, `${runLabel}-verify`);
  const ledger = analyzeDurabilityLedger(baselineLogs, cutLogs, verifyLogs);
  const artifact = { seed: shape.seed, iterations: iterationEvidence, domains: ledger };
  await artifacts.writeJson(`${runLabel}-ledger.json`, artifact);
  assertDurabilityLedger(ledger, config.iterations);
  await artifacts.event("durability_crash_cuts_complete", {
    seed: shape.seed,
    iterations: iterationEvidence,
    domains: ledger,
    elapsedMs: Math.round(performance.now() - startedAt),
  });
}

export function crashCutPlan(iterations: number, seed: number): CrashCutIteration[] {
  return Array.from({ length: iterations }, (_, index) => ({
    iteration: index + 1,
    sequence: index + 1,
    seed,
    cut: CRASH_CUT_IDENTITIES[(seed + index) % CRASH_CUT_IDENTITIES.length]!,
  }));
}

async function runCrashCutIteration(
  stack: ComposeStack,
  shape: WorkloadShape,
  environment: Readonly<Record<string, string>>,
  iteration: CrashCutIteration,
  runLabel: string,
): Promise<ReadonlyMap<string, string>> {
  let sqrzlPaused = false;
  try {
    if (
      iteration.cut === "blocked-provider-access" ||
      iteration.cut === "provider-recovery" ||
      iteration.cut === "restart-with-storage-inflight"
    ) {
      await stack.pauseSqrzl();
      sqrzlPaused = true;
    }
    const cut = await stack.startRoleContainers("durability-writer", 1, shape, {
      ...environment,
      DESTROYER_DURABILITY_ACTION: "cut",
      DESTROYER_DURABILITY_SEQUENCE: String(iteration.sequence),
      DESTROYER_WAIT_FOR_START_SIGNAL: "true",
    });
    await stack.waitForRoleEvent(cut, "live_producer_ready");
    await stack.signalRoleContainers(cut, "SIGUSR1");

    if (iteration.cut !== "request-dispatch") {
      await stack.waitForRoleEvent(cut, "durability_operations_dispatched");
    }
    if (iteration.cut === "provider-recovery") {
      await stack.unpauseSqrzl();
      sqrzlPaused = false;
      await sleep(25);
    } else if (iteration.cut === "acknowledgement") {
      await stack.waitForRoleEvent(cut, "durability_operation_acknowledged");
    } else if (iteration.cut === "broker-kill") {
      await sleep(10);
    }

    await stack.killFitz();
    if (sqrzlPaused) {
      await stack.unpauseSqrzl();
      sqrzlPaused = false;
    }
    await stack.restartFitz();
    return stack.finishRoleContainers(
      cut,
      `${runLabel}-iteration-${iteration.iteration.toString().padStart(3, "0")}-${iteration.cut}`,
    );
  } finally {
    if (sqrzlPaused) await stack.unpauseSqrzl().catch(() => undefined);
  }
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
    const observedDuplicate = findDuplicate(ledger[domain].observed);
    if (observedDuplicate !== undefined) {
      throw new Error(`${domain} observed sequence ${observedDuplicate} more than once after restart`);
    }
    for (const field of ["started", "acknowledged", "failed", "observed"] as const) {
      ledger[domain][field] = [...new Set(ledger[domain][field])].sort((left, right) => left - right);
    }
  }
  return ledger;
}

export function assertDurabilityLedger(ledger: DomainLedger, iterations = 1): void {
  for (const domain of DURABLE_DOMAINS) {
    const entry = ledger[domain];
    for (let sequence = 0; sequence <= iterations; sequence += 1) {
      if (!entry.started.includes(sequence)) {
        throw new Error(`${domain} did not start durability sequence ${sequence}`);
      }
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

function cutOutcomes(
  logs: ReadonlyMap<string, string>,
  sequence: number,
): Record<DurableDomain, "acknowledged" | "failed" | "ambiguous"> {
  const result = Object.fromEntries(
    DURABLE_DOMAINS.map((domain) => [domain, "ambiguous"]),
  ) as Record<DurableDomain, "acknowledged" | "failed" | "ambiguous">;
  for (const log of logs.values()) {
    for (const record of parseJsonRecords(log)) {
      const domain = durableDomain(record.domain);
      if (domain === undefined || record.sequence !== sequence) continue;
      if (record.event === "durability_operation_acknowledged") result[domain] = "acknowledged";
      if (record.event === "durability_operation_failed" && result[domain] !== "acknowledged") {
        result[domain] = "failed";
      }
    }
  }
  return result;
}

function sleep(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function emptyLedger(): DomainLedger {
  return {
    queue: { started: [], acknowledged: [], failed: [], observed: [] },
    kv: { started: [], acknowledged: [], failed: [], observed: [] },
    stream: { started: [], acknowledged: [], failed: [], observed: [] },
    schedule: { started: [], acknowledged: [], failed: [], observed: [] },
  };
}

function findDuplicate(values: readonly number[]): number | undefined {
  const seen = new Set<number>();
  for (const value of values) {
    if (seen.has(value)) return value;
    seen.add(value);
  }
  return undefined;
}

function durableDomain(value: unknown): DurableDomain | undefined {
  return typeof value === "string" && (DURABLE_DOMAINS as readonly string[]).includes(value)
    ? (value as DurableDomain)
    : undefined;
}
