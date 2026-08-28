import type { RunConfig } from "../config.js";
import {
  LATENCY_BUCKET_UPPER_MS,
  diagnosticWarnings,
  latencySummary,
  mergeLatencyHistograms,
  reconcileQueueOutcomes,
  rssGrowthAssessment,
  type LatencyHistogram,
  type NormalizedErrorClass,
  type PressureBrokerSample,
  type PressureWarning,
  type QueueClientOutcome,
  type QueueReconciliation,
  type RssGrowthAssessment,
  type StageMetrics,
} from "../pressure.js";
import { ALL_DOMAINS, type Domain, type WorkloadShape } from "../workloads/model.js";
import type { Artifacts } from "./artifacts.js";
import type { ComposeStack } from "./compose.js";
import { numericValue, parseJsonRecords } from "./workload-log.js";

type EvidenceStage = Omit<StageMetrics, "latency"> & {
  latencyHistogram: LatencyHistogram;
  latency: ReturnType<typeof latencySummary>;
};

type EvidenceDomain = {
  succeeded: number;
  failed: number;
  stages: Readonly<Record<string, EvidenceStage>>;
};

export type PressureClientEvidence = {
  container: string;
  worker: string;
  domains: Partial<Record<Domain, EvidenceDomain>>;
  queueOutcome?: QueueClientOutcome;
};

export type PressureEvidence = {
  scenario: "domain-pressure" | "soak";
  startedAt: string;
  completedAt: string;
  durationMs: number;
  sampleMs: number;
  requestTimeoutMs: number;
  selectedDomains: readonly Domain[];
  clients: readonly PressureClientEvidence[];
  aggregate: Partial<Record<Domain, EvidenceDomain>>;
  queueReconciliation: QueueReconciliation | null;
  brokerSnapshots: readonly PressureBrokerSample[];
  brokerSummary: {
    observedMailboxHighWaterDepth: number;
    finalQueuePending: number;
    finalRpcPending: number;
    ingressDispatchTimeoutsDelta: number;
    routerBackpressureDelta: number;
    routerHighLaneBackpressureDelta: number;
  };
  rssGrowthAssessment: RssGrowthAssessment;
  warnings: readonly PressureWarning[];
  assertionFailures: readonly string[];
};

export async function runPressureScenario(
  stack: ComposeStack,
  config: RunConfig,
  shape: WorkloadShape,
  artifacts: Artifacts,
  scenario: "domain-pressure" | "soak",
): Promise<void> {
  const startedAt = performance.now();
  const requestedDurationMs = scenario === "soak" ? config.durationMs : config.phaseMs;
  await artifacts.event("pressure_started", {
    scenario,
    clients: config.clientReplicas,
    durationMs: requestedDurationMs,
    sampleMs: config.sampleMs,
    domains: config.bombardDomains,
  });

  const assertionFailures: string[] = [];
  const samples: PressureBrokerSample[] = [];
  let clientLogs = new Map<string, string>();
  let pressureStartedAt = new Date();
  let pressureCompletedAt = pressureStartedAt;
  let samplingFailure: unknown;
  await stack.startClients(config.clientReplicas);
  try {
    const initialProgressSince = new Date();
    await stack.waitForAllClientDomains(initialProgressSince, config.clientReplicas);
    pressureStartedAt = new Date();
    try {
      await samplePressure(
        stack,
        artifacts,
        samples,
        requestedDurationMs,
        config.sampleMs,
        scenario === "soak",
      );
    } catch (error) {
      samplingFailure = error;
    }
    pressureCompletedAt = new Date();
  } finally {
    clientLogs = await stack.stopBombardClientsAndCapture(scenario);
  }

  if (samplingFailure !== undefined) {
    assertionFailures.push(`broker sampling failed: ${errorMessage(samplingFailure)}`);
  }
  if (clientLogs.size !== config.clientReplicas) {
    assertionFailures.push(
      `expected ${config.clientReplicas} bombard client logs, captured ${clientLogs.size}`,
    );
  }

  let clients: PressureClientEvidence[] = [];
  try {
    clients = analyzePressureLogs(clientLogs, config.bombardDomains);
  } catch (error) {
    assertionFailures.push(`pressure log analysis failed: ${errorMessage(error)}`);
  }

  try {
    assertProgressWindows(
      clientLogs,
      config.bombardDomains,
      pressureStartedAt.getTime(),
      pressureCompletedAt.getTime(),
    );
  } catch (error) {
    assertionFailures.push(errorMessage(error));
  }

  const unexpectedErrors = pressureUnexpectedErrors(clients, config.bombardDomains);
  if (unexpectedErrors.length > 0) {
    assertionFailures.push(`unexpected pressure errors: ${unexpectedErrors.join(", ")}`);
  }

  let queueReconciliation: QueueReconciliation | null = null;
  if (config.bombardDomains.includes("queue") && clients.length > 0) {
    try {
      queueReconciliation = await reconcilePressureQueue(stack, shape, clients);
    } catch (error) {
      assertionFailures.push(`queue reconciliation failed: ${errorMessage(error)}`);
    }
  }

  try {
    const quiescent = await stack.waitForPressureQuiescence();
    samples.push(quiescent);
    if (scenario === "soak") {
      await artifacts.append("soak-samples.ndjson", `${JSON.stringify(quiescent)}\n`);
    }
  } catch (error) {
    assertionFailures.push(`post-run quiescence failed: ${errorMessage(error)}`);
  }

  const warningInputs = clients.flatMap((client) =>
    Object.entries(client.domains).flatMap(([domain, evidence]) =>
      Object.entries(evidence?.stages ?? {}).map(([stage, metrics]) => ({
        client: client.worker,
        domain: domain as Domain,
        stage,
        latency: metrics.latencyHistogram,
      })),
    ),
  );
  const warnings = diagnosticWarnings(
    warningInputs,
    samples,
    config.requestTimeoutMs,
    config.bombardDomains,
  );
  for (const warning of warnings) {
    await artifacts.event("pressure_diagnostic_warning", warning);
  }

  const evidence: PressureEvidence = {
    scenario,
    startedAt: pressureStartedAt.toISOString(),
    completedAt: pressureCompletedAt.toISOString(),
    durationMs: pressureCompletedAt.getTime() - pressureStartedAt.getTime(),
    sampleMs: config.sampleMs,
    requestTimeoutMs: config.requestTimeoutMs,
    selectedDomains: config.bombardDomains,
    clients,
    aggregate: aggregateClients(clients, config.bombardDomains),
    queueReconciliation,
    brokerSnapshots: samples,
    brokerSummary: summarizeBrokerSnapshots(samples),
    rssGrowthAssessment: rssGrowthAssessment(config.bombardDomains),
    warnings,
    assertionFailures,
  };
  await artifacts.writeJson("pressure-evidence.json", evidence);
  await artifacts.event("pressure_complete", {
    scenario,
    clients: clients.length,
    warnings: warnings.length,
    assertionFailures: assertionFailures.length,
    elapsedMs: Math.round(performance.now() - startedAt),
  });
  if (assertionFailures.length > 0) {
    throw new Error(`Pressure assertions failed: ${assertionFailures.join("; ")}`);
  }
}

export function analyzePressureLogs(
  logs: ReadonlyMap<string, string>,
  domains: readonly Domain[],
): PressureClientEvidence[] {
  return [...logs.entries()].map(([container, log]) => {
    const stopped = parseJsonRecords(log).filter((record) => record.event === "stopped").at(-1);
    if (stopped === undefined) throw new Error(`Bombard client ${container} omitted stopped evidence`);
    const worker = stringValue(stopped.worker, `${container} worker`);
    const totals = objectValue(stopped.totals, `${worker} totals`);
    const stages = objectValue(stopped.stages, `${worker} stages`);
    const clientDomains: Partial<Record<Domain, EvidenceDomain>> = {};
    for (const domain of domains) {
      const domainTotal = objectValue(totals[domain], `${worker} ${domain} totals`);
      const domainStages = objectValue(stages[domain], `${worker} ${domain} stages`);
      const parsedStages: Record<string, EvidenceStage> = {};
      for (const [stage, value] of Object.entries(domainStages)) {
        parsedStages[stage] = parseStage(value, `${worker} ${domain}/${stage}`);
      }
      if (Object.keys(parsedStages).length === 0) {
        throw new Error(`${worker} ${domain} omitted stage evidence`);
      }
      clientDomains[domain] = {
        succeeded: numericValue(domainTotal.success, `${worker} ${domain}.success`),
        failed: numericValue(domainTotal.error, `${worker} ${domain}.error`),
        stages: parsedStages,
      };
    }
    const queueOutcome = domains.includes("queue")
      ? parseQueueOutcome(worker, stopped.queueOutcome)
      : undefined;
    return {
      container,
      worker,
      domains: clientDomains,
      ...(queueOutcome === undefined ? {} : { queueOutcome }),
    };
  });
}

export function assertProgressWindows(
  logs: ReadonlyMap<string, string>,
  domains: readonly Domain[],
  startedAtMs: number,
  completedAtMs: number,
  windowMs = 10_000,
): void {
  const duration = Math.max(1, completedAtMs - startedAtMs);
  const windows = Math.max(1, Math.floor(duration / windowMs));
  const missing: string[] = [];
  for (const [container, log] of logs) {
    const totals = Array.from({ length: windows }, () =>
      Object.fromEntries(domains.map((domain) => [domain, 0])) as Record<Domain, number>,
    );
    for (const record of parseJsonRecords(log)) {
      if (record.event !== "progress" || typeof record.timestamp !== "string") continue;
      const timestamp = Date.parse(record.timestamp);
      if (!Number.isFinite(timestamp) || timestamp < startedAtMs || timestamp > completedAtMs) continue;
      const index = Math.min(windows - 1, Math.floor((timestamp - startedAtMs) / windowMs));
      const window = objectValue(record.window, `${container} progress window`);
      for (const domain of domains) {
        const value = objectValue(window[domain], `${container} ${domain} progress`);
        totals[index]![domain] += numericValue(value.success, `${container} ${domain}.success`);
      }
    }
    for (const [index, window] of totals.entries()) {
      for (const domain of domains) {
        if (window[domain] === 0) missing.push(`${container}/${domain}/window-${index}`);
      }
    }
  }
  if (missing.length > 0) {
    throw new Error(`Missing required pressure progress: ${missing.slice(0, 50).join(", ")}`);
  }
}

async function samplePressure(
  stack: ComposeStack,
  artifacts: Artifacts,
  samples: PressureBrokerSample[],
  durationMs: number,
  sampleMs: number,
  persistSoakSamples: boolean,
): Promise<void> {
  const deadline = Date.now() + durationMs;
  let nextSampleAt = Date.now() + sampleMs;
  while (Date.now() < deadline) {
    await sleep(Math.max(0, Math.min(nextSampleAt - Date.now(), deadline - Date.now())));
    if (Date.now() >= deadline) break;
    const sample = await stack.pressureSnapshot();
    samples.push(sample);
    if (persistSoakSamples) {
      await artifacts.append("soak-samples.ndjson", `${JSON.stringify(sample)}\n`);
    }
    nextSampleAt += sampleMs;
  }
}

export function pressureUnexpectedErrors(
  clients: readonly PressureClientEvidence[],
  domains: readonly Domain[],
): string[] {
  return clients.flatMap((client) =>
    domains.flatMap((domain) => {
      const evidence = client.domains[domain];
      // Queue enqueue/complete timeouts have an explicitly unknown durable
      // outcome. They are correctness failures only when exact reconciliation
      // fails; definite stage failures still fail the pressure run here.
      const count = domain === "queue"
        ? Object.values(evidence?.stages ?? {}).reduce(
            (total, stage) => total + stage.failed,
            0,
          )
        : evidence?.failed ?? 0;
      return count === 0 ? [] : [`${client.worker}/${domain}=${count}`];
    }),
  );
}

async function reconcilePressureQueue(
  stack: ComposeStack,
  shape: WorkloadShape,
  clients: readonly PressureClientEvidence[],
): Promise<QueueReconciliation> {
  const workers = clients.map(({ worker }) => worker);
  const containers = await stack.startRoleContainers("pressure-reconciler", 1, shape, {
    DESTROYER_PRESSURE_WORKERS: workers.join(","),
  });
  const logs = await stack.finishRoleContainers(containers, "pressure-reconciler");
  const log = [...logs.values()][0];
  if (logs.size !== 1 || log === undefined) {
    throw new Error(`Expected one pressure reconciler log, found ${logs.size}`);
  }
  const complete = parseJsonRecords(log)
    .filter((record) => record.event === "pressure_queue_reconcile_complete")
    .at(-1);
  if (complete === undefined) throw new Error("Pressure reconciler omitted completion evidence");
  const observedRecord = objectValue(complete.observed, "pressure reconciler observed");
  const observed: Record<string, number[]> = {};
  for (const worker of workers) {
    observed[worker] = numericArray(observedRecord[worker], `${worker} observed`);
  }
  const outcomes = clients.map(({ queueOutcome, worker }) => {
    if (queueOutcome === undefined) throw new Error(`${worker} omitted queue outcome evidence`);
    return queueOutcome;
  });
  return reconcileQueueOutcomes(outcomes, observed);
}

function aggregateClients(
  clients: readonly PressureClientEvidence[],
  domains: readonly Domain[],
): Partial<Record<Domain, EvidenceDomain>> {
  const result: Partial<Record<Domain, EvidenceDomain>> = {};
  for (const domain of domains) {
    const stages: Record<string, EvidenceStage> = {};
    let succeeded = 0;
    let failed = 0;
    for (const client of clients) {
      const evidence = client.domains[domain];
      if (evidence === undefined) continue;
      succeeded += evidence.succeeded;
      failed += evidence.failed;
      for (const [stage, value] of Object.entries(evidence.stages)) {
        const current = stages[stage] ?? emptyEvidenceStage();
        mergeStage(current, value);
        stages[stage] = current;
      }
    }
    for (const stage of Object.values(stages)) stage.latency = latencySummary(stage.latencyHistogram);
    result[domain] = { succeeded, failed, stages };
  }
  return result;
}

function summarizeBrokerSnapshots(samples: readonly PressureBrokerSample[]): PressureEvidence["brokerSummary"] {
  const first = samples[0];
  const final = samples.at(-1);
  return {
    observedMailboxHighWaterDepth: samples.reduce(
      (maximum, sample) => Math.max(maximum, sample.router.currentMailboxDepth),
      0,
    ),
    finalQueuePending: numberField(final?.queue.messages_pending),
    finalRpcPending: numberField(final?.rpc.requests_pending),
    ingressDispatchTimeoutsDelta: counterDelta(
      first?.router.ingressDispatchTimeoutsTotal,
      final?.router.ingressDispatchTimeoutsTotal,
    ),
    routerBackpressureDelta: counterDelta(
      first?.router.routerBackpressureTotal,
      final?.router.routerBackpressureTotal,
    ),
    routerHighLaneBackpressureDelta: counterDelta(
      first?.router.routerHighLaneBackpressureTotal,
      final?.router.routerHighLaneBackpressureTotal,
    ),
  };
}

function parseStage(value: unknown, label: string): EvidenceStage {
  const record = objectValue(value, label);
  const latencyHistogram = parseLatency(record.latency, `${label} latency`);
  const errorClassesRecord = objectValue(record.errorClasses, `${label} error classes`);
  const errorClasses: Partial<Record<NormalizedErrorClass, number>> = {};
  for (const [name, count] of Object.entries(errorClassesRecord)) {
    errorClasses[name as NormalizedErrorClass] = numericValue(count, `${label} ${name}`);
  }
  const samples = Array.isArray(record.errorSamples) ? record.errorSamples : [];
  const errorSamples = samples.map((sample, index) => {
    const item = objectValue(sample, `${label} error sample ${index}`);
    return {
      class: stringValue(item.class, `${label} error sample class`) as NormalizedErrorClass,
      error: stringValue(item.error, `${label} error sample error`),
    };
  });
  const shutdownCancellations = record.expectedShutdownCancellations === undefined
    ? { failed: 0, ambiguous: 0 }
    : objectValue(record.expectedShutdownCancellations, `${label} expected shutdown cancellations`);
  return {
    started: numericValue(record.started, `${label}.started`),
    succeeded: numericValue(record.succeeded, `${label}.succeeded`),
    failed: numericValue(record.failed, `${label}.failed`),
    ambiguous: numericValue(record.ambiguous, `${label}.ambiguous`),
    expectedShutdownCancellations: {
      failed: numericValue(shutdownCancellations.failed, `${label} shutdown cancellation failures`),
      ambiguous: numericValue(shutdownCancellations.ambiguous, `${label} ambiguous shutdown cancellations`),
    },
    latencyHistogram,
    latency: latencySummary(latencyHistogram),
    errorClasses,
    errorSamples,
  };
}

function parseLatency(value: unknown, label: string): LatencyHistogram {
  const record = objectValue(value, label);
  const buckets = numericArray(record.buckets, `${label} buckets`);
  if (buckets.length !== LATENCY_BUCKET_UPPER_MS.length) {
    throw new Error(`${label} expected ${LATENCY_BUCKET_UPPER_MS.length} buckets, found ${buckets.length}`);
  }
  return {
    count: numericValue(record.count, `${label}.count`),
    totalMs: nonNegativeNumber(record.totalMs, `${label}.totalMs`),
    maxMs: nonNegativeNumber(record.maxMs, `${label}.maxMs`),
    buckets,
    overflow: numericValue(record.overflow, `${label}.overflow`),
  };
}

function parseQueueOutcome(worker: string, value: unknown): QueueClientOutcome {
  const record = objectValue(value, `${worker} queue outcome`);
  return {
    worker,
    acknowledged: numericArray(record.acknowledged, `${worker} acknowledged`),
    ambiguousEnqueues: numericArray(record.ambiguousEnqueues, `${worker} ambiguous enqueues`),
    failedEnqueues: numericArray(record.failedEnqueues, `${worker} failed enqueues`),
    completed: numericArray(record.completed, `${worker} completed`),
    ambiguousCompletions: numericArray(
      record.ambiguousCompletions,
      `${worker} ambiguous completions`,
    ),
  };
}

function emptyEvidenceStage(): EvidenceStage {
  const latencyHistogram: LatencyHistogram = {
    count: 0,
    totalMs: 0,
    maxMs: 0,
    buckets: LATENCY_BUCKET_UPPER_MS.map(() => 0),
    overflow: 0,
  };
  return {
    started: 0,
    succeeded: 0,
    failed: 0,
    ambiguous: 0,
    expectedShutdownCancellations: { failed: 0, ambiguous: 0 },
    latencyHistogram,
    latency: latencySummary(latencyHistogram),
    errorClasses: {},
    errorSamples: [],
  };
}

function mergeStage(target: EvidenceStage, source: EvidenceStage): void {
  target.started += source.started;
  target.succeeded += source.succeeded;
  target.failed += source.failed;
  target.ambiguous += source.ambiguous;
  target.expectedShutdownCancellations.failed += source.expectedShutdownCancellations.failed;
  target.expectedShutdownCancellations.ambiguous += source.expectedShutdownCancellations.ambiguous;
  target.latencyHistogram = mergeLatencyHistograms([
    target.latencyHistogram,
    source.latencyHistogram,
  ]);
  for (const [name, count] of Object.entries(source.errorClasses)) {
    const errorClass = name as NormalizedErrorClass;
    target.errorClasses[errorClass] = (target.errorClasses[errorClass] ?? 0) + (count ?? 0);
  }
  const errorSamples = [...target.errorSamples, ...source.errorSamples].slice(0, 10);
  target.errorSamples = errorSamples;
}

function numericArray(value: unknown, label: string): number[] {
  if (!Array.isArray(value)) throw new Error(`${label} must be an array`);
  return value.map((item) => numericValue(item, label));
}

function objectValue(value: unknown, label: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error(`${label} must be an object`);
  }
  return value as Record<string, unknown>;
}

function stringValue(value: unknown, label: string): string {
  if (typeof value !== "string" || value.length === 0) throw new Error(`${label} must be a string`);
  return value;
}

function nonNegativeNumber(value: unknown, label: string): number {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
    throw new Error(`${label} must be a non-negative number`);
  }
  return value;
}

function numberField(value: unknown): number {
  return typeof value === "number" && Number.isFinite(value) ? value : 0;
}

function counterDelta(first: number | undefined, final: number | undefined): number {
  return Math.max(0, (final ?? 0) - (first ?? 0));
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? `${error.name}: ${error.message}` : String(error);
}

function sleep(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

export const PRESSURE_DOMAINS = ALL_DOMAINS;
