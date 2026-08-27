import type { RunConfig } from "../config.js";
import { ALL_DOMAINS, type Domain, type WorkloadShape } from "../workloads/model.js";
import { RELIABILITY_RPC_PROBE_CALLS } from "../workloads/reliability-session-state.js";
import type { Artifacts } from "./artifacts.js";
import type { LiveDomain } from "./live-observability.js";
import {
  numericField,
  parseJsonRecords,
  recordField,
  requiredEvent,
} from "./workload-log.js";
import {
  nonNegativeInteger,
  waitForRegistrationCleanup,
  type ReliabilityComposeStack,
} from "./reliability-cleanup-helpers.js";

const LIVE_DOMAINS: readonly LiveDomain[] = [
  "kv",
  "stream",
  "notice",
  "rpc",
  "lease",
  "schedule",
];

export type ControlLaneCleanupLedger = {
  targets: number;
  cleanupCompletedWhileSaturated: boolean;
  cleanupPending: number;
  noticeSubscriptionsDelta: number;
  rpcWorkersDelta: number;
  scheduleSubscriptionsDelta: number;
  queueRedelivered: number;
  kvUncommittedValues: number;
  streamUncommittedRecords: number;
  leaseHeld: number;
  leasePendingWaiters: number;
  rpcProbeCalls: number;
  rpcProbeFailures: number;
  saturationProgress: Record<Domain, number>;
  canaryDomains: readonly Domain[];
  canaryOperationsPerDomain: number;
};

type ControlLaneRole = "control-lane-cleanup-under-saturation" | "canary";

export async function runControlLaneCleanupUnderSaturationScenario(
  stack: ReliabilityComposeStack<ControlLaneRole>,
  config: RunConfig,
  shape: WorkloadShape,
  artifacts: Artifacts,
): Promise<void> {
  const startedAt = performance.now();
  const targets = config.clientReplicas;
  const initialSnapshots = new Map(
    await Promise.all(
      LIVE_DOMAINS.map(async (domain) => [domain, await stack.liveDomainSnapshot(domain)] as const),
    ),
  );
  const saturationShape = {
    ...shape,
    namespace: `${shape.namespace}-control-saturation`,
    entriesPerResource: 1,
  };
  const targetShape = {
    ...shape,
    namespace: `${shape.namespace}-control-target`,
    entriesPerResource: 1,
  };
  const saturators = await stack.startRoleContainers(
    "control-lane-cleanup-under-saturation",
    config.clientReplicas,
    saturationShape,
    {
      DESTROYER_RELIABILITY_ACTION: "saturate",
      DESTROYER_PROGRESS_INTERVAL_MS: String(Math.min(config.sampleMs, 250)),
      DESTROYER_RECONNECT_TIMEOUT_MS: String(config.startupTimeoutMs),
    },
  );
  let saturatorsStopped = false;
  await artifacts.event("control_lane_cleanup_under_saturation_started", {
    saturators: config.clientReplicas,
    targets,
    domains: ALL_DOMAINS,
  });

  try {
    await stack.waitForRoleEvent(saturators, "control_lane_saturator_ready");
    await stack.waitForRoleEvent(saturators, "control_lane_saturation_progress");
    const progressBefore = await stack.roleLogs(saturators);
    const registrationBaseline = await currentRegistrationState(stack);

    const holders = await stack.startRoleContainers(
      "control-lane-cleanup-under-saturation",
      targets,
      targetShape,
      {
        DESTROYER_RELIABILITY_ACTION: "hold",
        DESTROYER_RECONNECT_TIMEOUT_MS: String(config.startupTimeoutMs),
      },
    );
    await stack.waitForRoleEvent(holders, "control_lane_cleanup_target_armed");
    await stack.killRoleContainers(holders, "control-lane-cleanup-targets");

    const cleanupState = await waitForRegistrationCleanup(
      stack,
      registrationBaseline,
      Math.min(config.startupTimeoutMs, 30_000),
    );

    const probes = await stack.startRoleContainers(
      "control-lane-cleanup-under-saturation",
      targets,
      targetShape,
      {
        DESTROYER_RELIABILITY_ACTION: "probe",
        DESTROYER_RECONNECT_TIMEOUT_MS: String(config.startupTimeoutMs),
      },
    );
    const probeLogs = await stack.finishRoleContainers(probes, "control-lane-cleanup-probes");

    const canaryShape = {
      ...shape,
      namespace: `${shape.namespace}-control-canary`,
      entriesPerResource: 1,
    };
    const canary = await stack.startRoleContainers("canary", 1, canaryShape);
    const canaryLogs = await stack.finishRoleContainers(canary, "control-lane-sibling-canary");
    const canaryComplete = requiredEvent(onlyLog(canaryLogs), "canary_complete");

    const progressAfter = await stack.roleLogs(saturators);
    const saturationProgress = saturationProgressDelta(progressBefore, progressAfter);

    await stack.signalRoleContainers(saturators, "SIGTERM");
    await stack.finishRoleContainers(saturators, "control-lane-saturators");
    saturatorsStopped = true;

    await stack.waitForPressureQuiescence();
    for (const domain of LIVE_DOMAINS) {
      const baseline = initialSnapshots.get(domain);
      if (baseline === undefined) throw new Error(`Missing initial ${domain} snapshot`);
      await stack.waitForLiveDomainQuiescence(
        domain,
        baseline,
        `control-lane-cleanup-${domain}`,
        true,
      );
    }

    const probeEvidence = aggregateProbeEvidence(probeLogs);
    const ledger: ControlLaneCleanupLedger = {
      targets,
      cleanupCompletedWhileSaturated: true,
      cleanupPending: cleanupState.cleanupPending,
      noticeSubscriptionsDelta:
        cleanupState.noticeSubscriptions - registrationBaseline.noticeSubscriptions,
      rpcWorkersDelta: cleanupState.rpcWorkers - registrationBaseline.rpcWorkers,
      scheduleSubscriptionsDelta:
        cleanupState.scheduleSubscriptions - registrationBaseline.scheduleSubscriptions,
      ...probeEvidence,
      saturationProgress,
      canaryDomains: domainArray(canaryComplete.domains),
      canaryOperationsPerDomain: numericField(canaryComplete, "operationsPerDomain"),
    };
    assertControlLaneCleanupUnderSaturation(ledger);
    await artifacts.writeJson("control-lane-cleanup-under-saturation-ledger.json", ledger);
    await artifacts.event("control_lane_cleanup_under_saturation_complete", {
      targets,
      saturationProgress,
      canaryOperationsPerDomain: ledger.canaryOperationsPerDomain,
      elapsedMs: Math.round(performance.now() - startedAt),
    });
  } finally {
    if (!saturatorsStopped) {
      await stack.signalRoleContainers(saturators, "SIGTERM").catch(() => undefined);
      await stack
        .finishRoleContainers(saturators, "control-lane-saturators-after-failure")
        .catch(() => undefined);
    }
  }
}

export function assertControlLaneCleanupUnderSaturation(
  ledger: ControlLaneCleanupLedger,
): void {
  if (!ledger.cleanupCompletedWhileSaturated) {
    throw new Error("Session cleanup did not complete while normal-lane saturation was active");
  }
  assertField(ledger, "cleanupPending", 0);
  assertField(ledger, "noticeSubscriptionsDelta", 0);
  assertField(ledger, "rpcWorkersDelta", 0);
  assertField(ledger, "scheduleSubscriptionsDelta", 0);
  assertField(ledger, "queueRedelivered", ledger.targets);
  assertField(ledger, "kvUncommittedValues", 0);
  assertField(ledger, "streamUncommittedRecords", 0);
  assertField(ledger, "leaseHeld", 0);
  assertField(ledger, "leasePendingWaiters", 0);
  assertField(ledger, "rpcProbeCalls", ledger.targets * RELIABILITY_RPC_PROBE_CALLS);
  assertField(ledger, "rpcProbeFailures", 0);
  for (const domain of ALL_DOMAINS) {
    if (ledger.saturationProgress[domain] <= 0) {
      throw new Error(`Normal ${domain} lane made no progress across the cleanup cut`);
    }
    if (!ledger.canaryDomains.includes(domain)) {
      throw new Error(`Sibling canary omitted ${domain}`);
    }
  }
  if (ledger.canaryOperationsPerDomain <= 0) {
    throw new Error("Sibling canary made no domain progress");
  }
}

export function saturationProgressDelta(
  before: ReadonlyMap<string, string>,
  after: ReadonlyMap<string, string>,
): Record<Domain, number> {
  const beforeTotals = aggregateLatestSaturationTotals(before);
  const afterTotals = aggregateLatestSaturationTotals(after);
  return Object.fromEntries(
    ALL_DOMAINS.map((domain) => {
      const delta = afterTotals[domain] - beforeTotals[domain];
      if (delta < 0) throw new Error(`${domain} saturation progress decreased`);
      return [domain, delta];
    }),
  ) as Record<Domain, number>;
}

function aggregateLatestSaturationTotals(
  logs: ReadonlyMap<string, string>,
): Record<Domain, number> {
  const totals = Object.fromEntries(ALL_DOMAINS.map((domain) => [domain, 0])) as Record<
    Domain,
    number
  >;
  for (const log of logs.values()) {
    const progress = parseJsonRecords(log)
      .filter((record) => record.event === "control_lane_saturation_progress")
      .at(-1);
    if (progress === undefined) throw new Error("Saturator omitted progress evidence");
    const domainTotals = recordField(progress, "totals");
    for (const domain of ALL_DOMAINS) {
      totals[domain] += numericField(recordField(domainTotals, domain), "success");
    }
  }
  return totals;
}

function aggregateProbeEvidence(
  logs: ReadonlyMap<string, string>,
): Pick<
  ControlLaneCleanupLedger,
  | "queueRedelivered"
  | "kvUncommittedValues"
  | "streamUncommittedRecords"
  | "leaseHeld"
  | "leasePendingWaiters"
  | "rpcProbeCalls"
  | "rpcProbeFailures"
> {
  const totals = {
    queueRedelivered: 0,
    kvUncommittedValues: 0,
    streamUncommittedRecords: 0,
    leaseHeld: 0,
    leasePendingWaiters: 0,
    rpcProbeCalls: 0,
    rpcProbeFailures: 0,
  };
  for (const log of logs.values()) {
    const complete = requiredEvent(log, "control_lane_cleanup_probe_complete");
    for (const field of Object.keys(totals) as Array<keyof typeof totals>) {
      totals[field] += numericField(complete, field);
    }
  }
  return totals;
}

async function currentRegistrationState(
  stack: Pick<ReliabilityComposeStack<string>, "liveDomainSnapshot">,
): Promise<{
  noticeSubscriptions: number;
  rpcWorkers: number;
  scheduleSubscriptions: number;
}> {
  const [notice, rpc, schedule] = await Promise.all([
    stack.liveDomainSnapshot("notice"),
    stack.liveDomainSnapshot("rpc"),
    stack.liveDomainSnapshot("schedule"),
  ]);
  return {
    noticeSubscriptions: nonNegativeInteger(notice.domain, "subscriptions_active"),
    rpcWorkers: nonNegativeInteger(rpc.domain, "workers_registered"),
    scheduleSubscriptions: nonNegativeInteger(schedule.domain, "subscriptions_active"),
  };
}

function domainArray(value: unknown): Domain[] {
  if (!Array.isArray(value)) throw new Error("Canary domains are not an array");
  return value.map((domain) => {
    if (typeof domain !== "string" || !(ALL_DOMAINS as readonly string[]).includes(domain)) {
      throw new Error(`Canary reported invalid domain ${String(domain)}`);
    }
    return domain as Domain;
  });
}

function onlyLog(logs: ReadonlyMap<string, string>): string {
  const log = [...logs.values()][0];
  if (logs.size !== 1 || log === undefined) throw new Error(`Expected one canary log, found ${logs.size}`);
  return log;
}

function assertField(
  ledger: ControlLaneCleanupLedger,
  field: keyof ControlLaneCleanupLedger,
  expected: number,
): void {
  const actual = ledger[field];
  if (actual !== expected) {
    throw new Error(`Control-lane cleanup ${field}=${String(actual)}/${expected}`);
  }
}
