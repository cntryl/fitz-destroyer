import type { WorkloadShape } from "../workloads/model.js";
import type { PressureBrokerSample } from "../pressure.js";
import type { LiveDomain, LiveDomainSnapshot } from "./live-observability.js";
import type { RoleContainer } from "./compose-model.js";

export type ReadinessObservation = {
  readinessDropped: boolean;
  finalReadinessStatus: number;
  samples: readonly number[];
};

export interface ReliabilityComposeStack<Role extends string> {
  startRoleContainers(
    role: Role,
    replicas: number,
    shape: WorkloadShape,
    extraEnv?: Readonly<Record<string, string>>,
  ): Promise<RoleContainer[]>;
  waitForRoleEvent(containers: readonly RoleContainer[], event: string): Promise<void>;
  roleLogs(containers: readonly RoleContainer[]): Promise<Map<string, string>>;
  finishRoleContainers(
    containers: readonly RoleContainer[],
    label: string,
  ): Promise<Map<string, string>>;
  signalRoleContainers(
    containers: readonly RoleContainer[],
    signal: "SIGTERM" | "SIGKILL" | "SIGUSR1",
  ): Promise<void>;
  killRoleContainers(
    containers: readonly RoleContainer[],
    label: string,
  ): Promise<Map<string, string>>;
  gracefulRestartFitz(): Promise<void>;
  liveDomainSnapshot(domain: LiveDomain): Promise<LiveDomainSnapshot>;
  waitForLiveDomainQuiescence(
    domain: LiveDomain,
    baseline: LiveDomainSnapshot,
    runLabel: string,
    allowDomainFailures?: boolean,
  ): Promise<LiveDomainSnapshot>;
  waitForPressureQuiescence(): Promise<PressureBrokerSample>;
}

export async function observeReadinessDuring(
  port: number,
  action: () => Promise<void>,
  sampleMs = 25,
): Promise<ReadinessObservation> {
  const samples = [await readinessStatus(port)];
  let settled = false;
  const poller = (async () => {
    while (!settled) {
      await sleep(sampleMs);
      samples.push(await readinessStatus(port));
    }
  })();

  let failure: unknown;
  try {
    await action();
  } catch (error) {
    failure = error;
  } finally {
    settled = true;
    await poller;
  }
  if (failure !== undefined) throw failure;

  const finalReadinessStatus = await readinessStatus(port);
  samples.push(finalReadinessStatus);
  return {
    readinessDropped: samples.some((status) => status !== 200),
    finalReadinessStatus,
    samples,
  };
}

export async function waitForRegistrationCleanup(
  stack: Pick<ReliabilityComposeStack<string>, "liveDomainSnapshot">,
  expected: {
    noticeSubscriptions: number;
    rpcWorkers: number;
    scheduleSubscriptions: number;
  },
  timeoutMs: number,
): Promise<{
  cleanupPending: number;
  noticeSubscriptions: number;
  rpcWorkers: number;
  scheduleSubscriptions: number;
}> {
  const deadline = Date.now() + timeoutMs;
  let state = await registrationState(stack);
  while (Date.now() < deadline && !registrationStateMatches(state, expected)) {
    await sleep(25);
    state = await registrationState(stack);
  }
  if (!registrationStateMatches(state, expected)) {
    throw new Error(`Session cleanup remained visible during saturation: ${JSON.stringify(state)}`);
  }
  return state;
}

export function nonNegativeInteger(
  record: Readonly<Record<string, unknown>>,
  field: string,
): number {
  const value = record[field];
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
    throw new Error(`${field} is not a non-negative integer`);
  }
  return value;
}

async function registrationState(
  stack: Pick<ReliabilityComposeStack<string>, "liveDomainSnapshot">,
): Promise<{
  cleanupPending: number;
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
    cleanupPending: Math.max(
      notice.cleanup.pending,
      rpc.cleanup.pending,
      schedule.cleanup.pending,
    ),
    noticeSubscriptions: nonNegativeInteger(notice.domain, "subscriptions_active"),
    rpcWorkers: nonNegativeInteger(rpc.domain, "workers_registered"),
    scheduleSubscriptions: nonNegativeInteger(schedule.domain, "subscriptions_active"),
  };
}

function registrationStateMatches(
  actual: {
    cleanupPending: number;
    noticeSubscriptions: number;
    rpcWorkers: number;
    scheduleSubscriptions: number;
  },
  expected: {
    noticeSubscriptions: number;
    rpcWorkers: number;
    scheduleSubscriptions: number;
  },
): boolean {
  return (
    actual.cleanupPending === 0 &&
    actual.noticeSubscriptions === expected.noticeSubscriptions &&
    actual.rpcWorkers === expected.rpcWorkers &&
    actual.scheduleSubscriptions === expected.scheduleSubscriptions
  );
}

async function readinessStatus(port: number): Promise<number> {
  try {
    const response = await fetch(`http://127.0.0.1:${port}/readyz`, {
      signal: AbortSignal.timeout(1_000),
    });
    return response.status;
  } catch {
    return 0;
  }
}

function sleep(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}
