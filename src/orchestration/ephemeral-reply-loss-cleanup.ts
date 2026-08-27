import type { RunConfig } from "../config.js";
import type { WorkloadShape } from "../workloads/model.js";
import type { Artifacts } from "./artifacts.js";
import type { ComposeStack } from "./compose.js";
import { fetchJsonObject, sleep } from "./compose-evidence.js";
import type { LiveDomainSnapshot } from "./live-observability.js";
import { numericField, requiredEvent } from "./workload-log.js";

export type EphemeralReplyLossEvidence = {
  queueRedelivered: number;
  queueWatchDeliveries: number;
  kvTransactions: number;
  kvWatchDeliveries: number;
  streamSessions: number;
  streamWatchDeliveries: number;
  noticeDeliveries: number;
  scheduleSubscriptions: number;
  leaseRoutesReacquired: number;
  leaseWatchDeliveries: number;
  rpcCallsCompleted: number;
};

export type EphemeralCleanupDomain =
  | "queue"
  | "kv"
  | "stream"
  | "notice"
  | "rpc"
  | "lease"
  | "schedule";

export type EphemeralArmedState = Readonly<{
  queue: Readonly<Record<string, unknown>>;
  kv: Readonly<Record<string, unknown>>;
  stream: Readonly<Record<string, unknown>>;
  notice: Readonly<Record<string, unknown>>;
  rpc: Readonly<Record<string, unknown>>;
  lease: Readonly<Record<string, unknown>>;
  schedule: Readonly<Record<string, unknown>>;
  queueResource: Readonly<Record<string, unknown>>;
}>;

const CLEANUP_DOMAINS: readonly EphemeralCleanupDomain[] = [
  "queue",
  "kv",
  "stream",
  "notice",
  "rpc",
  "lease",
  "schedule",
];
type StackRole = Parameters<ComposeStack["startRoleContainers"]>[0];

export function assertEphemeralReplyLossEvidence(log: string): EphemeralReplyLossEvidence {
  const complete = requiredEvent(log, "ephemeral_reply_loss_verifier_complete");
  const expected: EphemeralReplyLossEvidence = {
    queueRedelivered: 1,
    queueWatchDeliveries: 1,
    kvTransactions: 1,
    kvWatchDeliveries: 1,
    streamSessions: 1,
    streamWatchDeliveries: 1,
    noticeDeliveries: 1,
    scheduleSubscriptions: 1,
    leaseRoutesReacquired: 2,
    leaseWatchDeliveries: 1,
    rpcCallsCompleted: 1,
  };
  for (const [field, value] of Object.entries(expected)) {
    const actual = numericField(complete, field);
    if (actual !== value) throw new Error(`${field}=${actual}/${value}`);
  }
  return expected;
}

export function assertEphemeralReplyLossDispatch(log: string): number {
  const dispatched = requiredEvent(log, "ephemeral_reply_loss_dispatched");
  const requests = numericField(dispatched, "requests");
  const repliesReceived = numericField(dispatched, "repliesReceived");
  if (requests !== 1 && requests !== 11) throw new Error(`requests=${requests}; expected 1 or 11`);
  if (repliesReceived !== 0) {
    throw new Error(`${repliesReceived} ephemeral handle replies escaped the response-loss fault`);
  }
  return requests;
}

export async function runEphemeralReplyLossCleanupScenario(
  stack: ComposeStack,
  config: RunConfig,
  shape: WorkloadShape,
  artifacts: Artifacts,
): Promise<void> {
  const startedAt = performance.now();
  const baselines = new Map<EphemeralCleanupDomain, LiveDomainSnapshot>();
  for (const domain of CLEANUP_DOMAINS) {
    baselines.set(domain, await domainSnapshot(stack, domain));
  }
  await artifacts.event("ephemeral_reply_loss_cleanup_started", {
    domains: CLEANUP_DOMAINS,
    fault: "broker-to-client-handle-response-pause",
    expectedLostReplies: 12,
  });

  const preparer = await stack.startRoleContainers("ephemeral-reply-loss-preparer" as StackRole, 1, shape, {
    DESTROYER_EPHEMERAL_REPLY_ACTION: "prepare",
  });
  await stack.waitForRoleEvent(preparer, "ephemeral_reply_loss_preparer_ready");
  const victims = await stack.startRoleContainers("ephemeral-reply-loss-victim" as StackRole, 2, shape, {
    FITZ_URL: "ws://client-proxy:4090/ws",
    DESTROYER_EPHEMERAL_REPLY_ACTION: "victim",
    DESTROYER_REQUEST_TIMEOUT_MS: "10000",
    DESTROYER_WAIT_FOR_START_SIGNAL: "true",
  });
  await stack.waitForRoleEvent(victims, "live_producer_ready");

  let victimLogs: ReadonlyMap<string, string>;
  try {
    // Pause the proxy's upstream read side so Fitz can commit setup state while
    // every response remains queued until the victim is cut.
    await stack.setFaultProxy("client-proxy", { mode: "downstream-pause" });
    await stack.signalRoleContainers(victims, "SIGUSR1");
    await stack.waitForRoleEvent(victims, "ephemeral_reply_loss_dispatched");
    await waitForEphemeralStateArmed(stack, config, shape.namespace, artifacts);
    victimLogs = await stack.killRoleContainers(victims, "ephemeral-reply-loss-victims");
  } finally {
    await stack.setFaultProxy("client-proxy", { mode: "healthy" }).catch(() => undefined);
  }
  const preparerLogs = await stack.killRoleContainers(preparer, "ephemeral-reply-loss-preparer");
  if (preparerLogs.get("0") === undefined) throw new Error("Reply-loss preparer log was missing");
  const dispatched = [...victimLogs.values()].map(assertEphemeralReplyLossDispatch);
  if (dispatched.length !== 2 || dispatched.reduce((sum, count) => sum + count, 0) !== 12) {
    throw new Error(`Reply-loss victims dispatched [${dispatched.join(", ")}], expected 1 + 11`);
  }

  await waitForEveryDomainToQuiesce(stack, config, shape, baselines, artifacts, "after-victims");
  const verifier = await stack.startRoleContainers("ephemeral-reply-loss-verifier" as StackRole, 1, shape, {
    DESTROYER_EPHEMERAL_REPLY_ACTION: "verify",
  });
  const verifierLogs = await stack.finishRoleContainers(verifier, "ephemeral-reply-loss-verifier");
  const verifierLog = verifierLogs.get("0");
  if (verifierLog === undefined) throw new Error("Reply-loss verifier log was missing");
  const evidence = assertEphemeralReplyLossEvidence(verifierLog);
  await waitForEveryDomainToQuiesce(stack, config, shape, baselines, artifacts, "after-verifier");

  await artifacts.event("ephemeral_reply_loss_cleanup_complete", {
    lostReplies: 12,
    ...evidence,
    elapsedMs: Math.round(performance.now() - startedAt),
  });
}

export function isEphemeralDomainQuiescent(
  domain: EphemeralCleanupDomain,
  snapshot: LiveDomainSnapshot,
): boolean {
  if (snapshot.cleanup.pending !== 0 || snapshot.cleanup.oldestAgeMs !== 0) return false;
  const fields = domain === "queue"
    ? ["inflight_active"]
    : domain === "kv"
      ? ["transactions_active"]
      : domain === "stream"
        ? ["append_sessions_active", "subscriptions_active"]
        : domain === "notice"
          ? ["subscriptions_active", "routes_active"]
          : domain === "rpc"
            ? ["workers_registered", "requests_pending", "pending_routes_active"]
            : domain === "lease"
              ? ["leases_active", "waiter_depth"]
              : [
                  "schedules_active",
                  "subscriptions_active",
                  "pending_fire_claims",
                  "pending_ack_retries",
                ];
  return fields.every((field) => snapshot.domain[field] === 0);
}

export function isQueueResourceQuiescent(
  resource: Readonly<Record<string, unknown>>,
): boolean {
  return resource.messages_inflight === 0 && resource.subscriptions_active === 0;
}

export function isEphemeralStateArmed(state: EphemeralArmedState): boolean {
  return atLeast(state.queue, "inflight_active", 1) &&
    atLeast(state.kv, "transactions_active", 1) &&
    atLeast(state.stream, "append_sessions_active", 1) &&
    atLeast(state.stream, "subscriptions_active", 1) &&
    atLeast(state.notice, "subscriptions_active", 1) &&
    atLeast(state.rpc, "workers_registered", 1) &&
    atLeast(state.lease, "leases_active", 2) &&
    atLeastLeaseWaiter(state.lease) &&
    atLeast(state.schedule, "subscriptions_active", 1) &&
    atLeast(state.queueResource, "messages_inflight", 1) &&
    atLeast(state.queueResource, "subscriptions_active", 1);
}

function atLeastLeaseWaiter(record: Readonly<Record<string, unknown>>): boolean {
  if (atLeast(record, "waiter_depth", 1)) return true;
  const diagnostics = record.diagnostics;
  return typeof diagnostics === "object" && diagnostics !== null && !Array.isArray(diagnostics) &&
    atLeast(diagnostics as Readonly<Record<string, unknown>>, "waiter_count", 1);
}

async function waitForEveryDomainToQuiesce(
  stack: ComposeStack,
  config: RunConfig,
  shape: WorkloadShape,
  baselines: ReadonlyMap<EphemeralCleanupDomain, LiveDomainSnapshot>,
  artifacts: Artifacts,
  phase: string,
): Promise<void> {
  for (const domain of CLEANUP_DOMAINS) {
    const baseline = baselines.get(domain);
    if (baseline === undefined) throw new Error(`Missing ${domain} cleanup baseline`);
    const deadline = Date.now() + Math.min(config.startupTimeoutMs, 15_000);
    let snapshot = await domainSnapshot(stack, domain);
    while (Date.now() < deadline && !isEphemeralDomainQuiescent(domain, snapshot)) {
      await sleep(100);
      snapshot = await domainSnapshot(stack, domain);
    }
    await artifacts.writeJson(`ephemeral-reply-loss-${phase}-${domain}.json`, snapshot);
    if (!isEphemeralDomainQuiescent(domain, snapshot)) {
      throw new Error(`${domain} retained ephemeral state after reply loss: ${JSON.stringify(snapshot)}`);
    }
    if (snapshot.cleanup.failures > baseline.cleanup.failures) {
      throw new Error(
        `${domain} cleanup failures increased from ${baseline.cleanup.failures} to ${snapshot.cleanup.failures}`,
      );
    }
  }
  const queueResource = await waitForQueueResourceQuiescence(config, shape.namespace);
  await artifacts.writeJson(`ephemeral-reply-loss-${phase}-queue-resource.json`, queueResource);
}

async function domainSnapshot(
  stack: ComposeStack,
  domain: EphemeralCleanupDomain,
): Promise<LiveDomainSnapshot> {
  // ComposeStack's implementation accepts any admin domain; this scenario includes Queue's
  // reservation/subscription state in addition to the original live-domain helper union.
  return stack.liveDomainSnapshot(domain as Parameters<ComposeStack["liveDomainSnapshot"]>[0]);
}

async function waitForQueueResourceQuiescence(
  config: RunConfig,
  namespace: string,
): Promise<Readonly<Record<string, unknown>>> {
  const deadline = Date.now() + Math.min(config.startupTimeoutMs, 15_000);
  let resource = await queueResourceSnapshot(config, namespace);
  while (Date.now() < deadline && !isQueueResourceQuiescent(resource)) {
    await sleep(100);
    resource = await queueResourceSnapshot(config, namespace);
  }
  if (!isQueueResourceQuiescent(resource)) {
    throw new Error(`Queue resource retained reply-loss state: ${JSON.stringify(resource)}`);
  }
  return resource;
}

async function waitForEphemeralStateArmed(
  stack: ComposeStack,
  config: RunConfig,
  namespace: string,
  artifacts: Artifacts,
): Promise<void> {
  const deadline = Date.now() + Math.min(config.startupTimeoutMs, 8_000);
  let state = await ephemeralArmedState(stack, config, namespace);
  while (Date.now() < deadline && !isEphemeralStateArmed(state)) {
    await sleep(100);
    state = await ephemeralArmedState(stack, config, namespace);
  }
  await artifacts.writeJson("ephemeral-reply-loss-armed-state.json", state);
  if (!isEphemeralStateArmed(state)) {
    throw new Error(`Fitz did not expose every lost-reply state before the cut: ${JSON.stringify(state)}`);
  }
}

async function ephemeralArmedState(
  stack: ComposeStack,
  config: RunConfig,
  namespace: string,
): Promise<EphemeralArmedState> {
  // Notice's aggregate stats are backed by a lazy admin projection. Reading
  // the scoped subscription collection forces that projection to observe the
  // just-created registration before the stats snapshot below.
  await fetchJsonObject(
    `http://127.0.0.1:${config.port}/api/v1/all/notice/realms/destroyer/areas/${encodeURIComponent(namespace)}/resources/reply-loss-notice/subscriptions`,
    config.requestTimeoutMs,
  );
  const [queue, kv, stream, notice, rpc, lease, schedule, queueResource] = await Promise.all([
    domainSnapshot(stack, "queue"),
    domainSnapshot(stack, "kv"),
    domainSnapshot(stack, "stream"),
    domainSnapshot(stack, "notice"),
    domainSnapshot(stack, "rpc"),
    domainSnapshot(stack, "lease"),
    domainSnapshot(stack, "schedule"),
    queueResourceSnapshot(config, namespace),
  ]);
  return {
    queue: queue.domain,
    kv: kv.domain,
    stream: stream.domain,
    notice: notice.domain,
    rpc: rpc.domain,
    lease: lease.domain,
    schedule: schedule.domain,
    queueResource,
  };
}

function queueResourceSnapshot(
  config: RunConfig,
  namespace: string,
): Promise<Readonly<Record<string, unknown>>> {
  const encodedNamespace = encodeURIComponent(namespace);
  const url =
    `http://127.0.0.1:${config.port}/api/v1/all/queue/realms/destroyer/areas/${encodedNamespace}/resources/reply-loss-queue`;
  return fetchJsonObject(url, config.requestTimeoutMs);
}

function atLeast(
  record: Readonly<Record<string, unknown>>,
  field: string,
  minimum: number,
): boolean {
  const value = record[field];
  return typeof value === "number" && Number.isFinite(value) && value >= minimum;
}
