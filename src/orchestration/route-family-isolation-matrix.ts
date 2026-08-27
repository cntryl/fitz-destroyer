import { createDestroyerToken } from "../auth-token.js";
import type { RunConfig } from "../config.js";
import {
  routeFamilyIsolationPermissions,
  type RouteFamilyIdentity,
  type RouteFamilyIsolationAction,
} from "../workloads/route-family-isolation-matrix.js";
import { ALL_DOMAINS, type WorkloadShape } from "../workloads/model.js";
import type { Artifacts } from "./artifacts.js";
import type { ComposeStack, RoleContainer } from "./compose.js";
import { numericField, parseJsonRecords, requiredEvent } from "./workload-log.js";

const ROLE = "route-family-isolation-matrix";

export async function runRouteFamilyIsolationMatrixScenario(
  stack: ComposeStack,
  _config: RunConfig,
  shape: WorkloadShape,
  artifacts: Artifacts,
): Promise<void> {
  const startedAt = performance.now();
  const permissions = routeFamilyIsolationPermissions(shape.namespace);
  let holderA: RoleContainer[] = [];
  let holderB: RoleContainer[] = [];
  let holderAActive = false;
  let holderBActive = false;

  await artifacts.event("route_family_isolation_matrix_started", {
    identities: ["identity-a", "identity-b"],
    domains: ALL_DOMAINS,
    assertion: "identical logical routes remain isolated by authenticated RouteFamily",
  });

  try {
    holderA = await startRole(stack, shape, "identity-a", "hold", permissions);
    holderAActive = true;
    await stack.waitForRoleEvent(holderA, "route_family_isolation_holder_armed");

    holderB = await startRole(stack, shape, "identity-b", "hold", permissions);
    holderBActive = true;
    await stack.waitForRoleEvent(holderB, "route_family_isolation_holder_armed");

    await sleep(100);
    const holderALogs = await stack.roleLogs(holderA);
    const holderAEvidence = assertRouteFamilyArmedEvidence(
      requiredEvent(firstLog(holderALogs), "route_family_isolation_holder_armed"),
      "identity-a",
    );
    assertNoCrossFamilyNotice(firstLog(holderALogs), "identity-a");
    await stack.killRoleContainers(holderA, "route-family-holder-a-sigkill");
    holderAActive = false;

    const survivor = await startRole(
      stack,
      shape,
      "identity-b",
      "verify-survivor",
      permissions,
    );
    const survivorLogs = await stack.finishRoleContainers(
      survivor,
      "route-family-survivor-b",
    );
    const survivorEvidence = assertRouteFamilyProbeEvidence(
      requiredEvent(firstLog(survivorLogs), "route_family_isolation_probe_complete"),
      "identity-b",
      "verify-survivor",
    );

    const closed = await startRole(
      stack,
      shape,
      "identity-a",
      "verify-closed",
      permissions,
    );
    const closedLogs = await stack.finishRoleContainers(closed, "route-family-closed-a");
    const closedEvidence = assertRouteFamilyProbeEvidence(
      requiredEvent(firstLog(closedLogs), "route_family_isolation_probe_complete"),
      "identity-a",
      "verify-closed",
    );

    await stack.signalRoleContainers(holderB, "SIGTERM");
    const holderBLogs = await stack.finishRoleContainers(holderB, "route-family-holder-b");
    holderBActive = false;
    const holderBEvidence = assertRouteFamilyHolderEvidence(
      requiredEvent(firstLog(holderBLogs), "route_family_isolation_holder_complete"),
      "identity-b",
      2,
    );

    await artifacts.writeJson("route-family-isolation-evidence.json", {
      holderA: holderAEvidence,
      holderB: holderBEvidence,
      survivor: survivorEvidence,
      closed: closedEvidence,
    });
    await artifacts.event("route_family_isolation_matrix_complete", {
      identities: 2,
      domains: ALL_DOMAINS.length,
      holderDomainChecks: ALL_DOMAINS.length * 2,
      probeDomainChecks: ALL_DOMAINS.length * 2,
      crossFamilyDeliveries: 0,
      elapsedMs: Math.round(performance.now() - startedAt),
    });
  } finally {
    if (holderAActive) {
      await terminateRole(stack, holderA, "route-family-holder-a-cleanup");
    }
    if (holderBActive) {
      await terminateRole(stack, holderB, "route-family-holder-b-cleanup");
    }
  }
}

export function assertRouteFamilyHolderEvidence(
  record: Readonly<Record<string, unknown>>,
  identity: RouteFamilyIdentity,
  minimumOwnLiveOperations: number,
): Readonly<Record<string, unknown>> {
  if (record.identity !== identity) {
    throw new Error(`RouteFamily holder identity ${String(record.identity)} != ${identity}`);
  }
  const verifiedDomains = numericField(record, "verifiedDomains");
  if (verifiedDomains !== ALL_DOMAINS.length) {
    throw new Error(`${identity} verified ${verifiedDomains}/${ALL_DOMAINS.length} domains`);
  }
  const foreignNotices = numericField(record, "foreignNotices");
  if (foreignNotices !== 0) {
    throw new Error(`${identity} observed ${foreignNotices} cross-family Notice deliveries`);
  }
  const ownNotices = numericField(record, "ownNotices");
  if (ownNotices < minimumOwnLiveOperations) {
    throw new Error(`${identity} received ${ownNotices}/${minimumOwnLiveOperations} own Notices`);
  }
  const rpcResponses = numericField(record, "rpcResponses");
  if (rpcResponses !== 1) {
    throw new Error(`${identity} received ${rpcResponses}/1 self RPC responses`);
  }
  const rpcRequestsHandled = numericField(record, "rpcRequestsHandled");
  if (rpcRequestsHandled < minimumOwnLiveOperations) {
    throw new Error(
      `${identity} handled ${rpcRequestsHandled}/${minimumOwnLiveOperations} same-family RPC requests`,
    );
  }
  return record;
}

export function assertRouteFamilyArmedEvidence(
  record: Readonly<Record<string, unknown>>,
  identity: RouteFamilyIdentity,
): Readonly<Record<string, unknown>> {
  if (record.identity !== identity) {
    throw new Error(`RouteFamily holder identity ${String(record.identity)} != ${identity}`);
  }
  const verifiedDomains = numericField(record, "verifiedDomains");
  if (verifiedDomains !== ALL_DOMAINS.length) {
    throw new Error(`${identity} verified ${verifiedDomains}/${ALL_DOMAINS.length} domains`);
  }
  return record;
}

export function assertNoCrossFamilyNotice(log: string, identity: RouteFamilyIdentity): void {
  const foreign = parseJsonRecords(log).filter(
    (record) => record.event === "route_family_isolation_foreign_notice",
  );
  if (foreign.length !== 0) {
    throw new Error(`${identity} observed ${foreign.length} cross-family Notice deliveries`);
  }
}

export function assertRouteFamilyProbeEvidence(
  record: Readonly<Record<string, unknown>>,
  identity: RouteFamilyIdentity,
  action: Exclude<RouteFamilyIsolationAction, "hold">,
): Readonly<Record<string, unknown>> {
  if (record.identity !== identity || record.action !== action) {
    throw new Error(
      `RouteFamily probe identity/action ${String(record.identity)}/${String(record.action)} != ${identity}/${action}`,
    );
  }
  const verifiedDomains = numericField(record, "verifiedDomains");
  if (verifiedDomains !== ALL_DOMAINS.length) {
    throw new Error(`${identity} probe verified ${verifiedDomains}/${ALL_DOMAINS.length} domains`);
  }
  const leaseHeld = record.leaseHeldBeforeProbe;
  const rpcUnavailable = record.rpcUnavailableBeforeProbe;
  if (action === "verify-survivor") {
    if (leaseHeld !== true) throw new Error("surviving family lost Lease ownership");
    if (rpcUnavailable !== false) throw new Error("surviving family lost its RPC worker");
  } else {
    if (leaseHeld !== false) throw new Error("closed family retained Lease ownership");
    if (rpcUnavailable !== true) throw new Error("closed family retained its RPC worker");
  }
  return record;
}

async function startRole(
  stack: ComposeStack,
  shape: WorkloadShape,
  identity: RouteFamilyIdentity,
  action: RouteFamilyIsolationAction,
  permissions: readonly string[],
): Promise<RoleContainer[]> {
  return stack.startRoleContainers(ROLE as Parameters<ComposeStack["startRoleContainers"]>[0], 1, shape, {
    DESTROYER_JWT: createDestroyerToken(identity, permissions),
    DESTROYER_ROUTE_FAMILY_IDENTITY: identity,
    DESTROYER_ROUTE_FAMILY_ACTION: action,
    DESTROYER_OPERATIONS: "1",
  });
}

async function terminateRole(
  stack: ComposeStack,
  containers: readonly RoleContainer[],
  label: string,
): Promise<void> {
  await stack.signalRoleContainers(containers, "SIGTERM").catch(() => undefined);
  await stack.finishRoleContainers(containers, label).catch(async () => {
    await stack.killRoleContainers(containers, `${label}-sigkill`).catch(() => undefined);
  });
}

function firstLog(logs: ReadonlyMap<string, string>): string {
  const log = [...logs.values()][0];
  if (log === undefined) throw new Error("RouteFamily isolation worker log was missing");
  return log;
}

function sleep(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}
