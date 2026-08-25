import type { RunConfig } from "../config.js";
import type { WorkloadShape } from "../workloads/model.js";
import type { Artifacts } from "./artifacts.js";
import type { ComposeStack } from "./compose.js";
import { numericField, parseJsonRecords, requiredEvent } from "./workload-log.js";

export type LeaseAdmission = {
  participant: string;
  sequence: number;
  fencingToken: bigint;
};

export async function runLeaseContentionScenario(
  stack: ComposeStack,
  config: RunConfig,
  shape: WorkloadShape,
  artifacts: Artifacts,
): Promise<void> {
  const startedAt = performance.now();
  const contenders = await stack.startRoleContainers(
    "lease-contender",
    config.clientReplicas,
    shape,
    {
      DESTROYER_LEASE_ACTION: "contend",
      DESTROYER_WAIT_FOR_START_SIGNAL: "true",
    },
  );
  await stack.waitForRoleEvent(contenders, "live_producer_ready");
  await stack.signalRoleContainers(contenders, "SIGUSR1");
  const contenderLogs = await stack.finishRoleContainers(contenders, "lease-contenders");
  const admissions = leaseAdmissions(contenderLogs);
  assertLeaseAdmissions(
    admissions,
    config.clientReplicas * shape.entriesPerResource,
    config.clientReplicas,
  );

  const owner = await stack.startRoleContainers("lease-owner", 1, shape, {
    DESTROYER_LEASE_ACTION: "hold",
    DESTROYER_LEASE_PARTICIPANT: "owner",
  });
  await stack.waitForRoleEvent(owner, "lease_entered");
  const ownerLog = await stack.roleLogs(owner);
  const ownerAdmission = leaseAdmissions(ownerLog)[0];
  if (ownerAdmission === undefined) throw new Error("Lease owner never entered its callback");

  const waiter = await stack.startRoleContainers("lease-contender", 1, shape, {
    DESTROYER_LEASE_ACTION: "contend",
    DESTROYER_LEASE_PARTICIPANT: "waiter",
    DESTROYER_OPERATIONS: "1",
  });
  await stack.waitForRoleEvent(waiter, "lease_contender_ready");
  await stack.killRoleContainers(owner, "lease-owner-sigkill");
  const waiterLogs = await stack.finishRoleContainers(waiter, "lease-waiter-after-owner-kill");
  const waiterAdmission = leaseAdmissions(waiterLogs)[0];
  if (waiterAdmission === undefined) throw new Error("Lease waiter did not acquire after owner death");
  if (waiterAdmission.fencingToken <= ownerAdmission.fencingToken) {
    throw new Error(
      `Lease fencing did not advance after owner death: ${waiterAdmission.fencingToken} <= ${ownerAdmission.fencingToken}`,
    );
  }

  const probe = await stack.startRoleContainers("lease-probe", 1, shape, {
    DESTROYER_LEASE_ACTION: "probe",
  });
  await stack.finishRoleContainers(probe, "lease-quiescence-probe");
  await artifacts.writeJson("lease-contention-admissions.json", {
    contenders: serializableAdmissions(admissions),
    killedOwner: serializableAdmission(ownerAdmission),
    replacementOwner: serializableAdmission(waiterAdmission),
  });
  await artifacts.event("lease_contention_complete", {
    contenderAdmissions: admissions.length,
    participants: config.clientReplicas,
    ownerToken: ownerAdmission.fencingToken.toString(),
    replacementToken: waiterAdmission.fencingToken.toString(),
    elapsedMs: Math.round(performance.now() - startedAt),
  });
}

export function leaseAdmissions(logs: ReadonlyMap<string, string>): LeaseAdmission[] {
  const admissions: LeaseAdmission[] = [];
  for (const log of logs.values()) {
    for (const record of parseJsonRecords(log)) {
      if (record.event !== "lease_entered") continue;
      if (typeof record.participant !== "string" || typeof record.fencingToken !== "string") {
        throw new Error("Lease admission omitted participant or fencing token");
      }
      admissions.push({
        participant: record.participant,
        sequence: numericField(record, "sequence"),
        fencingToken: BigInt(record.fencingToken),
      });
    }
  }
  return admissions;
}

export function assertLeaseAdmissions(
  admissions: readonly LeaseAdmission[],
  expected: number,
  participants: number,
): void {
  if (admissions.length !== expected) {
    throw new Error(`Lease admitted ${admissions.length}/${expected} callbacks`);
  }
  const tokens = admissions.map(({ fencingToken }) => fencingToken);
  if (new Set(tokens.map(String)).size !== tokens.length) {
    throw new Error("Lease reused a fencing token across distinct ownership admissions");
  }
  const participantSet = new Set(admissions.map(({ participant }) => participant));
  if (participantSet.size !== participants) {
    throw new Error(`Lease admitted ${participantSet.size}/${participants} contenders`);
  }
  for (const participant of participantSet) {
    const sequences = admissions
      .filter((admission) => admission.participant === participant)
      .map(({ sequence }) => sequence)
      .sort((left, right) => left - right);
    const expectedSequences = Array.from({ length: expected / participants }, (_, index) => index);
    if (sequences.some((sequence, index) => sequence !== expectedSequences[index])) {
      throw new Error(`Lease contender ${participant} did not complete every sequence exactly once`);
    }
  }
}

export function requireLeaseProbe(log: string): void {
  const complete = requiredEvent(log, "lease_probe_complete");
  if (complete.isHeld !== false || numericField(complete, "pendingWaiters") !== 0) {
    throw new Error("Lease probe reported retained ownership or waiters");
  }
}

function serializableAdmissions(admissions: readonly LeaseAdmission[]): object[] {
  return admissions.map(serializableAdmission);
}

function serializableAdmission(admission: LeaseAdmission): object {
  return { ...admission, fencingToken: admission.fencingToken.toString() };
}
