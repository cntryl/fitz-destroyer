import type { RunConfig } from "../config.js";
import type { WorkloadShape } from "../workloads/model.js";
import type { Artifacts } from "./artifacts.js";
import type { ComposeStack } from "./compose.js";
import { numericValue, requiredEvent } from "./workload-log.js";

export type TransactionContentionLedger = {
  winner: number;
  conflicts: number;
  rollbackIsolated: boolean;
  deleteHidden: boolean;
  longLivedCleanup: boolean;
};

export async function runTransactionContentionScenario(
  stack: ComposeStack,
  _config: RunConfig,
  shape: WorkloadShape,
  artifacts: Artifacts,
): Promise<void> {
  const startedAt = performance.now();
  const environment = { DESTROYER_SEED: String(shape.seed) };
  const prepare = await stack.startRoleContainers("transaction-contender", 1, shape, {
    ...environment,
    DESTROYER_TRANSACTION_ACTION: "prepare",
  });
  const prepareLogs = await stack.finishRoleContainers(prepare, "transaction-prepare");

  const commitAtMs = Date.now() + 10_000;
  const contenders = await stack.startRoleContainers("transaction-contender", 2, shape, {
    ...environment,
    DESTROYER_TRANSACTION_ACTION: "contend",
    DESTROYER_TRANSACTION_COMMIT_AT_MS: String(commitAtMs),
  });
  const contenderLogs = await stack.finishRoleContainers(contenders, "transaction-contenders");
  const baseline = await stack.liveDomainSnapshot("kv");

  const holder = await stack.startRoleContainers("transaction-holder", 1, shape, {
    ...environment,
    DESTROYER_TRANSACTION_ACTION: "hold",
  });
  await stack.waitForRoleEvent(holder, "transaction_holder_ready");
  await stack.killRoleContainers(holder, "transaction-holder-killed");
  await stack.waitForLiveDomainQuiescence("kv", baseline, "transaction-holder-cleanup");

  const verifier = await stack.startRoleContainers("transaction-verifier", 1, shape, {
    ...environment,
    DESTROYER_TRANSACTION_ACTION: "verify",
  });
  const verifierLogs = await stack.finishRoleContainers(verifier, "transaction-verifier");
  const ledger = analyzeTransactionContention(prepareLogs, contenderLogs, verifierLogs);
  await artifacts.writeJson("transaction-contention-ledger.json", ledger);
  assertTransactionContention(ledger);
  await stack.waitForLiveDomainQuiescence("kv", baseline, "transaction-contention");
  await artifacts.event("transaction_contention_complete", {
    ...ledger,
    elapsedMs: Math.round(performance.now() - startedAt),
  });
}

export function analyzeTransactionContention(
  prepareLogs: ReadonlyMap<string, string>,
  contenderLogs: ReadonlyMap<string, string>,
  verifierLogs: ReadonlyMap<string, string>,
): TransactionContentionLedger {
  const prepare = requiredEvent(onlyLog(prepareLogs), "transaction_prepare_complete");
  const outcomes = [...contenderLogs.values()].map((log) =>
    requiredEvent(log, "transaction_contender_complete"),
  );
  const committed = outcomes.filter((record) => record.outcome === "committed");
  const rejected = outcomes.filter((record) => record.outcome === "rejected");
  const verifier = requiredEvent(onlyLog(verifierLogs), "transaction_cleanup_verified");
  if (committed.length !== 1 || rejected.length !== 1) {
    throw new Error(`Expected one KV winner and one conflict: ${JSON.stringify(outcomes)}`);
  }
  const committedWriter = numericValue(committed[0]?.writer, "committed transaction writer");
  const verifiedWinner = numericValue(verifier.winner, "transaction winner");
  if (committedWriter !== verifiedWinner) {
    throw new Error(`Committed KV writer ${committedWriter} != verified winner ${verifiedWinner}`);
  }
  return {
    winner: verifiedWinner,
    conflicts: rejected.length,
    rollbackIsolated: prepare.rollbackIsolated === true,
    deleteHidden: prepare.deleteHidden === true,
    longLivedCleanup: true,
  };
}

export function assertTransactionContention(ledger: TransactionContentionLedger): void {
  if (
    (ledger.winner !== 1 && ledger.winner !== 2) ||
    ledger.conflicts !== 1 ||
    !ledger.rollbackIsolated ||
    !ledger.deleteHidden ||
    !ledger.longLivedCleanup
  ) {
    throw new Error(`Transaction contention invariants failed: ${JSON.stringify(ledger)}`);
  }
}

function onlyLog(logs: ReadonlyMap<string, string>): string {
  const log = [...logs.values()][0];
  if (logs.size !== 1 || log === undefined) throw new Error(`Expected one workload log, found ${logs.size}`);
  return log;
}
