import type { RunConfig } from "../config.js";
import type { WorkloadShape } from "../workloads/model.js";
import type { Artifacts } from "./artifacts.js";
import type { ComposeStack } from "./compose.js";
import { numericValue, requiredEvent } from "./workload-log.js";

export type StreamReplayLedger = {
  winner: number;
  conflicts: number;
  offsetConflicts: number;
  records: number;
  pages: number;
  boundaryBytes: number;
  boundaryRecords: number;
};

export async function runStreamReplayScenario(
  stack: ComposeStack,
  _config: RunConfig,
  shape: WorkloadShape,
  artifacts: Artifacts,
): Promise<void> {
  const startedAt = performance.now();
  const operations = shape.resources * shape.entriesPerResource;
  const commitAtMs = Date.now() + 10_000;
  const contenders = await stack.startRoleContainers(
    "stream-replay-worker",
    2,
    { ...shape, entriesPerResource: operations },
    {
      DESTROYER_SEED: String(shape.seed),
      DESTROYER_STREAM_REPLAY_ACTION: "contend",
      DESTROYER_STREAM_REPLAY_COMMIT_AT_MS: String(commitAtMs),
    },
  );
  const contenderLogs = await stack.finishRoleContainers(contenders, "stream-replay-contenders");
  const verifier = await stack.startRoleContainers(
    "stream-replay-worker",
    1,
    { ...shape, entriesPerResource: operations },
    {
      DESTROYER_SEED: String(shape.seed),
      DESTROYER_STREAM_REPLAY_ACTION: "verify",
    },
  );
  const verifierLogs = await stack.finishRoleContainers(verifier, "stream-replay-verifier");
  const ledger = analyzeStreamReplay(contenderLogs, verifierLogs);
  await artifacts.writeJson("stream-replay-ledger.json", ledger);
  assertStreamReplay(ledger, operations);
  const baseline = await stack.liveDomainSnapshot("stream");
  await stack.waitForLiveDomainQuiescence("stream", baseline, "stream-replay");
  await artifacts.event("stream_replay_complete", {
    ...ledger,
    elapsedMs: Math.round(performance.now() - startedAt),
  });
}

export function analyzeStreamReplay(
  contenderLogs: ReadonlyMap<string, string>,
  verifierLogs: ReadonlyMap<string, string>,
): StreamReplayLedger {
  const outcomes = [...contenderLogs.values()].map((log) =>
    requiredEvent(log, "stream_replay_contender_complete"),
  );
  const committed = outcomes.filter((record) => record.outcome === "committed");
  const rejected = outcomes.filter((record) => record.outcome === "rejected");
  if (committed.length !== 1 || rejected.length !== 1) {
    throw new Error(`Expected one Stream winner and one conflict: ${JSON.stringify(outcomes)}`);
  }
  const log = [...verifierLogs.values()][0];
  if (verifierLogs.size !== 1 || log === undefined) {
    throw new Error(`Expected one Stream replay verifier log, found ${verifierLogs.size}`);
  }
  const record = requiredEvent(log, "stream_replay_complete");
  const committedWriter = numericValue(committed[0]?.writer, "committed Stream writer");
  const verifiedWinner = numericValue(record.winner, "stream winner");
  if (committedWriter !== verifiedWinner) {
    throw new Error(`Committed Stream writer ${committedWriter} != verified winner ${verifiedWinner}`);
  }
  return {
    winner: verifiedWinner,
    conflicts: rejected.length,
    offsetConflicts: numericValue(record.offsetConflicts, "stream offset conflicts"),
    records: numericValue(record.records, "stream records"),
    pages: numericValue(record.pages, "stream pages"),
    boundaryBytes: numericValue(record.boundaryBytes, "stream boundary bytes"),
    boundaryRecords: numericValue(record.boundaryRecords, "stream boundary records"),
  };
}

export function assertStreamReplay(ledger: StreamReplayLedger, expectedRecords: number): void {
  if (
    (ledger.winner !== 1 && ledger.winner !== 2) ||
    ledger.conflicts !== 1 ||
    ledger.offsetConflicts !== 1 ||
    ledger.records !== Math.max(1, expectedRecords) ||
    ledger.pages < 1 ||
    ledger.boundaryBytes !== 60_000 ||
    ledger.boundaryRecords !== 1
  ) {
    throw new Error(`Stream replay invariants failed: ${JSON.stringify(ledger)}`);
  }
}
