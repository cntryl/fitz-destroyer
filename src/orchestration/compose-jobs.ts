import type { RunConfig } from "../config.js";
import type { WorkloadShape } from "../workloads/model.js";
import type { Artifacts } from "./artifacts.js";
import type { CommandResult } from "./command.js";
import { elapsedMs } from "./compose-evidence.js";
import { numericValue, parseJsonRecords } from "./workload-log.js";

type ComposeRunner = (
  args: readonly string[],
  options?: { stream?: boolean; allowFailure?: boolean; ignoreOrphans?: boolean },
) => Promise<CommandResult>;

interface StorageFaultRecoveryOperations {
  stopFitz: () => Promise<void>;
  startFitz: () => Promise<void>;
}

export async function executeStorageFaultRecovery(
  operations: StorageFaultRecoveryOperations,
): Promise<void> {
  await operations.stopFitz();
  // Restarting Sqrzl remounts its local-driver tmpfs volume empty.
  await operations.startFitz();
}

export async function executeRecoveryJob(
  compose: ComposeRunner,
  artifacts: Artifacts,
  config: RunConfig,
  sequence: number,
  mode: "load" | "verify",
  shape: WorkloadShape,
  transport: "ws" | "tcp",
): Promise<void> {
  const startedAt = performance.now();
  await artifacts.event("client_job_started", { mode, transport });
  const result = await compose([
    "run", "--rm", "-T", "--no-deps",
    "-e", `DESTROYER_MODE=${mode}`,
    "-e", `DESTROYER_NAMESPACE=${shape.namespace}`,
    "-e", `DESTROYER_SEED=${shape.seed}`,
    "-e", `DESTROYER_RESOURCES=${shape.resources}`,
    "-e", `DESTROYER_ENTRIES=${shape.entriesPerResource}`,
    "-e", `DESTROYER_PAYLOAD_BYTES=${shape.payloadBytes}`,
    "-e", `DESTROYER_STARTUP_TIMEOUT_MS=${config.startupTimeoutMs}`,
    "-e", `DESTROYER_TRANSPORT=${transport}`,
    "-e", `FITZ_URL=${transport === "tcp" ? "tcp://fitz:4091" : "ws://fitz:4090/ws"}`,
    "client",
  ], { stream: true });
  await artifacts.write(
    `client-job-${sequence.toString().padStart(2, "0")}-${mode}.log`,
    `${result.stdout}${result.stderr}`,
  );
  const complete = parseJsonRecords(`${result.stdout}\n${result.stderr}`)
    .findLast((record) => record.event === "job_complete" && record.mode === mode);
  if (complete === undefined) throw new Error(`${mode} client job omitted completion evidence`);
  await artifacts.event("client_job_complete", {
    mode,
    transport,
    entries: numericValue(complete.entries, `${mode} client job entries`),
    workerElapsedMs: numericValue(complete.elapsedMs, `${mode} client job elapsedMs`),
    elapsedMs: elapsedMs(startedAt),
  });
}

export async function executeDiskFiller(
  compose: ComposeRunner,
  artifacts: Artifacts,
  target: "cache" | "storage",
  action: "fill" | "remove",
): Promise<number> {
  const result = await compose([
    "run", "--rm", "-T", "--no-deps",
    "-e", `DESTROYER_FILL_TARGET=${target}`,
    "-e", `DESTROYER_FILL_ACTION=${action}`,
    "disk-filler",
  ], { stream: true });
  const expected = `disk_filler_${action === "fill" ? "complete" : "removed"}`;
  const completion = parseJsonRecords(`${result.stdout}\n${result.stderr}`)
    .findLast((record) => record.event === expected);
  if (completion === undefined) throw new Error(`Disk filler omitted ${action} completion evidence`);
  const bytesWritten = action === "fill"
    ? numericValue(completion.bytesWritten, `${target} filler bytesWritten`)
    : 0;
  await artifacts.event("disk_filler_action_complete", { target, action, bytesWritten });
  return bytesWritten;
}
