import type { RunConfig } from "../config.js";
import type { WorkloadShape } from "../workloads/model.js";
import type { Artifacts } from "./artifacts.js";
import type { ComposeStack } from "./compose.js";
import { numericField, requiredEvent } from "./workload-log.js";

export async function runStreamGlobalRecoveryScenario(
  stack: ComposeStack,
  _config: RunConfig,
  shape: WorkloadShape,
  artifacts: Artifacts,
): Promise<void> {
  const startedAt = performance.now();
  const operations = Math.max(4, shape.resources * shape.entriesPerResource);
  const roleShape = { ...shape, entriesPerResource: operations };
  const load = await stack.startRoleContainers(
    "stream-global-recovery",
    1,
    roleShape,
    { DESTROYER_STREAM_GLOBAL_ACTION: "load" },
  );
  const loadLogs = await stack.finishRoleContainers(load, "stream-global-load");
  const loaded = numericField(
    requiredEvent([...loadLogs.values()][0] ?? "", "stream_global_load_complete"),
    "records",
  );
  await stack.discardFitzCacheAndRestart();
  const verify = await stack.startRoleContainers(
    "stream-global-recovery",
    1,
    roleShape,
    { DESTROYER_STREAM_GLOBAL_ACTION: "verify" },
  );
  const verifyLogs = await stack.finishRoleContainers(verify, "stream-global-verify");
  const complete = requiredEvent(
    [...verifyLogs.values()][0] ?? "",
    "stream_global_verify_complete",
  );
  const recovered = numericField(complete, "records");
  const pages = numericField(complete, "pages");
  assertStreamGlobalRecovery(loaded, recovered, pages, operations);
  await artifacts.event("stream_global_recovery_complete", {
    loaded,
    recovered,
    pages,
    elapsedMs: Math.round(performance.now() - startedAt),
  });
}

export function assertStreamGlobalRecovery(
  loaded: number,
  recovered: number,
  pages: number,
  expected: number,
): void {
  if (loaded !== expected || recovered !== expected || pages < 1) {
    throw new Error(
      `Global Stream recovery mismatch: loaded=${loaded}, recovered=${recovered}, pages=${pages}, expected=${expected}`,
    );
  }
}
