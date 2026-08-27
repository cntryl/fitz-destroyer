import type { RunConfig } from "../config.js";
import { totalDurableEntries, type WorkloadShape } from "../workloads/model.js";
import type { Artifacts } from "./artifacts.js";
import type { ComposeStack } from "./compose.js";
import { numericField, requiredEvent } from "./workload-log.js";

export async function runCacheAndDiskExhaustionScenario(
  stack: ComposeStack,
  _config: RunConfig,
  shape: WorkloadShape,
  artifacts: Artifacts,
): Promise<void> {
  const startedAt = performance.now();
  await stack.runRecoveryJob("load", shape);
  const results: Array<{ target: "cache" | "storage"; bytesFilled: number }> = [];
  let verifiedRecoveries = 0;

  try {
    for (const target of ["cache", "storage"] as const) {
      const bytesFilled = await stack.runDiskFiller(target, "fill");
      try {
        const probeShape = { ...shape, namespace: `${shape.namespace}-${target}`, entriesPerResource: 1 };
        const probes = await stack.startRoleContainers("exhaustion-probe", 1, probeShape, {
          DESTROYER_REQUEST_TIMEOUT_MS: "1500",
        });
        const logs = await stack.finishRoleContainers(probes, `${target}-exhaustion-probe`);
        const completion = requiredEvent(logs.get("0") ?? "", "exhaustion_probe_complete");
        if (numericField(completion, "acknowledged") !== 0 || numericField(completion, "rejected") !== 1) {
          throw new Error(`${target} exhaustion did not reject the synchronous mutation`);
        }
      } finally {
        await stack.runDiskFiller(target, "remove");
      }
      results.push({ target, bytesFilled });

      if (target === "cache") await stack.recycleFitzAfterFault();
      else await stack.recycleStorageAfterFault();
      await stack.runRecoveryJob("verify", shape).catch((error: unknown) => {
        throw new Error(`Acknowledged baseline did not recover after ${target} exhaustion: ${errorMessage(error)}`);
      });
      verifiedRecoveries += 1;
    }
  } catch (error) {
    await emitCompletion(artifacts, startedAt, results, verifiedRecoveries, shape, "failed");
    throw error;
  }

  await emitCompletion(artifacts, startedAt, results, verifiedRecoveries, shape, "passed");
}

async function emitCompletion(
  artifacts: Artifacts,
  startedAt: number,
  results: readonly { target: "cache" | "storage"; bytesFilled: number }[],
  verifiedRecoveries: number,
  shape: WorkloadShape,
  outcome: "passed" | "failed",
): Promise<void> {
  await artifacts.event("cache_and_disk_exhaustion_complete", {
    outcome,
    rejectedMutations: results.length,
    verified: verifiedRecoveries * totalDurableEntries(shape),
    results,
    elapsedMs: Math.round(performance.now() - startedAt),
  });
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? `${error.name}: ${error.message}` : String(error);
}
