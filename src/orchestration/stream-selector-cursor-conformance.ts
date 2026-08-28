import type { RunConfig } from "../config.js";
import type { WorkloadShape } from "../workloads/model.js";
import { assertStreamSelectorEvidence } from "../workloads/stream-selector-cursor-conformance.js";
import type { Artifacts } from "./artifacts.js";
import type { ComposeStack } from "./compose.js";
import { requiredEvent } from "./workload-log.js";

export async function runStreamSelectorCursorConformanceScenario(stack: ComposeStack, _config: RunConfig, shape: WorkloadShape, artifacts: Artifacts): Promise<void> {
  const startedAt = performance.now();
  const role = await stack.startRoleContainers("stream-selector-cursor-conformance", 1, shape);
  const logs = await stack.finishRoleContainers(role, "stream-selector-cursor-conformance");
  const log = logs.get("0");
  if (log === undefined) throw new Error("stream selector worker log was missing");
  const evidence = requiredEvent(log, "stream_selector_cursor_conformance_worker_complete");
  assertStreamSelectorEvidence(evidence);
  await artifacts.writeJson("stream-selector-cursor-conformance-evidence.json", evidence);
  await artifacts.event("stream_selector_cursor_conformance_complete", { ...evidence, elapsedMs: Math.round(performance.now() - startedAt) });
}
