import type { RunConfig } from "../config.js";
import type { WorkloadShape } from "../workloads/model.js";
import type { Artifacts } from "./artifacts.js";
import type { ComposeStack } from "./compose.js";
import { numericField, requiredEvent } from "./workload-log.js";

export async function runResponseEnvelopeBoundariesScenario(stack: ComposeStack, _config: RunConfig, shape: WorkloadShape, artifacts: Artifacts): Promise<void> {
  const startedAt = performance.now();
  const role = await stack.startRoleContainers("response-envelope-boundaries", 1, shape);
  const logs = await stack.finishRoleContainers(role, "response-envelope-boundaries");
  const log = logs.get("0");
  if (log === undefined) throw new Error("response-envelope-boundaries worker log was missing");
  const record = requiredEvent(log, "response_envelope_boundaries_worker_complete");
  const evidence = { domains: numericField(record, "domains"), exactFit: numericField(record, "exactFit"), oneOverRejected: numericField(record, "oneOverRejected"), canaryOperations: numericField(record, "canaryOperations") };
  if (evidence.domains !== 2 || evidence.exactFit !== 2 || evidence.oneOverRejected !== 1 || evidence.canaryOperations !== 2) throw new Error("response envelope boundary evidence was incomplete");
  await artifacts.writeJson("response-envelope-boundaries-evidence.json", evidence);
  await artifacts.event("response_envelope_boundaries_complete", { ...evidence, elapsedMs: Math.round(performance.now() - startedAt) });
}
