import type { RunConfig } from "../config.js";
import type { WorkloadShape } from "../workloads/model.js";
import { assertWildcardQuotaEvidence } from "../workloads/wildcard-registration-quota-reclamation.js";
import type { Artifacts } from "./artifacts.js";
import type { ComposeStack } from "./compose.js";
import { requiredEvent } from "./workload-log.js";

export async function runWildcardRegistrationQuotaReclamationScenario(stack: ComposeStack, _config: RunConfig, shape: WorkloadShape, artifacts: Artifacts): Promise<void> {
  const startedAt = performance.now();
  const role = await stack.startRoleContainers("wildcard-registration-quota-reclamation", 1, shape);
  const logs = await stack.finishRoleContainers(role, "wildcard-registration-quota-reclamation");
  const log = logs.get("0");
  if (log === undefined) throw new Error("wildcard quota worker log was missing");
  const evidence = requiredEvent(log, "wildcard_registration_quota_reclamation_worker_complete");
  assertWildcardQuotaEvidence(evidence);
  await artifacts.writeJson("wildcard-registration-quota-reclamation-evidence.json", evidence);
  await artifacts.event("wildcard_registration_quota_reclamation_complete", { ...evidence, elapsedMs: Math.round(performance.now() - startedAt) });
}
