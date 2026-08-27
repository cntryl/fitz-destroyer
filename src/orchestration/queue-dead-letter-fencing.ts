import type { RunConfig } from "../config.js";
import { queueRoute } from "../workloads/queue-dead-letter-fencing.js";
import type { WorkloadShape } from "../workloads/model.js";
import type { Artifacts } from "./artifacts.js";
import type { ComposeStack } from "./compose.js";
import { numericField, requiredEvent } from "./workload-log.js";

export async function runQueueDeadLetterFencingScenario(
  stack: ComposeStack,
  config: RunConfig,
  shape: WorkloadShape,
  artifacts: Artifacts,
): Promise<void> {
  const startedAt = performance.now();
  const workers = await stack.startRoleContainers("queue-dead-letter-fencing", 1, shape);
  const logs = await stack.finishRoleContainers(workers, "queue-dead-letter-fencing");
  const complete = requiredEvent(
    [...logs.values()][0] ?? "",
    "queue_dead_letter_fencing_worker_complete",
  );
  const path = deadLetterPath(queueRoute(shape.namespace));
  const list = await fetchJson(`http://127.0.0.1:${config.port}/api/v1/1${path}`);
  const messages = list.messages;
  if (!Array.isArray(messages) || messages.length !== 0) {
    throw new Error(`Rejected oversized Queue body created a dead letter: ${JSON.stringify(list)}`);
  }
  const missingFamilyStatus = await mutationStatus(
    `http://127.0.0.1:${config.port}/api/v1${path}/18446744073709551615/replay`,
    "POST",
  );
  const wrongFamilyStatus = await mutationStatus(
    `http://127.0.0.1:${config.port}/api/v1/2${path}/18446744073709551615/replay`,
    "POST",
  );
  assertQueueDeadLetterFencing(
    numericField(complete, "oversizedRejected"),
    numericField(complete, "staleCompletionRejected"),
    missingFamilyStatus,
    wrongFamilyStatus,
  );
  await artifacts.event("queue_dead_letter_fencing_complete", {
    oversizedRejected: 1,
    staleCompletionRejected: 1,
    redelivered: numericField(complete, "redelivered"),
    completed: numericField(complete, "completed"),
    deadLetters: 0,
    missingFamilyStatus,
    wrongFamilyStatus,
    elapsedMs: Math.round(performance.now() - startedAt),
  });
}

export function assertQueueDeadLetterFencing(
  oversizedRejected: number,
  staleCompletionRejected: number,
  missingFamilyStatus: number,
  wrongFamilyStatus: number,
): void {
  if (
    oversizedRejected !== 1 ||
    staleCompletionRejected !== 1 ||
    missingFamilyStatus < 400 ||
    wrongFamilyStatus < 400
  ) {
    throw new Error(
      `Queue dead-letter fencing failed: oversized=${oversizedRejected}, stale=${staleCompletionRejected}, missingFamily=${missingFamilyStatus}, wrongFamily=${wrongFamilyStatus}`,
    );
  }
}

function deadLetterPath(route: string): string {
  const parsed = new URL(route);
  const [area, resource] = parsed.pathname.slice(1).split("/");
  if (area === undefined || resource === undefined) throw new Error(`Invalid Queue route ${route}`);
  return `/queue/realms/${parsed.hostname}/areas/${area}/resources/${resource}/dead-letters`;
}

async function fetchJson(url: string): Promise<Record<string, unknown>> {
  const response = await fetch(url, { signal: AbortSignal.timeout(5_000) });
  if (!response.ok) throw new Error(`${new URL(url).pathname} returned HTTP ${response.status}`);
  return await response.json() as Record<string, unknown>;
}

async function mutationStatus(url: string, method: "POST" | "DELETE"): Promise<number> {
  const response = await fetch(url, { method, signal: AbortSignal.timeout(5_000) });
  await response.body?.cancel();
  return response.status;
}
