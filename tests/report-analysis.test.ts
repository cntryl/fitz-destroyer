import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { analyzeScenarioArtifacts } from "../src/report-analysis.js";

test("should_extract_normalized_failure_signals_and_structured_pressure_warnings", async () => {
  const directory = await mkdtemp(join(tmpdir(), "fitz-destroyer-report-analysis-"));
  try {
    await Promise.all([
      writeFile(
        join(directory, "compose.log"),
        [
          "Router delivery failed: Actor has stopped",
          "Message body 334 disappeared from storage",
          'Queue actor command reply failed: {"error":"timed out waiting on receive operation"}',
          "KV inventory estimate update failed",
          "fitz.connection.lost",
        ].join("\n"),
        "utf8",
      ),
      writeFile(
        join(directory, "client.log"),
        "fitz.connection.lost\nfitz.connection.lost\nConnection closed or reset\n",
        "utf8",
      ),
      writeFile(
        join(directory, "pressure-evidence.json"),
        JSON.stringify({
          warnings: [{
            code: "pending-growth",
            message: "Pending work grew",
            details: { finalPendingWork: [1, 2, 3] },
          }],
          brokerSummary: { finalQueuePending: 3, finalRpcPending: 0 },
        }),
        "utf8",
      ),
    ]);

    const analysis = await analyzeScenarioArtifacts(directory, {
      scenario: "domain-pressure",
      verdict: "failed",
      workloadDurationMs: 1_000,
    });
    assert.deepEqual(
      analysis.observations.map(({ code, occurrences, files }) => ({ code, occurrences, files })),
      [
        { code: "actor-stopped", occurrences: 1, files: ["compose.log"] },
        { code: "connection-loss", occurrences: 2, files: ["client.log"] },
        { code: "storage-object-missing", occurrences: 1, files: ["compose.log"] },
        { code: "queue-command-timeout", occurrences: 1, files: ["compose.log"] },
        { code: "kv-inventory-update-failure", occurrences: 1, files: ["compose.log"] },
      ],
    );
    assert.deepEqual(analysis.warnings, [{
      code: "pending-growth",
      message: "Pending work grew",
      details: { finalPendingWork: [1, 2, 3] },
    }]);
    assert.deepEqual(analysis.brokerSummary, { finalQueuePending: 3, finalRpcPending: 0 });

    const passingAnalysis = await analyzeScenarioArtifacts(directory, {
      scenario: "domain-pressure",
      verdict: "passed",
      workloadDurationMs: 1_000,
    });
    assert.deepEqual(passingAnalysis.observations, []);
    assert.equal(passingAnalysis.warnings.length, 1);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
