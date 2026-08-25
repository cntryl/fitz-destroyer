import assert from "node:assert/strict";
import test from "node:test";
import { buildDestroyerReport, renderDestroyerReport } from "../src/report.js";
import type { ConcreteScenario, ScenarioResult } from "../src/scenario.js";

test("should_build_an_ordered_report_from_scenario_artifacts", () => {
  const expected = ["clean-restart", "cache-loss"] as const;
  const report = buildDestroyerReport(
    expected,
    [result("cache-loss", "passed"), result("clean-restart", "passed")],
    "success",
    "success",
    "2026-08-25T12:00:00.000Z",
  );

  assert.equal(report.verdict, "passed");
  assert.equal(report.schemaVersion, 2);
  assert.deepEqual(report.results.map(({ scenario }) => scenario), expected);
  assert.deepEqual(report.totals, { expected: 2, reported: 2, passed: 2, failed: 0, missing: 0 });
  assert.deepEqual(report.timing, {
    recordedTotalMs: 2_500,
    slowestScenario: "clean-restart",
    slowestDurationMs: 1_250,
  });
});

test("should_fail_the_report_for_failed_missing_or_duplicate_scenarios", () => {
  const expected = ["clean-restart", "cache-loss", "chaos"] as const;
  const report = buildDestroyerReport(
    expected,
    [
      result("clean-restart", "passed"),
      result("clean-restart", "passed"),
      result("cache-loss", "failed"),
    ],
    "success",
    "failure",
  );

  assert.equal(report.verdict, "failed");
  assert.deepEqual(report.totals, { expected: 3, reported: 3, passed: 0, failed: 2, missing: 1 });
  assert.deepEqual(report.failureClasses, {
    setup: 1,
    workload: 0,
    assertion: 1,
    timeout: 0,
    cleanup: 0,
    missing: 1,
  });
  assert.match(renderDestroyerReport(report), /clean-restart.*FAIL/u);
  assert.match(renderDestroyerReport(report), /chaos.*MISSING/u);
});

test("should_include_repository_analysis_in_the_final_verdict", () => {
  const report = buildDestroyerReport(
    ["clean-restart"],
    [result("clean-restart", "passed")],
    "failure",
    "success",
  );

  assert.equal(report.verdict, "failed");
  assert.match(renderDestroyerReport(report), /Repository checks.*FAIL.*failure/u);
});

test("should_render_actionable_failure_evidence_and_bounded_diagnostic_tail", () => {
  const marker = "diagnostic-tail-marker";
  const failed = {
    ...result("cache-loss", "failed"),
    durationMs: 65_432,
    cleanupState: "preserved" as const,
    error: `Error: provider unavailable; TimeoutError: request expired ${"x".repeat(13_000)} ${marker}`,
    reportAnalysis: {
      observations: [{
        code: "actor-stopped",
        message: "Fitz reported delivery to an actor that had stopped",
        occurrences: 3,
        files: ["compose.log"],
      }],
      warnings: [{
        code: "pending-growth",
        message: "Pending work grew through the final samples",
        details: { finalPendingWork: [1, 2, 3] },
      }],
      brokerSummary: { finalQueuePending: 3 },
    },
  };
  const report = buildDestroyerReport(
    ["cache-loss"],
    [failed],
    "success",
    "failure",
    "2026-08-25T12:00:00.000Z",
    {
      repository: "cntryl/fitz-destroyer",
      commitSha: "0123456789abcdef",
      refName: "main",
      url: "https://github.com/cntryl/fitz-destroyer/actions/runs/123",
      attempt: "2",
    },
  );

  const markdown = renderDestroyerReport(report);
  assert.match(markdown, /What needs attention/u);
  assert.match(markdown, /Diagnostic warnings/u);
  assert.match(markdown, /Evidence signals/u);
  assert.match(markdown, /actor that had stopped.*3.*`cache-loss`/u);
  assert.match(markdown, /final queue pending=3/u);
  assert.match(markdown, /assertion.*1.*`cache-loss`/u);
  assert.match(markdown, /scenario-cache-loss-2/u);
  assert.match(markdown, /run-cache-loss/u);
  assert.match(markdown, /1m 5\.4s/u);
  assert.match(markdown, /characters omitted/u);
  assert.match(markdown, /diagnostic-tail-marker/u);
  assert.match(markdown, /commit \[`0123456`\]/u);
});

function result(scenario: ConcreteScenario, verdict: ScenarioResult["verdict"]): ScenarioResult {
  return {
    scenario,
    verdict,
    durationMs: 1_250,
    artifactPath: `/artifacts/${scenario}`,
    failureClassification: verdict === "passed" ? null : "assertion",
    cleanupState: "removed",
    runId: `run-${scenario}`,
    project: `project-${scenario}`,
    ...(verdict === "passed" ? {} : { error: "expected failure" }),
  };
}
