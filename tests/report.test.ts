import assert from "node:assert/strict";
import test from "node:test";
import { buildCiReport, renderCiReport } from "../src/report.js";
import type { ConcreteScenario, ScenarioResult } from "../src/scenario.js";

test("should_build_an_ordered_report_from_scenario_artifacts", () => {
  const expected = ["clean-restart", "cache-loss"] as const;
  const report = buildCiReport(
    expected,
    [result("cache-loss", "passed"), result("clean-restart", "passed")],
    "success",
    "success",
    "2026-08-25T12:00:00.000Z",
  );

  assert.equal(report.verdict, "passed");
  assert.deepEqual(report.results.map(({ scenario }) => scenario), expected);
  assert.deepEqual(report.totals, { expected: 2, reported: 2, passed: 2, failed: 0, missing: 0 });
});

test("should_fail_the_report_for_failed_missing_or_duplicate_scenarios", () => {
  const expected = ["clean-restart", "cache-loss", "chaos"] as const;
  const report = buildCiReport(
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
  assert.match(renderCiReport(report), /clean-restart.*FAIL/u);
  assert.match(renderCiReport(report), /chaos.*MISSING/u);
});

test("should_include_repository_analysis_in_the_final_verdict", () => {
  const report = buildCiReport(
    ["clean-restart"],
    [result("clean-restart", "passed")],
    "failure",
    "success",
  );

  assert.equal(report.verdict, "failed");
  assert.match(renderCiReport(report), /Analysis: failure/u);
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
