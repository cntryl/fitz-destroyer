import assert from "node:assert/strict";
import { mkdtemp, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { parseArgs } from "../src/config.js";
import { classifyFailure, type ConcreteScenario, type ScenarioResult } from "../src/scenario.js";
import { aggregateSuiteResults, allocateScenarioConfigs, runSuite } from "../src/suite.js";

test("should_allocate_a_distinct_ordered_port_to_each_suite_scenario", () => {
  const config = parseArgs(["all", "--port", "5000"], {});

  const allocated = allocateScenarioConfigs(config, ["clean-restart", "cache-loss", "chaos"]);

  assert.deepEqual(
    allocated.map(({ scenario, config: item }) => [scenario, item.port]),
    [
      ["clean-restart", 5_000],
      ["cache-loss", 5_001],
      ["chaos", 5_002],
    ],
  );
  assert.ok(allocated.every(({ config: item }) => item.keep === false));
});

test("should_reject_a_suite_port_range_beyond_tcp_limits", () => {
  const config = parseArgs(["all", "--port", "65535"], {});

  assert.throws(
    () => allocateScenarioConfigs(config, ["clean-restart", "cache-loss"]),
    /exceeds 65535/,
  );
});

test("should_continue_a_suite_after_failure_and_write_ordered_results", async () => {
  const rootDir = await mkdtemp(join(tmpdir(), "fitz-destroyer-suite-"));
  const config = { ...parseArgs(["all"], {}), rootDir };
  const called: ConcreteScenario[] = [];

  const summary = await runSuite(
    config,
    async (scenarioConfig, scenario, options) => {
      called.push(scenario);
      assert.equal(options.preserveFailure, false);
      assert.equal(scenarioConfig.keep, false);
      return result(scenario, scenario === "cache-loss" ? "failed" : "passed");
    },
    ["clean-restart", "cache-loss", "chaos"],
  );

  assert.deepEqual(called, ["clean-restart", "cache-loss", "chaos"]);
  assert.deepEqual(summary.totals, { passed: 2, failed: 1, scenarios: 3 });
  assert.deepEqual(summary.results.map(({ scenario }) => scenario), called);
  const persisted = JSON.parse(
    await readFile(join(rootDir, "artifacts", "suites", summary.suiteId, "summary.json"), "utf8"),
  ) as { results: ScenarioResult[] };
  assert.deepEqual(persisted.results.map(({ scenario }) => scenario), called);
});

test("should_aggregate_scenario_verdicts_without_reordering_results", () => {
  const results = [result("chaos", "failed"), result("clean-restart", "passed")];

  const summary = aggregateSuiteResults("suite", "start", "end", 42, results);

  assert.deepEqual(summary.totals, { passed: 1, failed: 1, scenarios: 2 });
  assert.deepEqual(summary.results.map(({ scenario }) => scenario), ["chaos", "clean-restart"]);
});

test("should_classify_setup_timeout_assertion_and_workload_failures", () => {
  assert.equal(classifyFailure(new Error("docker unavailable"), "setup"), "setup");
  assert.equal(classifyFailure(new Error("Timed out waiting for readiness")), "timeout");
  assert.equal(classifyFailure(new Error("expected one response")), "assertion");
  assert.equal(classifyFailure(new Error("socket closed")), "workload");
});

function result(
  scenario: ConcreteScenario,
  verdict: ScenarioResult["verdict"],
): ScenarioResult {
  return {
    scenario,
    verdict,
    durationMs: 1,
    artifactPath: `/tmp/${scenario}`,
    failureClassification: verdict === "failed" ? "assertion" : null,
    cleanupState: "removed",
    runId: scenario,
    project: scenario,
  };
}
