#!/usr/bin/env node

import { parseArgs, usage, type ScenarioName } from "./config.js";
import { runScenario, type ConcreteScenario } from "./scenario.js";

await main().catch((error: unknown) => {
  process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
  process.exitCode = 1;
});

async function main(): Promise<void> {
  if (process.argv.includes("--help") || process.argv.includes("-h")) {
    process.stdout.write(`${usage()}\n`);
    return;
  }
  const config = parseArgs(process.argv.slice(2));
  for (const scenario of expandScenarios(config.scenario)) {
    await runScenario(config, scenario);
  }
}

function expandScenarios(scenario: ScenarioName): readonly ConcreteScenario[] {
  return scenario === "all"
    ? [
        "clean-restart",
        "cache-loss",
        "notice-fanout",
        "schedule-delivery",
        "rpc-pressure",
        "rpc-stream-hose",
        "connection-storm",
        "domain-pressure",
        "chaos",
      ]
    : [scenario];
}
