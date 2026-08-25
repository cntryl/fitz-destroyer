#!/usr/bin/env node

import { parseArgs, usage } from "./config.js";
import { runScenario } from "./scenario.js";
import { runSuite } from "./suite.js";

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
  if (config.scenario === "all") {
    const summary = await runSuite(config, runScenario);
    if (summary.totals.failed > 0) process.exitCode = 1;
    return;
  }
  const result = await runScenario(config, config.scenario);
  if (result.verdict === "failed") process.exitCode = 1;
}
