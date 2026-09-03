import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

const ROOT = new URL("../", import.meta.url);

test("should_forward_actor_controls_through_the_published_image_compose_file", async () => {
  const compose = await readFile(new URL("compose.destroyer.yml", ROOT), "utf8");

  assert.match(compose, /cpus: "\$\{FITZ_CPU_LIMIT:-0\}"/u);
  assert.match(
    compose,
    /FITZ_DESTROYER_FAILPOINTS: "\$\{FITZ_DESTROYER_FAILPOINTS:-disabled\}"/u,
  );
});

test("should_run_every_family_actor_scenario_in_the_hosted_matrix", async () => {
  const workflow = await readFile(new URL(".github/workflows/destroyer.yml", ROOT), "utf8");
  const scenarios = [
    "family-actor-partial-failure-isolation",
    "same-shard-family-failure-isolation",
    "family-actor-exhaustion-readiness",
    "family-actor-degradation-observability",
    "family-actor-inflight-concurrent-failure",
  ];

  for (const scenario of scenarios) assert.match(workflow, new RegExp(`- ${scenario}\\n`, "u"));
});
