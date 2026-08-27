import type { RunConfig } from "../config.js";
import type { Artifacts } from "./artifacts.js";
import type { CommandResult } from "./command.js";
import { runCommand } from "./command.js";

type ComposeRunner = (
  args: readonly string[],
  options?: { stream?: boolean; allowFailure?: boolean; ignoreOrphans?: boolean },
) => Promise<CommandResult>;

export async function executeUpgradeReplacement(
  compose: ComposeRunner,
  stopFitz: () => Promise<void>,
  waitReady: () => Promise<void>,
  artifacts: Artifacts,
  config: RunConfig,
  environment: NodeJS.ProcessEnv,
  targetImage: string,
  project: string,
): Promise<{ sourceImageId: string; targetImageId: string; crossVersion: boolean }> {
  const sourceImageId = await runningServiceImageId(project, config.rootDir, "fitz");
  await stopFitz();
  await compose(["rm", "-f", "fitz"], { stream: true });
  environment.FITZ_IMAGE = targetImage;
  await compose(["up", "-d", "--no-deps", "--no-build", "--force-recreate", "fitz"], {
    stream: true,
  });
  await waitReady();
  const targetImageId = await runningServiceImageId(project, config.rootDir, "fitz");
  const crossVersion = sourceImageId !== targetImageId;
  if (config.upgradeFromImage !== undefined && !crossVersion) {
    throw new Error("FITZ_UPGRADE_FROM_IMAGE resolved to the same image as the upgrade target");
  }
  await artifacts.event("fitz_upgrade_replacement_complete", {
    sourceImage: config.upgradeFromImage ?? targetImage,
    targetImage,
    sourceImageId,
    targetImageId,
    crossVersion,
  });
  return { sourceImageId, targetImageId, crossVersion };
}

async function runningServiceImageId(
  project: string,
  cwd: string,
  service: string,
): Promise<string> {
  const ps = await runCommand("docker", [
    "ps", "--filter", `label=com.docker.compose.project=${project}`,
    "--filter", `label=com.docker.compose.service=${service}`, "--format", "{{.ID}}",
  ], { cwd });
  const containers = ps.stdout.trim().split("\n").filter(Boolean);
  const container = containers[0];
  if (containers.length !== 1 || container === undefined) {
    throw new Error(`Expected one running ${service} container, found ${containers.length}`);
  }
  const inspect = await runCommand("docker", ["inspect", "--format", "{{.Image}}", container], {
    cwd,
  });
  const imageId = inspect.stdout.trim();
  if (!/^sha256:[a-f0-9]{64}$/u.test(imageId)) throw new Error(`Invalid ${service} image ID '${imageId}'`);
  return imageId;
}
