import { spawn } from "node:child_process";

export type CommandResult = {
  stdout: string;
  stderr: string;
  exitCode: number;
};

type CommandOptions = {
  cwd: string;
  env?: NodeJS.ProcessEnv;
  stream?: boolean;
  allowFailure?: boolean;
};

export async function runCommand(
  executable: string,
  args: readonly string[],
  options: CommandOptions,
): Promise<CommandResult> {
  const child = spawn(executable, args, {
    cwd: options.cwd,
    env: { ...process.env, ...options.env },
    stdio: ["ignore", "pipe", "pipe"],
  });
  let stdout = "";
  let stderr = "";
  child.stdout.setEncoding("utf8");
  child.stderr.setEncoding("utf8");
  child.stdout.on("data", (chunk: string) => {
    stdout += chunk;
    if (options.stream) process.stdout.write(chunk);
  });
  child.stderr.on("data", (chunk: string) => {
    stderr += chunk;
    if (options.stream) process.stderr.write(chunk);
  });

  const exitCode = await new Promise<number>((resolve, reject) => {
    child.once("error", reject);
    child.once("close", (code) => resolve(code ?? 1));
  });
  const result = { stdout, stderr, exitCode };
  if (exitCode !== 0 && !options.allowFailure) {
    const details = `${stdout}\n${stderr}`.trim().slice(-8_000);
    throw new Error(`${executable} ${args.join(" ")} exited ${exitCode}\n${details}`);
  }
  return result;
}
