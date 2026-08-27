import { open, rm } from "node:fs/promises";

const target = requiredTarget(process.env.DESTROYER_FILL_TARGET);
const action = process.env.DESTROYER_FILL_ACTION ?? "fill";
const path = `/volumes/${target}/destroyer-exhaustion-filler`;

if (action === "remove") {
  await rm(path, { force: true });
  log("disk_filler_removed", { target, path });
} else if (action === "fill") {
  const handle = await open(path, "w");
  const block = Buffer.alloc(1024 * 1024, 0xa5);
  let bytesWritten = 0;
  let exhausted = false;
  try {
    while (true) {
      try {
        const result = await handle.write(block);
        bytesWritten += result.bytesWritten;
        if (result.bytesWritten !== block.length) {
          exhausted = true;
          break;
        }
      } catch (error) {
        if (errorCode(error) !== "ENOSPC") throw error;
        exhausted = true;
        break;
      }
    }
    await handle.sync().catch((error: unknown) => {
      if (errorCode(error) !== "ENOSPC") throw error;
      exhausted = true;
    });
  } finally {
    await handle.close();
  }
  if (!exhausted) throw new Error(`Volume ${target} accepted the entire unbounded filler write`);
  log("disk_filler_complete", { target, path, bytesWritten, exhausted });
} else {
  throw new Error(`DESTROYER_FILL_ACTION must be fill or remove, received '${action}'`);
}

function requiredTarget(value: string | undefined): "cache" | "storage" {
  if (value === "cache" || value === "storage") return value;
  throw new Error("DESTROYER_FILL_TARGET must be cache or storage");
}

function errorCode(error: unknown): unknown {
  return typeof error === "object" && error !== null && "code" in error
    ? (error as { code?: unknown }).code
    : undefined;
}

function log(event: string, fields: Readonly<Record<string, unknown>>): void {
  process.stdout.write(`${JSON.stringify({ timestamp: new Date().toISOString(), event, ...fields })}\n`);
}
