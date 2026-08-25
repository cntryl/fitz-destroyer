import { appendFile, mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";

export class Artifacts {
  readonly directory: string;
  readonly #eventsPath: string;

  private constructor(directory: string) {
    this.directory = directory;
    this.#eventsPath = join(directory, "events.ndjson");
  }

  static async create(rootDir: string, runId: string): Promise<Artifacts> {
    const directory = join(rootDir, "artifacts", runId);
    await mkdir(directory, { recursive: true });
    return new Artifacts(directory);
  }

  async event(event: string, fields: Record<string, unknown> = {}): Promise<void> {
    const record = { timestamp: new Date().toISOString(), event, ...fields };
    await appendFile(this.#eventsPath, `${JSON.stringify(record)}\n`, "utf8");
    process.stdout.write(`${JSON.stringify(record)}\n`);
  }

  async write(name: string, content: string): Promise<void> {
    await writeFile(join(this.directory, name), content, "utf8");
  }

  async writeJson(name: string, value: unknown): Promise<void> {
    await this.write(name, `${JSON.stringify(value, null, 2)}\n`);
  }
}
