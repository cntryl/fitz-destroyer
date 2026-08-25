import { access } from "node:fs/promises";
import { join } from "node:path";
import type { RunConfig } from "../config.js";
import { ALL_DOMAINS, type Domain, type WorkloadShape } from "../workloads/model.js";
import { Artifacts } from "./artifacts.js";
import { runCommand, type CommandResult } from "./command.js";
import {
  assertNoDomainFailures,
  cleanupDelta,
  cleanupMetrics,
  isLiveDomainQuiescent,
  type LiveDomain,
  type LiveDomainSnapshot,
} from "./live-observability.js";
import {
  runConnectionStormScenario,
  runNoticeFanoutScenario,
  runRpcPressureScenario,
} from "./live-scenarios.js";
import { numericValue, parseJsonRecords } from "./workload-log.js";

type ProgressRecord = {
  event?: string;
  window?: Partial<Record<Domain, { success?: number }>>;
};

type BombardTotals = Record<Domain, { success: number; error: number }>;

export type LiveRole =
  | "notice-publisher"
  | "notice-subscriber"
  | "schedule-producer"
  | "schedule-subscriber"
  | "rpc-caller"
  | "rpc-worker"
  | "rpc-stream-caller"
  | "rpc-stream-worker";

export type RoleContainer = {
  id: string;
  name: string;
  workerId: string;
};

type ContainerState = {
  status: string;
  exitCode: number;
};

export class ComposeStack {
  readonly #config: RunConfig;
  readonly #project: string;
  readonly #namespace: string;
  readonly #artifacts: Artifacts;
  readonly #env: NodeJS.ProcessEnv;
  #jobSequence = 0;

  constructor(
    config: RunConfig,
    project: string,
    namespace: string,
    artifacts: Artifacts,
  ) {
    this.#config = config;
    this.#project = project;
    this.#namespace = namespace;
    this.#artifacts = artifacts;
    this.#env = {
      FITZ_SOURCE_DIR: config.fitzSourceDir,
      FITZ_HOST_HTTP_PORT: String(config.port),
      FITZ_STORAGE_PREFIX: namespace,
      DESTROYER_NAMESPACE: namespace,
      DESTROYER_SEED: String(config.seed),
      DESTROYER_DOMAINS: config.bombardDomains.join(","),
    };
  }

  get project(): string {
    return this.#project;
  }

  async preflight(): Promise<void> {
    await access(join(this.#config.fitzSourceDir, "Dockerfile"));
    await runCommand("docker", ["version"], { cwd: this.#config.rootDir });
    await runCommand("docker", ["compose", "version"], { cwd: this.#config.rootDir });
    await this.#artifacts.event("preflight_complete", {
      project: this.#project,
      fitzSourceDir: this.#config.fitzSourceDir,
    });
  }

  async reset(): Promise<void> {
    await this.compose(["down", "--volumes", "--remove-orphans"], { allowFailure: true });
  }

  async build(): Promise<void> {
    const startedAt = performance.now();
    await this.#artifacts.event("build_started");
    await this.compose(["build", "fitz", "client"], { stream: true });
    await this.#artifacts.event("build_complete", { elapsedMs: elapsedMs(startedAt) });
  }

  async startCore(): Promise<void> {
    await this.compose(["up", "-d", "--no-build", "sqrzl", "fitz"], { stream: true });
    await this.waitReady();
  }

  async gracefulRestartFitz(): Promise<void> {
    await this.#artifacts.event("fitz_graceful_stop_started");
    await this.compose(["stop", "-t", "20", "fitz"], { stream: true });
    await this.#artifacts.event("fitz_graceful_stop_complete");
    await this.compose(["up", "-d", "--no-deps", "--no-build", "fitz"], { stream: true });
    await this.waitReady();
  }

  async stopFitz(): Promise<void> {
    await this.#artifacts.event("fitz_final_stop_started");
    await this.compose(["stop", "-t", "20", "fitz"], { stream: true, allowFailure: true });
    await this.#artifacts.event("fitz_final_stop_complete");
  }

  async discardFitzCacheAndRestart(): Promise<void> {
    await this.#artifacts.event("fitz_cache_discard_started");
    await this.compose(["stop", "-t", "20", "fitz"], { stream: true });
    await this.captureServiceLogs("fitz", "cache-loss-fitz");
    await this.compose(["rm", "-f", "fitz"], { stream: true });
    const volumeResult = await runCommand(
      "docker",
      [
        "volume",
        "ls",
        "--filter",
        `label=com.docker.compose.project=${this.#project}`,
        "--filter",
        "label=com.docker.compose.volume=fitz-cache",
        "--format",
        "{{.Name}}",
      ],
      { cwd: this.#config.rootDir },
    );
    const volumes = volumeResult.stdout.trim().split("\n").filter(Boolean);
    if (volumes.length !== 1) {
      throw new Error(`Expected exactly one Fitz cache volume, found ${volumes.length}`);
    }
    const volume = volumes[0];
    if (volume === undefined || !volume.startsWith(`${this.#project}_`)) {
      throw new Error(`Refusing to remove unresolved cache volume '${volume ?? ""}'`);
    }
    const inspected = await runCommand("docker", ["volume", "inspect", volume], {
      cwd: this.#config.rootDir,
    });
    await this.#artifacts.write("discarded-cache-volume.json", inspected.stdout);
    await runCommand("docker", ["volume", "rm", volume], { cwd: this.#config.rootDir });
    await this.#artifacts.event("fitz_cache_discard_complete", { volume });
    await this.compose(["up", "-d", "--no-deps", "--no-build", "fitz"], { stream: true });
    await this.waitReady();
  }

  async runRecoveryJob(mode: "load" | "verify", shape: WorkloadShape): Promise<void> {
    const startedAt = performance.now();
    await this.#artifacts.event("client_job_started", { mode });
    const result = await this.compose(
      [
        "run",
        "--rm",
        "-T",
        "--no-deps",
        "-e",
        `DESTROYER_MODE=${mode}`,
        "-e",
        `DESTROYER_NAMESPACE=${shape.namespace}`,
        "-e",
        `DESTROYER_SEED=${shape.seed}`,
        "-e",
        `DESTROYER_RESOURCES=${shape.resources}`,
        "-e",
        `DESTROYER_ENTRIES=${shape.entriesPerResource}`,
        "-e",
        `DESTROYER_PAYLOAD_BYTES=${shape.payloadBytes}`,
        "-e",
        `DESTROYER_STARTUP_TIMEOUT_MS=${this.#config.startupTimeoutMs}`,
        "client",
      ],
      { stream: true },
    );
    this.#jobSequence += 1;
    await this.#artifacts.write(
      `client-job-${this.#jobSequence.toString().padStart(2, "0")}-${mode}.log`,
      `${result.stdout}${result.stderr}`,
    );
    await this.#artifacts.event("client_job_complete", { mode, elapsedMs: elapsedMs(startedAt) });
  }

  async runNoticeFanout(
    shape: WorkloadShape,
    runLabel = "notice-fanout",
  ): Promise<void> {
    await runNoticeFanoutScenario(this, this.#config, this.#artifacts, shape, runLabel);
  }

  async runRpcPressure(
    shape: WorkloadShape,
    runLabel = "rpc-pressure",
  ): Promise<void> {
    await runRpcPressureScenario(this, this.#config, this.#artifacts, shape, runLabel);
  }

  async runConnectionStorm(shape: WorkloadShape): Promise<void> {
    await runConnectionStormScenario(this, this.#config, this.#artifacts, shape);
  }

  async runDomainPressure(replicas: number, phaseMs: number): Promise<void> {
    const startedAt = performance.now();
    const since = new Date();
    await this.#artifacts.event("domain_pressure_started", {
      replicas,
      domains: this.#config.bombardDomains,
      phaseMs,
    });
    await this.startClients(replicas);
    await this.waitForAllClientDomains(since, replicas);
    await sleep(phaseMs);

    const totals = await this.bombardTotalsSince(since, replicas);
    const errors = this.#config.bombardDomains.reduce(
      (sum, domain) => sum + totals[domain].error,
      0,
    );
    await this.stopClients();
    if (errors > 0) {
      throw new Error(`Domain pressure observed ${errors} client errors: ${JSON.stringify(totals)}`);
    }
    await this.#artifacts.event("domain_pressure_complete", {
      replicas,
      domains: this.#config.bombardDomains,
      totals,
      elapsedMs: elapsedMs(startedAt),
    });
  }

  async startClients(replicas: number): Promise<void> {
    await this.compose(
      ["up", "-d", "--no-deps", "--no-build", "--scale", `client=${replicas}`, "client"],
      { stream: true },
    );
  }

  async stopClients(): Promise<void> {
    await this.compose(["stop", "-t", "10", "client"], { stream: true, allowFailure: true });
  }

  async killFitz(): Promise<void> {
    await this.#artifacts.event("fitz_sigkill_started");
    await this.killAndRemoveService("fitz", "fitz-sigkill");
    await this.#artifacts.event("fitz_sigkill_complete");
  }

  async restartFitz(): Promise<void> {
    await this.compose(["up", "-d", "--no-deps", "--no-build", "fitz"], { stream: true });
    await this.waitReady();
  }

  async killOneClientAndRestore(replicas: number): Promise<string> {
    const containers = await this.serviceContainers("client");
    const target = containers.sort()[0];
    if (target === undefined) throw new Error("No running client container is available to kill");
    const inspect = await runCommand(
      "docker",
      ["inspect", "--format", "{{.Name}}", target],
      { cwd: this.#config.rootDir },
    );
    const name = inspect.stdout.trim();
    await this.#artifacts.event("client_sigkill_started", { containerId: target, name });
    await runCommand("docker", ["kill", "--signal", "SIGKILL", target], {
      cwd: this.#config.rootDir,
    });
    await this.captureContainerLogs(target, "client-sigkill");
    await runCommand("docker", ["rm", target], { cwd: this.#config.rootDir });
    await this.startClients(replicas);
    await this.#artifacts.event("client_replica_restored", { killedContainerId: target, name });
    return target;
  }

  async killSqrzlAndRestore(): Promise<void> {
    await this.#artifacts.event("sqrzl_sigkill_started");
    await this.killAndRemoveService("sqrzl", "sqrzl-sigkill");
    await this.#artifacts.event("sqrzl_sigkill_complete");
    await sleep(this.#config.phaseMs);
    await this.compose(["up", "-d", "--no-deps", "sqrzl"], { stream: true });
    await this.#artifacts.event("sqrzl_restarted");
    // A provider outage can stop an individual durable actor before the
    // process-level lease health path exits Fitz. Recycle the broker after the
    // emulator is back so every domain is reconstructed from Sqrzl instead of
    // trusting a momentarily stale readiness response.
    await this.killAndRemoveService("fitz", "fitz-after-sqrzl", true);
    await this.compose(["up", "-d", "--no-deps", "--no-build", "fitz"], { stream: true });
    await this.waitReady();
    await this.#artifacts.event("fitz_recycled_after_sqrzl_fault");
  }

  async replaceClients(replicas: number): Promise<void> {
    await this.#artifacts.event("client_replica_set_replacement_started", { replicas });
    const containers = await this.serviceContainers("client", true);
    await this.compose(["kill", "-s", "SIGKILL", "client"], {
      stream: true,
      allowFailure: true,
    });
    for (const container of containers) {
      await this.captureContainerLogs(container, "client-after-infrastructure-fault");
    }
    await this.compose(["rm", "-f", "client"], { stream: true, allowFailure: true });
    await this.startClients(replicas);
    await this.#artifacts.event("client_replica_set_replacement_complete", { replicas });
  }

  async waitForAllClientDomains(since: Date, replicas: number): Promise<void> {
    const deadline = Date.now() + this.#config.startupTimeoutMs;
    let lastStatus = "waiting for client containers";
    while (Date.now() < deadline) {
      const containers = await this.serviceContainers("client");
      if (containers.length === replicas) {
        const missingByContainer: Record<string, Domain[]> = {};
        for (const container of containers) {
          const logs = await runCommand(
            "docker",
            ["logs", "--since", since.toISOString(), container],
            { cwd: this.#config.rootDir, allowFailure: true },
          );
          const successes = parseWindowSuccesses(`${logs.stdout}\n${logs.stderr}`);
          const missing = this.#config.bombardDomains.filter((domain) => successes[domain] === 0);
          if (missing.length > 0) missingByContainer[container] = missing;
        }
        if (Object.keys(missingByContainer).length === 0) {
          await this.#artifacts.event("client_domain_progress_confirmed", {
            since: since.toISOString(),
            replicas,
            domains: this.#config.bombardDomains,
          });
          return;
        }
        lastStatus = JSON.stringify(missingByContainer);
      } else {
        lastStatus = `running replicas ${containers.length}/${replicas}`;
      }
      await sleep(1_000);
    }
    throw new Error(`Timed out waiting for fresh client success in every domain: ${lastStatus}`);
  }

  private async bombardTotalsSince(since: Date, replicas: number): Promise<BombardTotals> {
    const containers = await this.serviceContainers("client");
    if (containers.length !== replicas) {
      throw new Error(`Expected ${replicas} bombard clients, found ${containers.length}`);
    }
    const totals = emptyBombardTotals();
    for (const [index, container] of containers.entries()) {
      const logs = await runCommand(
        "docker",
        ["logs", "--since", since.toISOString(), container],
        { cwd: this.#config.rootDir, allowFailure: true },
      );
      const log = `${logs.stdout}\n${logs.stderr}`;
      await this.#artifacts.write(
        `domain-pressure-client-${index.toString().padStart(3, "0")}.log`,
        log,
      );
      const latest = parseJsonRecords(log)
        .filter((record) => record.event === "progress" && record.totals !== undefined)
        .at(-1)?.totals;
      if (latest === undefined || typeof latest !== "object" || latest === null) {
        throw new Error(`Bombard client ${container} did not report cumulative totals`);
      }
      for (const domain of this.#config.bombardDomains) {
        const value = (latest as Record<string, unknown>)[domain];
        if (typeof value !== "object" || value === null) {
          throw new Error(`Bombard client ${container} omitted ${domain} totals`);
        }
        const record = value as Record<string, unknown>;
        totals[domain].success += numericValue(record.success, `${domain}.success`);
        totals[domain].error += numericValue(record.error, `${domain}.error`);
      }
    }
    return totals;
  }

  async collect(): Promise<void> {
    const logs = await this.compose(["logs", "--no-color", "--timestamps"], {
      allowFailure: true,
    });
    await this.#artifacts.write("compose.log", `${logs.stdout}${logs.stderr}`);
    const ps = await this.compose(["ps", "--all", "--format", "json"], {
      allowFailure: true,
    });
    await this.#artifacts.write("compose-ps.json", ps.stdout || "[]\n");
  }

  async cleanup(): Promise<void> {
    await this.compose(["down", "--volumes", "--remove-orphans"], { stream: true });
    await this.#artifacts.event("cleanup_complete", { project: this.#project });
  }

  cleanupCommand(): string {
    return `docker compose -f ${join(this.#config.rootDir, "compose.yml")} --project-name ${this.#project} --profile clients down --volumes --remove-orphans`;
  }

  async waitForLiveDomainQuiescence(
    domain: LiveDomain,
    baseline: LiveDomainSnapshot,
    runLabel: string,
    allowDomainFailures = false,
  ): Promise<LiveDomainSnapshot> {
    const startedAt = performance.now();
    const deadline = Date.now() + Math.min(this.#config.startupTimeoutMs, 15_000);
    let snapshot = await this.liveDomainSnapshot(domain);
    while (Date.now() < deadline && !isLiveDomainQuiescent(domain, snapshot)) {
      await sleep(100);
      snapshot = await this.liveDomainSnapshot(domain);
    }

    await this.#artifacts.writeJson(`${runLabel}-stats-after.json`, snapshot);
    if (!isLiveDomainQuiescent(domain, snapshot)) {
      throw new Error(
        `${domain} did not quiesce after live clients disconnected: ${JSON.stringify(snapshot)}`,
      );
    }

    if (!allowDomainFailures) assertNoDomainFailures(domain, baseline.domain, snapshot.domain);
    const cleanup = cleanupDelta(baseline.cleanup, snapshot.cleanup);
    await this.#artifacts.event("live_domain_quiescent", {
      runLabel,
      domain,
      elapsedMs: elapsedMs(startedAt),
      cleanup,
    });
    return { domain: snapshot.domain, cleanup };
  }

  async liveDomainSnapshot(domain: LiveDomain): Promise<LiveDomainSnapshot> {
    const [domainStats, metrics] = await Promise.all([
      this.fetchJson(`/api/v1/all/${domain}/stats`),
      this.fetchJson("/api/v1/all/metrics"),
    ]);
    return {
      domain: domainStats,
      cleanup: cleanupMetrics(metrics),
    };
  }

  async waitForRpcWorkerCount(expected: number): Promise<void> {
    const deadline = Date.now() + this.#config.startupTimeoutMs;
    let registered: unknown = "unknown";
    while (Date.now() < deadline) {
      const snapshot = await this.liveDomainSnapshot("rpc");
      registered = snapshot.domain.workers_registered;
      if (registered === expected) return;
      await sleep(100);
    }
    throw new Error(`Timed out waiting for ${expected} RPC workers; observed ${String(registered)}`);
  }

  private async fetchJson(path: string): Promise<Readonly<Record<string, unknown>>> {
    const response = await fetch(`http://127.0.0.1:${this.#config.port}${path}`, {
      signal: AbortSignal.timeout(2_000),
    });
    if (!response.ok) {
      throw new Error(`${path} returned HTTP ${response.status}: ${(await response.text()).slice(0, 500)}`);
    }
    const value: unknown = await response.json();
    if (typeof value !== "object" || value === null || Array.isArray(value)) {
      throw new Error(`${path} did not return a JSON object`);
    }
    return value as Readonly<Record<string, unknown>>;
  }

  private async waitReady(): Promise<void> {
    const startedAt = performance.now();
    const deadline = Date.now() + this.#config.startupTimeoutMs;
    let lastError = "no response";
    while (Date.now() < deadline) {
      try {
        const response = await fetch(`http://127.0.0.1:${this.#config.port}/readyz`, {
          signal: AbortSignal.timeout(2_000),
        });
        if (response.status === 200) {
          await this.#artifacts.event("fitz_ready", { elapsedMs: elapsedMs(startedAt) });
          return;
        }
        lastError = `HTTP ${response.status}: ${(await response.text()).slice(0, 500)}`;
      } catch (error) {
        lastError = error instanceof Error ? error.message : String(error);
      }
      await sleep(250);
    }
    throw new Error(`Fitz did not become ready: ${lastError}`);
  }

  private async killAndRemoveService(
    service: "fitz" | "sqrzl",
    artifactLabel: string,
    allowMissing = false,
  ): Promise<void> {
    const containers = await this.serviceContainers(service, true);
    await this.compose(["kill", "-s", "SIGKILL", service], {
      stream: true,
      allowFailure: allowMissing,
    });
    for (const container of containers) {
      await this.captureContainerLogs(container, artifactLabel);
    }
    await this.compose(["rm", "-f", service], { stream: true, allowFailure: allowMissing });
  }

  private async captureServiceLogs(service: string, label: string): Promise<void> {
    for (const container of await this.serviceContainers(service, true)) {
      await this.captureContainerLogs(container, label);
    }
  }

  private async captureContainerLogs(container: string, label: string): Promise<void> {
    const logs = await runCommand("docker", ["logs", "--timestamps", container], {
      cwd: this.#config.rootDir,
      allowFailure: true,
    });
    await this.#artifacts.write(`${label}-${container.slice(0, 12)}.log`, `${logs.stdout}${logs.stderr}`);
  }

  async startRoleContainers(
    role: LiveRole,
    replicas: number,
    shape: WorkloadShape,
    extraEnv: Readonly<Record<string, string>> = {},
  ): Promise<RoleContainer[]> {
    const containers: RoleContainer[] = [];
    for (let index = 0; index < replicas; index += 1) {
      const workerId = String(index);
      const name = `${this.#project}-${role}-${index.toString().padStart(3, "0")}`;
      const environment = {
        DESTROYER_MODE: role,
        DESTROYER_NAMESPACE: shape.namespace,
        DESTROYER_WORKER_ID: workerId,
        DESTROYER_OPERATIONS: String(shape.entriesPerResource),
        DESTROYER_PAYLOAD_BYTES: String(shape.payloadBytes),
        DESTROYER_CONCURRENCY: String(this.#config.liveConcurrency),
        DESTROYER_ASYNC_HANDLER_CONCURRENCY: String(this.#config.liveConcurrency),
        DESTROYER_HANDLER_DELAY_MS: String(this.#config.handlerDelayMs),
        DESTROYER_JOB_TIMEOUT_MS: String(this.#config.startupTimeoutMs),
        ...extraEnv,
      };
      const args = ["run", "-d", "--no-deps", "--name", name];
      for (const [key, value] of Object.entries(environment)) {
        args.push("-e", `${key}=${value}`);
      }
      args.push("client");
      const result = await this.compose(args, { stream: true, ignoreOrphans: true });
      const id = result.stdout.trim().split("\n").at(-1)?.trim();
      if (id === undefined || !/^[a-f0-9]{12,64}$/u.test(id)) {
        throw new Error(`Could not resolve ${role} container ID from '${result.stdout.trim()}'`);
      }
      containers.push({ id, name, workerId });
    }
    await this.#artifacts.event("live_role_started", { role, replicas });
    return containers;
  }

  async waitForRoleEvent(containers: readonly RoleContainer[], event: string): Promise<void> {
    const deadline = Date.now() + this.#config.startupTimeoutMs;
    let missing = containers.map((container) => container.name);
    while (Date.now() < deadline) {
      missing = [];
      for (const container of containers) {
        const log = await this.containerLogs(container.id);
        if (!parseJsonRecords(log).some((record) => record.event === event)) {
          const state = await this.containerState(container.id);
          if (state.status === "exited" || state.status === "dead") {
            await this.captureRoleLogs(containers, container.name.split("-").slice(-3, -1).join("-"));
            throw new Error(`${container.name} exited ${state.exitCode} before ${event}`);
          }
          missing.push(container.name);
        }
      }
      if (missing.length === 0) {
        await this.#artifacts.event("live_role_ready", { event, replicas: containers.length });
        return;
      }
      await sleep(200);
    }
    await this.captureRoleLogs(containers, `${event}-timeout`);
    throw new Error(`Timed out waiting for ${event}: ${missing.join(", ")}`);
  }

  async finishRoleContainers(
    containers: readonly RoleContainer[],
    label: string,
  ): Promise<Map<string, string>> {
    const deadline = Date.now() + this.#config.startupTimeoutMs;
    let states = new Map<string, ContainerState>();
    while (Date.now() < deadline) {
      states = new Map();
      for (const container of containers) {
        states.set(container.id, await this.containerState(container.id));
      }
      if ([...states.values()].every((state) => state.status === "exited" || state.status === "dead")) {
        break;
      }
      await sleep(200);
    }
    const logs = await this.captureRoleLogs(containers, label);
    const running = containers.filter((container) => {
      const state = states.get(container.id);
      return state === undefined || (state.status !== "exited" && state.status !== "dead");
    });
    if (running.length > 0) {
      throw new Error(`Timed out waiting for ${label}: ${running.map((item) => item.name).join(", ")}`);
    }
    const failed = containers.filter((container) => states.get(container.id)?.exitCode !== 0);
    if (failed.length > 0) {
      const details = failed
        .map((container) => `${container.name}=${states.get(container.id)?.exitCode ?? "unknown"}`)
        .join(", ");
      throw new Error(`${label} containers failed: ${details}`);
    }
    await runCommand("docker", ["rm", ...containers.map(({ id }) => id)], {
      cwd: this.#config.rootDir,
    });
    await this.#artifacts.event("live_role_complete", { role: label, replicas: containers.length });
    return logs;
  }

  async signalRoleContainers(
    containers: readonly RoleContainer[],
    signal: "SIGTERM" | "SIGKILL" | "SIGUSR1",
  ): Promise<void> {
    await runCommand("docker", ["kill", "--signal", signal, ...containers.map(({ id }) => id)], {
      cwd: this.#config.rootDir,
      allowFailure: true,
    });
  }

  async killRoleContainers(
    containers: readonly RoleContainer[],
    label: string,
  ): Promise<Map<string, string>> {
    await this.signalRoleContainers(containers, "SIGKILL");
    const logs = await this.captureRoleLogs(containers, label);
    await runCommand("docker", ["rm", ...containers.map(({ id }) => id)], {
      cwd: this.#config.rootDir,
    });
    await this.#artifacts.event("live_role_killed", {
      role: label,
      replicas: containers.length,
    });
    return logs;
  }

  private async captureRoleLogs(
    containers: readonly RoleContainer[],
    label: string,
  ): Promise<Map<string, string>> {
    const logs = new Map<string, string>();
    for (const container of containers) {
      const log = await this.containerLogs(container.id);
      logs.set(container.workerId, log);
      await this.#artifacts.write(`${label}-${container.workerId.padStart(3, "0")}.log`, log);
    }
    return logs;
  }

  private async containerLogs(container: string): Promise<string> {
    const logs = await runCommand("docker", ["logs", container], {
      cwd: this.#config.rootDir,
      allowFailure: true,
    });
    return `${logs.stdout}${logs.stderr}`;
  }

  private async containerState(container: string): Promise<ContainerState> {
    const result = await runCommand(
      "docker",
      ["inspect", "--format", "{{.State.Status}} {{.State.ExitCode}}", container],
      { cwd: this.#config.rootDir },
    );
    const [status, exitCodeRaw] = result.stdout.trim().split(/\s+/u);
    const exitCode = Number(exitCodeRaw);
    if (status === undefined || !Number.isInteger(exitCode)) {
      throw new Error(`Invalid container state '${result.stdout.trim()}' for ${container}`);
    }
    return { status, exitCode };
  }

  private async serviceContainers(service: string, includeStopped = false): Promise<string[]> {
    const args = ["ps"];
    if (includeStopped) args.push("--all");
    args.push(
      "--filter",
      `label=com.docker.compose.project=${this.#project}`,
      "--filter",
      `label=com.docker.compose.service=${service}`,
      "--format",
      "{{.ID}}",
    );
    const result = await runCommand(
      "docker",
      args,
      { cwd: this.#config.rootDir },
    );
    return result.stdout.trim().split("\n").filter(Boolean);
  }

  private compose(
    args: readonly string[],
    options: { stream?: boolean; allowFailure?: boolean; ignoreOrphans?: boolean } = {},
  ): Promise<CommandResult> {
    return runCommand(
      "docker",
      [
        "compose",
        "-f",
        join(this.#config.rootDir, "compose.yml"),
        "--project-name",
        this.#project,
        "--profile",
        "clients",
        ...args,
      ],
      {
        cwd: this.#config.rootDir,
        env: {
          ...this.#env,
          ...(options.ignoreOrphans === true ? { COMPOSE_IGNORE_ORPHANS: "true" } : {}),
        },
        ...(options.stream === undefined ? {} : { stream: options.stream }),
        ...(options.allowFailure === undefined ? {} : { allowFailure: options.allowFailure }),
      },
    );
  }
}

function parseWindowSuccesses(logs: string): Record<Domain, number> {
  const successes = Object.fromEntries(ALL_DOMAINS.map((domain) => [domain, 0])) as Record<Domain, number>;
  for (const line of logs.split("\n")) {
    const jsonStart = line.indexOf("{");
    if (jsonStart < 0) continue;
    try {
      const record = JSON.parse(line.slice(jsonStart)) as ProgressRecord;
      if (record.event !== "progress" || record.window === undefined) continue;
      for (const domain of ALL_DOMAINS) successes[domain] += record.window[domain]?.success ?? 0;
    } catch {
      // npm prelude and Docker diagnostics are not workload progress records.
    }
  }
  return successes;
}

function emptyBombardTotals(): BombardTotals {
  return Object.fromEntries(
    ALL_DOMAINS.map((domain) => [domain, { success: 0, error: 0 }]),
  ) as BombardTotals;
}

function elapsedMs(startedAt: number): number {
  return Math.round(performance.now() - startedAt);
}

function sleep(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}
