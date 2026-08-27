import http from "node:http";
import net from "node:net";

const proxyName = process.env.FAULT_PROXY_NAME ?? "storage-proxy";
const eventPrefix = proxyName.replaceAll("-", "_");
const upstreamHost = process.env.FAULT_PROXY_UPSTREAM_HOST ??
  process.env.STORAGE_PROXY_UPSTREAM_HOST ?? "sqrzl";
const upstreamPort = integerEnv(
  "FAULT_PROXY_UPSTREAM_PORT",
  integerEnv("STORAGE_PROXY_UPSTREAM_PORT", 9_000),
);
const dataPort = integerEnv(
  "FAULT_PROXY_DATA_PORT",
  integerEnv("STORAGE_PROXY_DATA_PORT", 9_000),
);
const controlPort = integerEnv(
  "FAULT_PROXY_CONTROL_PORT",
  integerEnv("STORAGE_PROXY_CONTROL_PORT", 9_100),
);
let fault = { mode: "healthy", latencyMs: 0 };
const connections = new Set();

const dataServer = net.createServer((client) => {
  const connection = { client, upstream: undefined, timers: new Set() };
  connections.add(connection);
  client.setNoDelay(true);
  client.on("error", () => closeConnection(connection));
  client.on("close", () => closeConnection(connection));

  if (fault.mode === "reset") {
    client.destroy();
    return;
  }
  if (fault.mode === "partition") return;
  connectUpstream(connection);
});

const controlServer = http.createServer(async (request, response) => {
  if (request.method === "GET" && request.url === "/status") {
    json(response, 200, { ...fault, connections: connections.size });
    return;
  }
  if (request.method === "PUT" && request.url === "/fault") {
    try {
      const body = JSON.parse(await readBody(request));
      const mode = body?.mode;
      const latencyMs = body?.latencyMs ?? 0;
      if (!["healthy", "latency", "reset", "partition", "blackhole", "downstream-drop", "downstream-pause"].includes(mode)) {
        throw new Error(
          "mode must be healthy, latency, reset, partition, blackhole, downstream-drop, or downstream-pause",
        );
      }
      if (!Number.isSafeInteger(latencyMs) || latencyMs < 0 || latencyMs > 60_000) {
        throw new Error("latencyMs must be an integer between 0 and 60000");
      }
      fault = { mode, latencyMs: mode === "latency" ? latencyMs : 0 };
      for (const connection of connections) applyDownstreamReadState(connection);
      if (mode === "reset" || mode === "partition" || mode === "healthy") {
        for (const connection of [...connections]) closeConnection(connection);
      }
      process.stdout.write(`${JSON.stringify({ event: `${eventPrefix}_fault_changed`, ...fault })}\n`);
      json(response, 200, { ...fault, connections: connections.size });
    } catch (error) {
      json(response, 400, { error: error instanceof Error ? error.message : String(error) });
    }
    return;
  }
  json(response, 404, { error: "not found" });
});

dataServer.listen(dataPort, "0.0.0.0", () => {
  process.stdout.write(`${JSON.stringify({ event: `${eventPrefix}_data_ready`, dataPort, upstreamHost, upstreamPort })}\n`);
});
controlServer.listen(controlPort, "0.0.0.0", () => {
  process.stdout.write(`${JSON.stringify({ event: `${eventPrefix}_control_ready`, controlPort })}\n`);
});

function connectUpstream(connection) {
  if (connection.client.destroyed) return;
  const upstream = net.createConnection({ host: upstreamHost, port: upstreamPort });
  connection.upstream = upstream;
  upstream.setNoDelay(true);
  upstream.on("connect", () => {
    forward(connection, connection.client, upstream, "upstream");
    forward(connection, upstream, connection.client, "downstream");
    applyDownstreamReadState(connection);
  });
  upstream.on("error", () => closeConnection(connection));
  upstream.on("close", () => closeConnection(connection));
}

function forward(connection, source, destination, direction) {
  source.on("data", (chunk) => {
    if (fault.mode === "reset") {
      closeConnection(connection);
      return;
    }
    if (
      fault.mode === "partition" ||
      fault.mode === "blackhole" ||
      ((fault.mode === "downstream-drop" || fault.mode === "downstream-pause") &&
        direction === "downstream")
    ) return;
    if (fault.mode === "latency" && fault.latencyMs > 0) {
      const timer = setTimeout(() => {
        connection.timers.delete(timer);
        if (!destination.destroyed) destination.write(chunk);
      }, fault.latencyMs);
      connection.timers.add(timer);
      return;
    }
    if (!destination.destroyed) destination.write(chunk);
  });
  source.on("end", () => destination.end());
}

function applyDownstreamReadState(connection) {
  if (connection.upstream === undefined || connection.upstream.destroyed) return;
  if (fault.mode === "downstream-pause") connection.upstream.pause();
  else connection.upstream.resume();
}

function closeConnection(connection) {
  if (!connections.delete(connection)) return;
  for (const timer of connection.timers) clearTimeout(timer);
  connection.timers.clear();
  connection.client.destroy();
  connection.upstream?.destroy();
}

function integerEnv(name, fallback) {
  const value = Number(process.env[name] ?? fallback);
  if (!Number.isSafeInteger(value) || value < 1 || value > 65_535) {
    throw new Error(`${name} must be a valid TCP port`);
  }
  return value;
}

function readBody(request) {
  return new Promise((resolve, reject) => {
    let body = "";
    request.setEncoding("utf8");
    request.on("data", (chunk) => {
      body += chunk;
      if (body.length > 16_384) reject(new Error("request body is too large"));
    });
    request.on("end", () => resolve(body));
    request.on("error", reject);
  });
}

function json(response, status, value) {
  response.writeHead(status, { "content-type": "application/json" });
  response.end(`${JSON.stringify(value)}\n`);
}
