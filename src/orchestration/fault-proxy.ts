export type FaultProxyFault =
  | { mode: "healthy" }
  | { mode: "latency"; latencyMs: number }
  | { mode: "reset" }
  | { mode: "partition" }
  | { mode: "blackhole" }
  | { mode: "downstream-drop" };
