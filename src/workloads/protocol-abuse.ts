import type { LiveLog } from "./live.js";

type RawAttack = {
  name: string;
  frames: readonly (string | Uint8Array)[];
};

const CONNECT = new Uint8Array([1, 0, 0]);
const ATTACKS: readonly RawAttack[] = [
  { name: "text-before-connect", frames: ["this is not Fitz"] },
  { name: "empty-binary", frames: [new Uint8Array()] },
  { name: "truncated-tlv", frames: [new Uint8Array([1])] },
  { name: "domain-before-connect", frames: [new Uint8Array([100, 0, 0])] },
  { name: "unknown-extended-type", frames: [new Uint8Array([0xff, 0xff, 0xff, 0, 0])] },
  {
    name: "duplicate-tag-after-connect",
    frames: [CONNECT, new Uint8Array([100, 0, 0, 100, 0, 0])],
  },
  {
    name: "declared-value-truncated",
    frames: [new Uint8Array([1, 0xff, 0])],
  },
  {
    name: "oversize-frame",
    frames: [new Uint8Array(1_048_577)],
  },
];

export async function runProtocolAbuse(
  url: string,
  operations: number,
  concurrency: number,
  signal: AbortSignal,
  log: LiveLog,
): Promise<void> {
  let completed = 0;
  await runConcurrent(operations, concurrency, signal, async (sequence) => {
    const attack = ATTACKS[sequence % ATTACKS.length];
    if (attack === undefined) throw new Error("Protocol attack table is empty");
    const outcome = await sendAttack(url, attack, signal);
    completed += 1;
    log("protocol_attack_complete", { sequence, attack: attack.name, outcome });
  });
  log("protocol_abuse_complete", { completed, attacks: ATTACKS.map(({ name }) => name) });
}

export function protocolAttackNames(): readonly string[] {
  return ATTACKS.map(({ name }) => name);
}

async function sendAttack(
  url: string,
  attack: RawAttack,
  signal: AbortSignal,
): Promise<string> {
  const socket = new WebSocket(url);
  socket.binaryType = "arraybuffer";
  await waitForOpen(socket, signal);
  let closedByBroker = false;
  socket.addEventListener("close", () => {
    closedByBroker = true;
  });
  for (const frame of attack.frames) socket.send(frame);
  await sleepWithSignal(25, signal);
  if (socket.readyState === WebSocket.OPEN) socket.close(1000, "destroyer attack complete");
  await waitForClose(socket, signal);
  return closedByBroker ? "closed" : "client-closed";
}

function waitForOpen(socket: WebSocket, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error("Raw WebSocket open timed out")), 10_000);
    const complete = (callback: () => void): void => {
      clearTimeout(timeout);
      callback();
    };
    socket.addEventListener("open", () => complete(resolve), { once: true });
    socket.addEventListener("error", () => complete(() => reject(new Error("Raw WebSocket failed"))), {
      once: true,
    });
    signal.addEventListener("abort", () => complete(() => reject(signal.reason)), { once: true });
  });
}

function waitForClose(socket: WebSocket, signal: AbortSignal): Promise<void> {
  if (socket.readyState === WebSocket.CLOSED) return Promise.resolve();
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      socket.close();
      resolve();
    }, 1_000);
    socket.addEventListener("close", () => {
      clearTimeout(timeout);
      resolve();
    }, { once: true });
    signal.addEventListener("abort", () => {
      clearTimeout(timeout);
      socket.close();
      reject(signal.reason);
    }, { once: true });
  });
}

async function runConcurrent(
  count: number,
  concurrency: number,
  signal: AbortSignal,
  operation: (sequence: number) => Promise<void>,
): Promise<void> {
  let next = 0;
  const worker = async (): Promise<void> => {
    while (next < count) {
      signal.throwIfAborted();
      const sequence = next;
      next += 1;
      await operation(sequence);
    }
  };
  await Promise.all(Array.from({ length: Math.min(count, concurrency) }, worker));
}

function sleepWithSignal(milliseconds: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(resolve, milliseconds);
    signal.addEventListener("abort", () => {
      clearTimeout(timeout);
      reject(signal.reason);
    }, { once: true });
  });
}
