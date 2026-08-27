import { connect as connectTcp } from "node:net";

export type RawProtocolRecord = { type: number; payload: Uint8Array };

export type RawProtocolConnection = {
  readonly transport: "ws" | "tcp";
  send(frame: Uint8Array): Promise<void>;
  sendRaw(data: Uint8Array): Promise<void>;
  receive(timeoutMs: number): Promise<Uint8Array>;
  close(): Promise<void>;
  destroy(): void;
  waitForClose(timeoutMs: number): Promise<void>;
};

const textEncoder = new TextEncoder();

export function encodeTlv(type: number, payload: Uint8Array = new Uint8Array()): Uint8Array {
  if (!Number.isInteger(type) || type < 0 || type > 0xffff) {
    throw new Error(`TLV type ${type} is outside u16`);
  }
  if (payload.length > 0xffff) throw new Error(`TLV payload is too large: ${payload.length}`);
  const typeBytes = type <= 0xfe ? 1 : 3;
  const result = new Uint8Array(typeBytes + 2 + payload.length);
  let offset = 0;
  if (type <= 0xfe) result[offset++] = type;
  else {
    result[offset++] = 0xff;
    result[offset++] = type >>> 8;
    result[offset++] = type & 0xff;
  }
  result[offset++] = payload.length >>> 8;
  result[offset++] = payload.length & 0xff;
  result.set(payload, offset);
  return result;
}

export function decodeTlvs(frame: Uint8Array): RawProtocolRecord[] {
  const records: RawProtocolRecord[] = [];
  let offset = 0;
  while (offset < frame.length) {
    const marker = frame[offset++];
    if (marker === undefined) throw new Error("TLV frame ended in type");
    let type: number;
    if (marker === 0xff) {
      if (offset + 2 > frame.length) throw new Error("TLV frame ended in escaped type");
      type = (frame[offset++]! << 8) | frame[offset++]!;
    } else {
      type = marker;
    }
    if (offset + 2 > frame.length) throw new Error("TLV frame ended in length");
    const length = (frame[offset++]! << 8) | frame[offset++]!;
    if (offset + length > frame.length) {
      throw new Error(`TLV value is incomplete: need ${length}, have ${frame.length - offset}`);
    }
    records.push({ type, payload: frame.slice(offset, offset + length) });
    offset += length;
  }
  if (records.length === 0) throw new Error("TLV frame is empty");
  return records;
}

export function encodeString(value: string): Uint8Array {
  return encodeBytes(textEncoder.encode(value));
}

export function encodeBytes(value: Uint8Array): Uint8Array {
  const result = new Uint8Array(4 + value.length);
  new DataView(result.buffer).setUint32(0, value.length);
  result.set(value, 4);
  return result;
}

export function encodeU64(value: bigint): Uint8Array {
  const result = new Uint8Array(8);
  new DataView(result.buffer).setBigUint64(0, value);
  return result;
}

export function encodeU32(value: number): Uint8Array {
  if (!Number.isInteger(value) || value < 0 || value > 0xffff_ffff) {
    throw new Error(`u32 value ${value} is invalid`);
  }
  const result = new Uint8Array(4);
  new DataView(result.buffer).setUint32(0, value);
  return result;
}

export function concatBytes(...parts: readonly Uint8Array[]): Uint8Array {
  const result = new Uint8Array(parts.reduce((sum, part) => sum + part.length, 0));
  let offset = 0;
  for (const part of parts) {
    result.set(part, offset);
    offset += part.length;
  }
  return result;
}

export function encodeTcpFrame(frame: Uint8Array): Uint8Array {
  const result = new Uint8Array(4 + frame.length);
  new DataView(result.buffer).setUint32(0, frame.length);
  result.set(frame, 4);
  return result;
}

export function decodeStandardResponse(payload: Uint8Array): {
  ok: boolean;
  data: Uint8Array;
  errorCode?: number;
  errorMessage?: string;
} {
  if (payload.length === 0) throw new Error("standard response is empty");
  if (payload[0] === 0) return { ok: true, data: payload.slice(1) };
  if (payload[0] !== 1 || payload.length < 9) {
    throw new Error(`invalid standard response status ${payload[0]}`);
  }
  const view = new DataView(payload.buffer, payload.byteOffset, payload.byteLength);
  const errorCode = view.getUint32(1);
  const messageLength = view.getUint32(5);
  if (9 + messageLength > payload.length) throw new Error("standard response error is truncated");
  const errorMessage = new TextDecoder().decode(payload.slice(9, 9 + messageLength));
  return { ok: false, data: new Uint8Array(), errorCode, errorMessage };
}

export async function openRawWebSocket(url: string, timeoutMs: number): Promise<RawProtocolConnection> {
  const socket = new WebSocket(url);
  socket.binaryType = "arraybuffer";
  const frames: Uint8Array[] = [];
  const waiters: Array<{ resolve: (frame: Uint8Array) => void; reject: (error: Error) => void }> = [];
  let closed = socket.readyState === WebSocket.CLOSED;
  let closeError: Error | undefined;
  let resolveClosed: () => void = () => undefined;
  const closedPromise = new Promise<void>((resolve) => { resolveClosed = resolve; });
  const deliver = (frame: Uint8Array): void => {
    const waiter = waiters.shift();
    if (waiter) waiter.resolve(frame);
    else frames.push(frame);
  };
  socket.addEventListener("message", (event) => {
    const data = event.data;
    if (data instanceof ArrayBuffer) deliver(new Uint8Array(data));
    else if (ArrayBuffer.isView(data)) deliver(new Uint8Array(data.buffer, data.byteOffset, data.byteLength));
  });
  socket.addEventListener("error", () => {
    closeError = new Error("raw WebSocket failed");
    for (const waiter of waiters.splice(0)) waiter.reject(closeError);
  });
  socket.addEventListener("close", () => {
    closed = true;
    resolveClosed();
    const error = closeError ?? new Error("raw WebSocket closed");
    for (const waiter of waiters.splice(0)) waiter.reject(error);
  });
  await waitForSocketOpen(socket, timeoutMs);
  return {
    transport: "ws",
    async send(frame): Promise<void> {
      if (socket.readyState !== WebSocket.OPEN) throw new Error("raw WebSocket is not open");
      socket.send(frame);
    },
    async sendRaw(data): Promise<void> {
      if (socket.readyState !== WebSocket.OPEN) throw new Error("raw WebSocket is not open");
      socket.send(data);
    },
    receive(timeout): Promise<Uint8Array> {
      if (frames.length > 0) return Promise.resolve(frames.shift()!);
      if (closed) return Promise.reject(closeError ?? new Error("raw WebSocket closed"));
      return withTimeout(new Promise((resolve, reject) => waiters.push({ resolve, reject })), timeout, "WebSocket receive");
    },
    async close(): Promise<void> {
      if (socket.readyState === WebSocket.CLOSED) return;
      socket.close();
      await withTimeout(closedPromise, timeoutMs, "WebSocket close").catch(() => undefined);
    },
    destroy(): void { socket.close(); },
    waitForClose(timeout): Promise<void> { return withTimeout(closedPromise, timeout, "WebSocket close"); },
  };
}

export async function openRawTcp(host: string, port: number, timeoutMs: number): Promise<RawProtocolConnection> {
  const socket = connectTcp({ host, port });
  const frames: Uint8Array[] = [];
  const waiters: Array<{ resolve: (frame: Uint8Array) => void; reject: (error: Error) => void }> = [];
  let buffer = new Uint8Array();
  let closed = false;
  let closeError: Error | undefined;
  let resolveClosed: () => void = () => undefined;
  const closedPromise = new Promise<void>((resolve) => { resolveClosed = resolve; });
  const fail = (error: Error): void => {
    closeError = error;
    for (const waiter of waiters.splice(0)) waiter.reject(error);
  };
  socket.on("data", (chunk: Buffer) => {
    const next = new Uint8Array(buffer.length + chunk.length);
    next.set(buffer);
    next.set(chunk, buffer.length);
    buffer = next;
    while (buffer.length >= 4) {
      const length = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength).getUint32(0);
      if (buffer.length < 4 + length) break;
      const frame = buffer.slice(4, 4 + length);
      buffer = buffer.slice(4 + length);
      const waiter = waiters.shift();
      if (waiter) waiter.resolve(frame);
      else frames.push(frame);
    }
  });
  socket.on("error", fail);
  socket.on("close", () => {
    closed = true;
    resolveClosed();
    const error = closeError ?? new Error("raw TCP closed");
    for (const waiter of waiters.splice(0)) waiter.reject(error);
  });
  await withTimeout(new Promise<void>((resolve, reject) => {
    socket.once("connect", resolve);
    socket.once("error", reject);
  }), timeoutMs, "TCP connect");
  return {
    transport: "tcp",
    send(frame): Promise<void> {
      return sendRaw(encodeTcpFrame(frame));
    },
    sendRaw(data): Promise<void> {
      return sendRaw(data);
    },
    receive(timeout): Promise<Uint8Array> {
      if (frames.length > 0) return Promise.resolve(frames.shift()!);
      if (closed) return Promise.reject(closeError ?? new Error("raw TCP closed"));
      return withTimeout(new Promise((resolve, reject) => waiters.push({ resolve, reject })), timeout, "TCP receive");
    },
    async close(): Promise<void> {
      if (closed) return;
      socket.end();
      await withTimeout(closedPromise, timeoutMs, "TCP close").catch(() => socket.destroy());
    },
    destroy(): void { socket.destroy(); },
    waitForClose(timeout): Promise<void> { return withTimeout(closedPromise, timeout, "TCP close"); },
  };

  function sendRaw(data: Uint8Array): Promise<void> {
    if (closed) return Promise.reject(closeError ?? new Error("raw TCP closed"));
    return new Promise((resolve, reject) => socket.write(Buffer.from(data), (error) => error ? reject(error) : resolve()));
  }
}

async function waitForSocketOpen(socket: WebSocket, timeoutMs: number): Promise<void> {
  if (socket.readyState === WebSocket.OPEN) return;
  await withTimeout(new Promise<void>((resolve, reject) => {
    socket.addEventListener("open", () => resolve(), { once: true });
    socket.addEventListener("error", () => reject(new Error("raw WebSocket failed to open")), { once: true });
  }), timeoutMs, "WebSocket open");
}

function withTimeout<T>(promise: Promise<T>, timeoutMs: number, label: string): Promise<T> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`${label} timed out after ${timeoutMs}ms`)), timeoutMs);
    promise.then((value) => { clearTimeout(timer); resolve(value); }, (error: unknown) => {
      clearTimeout(timer);
      reject(error instanceof Error ? error : new Error(String(error)));
    });
  });
}
