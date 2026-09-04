const QUEUE_FULL = 4_005;
const INITIAL_DELAY_MS = 25;
const MAX_DELAY_MS = 250;

export function isQueueBackpressure(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    "domainCode" in error &&
    error.domainCode === QUEUE_FULL
  );
}

export async function retryQueueBackpressure<T>(
  operation: () => Promise<T>,
  onRetry: (attempt: number, delayMs: number, error: unknown) => void = () => undefined,
  wait: (delayMs: number) => Promise<void> = backpressureDelay,
): Promise<T> {
  let retries = 0;
  while (true) {
    try {
      return await operation();
    } catch (error) {
      if (!isQueueBackpressure(error)) throw error;
      retries += 1;
      const delayMs = Math.min(INITIAL_DELAY_MS * (2 ** (retries - 1)), MAX_DELAY_MS);
      onRetry(retries, delayMs, error);
      await wait(delayMs);
    }
  }
}

function backpressureDelay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}
