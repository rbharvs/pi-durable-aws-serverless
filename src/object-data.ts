import type { StagedReference } from "./input.ts";
import type { PublishedPayload } from "./payload.ts";

/** Thrown by an adapter only when a staged object is confirmed missing or definitely unusable. */
export class ObjectInvalid extends Error {
  constructor(cause: unknown) {
    super("Staged object invalid", { cause });
  }
}

/**
 * One namespace-scoped handle, owned and closed by one staged submission.
 *
 * Adapters use a fixed configured bucket, enforce the handle's namespace before I/O, and bound
 * transport time, buffering, and retries.
 *
 * `getStaged` returns the raw object bytes. It throws ObjectInvalid only for a confirmed missing
 * object; ambiguous authorization or transport failures throw other errors. A truncated transfer
 * must throw rather than end the body, or the caller reports invalid content. The caller verifies
 * the declared byte count and SHA-256, using readAtMost to bound total bytes rather than trusting
 * response metadata. Early iterator `return()` must release the transport (Node.js `Readable`
 * streams do). The adapter need not duplicate this aggregate byte count.
 *
 * `put` creates a new `p/<ns>/<UUID-v4>` object from validated canonical JSON without overwriting
 * any object. It resolves only after durable publication with a confirmed checksum. Published
 * objects must stay unchanged for the receipt lifetime. A failure may leave an orphan.
 */
export interface CommandObjects {
  getStaged(ref: StagedReference): Promise<AsyncIterable<Uint8Array>>;
  put(json: string): Promise<PublishedPayload>;
  close(): void | Promise<void>;
}

/**
 * Reads `body` into one buffer, or returns `undefined` as soon as it exceeds `limit` bytes.
 * Chunks are copied, so a producer cannot change them after they are read.
 */
export async function readAtMost(
  body: AsyncIterable<Uint8Array>,
  limit: number,
): Promise<Buffer | undefined> {
  // One allocation bounds memory even when the transport emits tiny chunks.
  const output = Buffer.alloc(limit);
  let bytes = 0;

  for await (const chunk of body) {
    if (!(chunk instanceof Uint8Array)) throw new TypeError("Expected a binary object stream");
    if (chunk.byteLength > limit - bytes) return undefined;
    output.set(chunk, bytes);
    bytes += chunk.byteLength;
  }

  return output.subarray(0, bytes);
}

/** History uploads are joined even when one worker fails; output retains input order. */
export const UPLOAD_CONCURRENCY = 4;

/** Stops assigning work after a failure, joins every started upload, and preserves result order. */
export async function uploadValues<T>(
  values: readonly T[],
  upload: (value: T, index: number) => Promise<PublishedPayload>,
): Promise<PublishedPayload[]> {
  const output: PublishedPayload[] = [];
  let cursor = 0;
  let failed = false;
  const results = await Promise.allSettled(
    Array.from({ length: Math.min(UPLOAD_CONCURRENCY, values.length) }, async () => {
      while (!failed && cursor < values.length) {
        const index = cursor++;
        try {
          output[index] = await upload(values[index]!, index);
        } catch (error) {
          failed = true;
          throw error;
        }
      }
    }),
  );
  for (const result of results) {
    if (result.status === "rejected") throw result.reason;
  }
  return output;
}
