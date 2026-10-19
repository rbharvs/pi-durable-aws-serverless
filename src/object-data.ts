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
 * the declared byte count and SHA-256 and may stop early through the iterator's `return()`, which
 * must release the transport (Node.js `Readable` streams do).
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
