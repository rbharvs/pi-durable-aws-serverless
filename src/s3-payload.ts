import type { Row } from "./sql/db.ts";
import { check, integer, parseSha256, text, type HarnessId } from "./domain.ts";
import { EnvelopeRejected, jsonText, type PublishedPayload } from "./payload.ts";
import { LIMITS } from "./limits.ts";

/** Payload planning needs immutable JSON objects, not an AWS client or SDK response. */
export interface PayloadObjects {
  /** Create a new immutable p/<namespace>/<UUID-v4> object; acknowledge durable bytes/checksum. */
  put(json: string): Promise<PublishedPayload>;
  /** Read the exact object as strict UTF-8 JSON, without stripping BOM or replacing bad bytes. */
  get(ref: PublishedPayload): Promise<string>;
}

export function objectSize(json: string): number {
  const bytes = Buffer.byteLength(json);

  if (bytes > LIMITS.maxObjectBytes)
    throw new EnvelopeRejected(`Object exceeds ${LIMITS.maxObjectBytes} bytes: ${bytes}`);

  jsonText(json);
  return bytes;
}

/** Exact metadata width of a future random immutable reference. No upload in planning. */
export function referenceEstimate(ns: HarnessId, bytes: number): readonly (string | number)[] {
  return [`p/${ns}/00000000-0000-4000-8000-000000000000`, "0".repeat(64), bytes];
}

export function reference(row: Row, ns: HarnessId): PublishedPayload {
  const bytes = integer(row.bytes);
  const key = text(row.object_key);
  check(bytes <= LIMITS.maxObjectBytes, "Object exceeds 16 MiB bound");
  check(
    /^p\/[a-zA-Z0-9_-]{1,80}\/[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(
      key,
    ),
    "Invalid object key",
  );
  const ref: PublishedPayload = {
    kind: "s3",
    key: key as PublishedPayload["key"],
    sha256: parseSha256(row.sha256),
    bytes,
  };

  check(ref.key.startsWith(`p/${ns}/`), "Foreign namespace object");

  return ref;
}
