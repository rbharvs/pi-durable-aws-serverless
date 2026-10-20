import {
  check,
  harnessId,
  object,
  parseSha256,
  text,
  type HarnessId,
  type Sha256,
} from "./domain.ts";
import { MAX_INPUT_BYTES, MAX_INLINE_INPUT_BYTES } from "./input.ts";
import { LIMITS } from "./limits.ts";

export class EnvelopeRejected extends Error {}

export function assertEnvelope(rows: number, bytes: number): void {
  if (rows > LIMITS.maxRows || bytes > LIMITS.maxTransactionBytes)
    throw new EnvelopeRejected(
      `Atomic envelope exceeded: rows=${rows}/${LIMITS.maxRows}, estimatedBytes=${bytes}/${LIMITS.maxTransactionBytes}`,
    );
}

/** History JSON has its own physical bound; it is not the command input format. */
export function jsonText(value: unknown): string {
  const json = text(value);
  JSON.parse(json);
  check(
    Buffer.from(json, "utf8").toString("utf8") === json && !json.includes("\u0000"),
    "Non UTF-8/NUL JSON text",
  );
  return json;
}

export type ObjectKey = string & { readonly __objectKey: unique symbol };

export type PublishedPayload = {
  readonly kind: "s3";
  readonly key: ObjectKey;
  readonly sha256: Sha256;
  readonly bytes: number;
};

export type StoredPayload = { readonly kind: "inline"; readonly json: string } | PublishedPayload;

/** Validates metadata, not object existence or durability. The publisher guarantees those. */
export function publishedPayload(ns: HarnessId, value: unknown): PublishedPayload {
  const record = object(value);
  const kind = record.kind;
  const key = text(record.key);
  const sha256 = parseSha256(record.sha256);
  const bytes = record.bytes;
  const prefix = `p/${harnessId(ns)}/`;

  check(kind === "s3", "Invalid published payload kind");
  check(
    key.startsWith(prefix) &&
      /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(
        key.slice(prefix.length),
      ),
    "Foreign namespace or invalid published key",
  );
  check(
    typeof bytes === "number" &&
      Number.isSafeInteger(bytes) &&
      bytes > MAX_INLINE_INPUT_BYTES &&
      bytes <= MAX_INPUT_BYTES,
    "Inbox object bounds",
  );

  return { kind, key: key as ObjectKey, sha256, bytes };
}
