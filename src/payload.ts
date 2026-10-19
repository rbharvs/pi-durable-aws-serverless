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
