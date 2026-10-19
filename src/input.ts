import type { TextContent, ImageContent } from "@earendil-works/pi-ai";
import type { UserInput } from "@earendil-works/pi-durable";
import { check, computeSha256, object, text, type Sha256 } from "./domain.ts";

export const MAX_INLINE_INPUT_BYTES = 256 * 1024;

export const MAX_INPUT_BYTES = 8 * 1024 * 1024;

export const MAX_IMAGE_BYTES = 6 * 1024 * 1024;

export const MAX_BLOCKS = 64;

export const MAX_IMAGES = 8;

export type AgentInput = {
  readonly content: UserInput;
  readonly whenBusy: "steer" | "followUp";
};

export type Command = { readonly kind: "inline"; readonly input: AgentInput };

export type CanonicalInput = {
  readonly input: AgentInput;
  readonly json: string;
  readonly sha256: Sha256;
  readonly bytes: number;
};

const IMAGE_MIME_TYPES: readonly string[] = ["image/png", "image/jpeg", "image/webp", "image/gif"];

/** The longest base64 string that can decode to `MAX_IMAGE_BYTES`; checked before decoding. */
const MAX_IMAGE_BASE64_LENGTH = Math.ceil(MAX_IMAGE_BYTES / 3) * 4;

function assertOnlyFields(record: Record<string, unknown>, allowed: readonly string[]): void {
  check(
    Object.keys(record).every((key) => allowed.includes(key)),
    "Unexpected input field",
  );
}

export function parseAgentInput(value: unknown): AgentInput {
  const record = object(value);

  assertOnlyFields(record, ["content", "whenBusy"]);

  // Read each caller property once, then validate and construct from those values.
  const source = record.content;
  const whenBusy = record.whenBusy;

  check(whenBusy === "steer" || whenBusy === "followUp", "Invalid whenBusy");

  return { content: parseContent(source), whenBusy };
}

function parseContent(source: unknown): UserInput {
  if (typeof source === "string") return source;

  check(Array.isArray(source), "Invalid or too many content blocks");
  // Capture the bound once; indexed traversal ignores caller iterators and rejects holes.
  const count = source.length;
  check(count >= 0 && count <= MAX_BLOCKS, "Invalid or too many content blocks");

  let imageCount = 0;

  return Array.from({ length: count }, (_, index): TextContent | ImageContent => {
    const block = object(source[index]);
    const type = block.type;

    if (type === "text") {
      assertOnlyFields(block, ["type", "text", "textSignature"]);

      return parseTextBlock(block);
    }

    check(type === "image", "Invalid content block type");
    assertOnlyFields(block, ["type", "data", "mimeType"]);
    // Count images before decoding, so an excess image is rejected without decoding it.
    imageCount += 1;
    check(imageCount <= MAX_IMAGES, "Too many images");

    return parseImageBlock(block);
  });
}

function parseTextBlock(block: Record<string, unknown>): TextContent {
  const result: TextContent = { type: "text", text: text(block.text) };
  const signature = block.textSignature;

  if (signature !== undefined) result.textSignature = text(signature);

  return result;
}

function parseImageBlock(block: Record<string, unknown>): ImageContent {
  const data = text(block.data);
  const mimeType = text(block.mimeType);

  check(IMAGE_MIME_TYPES.includes(mimeType), "Unsupported image MIME type");
  check(
    data.length > 0 &&
      data.length <= MAX_IMAGE_BASE64_LENGTH &&
      data.length % 4 === 0 &&
      /^[A-Za-z0-9+/]*={0,2}$/.test(data),
    "Invalid canonical base64 image",
  );

  const decoded = Buffer.from(data, "base64");

  // Re-encoding rejects non-canonical forms, such as nonzero padding bits.
  // Each image therefore has exactly one accepted encoding.
  check(
    decoded.length <= MAX_IMAGE_BYTES && decoded.toString("base64") === data,
    "Invalid canonical base64 image",
  );

  return { type: "image", data, mimeType };
}

/** Validates Pi input and returns its canonical JSON, byte length, and SHA-256. */
export function canonical(value: unknown): CanonicalInput {
  const input = parseAgentInput(value);
  const json = JSON.stringify(input);
  const bytes = Buffer.byteLength(json);

  check(bytes <= MAX_INPUT_BYTES, "Canonical input exceeds bounded size");

  return { input, json, bytes, sha256: computeSha256(json) };
}

export function canonicalCommand(value: unknown): CanonicalInput {
  const command = object(value);

  check(command.kind === "inline", "Invalid command kind");
  assertOnlyFields(command, ["kind", "input"]);

  const c = canonical(command.input);

  check(c.bytes <= MAX_INLINE_INPUT_BYTES, "Inline input exceeds 256 KiB");

  return c;
}

export function parseCommand(value: unknown): Command {
  return { kind: "inline", input: canonicalCommand(value).input };
}
