import type { TextContent, ImageContent } from "@earendil-works/pi-ai";
import type { UserInput } from "@earendil-works/pi-durable";
import { check, object, sha, text, type Sha256 } from "./domain.ts";

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

function fields(v: Record<string, unknown>, allowed: readonly string[]): void {
  check(
    Object.keys(v).every((k) => allowed.includes(k)),
    "Unexpected input field",
  );
}

export function parseAgentInput(value: unknown): AgentInput {
  const v = object(value);

  fields(v, ["content", "whenBusy"]);

  // Read each caller property once, then validate and construct from those values.
  const source = v.content,
    whenBusy = v.whenBusy;

  check(whenBusy === "steer" || whenBusy === "followUp", "Invalid whenBusy");

  let content: UserInput;

  if (typeof source === "string") content = source;
  else {
    check(Array.isArray(source), "Invalid or too many content blocks");
    // Capture the bound once; indexed traversal ignores caller iterators and rejects holes.
    const count = source.length;
    check(count >= 0 && count <= MAX_BLOCKS, "Invalid or too many content blocks");

    let images = 0;

    content = Array.from({ length: count }, (_, index): TextContent | ImageContent => {
      const block = object(source[index]),
        type = block.type;

      if (type === "text") {
        fields(block, ["type", "text", "textSignature"]);

        const result: TextContent = { type: "text", text: text(block.text) };

        const signature = block.textSignature;

        if (signature !== undefined) result.textSignature = text(signature);

        return result;
      }

      check(type === "image", "Invalid content block type");
      fields(block, ["type", "data", "mimeType"]);
      check(++images <= MAX_IMAGES, "Too many images");

      const data = text(block.data),
        mimeType = text(block.mimeType);

      check(
        ["image/png", "image/jpeg", "image/webp", "image/gif"].includes(mimeType),
        "Unsupported image MIME type",
      );
      check(
        data.length > 0 &&
          data.length <= Math.ceil(MAX_IMAGE_BYTES / 3) * 4 &&
          data.length % 4 === 0 &&
          /^[A-Za-z0-9+/]*={0,2}$/.test(data),
        "Invalid canonical base64 image",
      );

      const decoded = Buffer.from(data, "base64");

      check(
        decoded.length <= MAX_IMAGE_BYTES && decoded.toString("base64") === data,
        "Invalid canonical base64 image",
      );

      return { type: "image", data, mimeType };
    });
  }

  return { content, whenBusy };
}

export function canonical(value: unknown): CanonicalInput {
  const input = parseAgentInput(value),
    json = JSON.stringify(input),
    bytes = Buffer.byteLength(json);

  check(bytes <= MAX_INPUT_BYTES, "Canonical input exceeds bounded size");

  return { input, json, bytes, sha256: sha(json) };
}

export function canonicalCommand(value: unknown): CanonicalInput {
  const v = object(value);

  check(v.kind === "inline", "Invalid command kind");
  fields(v, ["kind", "input"]);

  const c = canonical(v.input);

  check(c.bytes <= MAX_INLINE_INPUT_BYTES, "Inline input exceeds 256 KiB");

  return c;
}

export function parseCommand(value: unknown): Command {
  return { kind: "inline", input: canonicalCommand(value).input };
}
