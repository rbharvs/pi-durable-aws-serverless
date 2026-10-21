import type { TextContent, ImageContent } from "@earendil-works/pi-ai";
import type { UserInput } from "@earendil-works/pi-durable";
import {
  check,
  computeSha256,
  harnessId,
  object,
  parseSha256,
  text,
  type HarnessId,
  type Sha256,
} from "./domain.ts";

export const MAX_INLINE_INPUT_BYTES = 256 * 1024;

/** Maximum raw staging object size and canonical input JSON size. */
export const MAX_INPUT_BYTES = 8 * 1024 * 1024;

export const MAX_IMAGE_BYTES = 6 * 1024 * 1024;

export const MAX_BLOCKS = 64;

export const MAX_IMAGES = 8;

/** Validated Pi content and the admission policy to use while the agent is busy. */
export type AgentInput = {
  readonly content: UserInput;
  readonly whenBusy: "steer" | "followUp";
};

/** Namespace-scoped staging key; unlike a published key, the caller may still mutate its bytes. */
export type StagedKey = string & { readonly __stagedKey: unique symbol };

/** Integrity of raw uploaded bytes, not the canonical input identity. */
export type StagedReference = {
  readonly key: StagedKey;
  readonly sha256: Sha256;
  readonly bytes: number;
};

/** Inline logical input or a reference to raw staging bytes that must still be read and verified. */
export type Command =
  | { readonly kind: "inline"; readonly input: AgentInput }
  | ({ readonly kind: "s3" } & StagedReference);

/** Internal parsed command; inline canonicalization happens only once per submission. */
export type PreparedCommand =
  | { readonly kind: "inline"; readonly canonical: CanonicalInput }
  | Extract<Command, { kind: "s3" }>;

export type CanonicalInput = {
  readonly input: AgentInput;
  readonly json: string;
  readonly sha256: Sha256;
  readonly bytes: number;
};

const IMAGE_MIME_TYPES: readonly string[] = ["image/png", "image/jpeg", "image/webp", "image/gif"];

/** The longest base64 string that can decode to `MAX_IMAGE_BYTES`; checked before decoding. */
const MAX_IMAGE_BASE64_LENGTH = Math.ceil(MAX_IMAGE_BYTES / 3) * 4;

// Root object -> content array -> block object; all block fields are strings.
const MAX_STAGED_JSON_DEPTH = 3;

// Without duplicate fields, supported shapes need at most 386 colons/commas:
// root 3 + array 63 + 64 blocks * 5.
// Leave conservative headroom without allowing an unbounded parsed value graph.
const MAX_STAGED_JSON_SEPARATORS = 1024;

function assertOnlyFields(record: Record<string, unknown>, allowed: readonly string[]): void {
  check(
    Object.keys(record).every((key) => allowed.includes(key)),
    "Unexpected input field",
  );
}

/**
 * Validates the namespace-scoped key, SHA-256 syntax, and declared byte bound without I/O.
 * Does not prove object existence or integrity; submission checks the fetched raw bytes.
 */
export function stagedReference(ns: HarnessId, value: unknown): StagedReference {
  const record = object(value);
  const key = text(record.key);
  const sha256 = parseSha256(record.sha256);
  const bytes = record.bytes;
  const prefix = `in/${harnessId(ns)}/`;
  const name = key.slice(prefix.length);

  check(
    key.startsWith(prefix) && /^[A-Za-z0-9._-]{1,128}$/.test(name) && name !== "." && name !== "..",
    "Foreign namespace or invalid staging key",
  );
  check(
    typeof bytes === "number" &&
      Number.isSafeInteger(bytes) &&
      bytes > 0 &&
      bytes <= MAX_INPUT_BYTES,
    "Input object exceeds bounded size",
  );

  return { key: key as StagedKey, sha256, bytes };
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

/**
 * Bounds allocation before JSON.parse, not JSON syntax. Quotes and escaped characters hide
 * structural punctuation. JSON.parse and Pi validation still decide whether input is valid.
 * All raw structure counts, including duplicate-key values that JSON.parse would discard.
 */
function assertStagedJsonStructure(json: string): void {
  let depth = 0;
  let separators = 0;
  let inString = false;

  for (let index = 0; index < json.length; index++) {
    const character = json[index];

    if (inString) {
      if (character === "\\") index++;
      else if (character === '"') inString = false;
    } else if (character === '"') {
      inString = true;
    } else if (character === "{" || character === "[") {
      depth++;
      check(depth <= MAX_STAGED_JSON_DEPTH, "Staged JSON nesting exceeds bounded depth");
    } else if (character === "}" || character === "]") {
      depth--;
      check(depth >= 0, "Invalid staged JSON nesting");
    } else if (character === "," || character === ":") {
      separators++;
      check(separators <= MAX_STAGED_JSON_SEPARATORS, "Staged JSON has too much structure");
    }
  }
}

/** Decodes raw staged bytes as strict UTF-8 JSON and validates them as Pi input. Internal. */
export function canonicalStagedInput(raw: Uint8Array): CanonicalInput {
  check(raw.byteLength <= MAX_INPUT_BYTES, "Input object exceeds bounded size");

  // Keep a BOM, so JSON.parse rejects it instead of silently changing the raw content.
  const json = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(raw);

  assertStagedJsonStructure(json);

  return canonical(JSON.parse(json));
}

export function prepareCommand(value: unknown, ns?: HarnessId): PreparedCommand {
  const command = object(value);
  const kind = command.kind;

  if (kind === "inline") {
    assertOnlyFields(command, ["kind", "input"]);

    const c = canonical(command.input);

    check(c.bytes <= MAX_INLINE_INPUT_BYTES, "Inline input exceeds 256 KiB");

    return { kind, canonical: c };
  }

  check(kind === "s3", "Invalid command kind");
  check(ns !== undefined, "Missing staging namespace");
  assertOnlyFields(command, ["kind", "key", "sha256", "bytes"]);

  return { kind, ...stagedReference(ns, command) };
}

/** Inline commands need no namespace. Staged commands require their owning Harness ID. */
export function parseCommand(value: unknown, ns?: HarnessId): Command {
  const command = prepareCommand(value, ns);

  return command.kind === "inline" ? { kind: "inline", input: command.canonical.input } : command;
}
