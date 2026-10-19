import { describe, it, expect } from "vitest";
import {
  canonical,
  parseCommand,
  MAX_INLINE_INPUT_BYTES,
  MAX_BLOCKS,
  MAX_IMAGES,
} from "./input.ts";
import { withChangingProperty } from "../tests/shared/input.ts";

const plain = { content: "hello", whenBusy: "followUp" as const };

const image = { type: "image", data: "AQID", mimeType: "image/png" };

/** Builds the wire command for an inline Pi input. */
const inlineCommand = (input: unknown) => ({ kind: "inline", input });

describe("canonical Pi input boundary", () => {
  it("preserves supported textSignature rather than silently dropping or rejecting it", () => {
    const value = {
      content: [
        {
          textSignature: "opaque\\u0000",
          text: "\ud800\u0000🚀",
          type: "text",
        },
      ],
      whenBusy: "steer",
    };
    const c = canonical(value);

    expect(c.input).toEqual(value);
    expect(c.sha256).not.toBe(
      canonical({
        ...value,
        content: [{ type: "text", text: "\ud800\u0000🚀" }],
      }).sha256,
    );
    expect(c.json).toContain("\\ud800");
    expect(canonical(plain).sha256).not.toBe(
      canonical({ ...plain, content: [{ type: "text", text: "hello" }] }).sha256,
    );
  });
  it("keeps block order and fixed validated key order", () => {
    expect(
      canonical({
        whenBusy: "followUp",
        content: [{ mimeType: "image/png", data: "AQID", type: "image" }],
      }).json,
    ).toBe(canonical({ content: [image], whenBusy: "followUp" }).json);
    expect(
      canonical({
        content: [image, { type: "text", text: "a" }],
        whenBusy: "steer",
      }).sha256,
    ).not.toBe(
      canonical({
        content: [{ type: "text", text: "a" }, image],
        whenBusy: "steer",
      }).sha256,
    );
  });
  for (const [name, value, message] of [
    ['whenBusy "reject"', { ...plain, whenBusy: "reject" }, "Invalid whenBusy"],
    ["an unknown top-level field", { ...plain, entry: {} }, "Unexpected input field"],
    [
      "an unknown block field",
      { ...plain, content: [{ type: "text", text: "a", other: true }] },
      "Unexpected input field",
    ],
    [
      "a non-string textSignature",
      { ...plain, content: [{ type: "text", text: "a", textSignature: 1 }] },
      "Expected string",
    ],
    [
      "an unsupported image MIME type",
      { ...plain, content: [{ ...image, mimeType: "text/plain" }] },
      "Unsupported image MIME type",
    ],
    [
      "base64 with whitespace",
      { ...plain, content: [{ ...image, data: "AQ ID" }] },
      "Invalid canonical base64 image",
    ],
    [
      "base64 with nonzero padding bits",
      { ...plain, content: [{ ...image, data: "AR==" }] },
      "Invalid canonical base64 image",
    ],
    [
      "base64 without padding",
      { ...plain, content: [{ ...image, data: "AQI" }] },
      "Invalid canonical base64 image",
    ],
    [
      "too many blocks",
      {
        ...plain,
        content: Array.from({ length: MAX_BLOCKS + 1 }, () => ({
          type: "text",
          text: "",
        })),
      },
      "Invalid or too many content blocks",
    ],
    [
      "too many images",
      {
        ...plain,
        content: Array.from({ length: MAX_IMAGES + 1 }, () => image),
      },
      "Too many images",
    ],
  ] as const)
    it(`rejects ${name}`, () => expect(() => canonical(value)).toThrow(message));
  it("validates and stores only the first value read from each input property", () => {
    const later = [{ type: "text", text: "later" }];

    expect(
      canonical(withChangingProperty({ whenBusy: "followUp" }, "content", { first: "x", later }))
        .json,
    ).toBe('{"content":"x","whenBusy":"followUp"}');
    expect(
      canonical(
        withChangingProperty({ content: "x" }, "whenBusy", { first: "followUp", later: "reject" }),
      ).json,
    ).toBe('{"content":"x","whenBusy":"followUp"}');
    expect(
      canonical({
        ...plain,
        content: [
          withChangingProperty({ type: "text", text: "a" }, "textSignature", {
            first: "first",
            later: "later",
          }),
        ],
      }).json,
    ).toBe(
      '{"content":[{"type":"text","text":"a","textSignature":"first"}],"whenBusy":"followUp"}',
    );
    expect(
      canonical({
        ...plain,
        content: [withChangingProperty({ ...image }, "type", { first: "image", later: "text" })],
      }).json,
    ).toBe(
      '{"content":[{"type":"image","data":"AQID","mimeType":"image/png"}],"whenBusy":"followUp"}',
    );
  });
  it("accepts empty Pi inputs with distinct canonical identities", () => {
    const text = canonical({ ...plain, content: "" });
    const blocks = canonical({ ...plain, content: [] });

    expect(text.json).toBe('{"content":"","whenBusy":"followUp"}');
    expect(blocks.json).toBe('{"content":[],"whenBusy":"followUp"}');
    expect(text.sha256).not.toBe(blocks.sha256);
  });
  it("enforces the inline cap on canonical UTF-8 bytes and requires a namespace for staging", () => {
    const envelope = Buffer.byteLength(JSON.stringify({ ...plain, content: "" }));
    const largest = "x".repeat(MAX_INLINE_INPUT_BYTES - envelope);

    expect(parseCommand(inlineCommand({ ...plain, content: largest }))).toEqual({
      kind: "inline",
      input: { ...plain, content: largest },
    });
    // Same character count, one more UTF-8 byte.
    expect(() =>
      parseCommand(inlineCommand({ ...plain, content: largest.slice(1) + "é" })),
    ).toThrow("Inline input exceeds 256 KiB");
    expect(() =>
      parseCommand(inlineCommand({ ...plain, content: "x".repeat(MAX_INLINE_INPUT_BYTES) })),
    ).toThrow("Inline input exceeds 256 KiB");
    expect(() => parseCommand({ ...inlineCommand(plain), extra: true })).toThrow(
      "Unexpected input field",
    );
    expect(() =>
      parseCommand({
        kind: "s3",
        key: "in/boundary/a",
        sha256: "0".repeat(64),
        bytes: 2,
      }),
    ).toThrow("Missing staging namespace");
    expect(() => parseCommand(inlineCommand({ ...plain, whenBusy: "reject" }))).toThrow(
      "Invalid whenBusy",
    );
  });
});

it("canonicalizes indexed blocks without invoking a caller iterator", () => {
  let iteratorCalls = 0;
  const content = [{ type: "text", text: "indexed" }];
  Object.defineProperty(content, Symbol.iterator, {
    value: function* () {
      iteratorCalls++;
      for (let i = 0; i <= MAX_BLOCKS; i++) yield { type: "text", text: "iterator" };
    },
  });
  expect(canonical({ content, whenBusy: "followUp" })).toEqual(
    canonical({ content: [{ type: "text", text: "indexed" }], whenBusy: "followUp" }),
  );
  expect(iteratorCalls).toBe(0);
});

it("captures array length once before bounded indexed traversal", () => {
  let lengthReads = 0;
  const blocks = Array.from({ length: MAX_BLOCKS + 10 }, (_, i) => ({
    type: "text",
    text: String(i),
  }));
  const content = new Proxy(blocks, {
    get(target, property, receiver) {
      if (property === "length") return ++lengthReads === 1 ? 1 : target.length;
      return Reflect.get(target, property, receiver);
    },
  });
  expect(canonical({ content, whenBusy: "followUp" })).toEqual(
    canonical({ content: [{ type: "text", text: "0" }], whenBusy: "followUp" }),
  );
  expect(lengthReads).toBe(1);
});

it("rejects indexed holes even if a caller iterator supplies valid blocks", () => {
  const content: unknown[] = [];
  content.length = 1;
  Object.defineProperty(content, Symbol.iterator, {
    value: function* () {
      yield { type: "text", text: "not the indexed value" };
    },
  });
  expect(() => canonical({ content, whenBusy: "followUp" })).toThrow();
});
