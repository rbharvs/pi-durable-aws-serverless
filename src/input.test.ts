import { describe, it, expect } from "vitest";
import {
  canonical,
  parseCommand,
  MAX_INLINE_INPUT_BYTES,
  MAX_BLOCKS,
  MAX_IMAGES,
} from "./input.ts";
import { changing } from "../tests/shared/input.ts";

const plain = { content: "hello", whenBusy: "followUp" as const };

const image = { type: "image", data: "AQID", mimeType: "image/png" };

const inline = (input: unknown) => ({ kind: "inline", input });

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
  for (const [name, value] of [
    ["reject", { ...plain, whenBusy: "reject" }],
    ["unknown top-level", { ...plain, entry: {} }],
    ["unknown block field", { ...plain, content: [{ type: "text", text: "a", other: true }] }],
    [
      "wrong signature type",
      { ...plain, content: [{ type: "text", text: "a", textSignature: 1 }] },
    ],
    ["MIME", { ...plain, content: [{ ...image, mimeType: "text/plain" }] }],
    ["base64 whitespace", { ...plain, content: [{ ...image, data: "AQ ID" }] }],
    ["base64 bad bits", { ...plain, content: [{ ...image, data: "AR==" }] }],
    ["base64 padding", { ...plain, content: [{ ...image, data: "AQI" }] }],
    [
      "too many blocks",
      {
        ...plain,
        content: Array.from({ length: MAX_BLOCKS + 1 }, () => ({
          type: "text",
          text: "",
        })),
      },
    ],
    [
      "too many images",
      {
        ...plain,
        content: Array.from({ length: MAX_IMAGES + 1 }, () => image),
      },
    ],
  ] as const)
    it(`rejects ${name}`, () => expect(() => canonical(value)).toThrow());
  it("validates and stores only the first value read from each input property", () => {
    const later = [{ type: "text", text: "later" }];

    expect(canonical(changing({ whenBusy: "followUp" }, "content", "x", later)).json).toBe(
      '{"content":"x","whenBusy":"followUp"}',
    );
    expect(canonical(changing({ content: "x" }, "whenBusy", "followUp", "reject")).json).toBe(
      '{"content":"x","whenBusy":"followUp"}',
    );
    expect(
      canonical({
        ...plain,
        content: [changing({ type: "text", text: "a" }, "textSignature", "first", "later")],
      }).json,
    ).toBe(
      '{"content":[{"type":"text","text":"a","textSignature":"first"}],"whenBusy":"followUp"}',
    );
    expect(
      canonical({ ...plain, content: [changing({ ...image }, "type", "image", "text")] }).json,
    ).toBe(
      '{"content":[{"type":"image","data":"AQID","mimeType":"image/png"}],"whenBusy":"followUp"}',
    );
  });
  it("accepts empty Pi inputs with distinct canonical identities", () => {
    const text = canonical({ ...plain, content: "" }),
      blocks = canonical({ ...plain, content: [] });

    expect(text.json).toBe('{"content":"","whenBusy":"followUp"}');
    expect(blocks.json).toBe('{"content":[],"whenBusy":"followUp"}');
    expect(text.sha256).not.toBe(blocks.sha256);
  });
  it("enforces the inline cap on canonical UTF-8 bytes and accepts only the inline transport", () => {
    const envelope = Buffer.byteLength(JSON.stringify({ ...plain, content: "" })),
      largest = "x".repeat(MAX_INLINE_INPUT_BYTES - envelope);

    expect(parseCommand(inline({ ...plain, content: largest })).input.content).toBe(largest);
    // Same character count, one more UTF-8 byte.
    expect(() => parseCommand(inline({ ...plain, content: largest.slice(1) + "é" }))).toThrow(
      "Inline input exceeds 256 KiB",
    );
    expect(() =>
      parseCommand(inline({ ...plain, content: "x".repeat(MAX_INLINE_INPUT_BYTES) })),
    ).toThrow("Inline input exceeds 256 KiB");
    expect(() => parseCommand({ ...inline(plain), extra: true })).toThrow("Unexpected input field");
    expect(() =>
      parseCommand({
        kind: "s3",
        key: "in/boundary/a",
        sha256: "0".repeat(64),
        bytes: 2,
      }),
    ).toThrow("Invalid command kind");
    expect(() => parseCommand(inline({ ...plain, whenBusy: "reject" }))).toThrow(
      "Invalid whenBusy",
    );
  });
});
