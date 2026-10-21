import { expect, it } from "vitest";
import { canonical, canonicalStagedInput } from "./input.ts";

const staged = (raw: string | Uint8Array) => canonicalStagedInput(Buffer.from(raw));

const punctuation = '[]{}:,"\\'.repeat(2048);
const textBlock = { type: "text", text: punctuation, textSignature: punctuation };
const imageBlock = { type: "image", data: "AQID", mimeType: "image/png" };

for (const [name, content] of [
  ["text with structural punctuation, quotes, and backslashes", punctuation],
  ["empty blocks", []],
  ["64 text blocks with signatures", Array.from({ length: 64 }, () => textBlock)],
  [
    "8 images and 56 signed text blocks",
    [
      ...Array.from({ length: 8 }, () => imageBlock),
      ...Array.from({ length: 56 }, () => textBlock),
    ],
  ],
] as const) {
  it(`accepts ${name} without charging string contents against structure bounds`, () => {
    const input = { content, whenBusy: "followUp" };
    // Reordered fields and whitespace must not change canonical identity.
    const raw = JSON.stringify({ whenBusy: input.whenBusy, content }, null, 1);

    expect(staged(raw)).toEqual(canonical(input));
  });
}

it("handles Unicode-escaped punctuation, quotes, backslashes, and lone surrogates inside strings", () => {
  const raw = String.raw`{"content":"\u005b\u005d\u007b\u007d\u002c\u003a\u0022\u005c\ud800","whenBusy":"steer"}`;

  expect(staged(raw)).toEqual(
    canonical({
      content: '[]{},:"\\\ud800',
      whenBusy: "steer",
    }),
  );
});

it("accepts a fixed 8 MiB valid JSON input even when its text is all structural punctuation", () => {
  const maximumBytes = 8 * 1024 * 1024;
  const envelope = Buffer.byteLength(JSON.stringify({ content: "", whenBusy: "followUp" }));
  const content = "[".repeat(maximumBytes - envelope);
  const raw = JSON.stringify({ content, whenBusy: "followUp" });
  const result = staged(raw);

  expect(Buffer.byteLength(raw)).toBe(maximumBytes);
  expect(result.bytes).toBe(maximumBytes);
  expect(result.input.content).toBe(content);
});

for (const [name, raw, message] of [
  ["deep arrays", "[".repeat(4096) + "]".repeat(4096), "Staged JSON nesting exceeds bounded depth"],
  [
    "deep block value",
    '{"content":[{"type":"text","text":{"nested":"x"}}],"whenBusy":"steer"}',
    "Staged JSON nesting exceeds bounded depth",
  ],
  [
    "wide array",
    "[" + Array.from({ length: 2048 }, () => "{}").join(",") + "]",
    "Staged JSON has too much structure",
  ],
  [
    "wide object",
    "{" + Array.from({ length: 2048 }, (_, index) => `"key${index}":0`).join(",") + "}",
    "Staged JSON has too much structure",
  ],
] as const) {
  it(`rejects ${name} at the raw structure boundary`, () => {
    expect(() => staged(raw)).toThrow(message);
  });
}

for (const [name, discarded, message] of [
  ["deep", "[[[[]]]]", "Staged JSON nesting exceeds bounded depth"],
  [
    "wide",
    "[" + Array.from({ length: 2048 }, () => "{}").join(",") + "]",
    "Staged JSON has too much structure",
  ],
] as const) {
  it(`counts discarded ${name} duplicate-key values rather than promising arbitrary last-key-wins parity`, () => {
    const raw = `{"content":${discarded},"content":"after","whenBusy":"followUp"}`;

    // The final value would be valid, but the raw transport structure is outside its budget.
    expect(JSON.parse(raw)).toEqual({ content: "after", whenBusy: "followUp" });
    expect(() => staged(raw)).toThrow(message);
  });
}

it("accepts repeated array values within the raw structure budget", () => {
  const raw = '{"content":[],"content":[],"content":[],"whenBusy":"followUp"}';

  expect(staged(raw)).toEqual(canonical({ content: [], whenBusy: "followUp" }));
});

it("still accepts small duplicate-key input within the raw structure budget", () => {
  expect(staged('{"content":"before","content":"after","whenBusy":"followUp"}')).toEqual(
    canonical({ content: "after", whenBusy: "followUp" }),
  );
});

for (const raw of [
  '{"content":"unterminated',
  '{"content":"bad\\',
  '{"content" "x","whenBusy":"steer"}',
]) {
  it(`leaves malformed JSON rejection to JSON.parse: ${raw}`, () => {
    expect(() => staged(raw)).toThrow();
  });
}

it("decodes multibyte UTF-8 but rejects malformed UTF-8 and a byte order mark", () => {
  const json = JSON.stringify({ content: "é🚀", whenBusy: "steer" });

  expect(staged(json)).toEqual(canonical({ content: "é🚀", whenBusy: "steer" }));
  const malformed = Buffer.concat([
    Buffer.from('{"content":"a'),
    Buffer.from([0xff]),
    Buffer.from('b","whenBusy":"steer"}'),
  ]);

  expect(() => staged(malformed)).toThrow();
  expect(staged(malformed.toString("utf8"))).toEqual(
    canonical({ content: "a\ufffdb", whenBusy: "steer" }),
  );
  expect(() =>
    staged(Buffer.concat([Buffer.from([0xef, 0xbb, 0xbf]), Buffer.from(json)])),
  ).toThrow();
});
