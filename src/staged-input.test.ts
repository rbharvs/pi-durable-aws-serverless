import { expect, it } from "vitest";
import { computeSha256, harnessId } from "./domain.ts";
import { canonical, MAX_IMAGE_BYTES, MAX_INPUT_BYTES, parseCommand } from "./input.ts";
import { withChangingProperty } from "../tests/shared/input.ts";

const ns = harnessId("input_bounds");
const reference = { kind: "s3", key: `in/${ns}/input.json`, sha256: computeSha256("{}"), bytes: 2 };

it("parses detached namespace-scoped references with numeric raw byte counts", () => {
  expect(parseCommand(reference, ns)).toEqual(reference);
  expect(parseCommand({ ...reference, bytes: MAX_INPUT_BYTES }, ns)).toEqual({
    ...reference,
    bytes: MAX_INPUT_BYTES,
  });
  expect(
    parseCommand(
      withChangingProperty({ ...reference }, "bytes", { first: 2, later: MAX_INPUT_BYTES + 1 }),
      ns,
    ),
  ).toEqual({ ...reference, bytes: 2 });
});

for (const [name, fields] of [
  ["foreign namespace", { key: "in/other/input" }],
  ["namespace prefix collision", { key: `in/${ns}_other/input` }],
  ["publication key", { key: `p/${ns}/00000000-0000-4000-8000-000000000001` }],
  ["URL", { key: "https://example.com/input" }],
  ["path traversal", { key: `in/${ns}/../input` }],
  ["encoded separator", { key: `in/${ns}/a%2fb` }],
  ["empty name", { key: `in/${ns}/` }],
  ["dot segment", { key: `in/${ns}/.` }],
  ["parent dot segment", { key: `in/${ns}/..` }],
  ["overlong name", { key: `in/${ns}/${"x".repeat(129)}` }],
  ["uppercase digest", { sha256: "A".repeat(64) }],
  ["short digest", { sha256: "a".repeat(63) }],
  ["string bytes", { bytes: "2" }],
  ["zero bytes", { bytes: 0 }],
  ["negative bytes", { bytes: -1 }],
  ["fractional bytes", { bytes: 1.5 }],
  ["nonfinite bytes", { bytes: Infinity }],
  ["raw overflow", { bytes: MAX_INPUT_BYTES + 1 }],
  ["extra field", { bucket: "caller-selected" }],
] as const) {
  it(`rejects staged reference ${name}`, () => {
    expect(() => parseCommand({ ...reference, ...fields }, ns)).toThrow();
  });
}

for (const name of ["input.json", ".hidden", "part..json", "..."]) {
  it(`accepts the ordinary staging filename ${name}`, () => {
    const command = { ...reference, key: `in/${ns}/${name}` };

    expect(parseCommand(command, ns)).toEqual(command);
  });
}

it("keeps the 6 MiB decoded-image bound separate from the aggregate JSON bound", () => {
  const image = {
    type: "image",
    mimeType: "image/png",
    data: Buffer.alloc(MAX_IMAGE_BYTES).toString("base64"),
  };
  const input = { content: [image], whenBusy: "followUp" };

  // An individually valid image still exceeds 8 MiB after base64 and JSON overhead.
  expect(() => canonical(input)).toThrow("Canonical input exceeds bounded size");
  expect(() =>
    canonical({
      ...input,
      content: [{ ...image, data: Buffer.alloc(MAX_IMAGE_BYTES + 1).toString("base64") }],
    }),
  ).toThrow("Invalid canonical base64 image");
});

it("preserves the 8 MiB canonical bound using UTF-8 bytes", () => {
  const maximumCanonicalBytes = 8 * 1024 * 1024;
  const empty = canonical({ content: "", whenBusy: "followUp" });
  const input = {
    content: "é".repeat((maximumCanonicalBytes - empty.bytes) / 2),
    whenBusy: "followUp",
  };

  expect(canonical(input).bytes).toBe(maximumCanonicalBytes);
  expect(() => canonical({ ...input, content: input.content + "x" })).toThrow(
    "Canonical input exceeds bounded size",
  );
  expect(() => parseCommand({ kind: "inline", input })).toThrow("Inline input exceeds 256 KiB");
});
