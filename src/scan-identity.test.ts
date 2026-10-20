import { expect, it } from "vitest";
import { StorageRequestError } from "@earendil-works/pi-durable";
import { scanStart, nextCursor } from "./sql/scan.ts";
import { idFromNumber, seqFromNumber } from "./ids.ts";

it("copied scan and ID helpers preserve public Pi selector/error conventions", () => {
  expect(idFromNumber(-1)).toBe(-1);
  expect(seqFromNumber(0)).toBe(0);
  for (const fallback of ["ascending", "descending"] as const) {
    expect(scanStart(undefined, undefined, fallback)).toEqual({
      order: fallback,
      after: undefined,
    });
    for (const after of [-1, 0, Number.MAX_SAFE_INTEGER])
      expect(scanStart(undefined, { after }, fallback)).toEqual({ order: fallback, after });
  }
  for (const order of ["ascending", "descending"] as const) {
    expect(scanStart(order, undefined, "ascending")).toEqual({ order, after: undefined });
    expect(scanStart(undefined, { after: -2, order }, "ascending")).toEqual({ order, after: -2 });
    expect(nextCursor(idFromNumber(7), order)).toEqual({ after: 7, order });
  }
  expect(() => scanStart("ascending", { after: 1, order: "descending" }, "ascending")).toThrow(
    "The cursor continues a descending scan; the query asks for ascending",
  );
  for (const cursor of [{}, { after: 1.5 }, { after: Infinity }, { after: 1, order: "invalid" }])
    expect(() => scanStart(undefined, cursor, "ascending")).toThrow(StorageRequestError);
});
