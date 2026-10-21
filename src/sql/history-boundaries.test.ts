import { expect, it } from "vitest";
import { harnessId, computeSha256 } from "../domain.ts";
import type { PublishedPayload } from "../payload.ts";
import { Payloads } from "./payloads.ts";

it("a planned payload owns its immutable JSON and namespace before upload starts", async () => {
  const ns = harnessId("immutable_plan");
  const original = JSON.stringify({ text: "x".repeat(300 * 1024) });
  const uploads: string[] = [];
  const planner = new Payloads(ns, {
    async put(json) {
      uploads.push(json);
      return {
        kind: "s3",
        key: `p/${ns}/00000000-0000-4000-8000-000000000000` as PublishedPayload["key"],
        sha256: computeSha256(json),
        bytes: Buffer.byteLength(json),
      };
    },
    async get() {
      throw new Error("No reads expected during planning");
    },
  });
  const value = { ns, table: "records" as const, id: 20, json: original };
  const values = [value];
  const plan = planner.plan(values, 0);
  value.ns = harnessId("mutated");
  value.json = '"different"';
  values.length = 0;
  expect(uploads).toEqual([]);
  const prepared = await plan.upload();
  expect(uploads).toEqual([original]);
  expect(prepared).toHaveLength(1);
  expect(prepared[0]!.before[0]!.params).toEqual([
    ns,
    expect.any(String),
    `p/${ns}/00000000-0000-4000-8000-000000000000`,
    computeSha256(original),
    Buffer.byteLength(original),
  ]);
});
