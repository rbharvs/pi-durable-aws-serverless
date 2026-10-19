import { expect, it } from "vitest";
import { requestId } from "../../src/domain.ts";
import { MAX_INLINE_INPUT_BYTES } from "../../src/input.ts";
import { namespace } from "./postgres.ts";
import { inlineInput } from "../shared/input.ts";

const plain = { content: "hello", whenBusy: "followUp" as const };

it("receipts require identity, payload, hash and time and reject replacement or oversized UTF-8", async () => {
  const f = await namespace("constraints"),
    db = f.control.inspectionDb;

  try {
    await f.control.receive(f.ns, requestId("inline"), inlineInput(plain));
    const before = await f.inbox();

    for (const column of ["ns", "request_id", "payload_sha256", "json", "received_at"])
      await expect(
        db.query(`UPDATE inbox SET ${column}=NULL WHERE ns=$1`, [f.ns]),
      ).rejects.toMatchObject({ code: "23502" });
    await expect(
      db.query("INSERT INTO inbox SELECT * FROM inbox WHERE ns=$1", [f.ns]),
    ).rejects.toMatchObject({ code: "23505" });
    await expect(
      db.query("UPDATE inbox SET json=repeat('é',131073) WHERE ns=$1", [f.ns]),
    ).rejects.toMatchObject({ code: "23514" });
    expect(await f.inbox()).toEqual(before);
  } finally {
    await f.finish();
  }
});

it("the largest canonical inline input fits the inline CHECK exactly", async () => {
  const f = await namespace("inline_bound");
  const envelope = Buffer.byteLength(JSON.stringify({ ...plain, content: "" })),
    largest = { ...plain, content: "é".repeat((MAX_INLINE_INPUT_BYTES - envelope) / 2) };

  try {
    const reply = await f.control.receive(f.ns, requestId("largest"), inlineInput(largest));

    expect(reply.duplicate).toBe(false);
    expect(
      await f.control.inspectionDb.query(
        "SELECT octet_length(json) AS bytes, length(json) AS characters FROM inbox WHERE ns=$1",
        [f.ns],
      ),
    ).toEqual([{ bytes: MAX_INLINE_INPUT_BYTES, characters: envelope + largest.content.length }]);
  } finally {
    await f.finish();
  }
});
