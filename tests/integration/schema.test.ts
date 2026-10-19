import { expect, it } from "vitest";
import { requestId } from "../../src/domain.ts";
import { MAX_INLINE_INPUT_BYTES } from "../../src/input.ts";
import { namespace } from "./postgres.ts";
import { acceptedInput } from "../shared/input.ts";

const plain = { content: "hello", whenBusy: "followUp" as const };

it("receipts require identity, payload, hash and time and reject replacement or oversized UTF-8", async () => {
  const store = await namespace("constraints");
  const db = store.authority.inspectionDb;

  try {
    await store.authority.receive(store.ns, requestId("inline"), acceptedInput(plain));
    const before = await store.inbox();

    for (const column of ["ns", "request_id", "payload_sha256", "json", "received_at"])
      await expect(
        db.query(`UPDATE inbox SET ${column}=NULL WHERE ns=$1`, [store.ns]),
      ).rejects.toMatchObject({ code: "23502" });
    await expect(
      db.query("INSERT INTO inbox SELECT * FROM inbox WHERE ns=$1", [store.ns]),
    ).rejects.toMatchObject({ code: "23505" });
    await expect(
      // 131073 two-byte characters are 262146 bytes, two over the 256 KiB cap.
      db.query("UPDATE inbox SET json=repeat('é',131073) WHERE ns=$1", [store.ns]),
    ).rejects.toMatchObject({ code: "23514" });
    expect(await store.inbox()).toEqual(before);
  } finally {
    await store.finish();
  }
});

it("the largest canonical inline input fits the inline CHECK exactly", async () => {
  const store = await namespace("inline_bound");
  const envelope = Buffer.byteLength(JSON.stringify({ ...plain, content: "" }));
  // Two UTF-8 bytes per "é" fill the cap exactly.
  const largest = { ...plain, content: "é".repeat((MAX_INLINE_INPUT_BYTES - envelope) / 2) };

  try {
    const reply = await store.authority.receive(
      store.ns,
      requestId("largest"),
      acceptedInput(largest),
    );

    expect(reply.duplicate).toBe(false);
    expect(
      await store.authority.inspectionDb.query(
        "SELECT octet_length(json) AS bytes, length(json) AS characters FROM inbox WHERE ns=$1",
        [store.ns],
      ),
    ).toEqual([{ bytes: MAX_INLINE_INPUT_BYTES, characters: envelope + largest.content.length }]);
  } finally {
    await store.finish();
  }
});
