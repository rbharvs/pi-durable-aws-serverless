import { expect, it } from "vitest";
import { EnvelopeRejected } from "../../src/payload.ts";
import { context, dataEntry, entryId, historyFixture, memoryObjects } from "./history-support.ts";
import { documentCreate, documentId } from "./document-support.ts";
import { task, taskId } from "./task-support.ts";

it("a real SQL error after document owner publication rolls back repeated tasks, entries, revisions and payload relinks together", async () => {
  let armed = false;
  const f = await historyFixture("document_rollback", {
    objects: memoryObjects,
    hook: async (phase, sql) => {
      if (armed && phase === "before" && sql.startsWith("INSERT INTO revisions")) {
        armed = false;
        // A server error on the same transaction, not a synthetic confirmed-abort claim.
        await f.db.query("SELECT 1/0");
      }
    },
  });
  try {
    await f.storage.commit([documentCreate(), task(30)], context);
    const before = await f.snapshot();
    armed = true;
    await expect(
      f.storage.commit(
        [
          task(30),
          task(30, { status: "terminal", outcome: { status: "completed", result: null } }),
          dataEntry(40, "x".repeat(300 * 1024)),
          documentCreate(21, "replacement"),
          { type: "document.retire", id: documentId(20) },
        ],
        context,
      ),
    ).rejects.toMatchObject({ code: "22012" });
    expect(await f.snapshot()).toEqual(before);
    expect(f.memory.operations.filter((op) => op.operation === "PUT")).toHaveLength(1); // orphan is permitted, never referenced
    const cold = await f.cold();
    expect((await cold.task(taskId(30), context))?.state.status).toBe("pending");
    expect(await cold.entry(entryId(40), context)).toBeUndefined();
    expect(await cold.document(documentId(21), "current", context)).toBeUndefined();
    expect((await cold.document(documentId(20), "current", context))?.value.count).toBe(1);
  } finally {
    await f.finish();
  }
});

for (const size of [17, 33]) {
  it(`rejects a ${size} MiB document ${size === 17 ? "object" : "logical batch"} before any upload or durable write`, async () => {
    const f = await historyFixture("document_byte_bound", { objects: memoryObjects });
    try {
      const before = await f.snapshot();
      const create = documentCreate();
      await expect(
        f.storage.commit(
          [
            {
              ...create,
              content: {
                kind: "base",
                version: 1,
                value: { text: "x".repeat(size * 1024 * 1024) },
              },
            },
          ],
          context,
        ),
      ).rejects.toBeInstanceOf(EnvelopeRejected);
      expect(f.memory.operations).toEqual([]);
      expect(await f.snapshot()).toEqual(before);
      expect(await f.storage.commit([documentCreate()], context)).toBe(1);
    } finally {
      await f.finish();
    }
  });
}

it("revisions require scoped identity and payload metadata and cannot overwrite an earlier sequence", async () => {
  const f = await historyFixture("revision_schema");
  try {
    await f.storage.commit([documentCreate()], context);
    for (const column of [
      "ns",
      "document_id",
      "seq",
      "kind",
      "version",
      "payload_kind",
      "payload_ref",
    ]) {
      await expect(
        f.control.query(`UPDATE revisions SET ${column}=NULL WHERE ns=$1`, [f.ns]),
      ).rejects.toMatchObject({ code: "23502" });
    }
    await expect(
      f.control.query("INSERT INTO revisions SELECT * FROM revisions WHERE ns=$1", [f.ns]),
    ).rejects.toMatchObject({ code: "23505" });
    await expect(
      f.control.query("UPDATE revisions SET payload_kind='other' WHERE ns=$1", [f.ns]),
    ).rejects.toMatchObject({ code: "23514" });
    const cold = await f.cold();
    expect((await cold.document(documentId(20), "current", context))?.value.count).toBe(1);
  } finally {
    await f.finish();
  }
});
