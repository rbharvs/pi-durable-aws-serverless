import { expect, it } from "vitest";
import { errorCode, requestId, sha } from "../../src/domain.ts";
import { MAX_INLINE_INPUT_BYTES } from "../../src/input.ts";
import { namespace } from "./postgres.ts";
import { inlineInput } from "../shared/input.ts";

const plain = { content: "hello", whenBusy: "followUp" as const };

it("harness and inbox enforce every declared primary key, NOT NULL and CHECK constraint", async () => {
  const f = await namespace("declared_constraints"),
    db = f.control.inspectionDb;
  const rejects = async (sql: string, code: string) => {
    let actual: string | undefined;

    try {
      await db.query(sql, [f.ns]);
    } catch (error) {
      actual = errorCode(error);
    }

    expect(actual, sql).toBe(code);
  };

  try {
    await f.control.receive(f.ns, requestId("inline"), inlineInput(plain));
    await db.query(
      `INSERT INTO inbox(ns,request_id,payload_sha256,payload_kind,json,object_key,object_bytes,received_at,submission_id)
       VALUES($1,'s3',$2,'s3',NULL,$3,262145,0,NULL)`,
      [f.ns, sha("object"), `p/${f.ns}/00000000-0000-4000-8000-000000000000`],
    );

    const columns = await db.query(
      `SELECT table_name,column_name,is_nullable FROM information_schema.columns
       WHERE table_schema=current_schema() AND table_name IN ('harness','inbox') ORDER BY table_name,ordinal_position`,
    );

    expect(columns.map((c) => `${c.table_name}.${c.column_name}:${c.is_nullable}`)).toEqual([
      "harness.ns:NO",
      "harness.epoch:NO",
      "harness.owner:YES",
      "harness.lease_until:NO",
      "harness.next_seq:NO",
      "harness.next_id:NO",
      "inbox.ns:NO",
      "inbox.request_id:NO",
      "inbox.payload_sha256:NO",
      "inbox.payload_kind:NO",
      "inbox.json:YES",
      "inbox.object_key:YES",
      "inbox.object_bytes:YES",
      "inbox.received_at:NO",
      "inbox.submission_id:YES",
    ]);

    for (const c of columns.filter((c) => c.is_nullable === "NO"))
      await rejects(`UPDATE ${c.table_name} SET ${c.column_name}=NULL WHERE ns=$1`, "23502");
    for (const table of ["harness", "inbox"])
      await rejects(`INSERT INTO ${table} SELECT * FROM ${table} WHERE ns=$1`, "23505");
    for (const assignment of [
      "payload_kind='bad'",
      "object_key='in/invalid/a'",
      "object_bytes=1",
      "json=NULL",
      "json=repeat('é',131073)",
    ])
      await rejects(`UPDATE inbox SET ${assignment} WHERE ns=$1 AND request_id='inline'`, "23514");
    for (const assignment of [
      "json='{}'",
      "object_key=NULL",
      "object_bytes=NULL",
      "object_bytes=262144",
      "object_bytes=8388609",
    ])
      await rejects(`UPDATE inbox SET ${assignment} WHERE ns=$1 AND request_id='s3'`, "23514");
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
