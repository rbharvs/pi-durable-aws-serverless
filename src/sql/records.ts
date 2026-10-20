import type { ConversationRecord, EntryRecord } from "@earendil-works/pi-durable";
import type { Row } from "./db.ts";
import type { JsonValue } from "@earendil-works/chord";
import { check, integer, object, text, computeSha256 } from "../domain.ts";

export function assertJson(value: unknown): asserts value is JsonValue {
  if (value === null || typeof value === "string" || typeof value === "boolean") return;

  if (typeof value === "number") {
    check(Number.isFinite(value), "Non-finite JSON number");

    return;
  }

  check(typeof value === "object", "Not a JSON value");
  for (const item of Object.values(value)) assertJson(item);
}

/** JSON metadata is numeric; decimal-string parsing belongs only to SQL columns. */
function jsonId(value: unknown): void {
  check(typeof value === "number" && Number.isSafeInteger(value) && value > 0, "Invalid JSON ID");
}

function jsonPoint(value: unknown): void {
  check(
    typeof value === "number" && Number.isSafeInteger(value) && value >= 0,
    "Invalid JSON history point",
  );
}

function absent(v: Record<string, unknown>, ...names: string[]): void {
  for (const name of names) check(v[name] === undefined, `Unexpected ${name}`);
}

/** Pi trusts record semantics. At the DB boundary validate the storage envelope and state shapes. */
export function recordShape(value: unknown, type: string): void {
  const v = object(value);

  assertJson(v);
  jsonId(v.id);

  if (type === "conversation") {
    for (const key of ["owner", "parent"])
      if (v[key] !== undefined) {
        const edge = object(v[key]);

        jsonId(edge.conversationId);
        if (key === "owner") jsonId(edge.taskId);
        else jsonPoint(edge.at);
      }

    return;
  }

  jsonId(v.conversationId);

  if (type === "entry") {
    text(v.kind);
    if (v.head !== undefined) jsonPoint(v.head);
    if (v.byTaskId !== undefined) integer(v.byTaskId);

    // Message bodies are Pi/provider-owned JSON. Validate container/role, not provider-specific content.
    if (v.model !== undefined) {
      check(Array.isArray(v.model), "Invalid model messages");
      for (const message of v.model)
        check(
          ["system", "user", "assistant", "toolResult"].includes(text(object(message).role)),
          "Invalid message role",
        );
    }

    if (v.edits !== undefined) {
      check(Array.isArray(v.edits), "Invalid context edits");

      for (const item of v.edits) {
        const edit = object(item);

        integer(edit.target);
        if (edit.action === "omit") absent(edit, "messages");
        else
          check(edit.action === "replace" && Array.isArray(edit.messages), "Invalid context edit");
      }
    }

    return;
  }

  throw new Error("Invalid record type");
}

export type Records = { conversation: ConversationRecord; entry: EntryRecord };
export type RecordType = keyof Records;

/** Shape policy is checked once; each SQL index is then compared with the decoded source. */
export function validateRecord(value: unknown, row: Row, type: RecordType): void {
  recordShape(value, type);

  const v = object(value);

  check(v.id === integer(row.id), "Record ID mismatch");

  switch (type) {
    case "conversation": {
      const owner = v.owner === undefined ? undefined : object(v.owner);

      check(
        (owner?.conversationId ?? null) ===
          (row.owner_conversation_id === null ? null : integer(row.owner_conversation_id)) &&
          (owner?.taskId ?? null) ===
            (row.owner_task_id === null ? null : integer(row.owner_task_id)),
        "Conversation owner mismatch",
      );
      break;
    }

    case "entry":
      check(v.conversationId === integer(row.conversation_id), "Conversation ID mismatch");
      check(computeSha256(JSON.stringify(v.kind)) === row.kind_hash, "Kind mismatch");
      check((v.head !== undefined) === row.has_head, "Head mismatch");
      break;
  }
}
