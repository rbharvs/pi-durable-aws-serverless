import type {
  ConversationRecord,
  EntryRecord,
  TaskRecord,
  SubmissionRecord,
} from "@earendil-works/pi-durable";
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

function jsonNumber(value: unknown): void {
  check(typeof value === "number" && Number.isFinite(value), "Invalid JSON number");
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
    if (v.byTaskId !== undefined) jsonId(v.byTaskId);

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

  if (type === "task") {
    text(v.kind);
    jsonNumber(v.version);
    // Pi omits void input during its JSON copy; present values were checked by assertJson(v).
    check(
      typeof v.background === "boolean" && typeof v.abortRequested === "boolean",
      "Invalid task flags",
    );
    if (v.owner !== undefined) jsonId(v.owner);
    for (const key of ["startedAt", "endedAt"]) if (v[key] !== undefined) jsonNumber(v[key]);
    if (v.abortReason !== undefined) check(v.abortReason === "restart", "Invalid abort reason");
    if (v.abandonOnRestart !== undefined)
      check(v.abandonOnRestart === true, "Invalid abandon flag");

    const state = object(v.state);
    switch (state.status) {
      case "pending":
      case "running":
      case "waiting":
        assertJson(state.checkpoint);
        absent(state, "outcome");
        if (state.status === "waiting") {
          check(Array.isArray(state.on), "Invalid waiting tasks");
          for (const id of state.on) jsonId(id);
          check(
            state.policy === "failFast" || state.policy === "allSettled",
            "Invalid join policy",
          );
        }
        if (v.memos !== undefined) object(v.memos);
        break;
      case "completing":
      case "terminal": {
        absent(state, "checkpoint");
        absent(v, "memos");
        const outcome = object(state.outcome);
        switch (outcome.status) {
          case "completed":
            // A void result is omitted by Pi. Present results already passed assertJson(v).
            absent(outcome, "error", "reason");
            break;
          case "failed":
          case "faulted":
            text(object(outcome.error).message);
            absent(outcome, "reason");
            if (outcome.status === "faulted") absent(outcome, "result");
            break;
          case "aborted":
            if (outcome.reason !== undefined) text(outcome.reason);
            absent(outcome, "error");
            break;
          case "orphaned":
            text(outcome.reason);
            absent(outcome, "result", "error");
            break;
          default:
            throw new Error("Invalid task outcome");
        }
        break;
      }
      default:
        throw new Error("Invalid task status");
    }
    return;
  }

  check(type === "submission" && (v.type === "input" || v.type === "write"), "Invalid record type");
  if (v.requestId !== undefined) text(v.requestId);
  switch (v.status) {
    case "queued":
      absent(v, "entry", "answer", "reason", "detail");
      break;
    case "placed":
      check(v.type === "input", "Write cannot be placed");
      jsonId(v.entry);
      absent(v, "answer", "reason", "detail");
      break;
    case "done":
      jsonId(v.entry);
      if (v.type === "input") jsonId(v.answer);
      else absent(v, "answer");
      absent(v, "reason", "detail");
      break;
    case "unanswered":
      text(v.reason);
      absent(v, "answer");
      if (v.type === "input") {
        if (v.entry !== undefined) jsonId(v.entry);
      } else absent(v, "entry");
      break;
    default:
      throw new Error("Invalid submission status");
  }
}

export type Records = {
  conversation: ConversationRecord;
  entry: EntryRecord;
  task: TaskRecord<JsonValue, JsonValue, JsonValue>;
  submission: SubmissionRecord;
};
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

    case "task":
      check(v.conversationId === integer(row.conversation_id), "Conversation ID mismatch");
      check(computeSha256(JSON.stringify(v.kind)) === row.kind_hash, "Kind mismatch");
      check(object(v.state).status === row.status, "Task state mismatch");
      check(
        v.background === row.background && v.abortRequested === row.abort_requested,
        "Task flags mismatch",
      );
      break;

    case "submission":
      check(v.conversationId === integer(row.conversation_id), "Conversation ID mismatch");
      check(v.status === row.status, "Submission state mismatch");
      check(
        (v.requestId === undefined ? null : computeSha256(JSON.stringify(v.requestId))) ===
          row.request_hash,
        "Request mismatch",
      );
      break;
  }
}
