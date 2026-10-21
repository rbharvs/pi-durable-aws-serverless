import type { StorageWrite } from "@earendil-works/pi-durable";
import { assertValidOp } from "@earendil-works/chord/delta";
import type { JsonValue } from "@earendil-works/chord";
import { check, integer, object, text } from "./domain.ts";

export function assertJson(value: unknown): asserts value is JsonValue {
  if (value === null || typeof value === "string" || typeof value === "boolean") return;

  if (typeof value === "number") {
    check(Number.isFinite(value), "Non-finite JSON number");

    return;
  }

  check(typeof value === "object", "Not a JSON value");
  for (const item of Object.values(value)) {
    assertJson(item);
  }
}

export function jsonObject(value: unknown): { [key: string]: JsonValue } {
  assertJson(value);
  return object(value) as { [key: string]: JsonValue };
}

/** JSON metadata is numeric; decimal-string parsing belongs only to SQL columns. */
function jsonId(value: unknown): void {
  check(typeof value === "number" && Number.isSafeInteger(value) && value > 0, "Invalid JSON ID");
}

function jsonNumber(value: unknown): void {
  check(typeof value === "number" && Number.isFinite(value), "Invalid JSON number");
}

function jsonPoint(value: unknown): asserts value is number {
  check(
    typeof value === "number" && Number.isSafeInteger(value) && value >= 0,
    "Invalid JSON history point",
  );
}

function absent(record: Record<string, unknown>, ...names: string[]): void {
  for (const name of names) {
    check(record[name] === undefined, `Unexpected ${name}`);
  }
}

/** Pi trusts record semantics. At the adapter boundary validate the storage envelope and state shapes. */
export function recordShape(value: unknown, type: string): void {
  const record = object(value);

  assertJson(record);
  jsonId(record.id);

  if (type === "conversation") {
    for (const key of ["owner", "parent"]) {
      if (record[key] !== undefined) {
        const edge = object(record[key]);

        jsonId(edge.conversationId);
        if (key === "owner") jsonId(edge.taskId);
        else jsonPoint(edge.at);
      }
    }

    return;
  }

  if (type === "document") {
    const scope = object(record.scope);

    check(["session", "conversation", "task"].includes(text(scope.kind)), "Invalid document scope");
    jsonPoint(record.createdAt);
    if (record.retiredAt !== undefined) {
      jsonPoint(record.retiredAt);
      check(record.retiredAt >= record.createdAt, "Invalid document lifetime");
    }
    text(record.kind);
    if (record.key !== undefined) text(record.key);

    if (scope.kind === "conversation") {
      jsonId(scope.conversationId);
      check(
        record.history === "latest" || record.history === "rewindable",
        "Invalid document history",
      );
      check(
        ["current", "initial", ...(record.history === "rewindable" ? ["asOf"] : [])].includes(
          text(record.fork),
        ),
        "Invalid document fork",
      );
    } else {
      absent(record, "history", "fork");
      if (scope.kind === "task") jsonId(scope.taskId);
    }

    return;
  }

  jsonId(record.conversationId);

  if (type === "entry") {
    text(record.kind);
    if (record.head !== undefined) jsonPoint(record.head);
    if (record.byTaskId !== undefined) jsonId(record.byTaskId);

    // Message bodies are Pi/provider-owned JSON. Validate container/role, not provider-specific content.
    if (record.model !== undefined) {
      check(Array.isArray(record.model), "Invalid model messages");
      for (const message of record.model) {
        check(
          ["system", "user", "assistant", "toolResult"].includes(text(object(message).role)),
          "Invalid message role",
        );
      }
    }

    if (record.edits !== undefined) {
      check(Array.isArray(record.edits), "Invalid context edits");

      for (const item of record.edits) {
        const edit = object(item);

        integer(edit.target);
        if (edit.action === "omit") absent(edit, "messages");
        else {
          check(edit.action === "replace" && Array.isArray(edit.messages), "Invalid context edit");
        }
      }
    }

    return;
  }

  if (type === "task") {
    text(record.kind);
    jsonNumber(record.version);
    // Pi omits void input during its JSON copy; present values were checked by assertJson(record).
    check(
      typeof record.background === "boolean" && typeof record.abortRequested === "boolean",
      "Invalid task flags",
    );
    if (record.owner !== undefined) jsonId(record.owner);
    for (const key of ["startedAt", "endedAt"]) {
      if (record[key] !== undefined) jsonNumber(record[key]);
    }
    if (record.abortReason !== undefined) {
      check(record.abortReason === "restart", "Invalid abort reason");
    }
    if (record.abandonOnRestart !== undefined) {
      check(record.abandonOnRestart === true, "Invalid abandon flag");
    }

    const state = object(record.state);
    switch (state.status) {
      case "pending":
      case "running":
      case "waiting":
        assertJson(state.checkpoint);
        absent(state, "outcome");
        if (state.status === "waiting") {
          check(Array.isArray(state.on), "Invalid waiting tasks");
          for (const id of state.on) {
            jsonId(id);
          }
          check(
            state.policy === "failFast" || state.policy === "allSettled",
            "Invalid join policy",
          );
        }
        if (record.memos !== undefined) object(record.memos);
        break;
      case "completing":
      case "terminal": {
        absent(state, "checkpoint");
        absent(record, "memos");
        const outcome = object(state.outcome);
        switch (outcome.status) {
          case "completed":
            // A void result is omitted by Pi. Present results already passed assertJson(record).
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

  check(
    type === "submission" && (record.type === "input" || record.type === "write"),
    "Invalid record type",
  );
  if (record.requestId !== undefined) text(record.requestId);
  switch (record.status) {
    case "queued":
      absent(record, "entry", "answer", "reason", "detail");
      break;
    case "placed":
      check(record.type === "input", "Write cannot be placed");
      jsonId(record.entry);
      absent(record, "answer", "reason", "detail");
      break;
    case "done":
      jsonId(record.entry);
      if (record.type === "input") jsonId(record.answer);
      else absent(record, "answer");
      absent(record, "reason", "detail");
      break;
    case "unanswered":
      text(record.reason);
      absent(record, "answer");
      if (record.type === "input") {
        if (record.entry !== undefined) jsonId(record.entry);
      } else absent(record, "entry");
      break;
    default:
      throw new Error("Invalid submission status");
  }
}

/** Validate the normalized batch before any adapter mutation. */
export function writeShape(write: StorageWrite): void {
  switch (write.type) {
    case "document.create":
    case "document.copy":
      recordShape({ ...write.record, createdAt: 0 }, "document");
      check(
        !("createdAt" in write.record) && !("retiredAt" in write.record),
        "Storage stamps document lifetime",
      );
      if (write.type === "document.copy") {
        jsonId(write.source.id);
        if (write.source.at !== "current") jsonPoint(write.source.at);
        return;
      }
      check(write.content.kind === "base", "Document creation requires a base");
      break;
    case "document.change":
    case "document.retire":
      jsonId(write.id);
      if (write.type === "document.retire") return;
      break;
    case "conversation":
    case "entry":
    case "task":
    case "submission":
      recordShape(write.value, write.type);
      return;
    default:
      throw new Error("Unsupported write type");
  }
  jsonNumber(write.content.version);
  if (write.content.kind === "base") jsonObject(write.content.value);
  else {
    check(
      write.content.kind === "delta" && Array.isArray(write.content.ops),
      "Invalid document content",
    );
    for (const op of write.content.ops) {
      assertValidOp(op);
    }
  }
}
