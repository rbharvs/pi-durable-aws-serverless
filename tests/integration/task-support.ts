import type { JsonValue } from "@earendil-works/chord";
import type {
  SubmissionId,
  SubmissionRecord,
  TaskId,
  TaskRecord,
} from "@earendil-works/pi-durable";
import { idFromNumber } from "../../src/ids.ts";
import { rootId } from "./history-support.ts";

export type StoredTask = TaskRecord<JsonValue, JsonValue, JsonValue>;
export const taskId = (n: number) => idFromNumber<TaskId<JsonValue>>(n);
export const submissionId = (n: number) => idFromNumber<SubmissionId>(n);

export function task(n = 20, state: StoredTask["state"] = { status: "pending", checkpoint: null }) {
  const base = {
    id: taskId(n),
    conversationId: rootId,
    kind: "test",
    version: 1,
    input: null,
    background: false,
    abortRequested: false,
  };
  const value: StoredTask & { memos?: never } =
    state.status === "pending" || state.status === "running" || state.status === "waiting"
      ? { ...base, state }
      : { ...base, state };
  return { type: "task" as const, value };
}

export const submission = (n = 30, requestId?: string) => ({
  type: "submission" as const,
  value: {
    id: submissionId(n),
    conversationId: rootId,
    type: "input" as const,
    status: "queued" as const,
    ...(requestId === undefined ? {} : { requestId }),
  } satisfies SubmissionRecord,
});
