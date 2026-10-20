import { expect, it } from "vitest";
import {
  StorageRequestError,
  type ConversationId,
  type TaskId,
  type SubmissionId,
} from "@earendil-works/pi-durable";
import { PayloadCorrupt } from "../../src/sql/payloads.ts";
import { BEGIN_WRITE } from "../../src/sql/db.ts";
import type { StorageWrite } from "@earendil-works/pi-durable";
import { context, dataEntry, entry, historyFixture, memoryObjects } from "./history-support.ts";
import { submission, submissionId, task, taskId } from "./task-support.ts";

for (const bad of ["20", 0, -1, 1.5, Number.MAX_SAFE_INTEGER + 1])
  for (const field of [
    "task.id",
    "task.conversationId",
    "task.owner",
    "task.on",
    "submission.id",
    "submission.conversationId",
    "submission.entry",
    "submission.answer",
    "entry.byTaskId",
  ])
    it(`rejects non-ID ${JSON.stringify(bad)} in ${field} before write SQL`, async () => {
      const f = await historyFixture("new_identity");
      try {
        const write = field.startsWith("entry")
          ? entry(40)
          : field.startsWith("task")
            ? task()
            : submission();
        const value: Record<string, unknown> = write.value;
        const key = field.split(".")[1]!;
        if (key === "on")
          value.state = { status: "waiting", checkpoint: null, on: [bad], policy: "allSettled" };
        else if (key === "entry" || key === "answer")
          Object.assign(value, { status: "done", entry: 40, answer: 41, [key]: bad });
        else value[key] = bad;
        await expect(f.storage.commit([write], context)).rejects.toBeInstanceOf(
          StorageRequestError,
        );
        expect(f.trace.some((q) => q.sql === BEGIN_WRITE)).toBe(false);
      } finally {
        await f.finish();
      }
    });

for (const value of [0, -1, -Number.MAX_SAFE_INTEGER, Number.MAX_SAFE_INTEGER])
  it(`treats signed safe selector ${value} as an ordinary empty read`, async () => {
    const f = await historyFixture("new_selector");
    try {
      expect(await f.storage.task(value as TaskId, context)).toBeUndefined();
      expect(await f.storage.submission(value as SubmissionId, context)).toBeUndefined();
      expect(
        await f.storage.submissionByRequest(value as ConversationId, "x", context),
      ).toBeUndefined();
      expect(
        (
          await f.storage.scanTasks(
            { conversationId: value as ConversationId },
            10,
            undefined,
            context,
          )
        ).items,
      ).toEqual([]);
      expect(
        (
          await f.storage.scanSubmissions(
            { conversationId: value as ConversationId },
            10,
            undefined,
            context,
          )
        ).items,
      ).toEqual([]);
    } finally {
      await f.finish();
    }
  });

for (const value of ["20", 1.5, Number.MAX_SAFE_INTEGER + 1, -Number.MAX_SAFE_INTEGER - 1])
  it(`rejects unsafe/non-number selector ${JSON.stringify(value)}`, async () => {
    const f = await historyFixture("bad_selector");
    try {
      for (const operation of [
        () => f.storage.task(value as TaskId, context),
        () => f.storage.submission(value as SubmissionId, context),
        () => f.storage.submissionByRequest(value as ConversationId, "x", context),
        () =>
          f.storage.scanTasks({ conversationId: value as ConversationId }, 10, undefined, context),
        () =>
          f.storage.scanSubmissions(
            { conversationId: value as ConversationId },
            10,
            undefined,
            context,
          ),
      ])
        await expect(operation()).rejects.toBeInstanceOf(StorageRequestError);
    } finally {
      await f.finish();
    }
  });

const corruptions: {
  type: "task" | "submission" | "entry";
  name: string;
  patch: Record<string, unknown>;
}[] = [
  ...["20", 0, -1, 1.5, Number.MAX_SAFE_INTEGER + 1].flatMap((value) => [
    { type: "task" as const, name: `id ${value}`, patch: { id: value } },
    { type: "task" as const, name: `owner ${value}`, patch: { owner: value } },
    {
      type: "task" as const,
      name: `join ${value}`,
      patch: { state: { status: "waiting", checkpoint: null, on: [value], policy: "allSettled" } },
    },
    {
      type: "submission" as const,
      name: `entry ${value}`,
      patch: { status: "placed", entry: value },
    },
    {
      type: "submission" as const,
      name: `answer ${value}`,
      patch: { status: "done", entry: 40, answer: value },
    },
    { type: "entry" as const, name: `byTaskId ${value}`, patch: { byTaskId: value } },
  ]),
  { type: "task", name: "conversation", patch: { conversationId: "1" } },
  { type: "task", name: "version", patch: { version: "1" } },
  { type: "task", name: "startedAt", patch: { startedAt: "1" } },
  { type: "task", name: "endedAt", patch: { endedAt: "1" } },
  { type: "task", name: "status", patch: { state: { status: "unknown" } } },
  { type: "task", name: "checkpoint", patch: { state: { status: "pending" } } },
  {
    type: "task",
    name: "policy",
    patch: { state: { status: "waiting", checkpoint: null, on: [], policy: "unknown" } },
  },
  {
    type: "task",
    name: "outcome",
    patch: { state: { status: "terminal", outcome: { status: "unknown" } } },
  },
  { type: "task", name: "memos", patch: { memos: [] } },
  {
    type: "task",
    name: "terminal memos",
    patch: {
      state: { status: "terminal", outcome: { status: "completed", result: null } },
      memos: {},
    },
  },
  { type: "task", name: "flags", patch: { background: "false" } },
  { type: "task", name: "abortReason", patch: { abortReason: "unknown" } },
  { type: "task", name: "abandonOnRestart", patch: { abandonOnRestart: false } },
  { type: "submission", name: "type", patch: { type: "unknown" } },
  { type: "submission", name: "status", patch: { status: "unknown" } },
  { type: "submission", name: "request", patch: { requestId: 12 } },
  { type: "submission", name: "queued entry", patch: { entry: 40 } },
  {
    type: "submission",
    name: "placed write",
    patch: { type: "write", status: "placed", entry: 40 },
  },
  {
    type: "submission",
    name: "unanswered write entry",
    patch: { type: "write", status: "unanswered", entry: 40, reason: "failed" },
  },
];
for (const { type, name, patch } of corruptions)
  it(`classifies stored ${type} ${name} as PayloadCorrupt, never a caller request error`, async () => {
    const f = await historyFixture("state_corrupt");
    try {
      const write: StorageWrite =
        type === "task" ? task(20) : type === "submission" ? submission(20) : entry(20);
      await f.storage.commit([write], context);
      await f.control.query("UPDATE inline_payloads SET json=$2 WHERE ns=$1", [
        f.ns,
        JSON.stringify({ ...write.value, ...patch }),
      ]);
      const read =
        type === "task"
          ? () => f.storage.task(taskId(20), context)
          : type === "submission"
            ? () => f.storage.submission(submissionId(20), context)
            : () => f.storage.entry(entry(20).value.id, context);
      await expect(read()).rejects.toBeInstanceOf(PayloadCorrupt);
      expect(f.storage.status).toBe("open");
    } finally {
      await f.finish();
    }
  });

// Representative read-corruption cases must also fail as writes, without persisting poison.
for (const { type, name, patch } of corruptions.filter(({ name }) =>
  [
    "checkpoint",
    "policy",
    "terminal memos",
    "abortReason",
    "type",
    "request",
    "placed write",
    "unanswered write entry",
  ].includes(name),
))
  it(`rejects malformed ${type} ${name} before queueing SQL or uploads`, async () => {
    const f = await historyFixture("invalid_record_write", { objects: memoryObjects });
    try {
      const healthy = type === "task" ? task(20) : submission(20);
      const bad = type === "task" ? task(21) : submission(21);
      const write = { ...bad, value: { ...bad.value, ...patch } } as StorageWrite;
      await f.storage.commit([healthy], context);
      const before = await f.snapshot();
      f.trace.length = 0;
      await expect(
        f.storage.commit([dataEntry(99, "x".repeat(300 * 1024)), write], context),
      ).rejects.toBeInstanceOf(StorageRequestError);
      expect(f.trace).toEqual([]);
      expect(f.memory.operations).toEqual([]);
      expect(await f.snapshot()).toEqual(before);
      expect(f.storage.status).toBe("open");
      const cold = await f.cold();
      const page =
        type === "task"
          ? await cold.scanTasks({}, 10, undefined, context)
          : await cold.scanSubmissions({}, 10, undefined, context);
      expect(page.items).toEqual([healthy.value]);
    } finally {
      await f.finish();
    }
  });

// Missing results are valid for Pi void tasks (see task-pi-records.test.ts).
// Non-JSON results must still fail: this JSON number overflows to Infinity on decode.
for (const status of ["completing", "terminal"] as const)
  it(`rejects a non-finite ${status} result through point reads and scans`, async () => {
    const f = await historyFixture("non_json_result");
    try {
      const write = task(20, { status, outcome: { status: "completed", result: null } });
      await f.storage.commit([write], context);
      const json = JSON.stringify(write.value).replace('"result":null', '"result":1e400');
      await f.control.query("UPDATE inline_payloads SET json=$2 WHERE ns=$1", [f.ns, json]);
      await expect(f.storage.task(taskId(20), context)).rejects.toMatchObject({
        cause: { message: "Non-finite JSON number" },
      });
      await expect(f.storage.scanTasks({ status }, 10, undefined, context)).rejects.toBeInstanceOf(
        PayloadCorrupt,
      );
    } finally {
      await f.finish();
    }
  });

for (const [type, assignment] of [
  ["task", "conversation_id=2"],
  ["task", "kind_hash='wrong'"],
  ["task", "status='running'"],
  ["task", "abort_requested=true"],
  ["task", "background=true"],
  ["submission", "conversation_id=2"],
  ["submission", "status='done'"],
  ["submission", "request_hash='wrong'"],
] as const)
  it(`rejects incoherent ${type} index ${assignment} through point reads and scans`, async () => {
    const f = await historyFixture("state_index");
    try {
      await f.storage.commit([type === "task" ? task(20) : submission(20, "request")], context);
      await f.control.query(`UPDATE records SET ${assignment} WHERE ns=$1`, [f.ns]);
      const read =
        type === "task"
          ? f.storage.task(taskId(20), context)
          : f.storage.submission(submissionId(20), context);
      await expect(read).rejects.toBeInstanceOf(PayloadCorrupt);
      const scan =
        type === "task"
          ? f.storage.scanTasks({}, 10, undefined, context)
          : f.storage.scanSubmissions({}, 10, undefined, context);
      await expect(scan).rejects.toBeInstanceOf(PayloadCorrupt);
    } finally {
      await f.finish();
    }
  });
