import { expect, it } from "vitest";
import {
  StorageRequestError,
  type Cursor,
  type SubmissionRecord,
  type TaskQuery,
} from "@earendil-works/pi-durable";
import { computeSha256 } from "../../src/domain.ts";
import type { StorageWrite } from "@earendil-works/pi-durable";
import { BEGIN_WRITE } from "../../src/sql/db.ts";
import {
  context,
  conversation,
  conversationId,
  entry,
  entryId,
  historyFixture,
  queryGate,
  rootId,
} from "./history-support.ts";
import { submission, submissionId, task, taskId, type StoredTask } from "./task-support.ts";

it("atomically stores queued submission, pending task and task-stamped entry, then resolves exact request identities cold", async () => {
  const f = await historyFixture("native_admission");
  const request = "exact\u0000 request 😀";
  const writes = [
    conversation(),
    task(),
    submission(30, request),
    entry(40, { byTaskId: taskId(20) }),
  ];
  try {
    const seq = await f.storage.commit(writes, context);
    const cold = await f.cold();
    expect(await cold.task(taskId(20), context)).toEqual(writes[1]!.value);
    expect(await cold.submission(submissionId(30), context)).toEqual(writes[2]!.value);
    expect(await cold.entry(entryId(40), context)).toEqual({
      entry: writes[3]!.value,
      commitSeq: seq,
    });
    expect(await cold.submissionByRequest(rootId, request, context)).toEqual(writes[2]!.value);
    expect(await cold.submissionByRequest(rootId, "different", context)).toBeUndefined();
    expect(await cold.submissionByRequest(conversationId(2), request, context)).toBeUndefined();
    expect(await cold.submissionByRequest(rootId, "", context)).toBeUndefined();
    const rows = await f.control.query("SELECT next_seq,next_id FROM harness WHERE ns=$1", [f.ns]);
    expect(rows).toEqual([{ next_seq: "2", next_id: "41" }]);
  } finally {
    await f.finish();
  }
});

it("does not confuse a hash candidate with the exact logical submission request", async () => {
  const f = await historyFixture("request_candidate", {
    hook: async (phase, sql, params) => {
      // Simulate a hash collision at the candidate selector, without corrupting either stored envelope.
      if (phase === "before" && sql.includes("AND request_hash=$3"))
        (params as unknown[])[2] = computeSha256(JSON.stringify("other"));
    },
  });
  try {
    await f.storage.commit([submission(30, "other")], context);
    expect(await f.storage.submissionByRequest(rootId, "wanted", context)).toBeUndefined();
    expect(await f.storage.submissionByRequest(rootId, "other", context)).toEqual(
      submission(30, "other").value,
    );
  } finally {
    await f.finish();
  }
});

it("persists valid optional undefined fields as absent rather than rejecting Pi-owned records", async () => {
  const f = await historyFixture("optional_fields");
  try {
    const write = {
      type: "task" as const,
      value: {
        ...task().value,
        owner: undefined,
        startedAt: undefined,
        abortReason: undefined,
        memos: undefined,
      },
    };
    await f.storage.commit(
      [write, { ...submission(), value: { ...submission().value, requestId: undefined } }],
      context,
    );
    expect(await f.storage.task(taskId(20), context)).toEqual(task().value);
    expect(await f.storage.submission(submissionId(30), context)).toEqual(submission().value);
  } finally {
    await f.finish();
  }
});

it("rolls back task, submission and entry together when publication fails midway", async () => {
  let armed = true;
  const f = await historyFixture("admission_atomic", {
    hook: async (phase, sql, params) => {
      if (
        armed &&
        phase === "after" &&
        sql.startsWith("INSERT INTO records(") &&
        params[2] === "submission"
      ) {
        armed = false;
        throw Error("publication failed");
      }
    },
  });
  try {
    const before = await f.snapshot();
    const writes = [entry(40), task(), submission()];
    await expect(f.storage.commit(writes, context)).rejects.toThrow("publication failed");
    expect(await f.snapshot()).toEqual(before);
    expect(await f.storage.task(taskId(20), context)).toBeUndefined();
    expect(await f.storage.submission(submissionId(30), context)).toBeUndefined();
    expect(await f.storage.entry(entryId(40), context)).toBeUndefined();
    await f.storage.commit(writes, context);
    expect(await f.storage.task(taskId(20), context)).toEqual(task().value);
  } finally {
    await f.finish();
  }
});

const outcomes: Extract<StoredTask["state"], { status: "terminal" }>["outcome"][] = [
  { status: "completed", result: { answer: 42 } },
  { status: "failed", error: { message: "expected", detail: [1, null] }, result: false },
  { status: "aborted", reason: "cancelled", result: [] },
  { status: "orphaned", reason: "definition unavailable" },
  { status: "faulted", error: { message: "contract", detail: { failed: true } } },
];
const states: StoredTask["state"][] = [
  { status: "pending", checkpoint: { next: 1 } },
  { status: "running", checkpoint: [true, null] },
  { status: "waiting", checkpoint: null, on: [taskId(50)], policy: "allSettled" },
  { status: "waiting", checkpoint: 0, on: [], policy: "failFast" },
  ...outcomes.flatMap((outcome) => [
    { status: "completing" as const, outcome },
    { status: "terminal" as const, outcome },
  ]),
];

for (const [i, state] of states.entries())
  it(`replaces and cold-reads Pi task state ${state.status}/${i}, preserving metadata and flags`, async () => {
    const f = await historyFixture("task_state");
    try {
      await f.storage.commit([task()], context);
      const base = {
        ...task(20, state).value,
        input: { original: [1, "x"] },
        owner: taskId(19),
        background: true,
        abortRequested: true,
        abortReason: "restart" as const,
        abandonOnRestart: true as const,
        startedAt: 12,
        ...(state.status === "terminal" ? { endedAt: 34 } : {}),
      };
      const value: StoredTask =
        state.status === "pending" || state.status === "running" || state.status === "waiting"
          ? { ...base, state, memos: { retained: { yes: true } } }
          : { ...base, state };
      await f.storage.commit([{ type: "task", value }], context);
      const cold = await f.cold();
      expect(await cold.task(taskId(20), context)).toEqual(value);
      const query = {
        conversationId: rootId,
        kind: "test",
        status: state.status,
        background: true,
        abortRequested: true,
      };
      expect((await cold.scanTasks(query, 10, undefined, context)).items).toEqual([value]);
      for (const mismatch of [
        { conversationId: conversationId(9) },
        { kind: "other" },
        { background: false },
        { abortRequested: false },
      ])
        expect(
          (await cold.scanTasks({ ...query, ...mismatch }, 10, undefined, context)).items,
        ).toEqual([]);
    } finally {
      await f.finish();
    }
  });

const submissionStates: SubmissionRecord[] = [
  { ...submission().value },
  { ...submission().value, status: "placed", entry: entryId(40) },
  { ...submission().value, status: "done", entry: entryId(40), answer: entryId(41) },
  { ...submission().value, status: "unanswered", reason: "restart", detail: { info: [1, null] } },
  { ...submission().value, status: "unanswered", entry: entryId(40), reason: "cancelled" },
  { ...submission().value, type: "write" },
  { ...submission().value, type: "write", status: "done", entry: entryId(40) },
  { ...submission().value, type: "write", status: "unanswered", reason: "failed", detail: false },
];
for (const [i, state] of submissionStates.entries())
  it(`replaces and cold-reads Pi submission state ${state.type}/${state.status}/${i}`, async () => {
    const f = await historyFixture("submission_state");
    try {
      await f.storage.commit([submission(30, "old")], context);
      const value = { ...state, requestId: i === 0 ? "" : "new\u0000😀" };
      await f.storage.commit([{ type: "submission", value }], context);
      const cold = await f.cold();
      expect(await cold.submission(submissionId(30), context)).toEqual(value);
      expect(
        (
          await cold.scanSubmissions(
            { conversationId: rootId, status: state.status },
            10,
            undefined,
            context,
          )
        ).items,
      ).toEqual([value]);
      expect(
        (await cold.scanSubmissions({ conversationId: conversationId(9) }, 10, undefined, context))
          .items,
      ).toEqual([]);
      expect(await cold.submissionByRequest(rootId, "old", context)).toBeUndefined();
      expect(await cold.submissionByRequest(rootId, value.requestId, context)).toEqual(value);
    } finally {
      await f.finish();
    }
  });

it("scans tasks and submissions in both orders across cursors, without borrowing a tenant's records", async () => {
  const f = await historyFixture("record_pages");
  const other = await historyFixture("record_pages_other");
  try {
    const writes = [task(20), task(22), task(24), submission(30), submission(32), submission(34)];
    await f.storage.commit(writes, context);
    await other.storage.commit([task(21), submission(31)], context);
    for (const type of ["task", "submission"] as const) {
      const scan =
        type === "task"
          ? f.storage.scanTasks.bind(f.storage)
          : f.storage.scanSubmissions.bind(f.storage);
      for (const order of ["ascending", "descending"] as const) {
        const ids: number[] = [];
        let cursor: Cursor | undefined;
        do {
          const page = await scan(cursor ? {} : { order }, 1, cursor, context);
          ids.push(...page.items.map((r) => r.id));
          cursor = page.next;
        } while (cursor);
        const expected = type === "task" ? [20, 22, 24] : [30, 32, 34];
        expect(ids).toEqual(order === "ascending" ? expected : expected.reverse());
        const first = await scan({ order }, 1, undefined, context);
        await expect(
          scan(
            { order: order === "ascending" ? "descending" : "ascending" },
            1,
            first.next,
            context,
          ),
        ).rejects.toBeInstanceOf(StorageRequestError);
      }
    }
    expect(await f.storage.task(taskId(21), context)).toBeUndefined();
    expect(await f.storage.submission(submissionId(31), context)).toBeUndefined();
  } finally {
    await Promise.all([f.finish(), other.finish()]);
  }
});

it("detaches replacement batches, query flags and cursors before waiting for the connection gate", async () => {
  const gate = queryGate(
    "before",
    (sql) => sql.startsWith("SELECT") && sql.includes("AND id=$2 AND type=$3"),
  );
  const f = await historyFixture("record_detach", { hook: gate.hook });
  let blocked: Promise<unknown> | undefined;
  let committed: Promise<unknown> | undefined;
  let scanned: Promise<unknown> | undefined;
  try {
    await f.storage.commit([task()], context);
    gate.arm();
    blocked = f.storage.task(taskId(20), context);
    await gate.reached;
    const write = { ...task(), value: { ...task().value, input: { original: true } } };
    const batch: StorageWrite[] = [write, submission()];
    committed = f.storage.commit(batch, context);
    const query: TaskQuery = { background: false };
    const cursor = { after: 0, order: "ascending" as const };
    scanned = f.storage.scanTasks(query, 10, cursor, context);
    write.value.input.original = false;
    batch.length = 0;
    Object.assign(query, { background: true });
    cursor.after = 999;
    gate.release();
    await Promise.all([blocked, committed]);
    expect(await scanned).toMatchObject({ items: [{ input: { original: true } }] });
    expect(await f.storage.submission(submissionId(30), context)).toEqual(submission().value);
  } finally {
    gate.release();
    await Promise.allSettled([blocked, committed, scanned]);
    await f.finish();
  }
});

for (const first of [conversation(20), entry(20), task(20), submission(20)])
  for (const second of [conversation(20), entry(20), task(20), submission(20)].filter(
    (w) => w.type !== first.type,
  ))
    for (const existing of [true, false])
      it(`rejects ${first.type}/${second.type} ID collision atomically (${existing ? "stored" : "batch"})`, async () => {
        const f = await historyFixture("record_collision");
        try {
          if (existing) await f.storage.commit([first], context);
          const before = await f.snapshot();
          f.trace.length = 0;
          await expect(
            f.storage.commit([entry(99), ...(existing ? [] : [first]), second], context),
          ).rejects.toThrow(/belongs|two record types|more than once/);
          expect(f.trace.some((q) => q.sql === BEGIN_WRITE)).toBe(false);
          expect(await f.snapshot()).toEqual(before);
        } finally {
          await f.finish();
        }
      });
