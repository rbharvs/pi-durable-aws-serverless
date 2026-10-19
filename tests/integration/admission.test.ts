import type pg from "pg";
import { expect, inject, it } from "vitest";
import {
  apiHandler,
  requestId,
  type ApiDependencies,
  type SubmitResponse,
} from "../../src/index.ts";
import { SqlAuthority } from "../../src/sql/authority.ts";
import { postgresProvider } from "../../src/sql/connection.ts";
import { Database } from "../../src/sql/db.ts";
import { canonical, MAX_INLINE_INPUT_BYTES } from "../../src/input.ts";
import {
  confirmedAbort,
  faultOnce,
  namespace,
  provider,
  statements,
  traced,
  wrap,
  type Query,
  type QueryHook,
} from "./postgres.ts";
import { acceptedInput, withChangingProperty } from "../shared/input.ts";

const plain = { content: "hello", whenBusy: "followUp" as const };

const image = { type: "image", data: "AQID", mimeType: "image/png" };

/** Builds the wire command for an inline Pi input. */
const inlineCommand = (input: unknown) => ({ kind: "inline", input });

/** The actual composition: one fresh real pg connection per invocation, owned by the handler. */
const connectSqlAuthority = async () =>
  new SqlAuthority(await Database.connect(provider), provider);

const connectTracedAuthority = (hook: QueryHook, trace: Query[]) => async () =>
  (await traced(hook, trace)).authority;

/** A submit handler over real PostgreSQL that records wakes, events, and reports. */
function submitApi(overrides: Partial<ApiDependencies> = {}) {
  const wakes: string[] = [];
  const events: Record<string, unknown>[] = [];
  const reports: unknown[] = [];
  const run = apiHandler({
    connectAuthority: connectSqlAuthority,
    dispatch: {
      wake: async (ns) => {
        wakes.push(ns);
      },
    },
    event: (name, fields) => events.push({ name, ...fields }),
    report: (cause) => reports.push(cause),
    ...overrides,
  });

  return { run, wakes, events, reports };
}

/** Returns `receivedAt` from a success response, or fails the test. */
function receivedAtOf(response: SubmitResponse): number {
  if (!response.ok) throw Error(`Expected acceptance, got ${response.code}`);

  return response.receivedAt;
}

/** A real pg client whose close completes, then reports a failed acknowledgement. */
function failingClose(client: pg.Client, closes: { count: number }): pg.Client {
  return new Proxy(client, {
    get(target, prop) {
      if (prop === "end")
        return async () => {
          closes.count++;
          await target.end();

          throw Error("close acknowledgement failed");
        };

      const value: unknown = Reflect.get(target, prop);

      return typeof value === "function" ? value.bind(target) : value;
    },
  });
}

const unavailable = { ok: false, code: "unavailable", retryable: true };

it("acknowledges an immutable canonical receipt that a fresh connection reads after close", async () => {
  const store = await namespace("accept");
  const api = submitApi();
  const input = {
    content: [{ type: "text", text: "a", textSignature: "s" }, image],
    whenBusy: "followUp",
  };

  try {
    const first = await api.run({ ns: store.ns, request: "one", command: inlineCommand(input) });

    expect(first).toEqual({ ok: true, duplicate: false, receivedAt: expect.any(Number) });
    expect(Math.abs(receivedAtOf(first) - Date.now())).toBeLessThan(60000);
    expect(api.wakes).toEqual([store.ns]);
    expect(api.events).toEqual([
      { name: "durable-ack", ns: store.ns, request: "one", duplicate: false },
    ]);

    const reader = await connectSqlAuthority();
    const expected = canonical(input);

    try {
      expect(await reader.get(store.ns, requestId("one"))).toEqual({
        kind: "received",
        ns: store.ns,
        request: "one",
        hash: expected.sha256,
        payload: { kind: "inline", json: expected.json },
        receivedAt: receivedAtOf(first),
      });
    } finally {
      await reader.close();
    }

    const reordered = {
      whenBusy: "followUp",
      content: [
        { textSignature: "s", text: "a", type: "text" },
        { mimeType: "image/png", data: "AQID", type: "image" },
      ],
    };

    expect(
      await api.run({ ns: store.ns, request: "one", command: inlineCommand(reordered) }),
    ).toEqual({
      ...first,
      duplicate: true,
    });

    const before = await store.inbox();

    for (const changed of [
      { ...input, whenBusy: "steer" },
      { ...input, content: [input.content[0], { ...image, data: "AQIE" }] },
      { ...input, content: [...input.content].reverse() },
      { ...input, content: [{ type: "text", text: "a", textSignature: "other" }, image] },
      { ...input, content: [{ type: "text", text: "a" }, image] },
    ])
      expect(
        await api.run({ ns: store.ns, request: "one", command: inlineCommand(changed) }),
      ).toEqual({
        ok: false,
        code: "request-conflict",
        retryable: false,
      });

    expect(await store.inbox()).toEqual(before);
    expect(before).toHaveLength(1);
    expect(api.wakes).toEqual([store.ns, store.ns]);
  } finally {
    await store.finish();
  }
});

it("five concurrent equal requests produce exactly one first acceptance of arbitrary strings", async () => {
  const store = await namespace("api_race");
  const api = submitApi();
  const input = {
    content: [
      { type: "text", text: "\ud800 \udc00x\u0000 🚀 __proto__", textSignature: "\\u0000" },
    ],
    whenBusy: "steer",
  };

  try {
    const replies = await Promise.all(
      Array.from({ length: 5 }, () =>
        api.run({ ns: store.ns, request: "race", command: inlineCommand(input) }),
      ),
    );

    expect(replies.filter((r) => r.ok && !r.duplicate)).toHaveLength(1);
    expect(new Set(replies.map(receivedAtOf)).size).toBe(1);
    expect(api.wakes).toHaveLength(5);

    const rows = await store.inbox();

    expect(rows).toHaveLength(1);
    expect(rows[0].json).toBe(canonical(input).json);
    expect(JSON.parse(String(rows[0].json))).toEqual(input);
  } finally {
    await store.finish();
  }
});

it("rejects invalid input without acquiring a connection or creating state", async () => {
  const store = await namespace("api_invalid");
  let connects = 0;
  const api = submitApi({
    connectAuthority: async () => {
      connects++;

      return connectSqlAuthority();
    },
  });

  const sparse: unknown[] = [];
  const sparseWithText: unknown[] = [];

  sparse.length = 1;
  sparseWithText[1] = { type: "text", text: "after hole" };

  try {
    for (const command of [
      { kind: "s3", key: `in/${store.ns}/a`, sha256: "0".repeat(64), bytes: 2 },
      inlineCommand({ ...plain, content: "x".repeat(MAX_INLINE_INPUT_BYTES) }),
      inlineCommand({ ...plain, content: [{ ...image, data: "AR==" }] }),
      inlineCommand({ ...plain, whenBusy: "reject" }),
      inlineCommand({ ...plain, content: sparse }),
      inlineCommand({ ...plain, content: sparseWithText }),
      { ...inlineCommand(plain), extra: true },
    ])
      expect(await api.run({ ns: store.ns, request: "bad", command })).toEqual({
        ok: false,
        code: "invalid-input",
        retryable: false,
      });

    expect(
      await api.run({ ns: "bad/path", request: "bad", command: inlineCommand(plain) }),
    ).toMatchObject({
      code: "invalid-input",
    });
    expect(
      await api.run({ ns: store.ns, request: "bad request", command: inlineCommand(plain) }),
    ).toMatchObject({ code: "invalid-input" });
    expect(connects).toBe(0);
    expect(api.wakes).toEqual([]);
    expect(await store.inbox()).toEqual([]);
  } finally {
    await store.finish();
  }
});

it("detaches the request before its first await", async () => {
  const store = await namespace("api_detach");
  const input = {
    ns: store.ns,
    request: "detach",
    command: inlineCommand({ ...plain, content: "original" }),
  };
  const api = submitApi({
    connectAuthority: async () => {
      input.command = inlineCommand({ ...plain, content: "mutated" });
      input.request = "other";

      return connectSqlAuthority();
    },
  });

  try {
    expect(await api.run(input)).toMatchObject({ ok: true, duplicate: false });

    const rows = await store.inbox();

    expect(rows.map((r) => [r.request_id, r.json])).toEqual([
      ["detach", canonical({ ...plain, content: "original" }).json],
    ]);
  } finally {
    await store.finish();
  }
});

it("stores the validated first read of an accessor input, not a caller-owned value mutated later", async () => {
  const store = await namespace("api_accessor");
  const later: unknown[] = [];
  const input = withChangingProperty({ whenBusy: "followUp" }, "content", { first: "x", later });
  const api = submitApi({
    connectAuthority: async () => {
      later.push({ type: "text", text: "mutated after validation" });

      return connectSqlAuthority();
    },
  });

  try {
    expect(
      await api.run({ ns: store.ns, request: "accessor", command: inlineCommand(input) }),
    ).toMatchObject({
      ok: true,
      duplicate: false,
    });
    expect((await store.inbox()).map((r) => r.json)).toEqual([
      '{"content":"x","whenBusy":"followUp"}',
    ]);
  } finally {
    await store.finish();
  }
});

it("a confirmed INSERT abort is re-read and then acknowledged once", async () => {
  const store = await namespace("api_abort");
  const trace: Query[] = [];
  const api = submitApi({
    connectAuthority: connectTracedAuthority(
      faultOnce("before", "INSERT INTO inbox", confirmedAbort),
      trace,
    ),
  });

  try {
    expect(
      await api.run({ ns: store.ns, request: "one", command: inlineCommand(plain) }),
    ).toMatchObject({
      ok: true,
      duplicate: false,
    });
    expect(statements(trace, "INSERT INTO inbox")).toHaveLength(2);
    expect(await store.inbox()).toHaveLength(1);
  } finally {
    await store.finish();
  }
});

for (const applied of [true, false])
  it(`a lost receipt acknowledgement (applied=${applied}) is reported as unknown, never as rejection`, async () => {
    const store = await namespace("api_lost_ack");
    const trace: Query[] = [];
    const lossy = submitApi({
      connectAuthority: connectTracedAuthority(
        faultOnce(applied ? "after" : "before", "INSERT INTO inbox", () =>
          Error("lost receipt acknowledgement"),
        ),
        trace,
      ),
    });
    const submission = { ns: store.ns, request: "unknown", command: inlineCommand(plain) };

    try {
      // Applied and unapplied outcomes are indistinguishable to the caller, so the response is identical.
      expect(await lossy.run(submission)).toEqual(unavailable);
      expect(statements(trace, "INSERT INTO inbox")).toHaveLength(1);
      expect(lossy.wakes).toEqual([]);
      expect(lossy.events).toEqual([
        { name: "submit-failed", code: "unavailable", phase: "receipt-write" },
      ]);
      expect(lossy.reports[0]).toMatchObject({
        phase: "receipt-write",
        cause: { message: "lost receipt acknowledgement" },
      });
      expect(await store.inbox()).toHaveLength(applied ? 1 : 0);

      const retry = await submitApi().run(submission);

      expect(retry).toMatchObject({ ok: true, duplicate: applied });
      if (applied) expect(receivedAtOf(retry)).toBe(Number((await store.inbox())[0].received_at));
    } finally {
      await store.finish();
    }
  });

it("a receipt-read fault is unavailable and never writes", async () => {
  const store = await namespace("api_read_fault");
  const trace: Query[] = [];
  const api = submitApi({
    connectAuthority: connectTracedAuthority(
      faultOnce("before", "SELECT * FROM inbox", () => Error("SELECT private read failed")),
      trace,
    ),
  });

  try {
    const response = await api.run({ ns: store.ns, request: "one", command: inlineCommand(plain) });

    expect(response).toEqual(unavailable);
    expect(api.reports[0]).toMatchObject({
      phase: "receipt-write",
      cause: { message: "SELECT private read failed" },
    });
    expect(api.events).toEqual([
      { name: "submit-failed", code: "unavailable", phase: "receipt-write" },
    ]);
    expect(statements(trace, "INSERT INTO inbox")).toEqual([]);
    expect(await store.inbox()).toEqual([]);
    expect(api.wakes).toEqual([]);
  } finally {
    await store.finish();
  }
});

for (const winner of ["identical", "different"] as const)
  it(`an INSERT race with a ${winner} winner returns the winner's outcome`, async () => {
    const store = await namespace("api_insert_race");
    const trace: Query[] = [];
    const other = { ...plain, content: "race winner" };
    let raced = false;
    let winnerTime: number | undefined;
    const api = submitApi({
      connectAuthority: connectTracedAuthority(async (phase, sql) => {
        if (raced || phase !== "before" || !sql.startsWith("INSERT INTO inbox")) return;
        raced = true;
        winnerTime = (
          await store.authority.receive(
            store.ns,
            requestId("race"),
            acceptedInput(winner === "identical" ? plain : other),
          )
        ).item.receivedAt;
      }, trace),
    });

    try {
      const response = await api.run({
        ns: store.ns,
        request: "race",
        command: inlineCommand(plain),
      });

      if (winner === "identical")
        expect(response).toEqual({ ok: true, duplicate: true, receivedAt: winnerTime });
      else expect(response).toEqual({ ok: false, code: "request-conflict", retryable: false });
      expect(statements(trace, "INSERT INTO inbox")).toHaveLength(1);
      expect(raced).toBe(true);
      expect(api.wakes).toEqual(winner === "identical" ? [store.ns] : []);
      expect((await store.authority.get(store.ns, requestId("race")))?.payload).toEqual(
        acceptedInput(winner === "identical" ? plain : other).payload,
      );
    } finally {
      await store.finish();
    }
  });

it("a failed wake does not undo the durable acknowledgement", async () => {
  const store = await namespace("api_failed_wake");
  let observed: unknown;
  const api = submitApi({
    dispatch: {
      wake: async (ns) => {
        observed = await store.authority.get(ns, requestId("one"));

        throw Error("missing wake");
      },
    },
  });

  try {
    const response = await api.run({ ns: store.ns, request: "one", command: inlineCommand(plain) });

    expect(response).toMatchObject({ ok: true, duplicate: false });
    expect(observed).toMatchObject({ kind: "received", receivedAt: receivedAtOf(response) });
    expect(api.events).toEqual([
      { name: "durable-ack", ns: store.ns, request: "one", duplicate: false },
      { name: "hint-failed", ns: store.ns },
    ]);
    expect(api.reports).toEqual([]);
  } finally {
    await store.finish();
  }
});

it("a known receipt survives wake, observer, report and close failures with the close attempted once", async () => {
  const store = await namespace("api_diagnostics");
  const closes = { count: 0 };
  const clients: pg.Client[] = [];
  const run = apiHandler({
    connectAuthority: async () => {
      const client = await provider.connect();

      clients.push(client);

      return new SqlAuthority(new Database(failingClose(client, closes)), provider);
    },
    dispatch: {
      wake: async () => {
        throw Error("wake");
      },
    },
    event: () => {
      throw Error("observer");
    },
    report: () => {
      throw Error("report");
    },
  });

  try {
    expect(
      await run({ ns: store.ns, request: "one", command: inlineCommand(plain) }),
    ).toMatchObject({
      ok: true,
      duplicate: false,
    });
    expect(closes.count).toBe(1);
    await expect(clients[0].query("SELECT 1")).rejects.toThrow();
    expect(await store.inbox()).toHaveLength(1);
  } finally {
    await store.finish();
  }
});

it("an unknown outcome stays unavailable when cleanup fails, and cleanup is reported without causes", async () => {
  const store = await namespace("api_cleanup");
  const closes = { count: 0 };
  const api = submitApi({
    connectAuthority: async () => {
      const client = wrap(
        await provider.connect(),
        faultOnce("after", "INSERT INTO inbox", () => Error("SELECT private lost acknowledgement")),
      );

      return new SqlAuthority(new Database(failingClose(client, closes)), provider);
    },
  });

  try {
    expect(await api.run({ ns: store.ns, request: "one", command: inlineCommand(plain) })).toEqual(
      unavailable,
    );
    expect(closes.count).toBe(1);
    expect(api.events).toEqual([
      { name: "submit-failed", code: "unavailable", phase: "receipt-write" },
      { name: "cleanup-failed", resource: "authority" },
    ]);
    expect(JSON.stringify(api.events)).not.toMatch(/SELECT|private|close acknowledgement/);
  } finally {
    await store.finish();
  }
});

it("a real connection acquisition failure is unavailable and keeps provider details private", async () => {
  const { port } = inject("postgres");
  const wrongPassword = postgresProvider(port, "synthetic-wrong-password", "integration");
  const api = submitApi({
    connectAuthority: async () =>
      new SqlAuthority(await Database.connect(wrongPassword), wrongPassword),
  });
  const response = await api.run({
    ns: "api_acquire",
    request: "one",
    command: inlineCommand(plain),
  });

  expect(response).toEqual(unavailable);
  expect(api.events).toEqual([{ name: "submit-failed", code: "unavailable", phase: "acquire" }]);
  expect(api.reports[0]).toMatchObject({ phase: "acquire", cause: { code: "28P01" } });
  expect(JSON.stringify([response, api.events])).not.toMatch(/password|postgres/);
  expect(api.wakes).toEqual([]);
});

it("reports unavailable with the original cause when a unique violation has no visible winner", async () => {
  const store = await namespace("missing_winner");
  const trace: Query[] = [];
  const original = Object.assign(Error("injected unique violation without a visible winner"), {
    code: "23505",
  });
  const api = submitApi({
    connectAuthority: connectTracedAuthority(async (phase, sql) => {
      if (phase === "before" && sql.startsWith("INSERT INTO inbox")) throw original;
    }, trace),
  });
  try {
    expect(await api.run({ ns: store.ns, request: "one", command: inlineCommand(plain) })).toEqual(
      unavailable,
    );
    expect(api.reports[0]).toMatchObject({ phase: "receipt-write", cause: original });
    expect((api.reports[0] as { cause: unknown }).cause).toBe(original);
    expect(await store.inbox()).toEqual([]);
    expect(api.wakes).toEqual([]);
    expect(statements(trace, "INSERT INTO inbox")).toHaveLength(1);
  } finally {
    await store.finish();
  }
});
