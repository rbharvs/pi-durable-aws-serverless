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
  abort,
  namespace,
  once,
  provider,
  statements,
  traced,
  wrap,
  type Query,
  type QueryHook,
} from "./postgres.ts";
import { changing, inlineInput } from "../shared/input.ts";

const plain = { content: "hello", whenBusy: "followUp" as const };

const image = { type: "image", data: "AQID", mimeType: "image/png" };

const inline = (input: unknown) => ({ kind: "inline", input });

/** The actual composition: one fresh real pg connection per invocation, owned by the handler. */
const connect = async () => new SqlAuthority(await Database.connect(provider), provider);

const tracedConnect = (hook: QueryHook, trace: Query[]) => async () =>
  (await traced(hook, trace)).control;

function api(overrides: Partial<ApiDependencies> = {}) {
  const hints: string[] = [],
    events: Record<string, unknown>[] = [],
    reports: unknown[] = [];
  const run = apiHandler({
    connectAuthority: connect,
    dispatch: {
      wake: async (ns) => {
        hints.push(ns);
      },
    },
    event: (name, fields) => events.push({ name, ...fields }),
    report: (cause) => reports.push(cause),
    ...overrides,
  });

  return { run, hints, events, reports };
}

function accepted(response: SubmitResponse): number {
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
  const f = await namespace("accept"),
    a = api();
  const input = {
    content: [{ type: "text", text: "a", textSignature: "s" }, image],
    whenBusy: "followUp",
  };

  try {
    const first = await a.run({ ns: f.ns, request: "one", command: inline(input) });

    expect(first).toEqual({ ok: true, duplicate: false, receivedAt: expect.any(Number) });
    expect(Math.abs(accepted(first) - Date.now())).toBeLessThan(60000);
    expect(a.hints).toEqual([f.ns]);
    expect(a.events).toEqual([{ name: "durable-ack", ns: f.ns, request: "one", duplicate: false }]);

    const reader = await connect(),
      c = canonical(input);

    try {
      expect(await reader.get(f.ns, requestId("one"))).toEqual({
        kind: "received",
        ns: f.ns,
        request: "one",
        hash: c.sha256,
        payload: { kind: "inline", json: c.json },
        receivedAt: accepted(first),
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

    expect(await a.run({ ns: f.ns, request: "one", command: inline(reordered) })).toEqual({
      ...first,
      duplicate: true,
    });

    const before = await f.inbox();

    for (const changed of [
      { ...input, whenBusy: "steer" },
      { ...input, content: [input.content[0], { ...image, data: "AQIE" }] },
      { ...input, content: [...input.content].reverse() },
      { ...input, content: [{ type: "text", text: "a", textSignature: "other" }, image] },
      { ...input, content: [{ type: "text", text: "a" }, image] },
    ])
      expect(await a.run({ ns: f.ns, request: "one", command: inline(changed) })).toEqual({
        ok: false,
        code: "request-conflict",
        retryable: false,
      });

    expect(await f.inbox()).toEqual(before);
    expect(before).toHaveLength(1);
    expect(a.hints).toEqual([f.ns, f.ns]);
  } finally {
    await f.finish();
  }
});

it("five concurrent equal requests produce exactly one first acceptance of arbitrary strings", async () => {
  const f = await namespace("api_race"),
    a = api();
  const input = {
    content: [
      { type: "text", text: "\ud800 \udc00x\u0000 🚀 __proto__", textSignature: "\\u0000" },
    ],
    whenBusy: "steer",
  };

  try {
    const replies = await Promise.all(
      Array.from({ length: 5 }, () => a.run({ ns: f.ns, request: "race", command: inline(input) })),
    );

    expect(replies.filter((r) => r.ok && !r.duplicate)).toHaveLength(1);
    expect(new Set(replies.map(accepted)).size).toBe(1);
    expect(a.hints).toHaveLength(5);

    const rows = await f.inbox();

    expect(rows).toHaveLength(1);
    expect(rows[0].json).toBe(canonical(input).json);
    expect(JSON.parse(String(rows[0].json))).toEqual(input);
  } finally {
    await f.finish();
  }
});

it("rejects invalid input without acquiring a connection or creating state", async () => {
  const f = await namespace("api_invalid");
  let connects = 0;
  const a = api({
    connectAuthority: async () => {
      connects++;

      return connect();
    },
  });

  const sparse: unknown[] = [],
    sparseWithText: unknown[] = [];

  sparse.length = 1;
  sparseWithText[1] = { type: "text", text: "after hole" };

  try {
    for (const command of [
      { kind: "s3", key: `in/${f.ns}/a`, sha256: "0".repeat(64), bytes: 2 },
      inline({ ...plain, content: "x".repeat(MAX_INLINE_INPUT_BYTES) }),
      inline({ ...plain, content: [{ ...image, data: "AR==" }] }),
      inline({ ...plain, whenBusy: "reject" }),
      inline({ ...plain, content: sparse }),
      inline({ ...plain, content: sparseWithText }),
      { ...inline(plain), extra: true },
    ])
      expect(await a.run({ ns: f.ns, request: "bad", command })).toEqual({
        ok: false,
        code: "invalid-input",
        retryable: false,
      });

    expect(await a.run({ ns: "bad/path", request: "bad", command: inline(plain) })).toMatchObject({
      code: "invalid-input",
    });
    expect(await a.run({ ns: f.ns, request: "bad request", command: inline(plain) })).toMatchObject(
      { code: "invalid-input" },
    );
    expect(connects).toBe(0);
    expect(a.hints).toEqual([]);
    expect(await f.inbox()).toEqual([]);
  } finally {
    await f.finish();
  }
});

it("detaches the request before its first await", async () => {
  const f = await namespace("api_detach");
  const input = { ns: f.ns, request: "detach", command: inline({ ...plain, content: "original" }) };
  const a = api({
    connectAuthority: async () => {
      input.command = inline({ ...plain, content: "mutated" });
      input.request = "other";

      return connect();
    },
  });

  try {
    expect(await a.run(input)).toMatchObject({ ok: true, duplicate: false });

    const rows = await f.inbox();

    expect(rows.map((r) => [r.request_id, r.json])).toEqual([
      ["detach", canonical({ ...plain, content: "original" }).json],
    ]);
  } finally {
    await f.finish();
  }
});

it("stores the validated first read of an accessor input, not a caller-owned value mutated later", async () => {
  const f = await namespace("api_accessor"),
    later: unknown[] = [];
  const input = changing({ whenBusy: "followUp" }, "content", "x", later);
  const a = api({
    connectAuthority: async () => {
      later.push({ type: "text", text: "mutated after validation" });

      return connect();
    },
  });

  try {
    expect(await a.run({ ns: f.ns, request: "accessor", command: inline(input) })).toMatchObject({
      ok: true,
      duplicate: false,
    });
    expect((await f.inbox()).map((r) => r.json)).toEqual(['{"content":"x","whenBusy":"followUp"}']);
  } finally {
    await f.finish();
  }
});

it("a confirmed INSERT abort is re-read and then acknowledged once", async () => {
  const f = await namespace("api_abort"),
    trace: Query[] = [];
  const a = api({
    connectAuthority: tracedConnect(once("before", "INSERT INTO inbox", abort), trace),
  });

  try {
    expect(await a.run({ ns: f.ns, request: "one", command: inline(plain) })).toMatchObject({
      ok: true,
      duplicate: false,
    });
    expect(statements(trace, "INSERT INTO inbox")).toHaveLength(2);
    expect(await f.inbox()).toHaveLength(1);
  } finally {
    await f.finish();
  }
});

for (const applied of [true, false])
  it(`a lost receipt acknowledgement (applied=${applied}) is reported as unknown, never as rejection`, async () => {
    const f = await namespace("api_lost_ack"),
      trace: Query[] = [];
    const lossy = api({
      connectAuthority: tracedConnect(
        once(applied ? "after" : "before", "INSERT INTO inbox", () =>
          Error("lost receipt acknowledgement"),
        ),
        trace,
      ),
    });
    const request = { ns: f.ns, request: "unknown", command: inline(plain) };

    try {
      // Applied and unapplied outcomes are indistinguishable to the caller, so the response is identical.
      expect(await lossy.run(request)).toEqual(unavailable);
      expect(statements(trace, "INSERT INTO inbox")).toHaveLength(1);
      expect(lossy.hints).toEqual([]);
      expect(lossy.events).toEqual([
        { name: "submit-failed", code: "unavailable", phase: "receipt-write" },
      ]);
      expect(lossy.reports[0]).toMatchObject({
        phase: "receipt-write",
        cause: { message: "lost receipt acknowledgement" },
      });
      expect(await f.inbox()).toHaveLength(applied ? 1 : 0);

      const retry = await api().run(request);

      expect(retry).toMatchObject({ ok: true, duplicate: applied });
      if (applied) expect(accepted(retry)).toBe(Number((await f.inbox())[0].received_at));
    } finally {
      await f.finish();
    }
  });

it("a receipt-read fault is unavailable and never writes", async () => {
  const f = await namespace("api_read_fault"),
    trace: Query[] = [];
  const a = api({
    connectAuthority: tracedConnect(
      once("before", "SELECT * FROM inbox", () => Error("SELECT private read failed")),
      trace,
    ),
  });

  try {
    const response = await a.run({ ns: f.ns, request: "one", command: inline(plain) });

    expect(response).toEqual(unavailable);
    expect(a.reports[0]).toMatchObject({
      phase: "receipt-write",
      cause: { message: "SELECT private read failed" },
    });
    expect(a.events).toEqual([
      { name: "submit-failed", code: "unavailable", phase: "receipt-write" },
    ]);
    expect(statements(trace, "INSERT INTO inbox")).toEqual([]);
    expect(await f.inbox()).toEqual([]);
    expect(a.hints).toEqual([]);
  } finally {
    await f.finish();
  }
});

for (const winner of ["identical", "different"] as const)
  it(`an INSERT race with a ${winner} winner returns the winner's outcome`, async () => {
    const f = await namespace("api_insert_race"),
      trace: Query[] = [],
      other = { ...plain, content: "race winner" };
    let raced = false,
      winnerTime: number | undefined;
    const a = api({
      connectAuthority: tracedConnect(async (phase, sql) => {
        if (raced || phase !== "before" || !sql.startsWith("INSERT INTO inbox")) return;
        raced = true;
        winnerTime = (
          await f.control.receive(
            f.ns,
            requestId("race"),
            inlineInput(winner === "identical" ? plain : other),
          )
        ).item.receivedAt;
      }, trace),
    });

    try {
      const response = await a.run({ ns: f.ns, request: "race", command: inline(plain) });

      if (winner === "identical")
        expect(response).toEqual({ ok: true, duplicate: true, receivedAt: winnerTime });
      else expect(response).toEqual({ ok: false, code: "request-conflict", retryable: false });
      expect(statements(trace, "INSERT INTO inbox")).toHaveLength(1);
      expect(raced).toBe(true);
      expect(a.hints).toEqual(winner === "identical" ? [f.ns] : []);
      expect((await f.control.get(f.ns, requestId("race")))?.payload).toEqual(
        inlineInput(winner === "identical" ? plain : other).payload,
      );
    } finally {
      await f.finish();
    }
  });

it("a failed wake does not undo the durable acknowledgement", async () => {
  const f = await namespace("api_failed_wake");
  let observed: unknown;
  const a = api({
    dispatch: {
      wake: async (ns) => {
        observed = await f.control.get(ns, requestId("one"));

        throw Error("missing wake");
      },
    },
  });

  try {
    const response = await a.run({ ns: f.ns, request: "one", command: inline(plain) });

    expect(response).toMatchObject({ ok: true, duplicate: false });
    expect(observed).toMatchObject({ kind: "received", receivedAt: accepted(response) });
    expect(a.events).toEqual([
      { name: "durable-ack", ns: f.ns, request: "one", duplicate: false },
      { name: "hint-failed", ns: f.ns },
    ]);
    expect(a.reports).toEqual([]);
  } finally {
    await f.finish();
  }
});

it("a known receipt survives wake, observer, report and close failures with the close attempted once", async () => {
  const f = await namespace("api_diagnostics"),
    closes = { count: 0 },
    clients: pg.Client[] = [];
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
    expect(await run({ ns: f.ns, request: "one", command: inline(plain) })).toMatchObject({
      ok: true,
      duplicate: false,
    });
    expect(closes.count).toBe(1);
    await expect(clients[0].query("SELECT 1")).rejects.toThrow();
    expect(await f.inbox()).toHaveLength(1);
  } finally {
    await f.finish();
  }
});

it("an unknown outcome stays unavailable when cleanup fails, and cleanup is reported without causes", async () => {
  const f = await namespace("api_cleanup"),
    closes = { count: 0 };
  const a = api({
    connectAuthority: async () => {
      const client = wrap(
        await provider.connect(),
        once("after", "INSERT INTO inbox", () => Error("SELECT private lost acknowledgement")),
      );

      return new SqlAuthority(new Database(failingClose(client, closes)), provider);
    },
  });

  try {
    expect(await a.run({ ns: f.ns, request: "one", command: inline(plain) })).toEqual(unavailable);
    expect(closes.count).toBe(1);
    expect(a.events).toEqual([
      { name: "submit-failed", code: "unavailable", phase: "receipt-write" },
      { name: "cleanup-failed", resource: "authority" },
    ]);
    expect(JSON.stringify(a.events)).not.toMatch(/SELECT|private|close acknowledgement/);
  } finally {
    await f.finish();
  }
});

it("a real connection acquisition failure is unavailable and keeps provider details private", async () => {
  const { port } = inject("postgres"),
    wrongPassword = postgresProvider(port, "synthetic-wrong-password", "integration");
  const a = api({
    connectAuthority: async () =>
      new SqlAuthority(await Database.connect(wrongPassword), wrongPassword),
  });
  const response = await a.run({ ns: "api_acquire", request: "one", command: inline(plain) });

  expect(response).toEqual(unavailable);
  expect(a.events).toEqual([{ name: "submit-failed", code: "unavailable", phase: "acquire" }]);
  expect(a.reports[0]).toMatchObject({ phase: "acquire", cause: { code: "28P01" } });
  expect(JSON.stringify([response, a.events])).not.toMatch(/password|postgres/);
  expect(a.hints).toEqual([]);
});

it("reports unavailable with the original cause when a unique violation has no visible winner", async () => {
  const f = await namespace("missing_winner"),
    trace: Query[] = [];
  const original = Object.assign(Error("injected unique violation without a visible winner"), {
    code: "23505",
  });
  const a = api({
    connectAuthority: tracedConnect(async (phase, sql) => {
      if (phase === "before" && sql.startsWith("INSERT INTO inbox")) throw original;
    }, trace),
  });
  try {
    expect(await a.run({ ns: f.ns, request: "one", command: inline(plain) })).toEqual(unavailable);
    expect(a.reports[0]).toMatchObject({ phase: "receipt-write", cause: original });
    expect((a.reports[0] as { cause: unknown }).cause).toBe(original);
    expect(await f.inbox()).toEqual([]);
    expect(a.hints).toEqual([]);
    expect(statements(trace, "INSERT INTO inbox")).toHaveLength(1);
  } finally {
    await f.finish();
  }
});
