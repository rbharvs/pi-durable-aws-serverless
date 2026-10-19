import { expect, it } from "vitest";
import { Database } from "../../src/sql/db.ts";
import { harnessId, requestId, sha, type HarnessId, type Sha256 } from "../../src/domain.ts";
import { canonical, MAX_INLINE_INPUT_BYTES } from "../../src/input.ts";
import type { AcceptedInput } from "../../src/submit.ts";
import { Control } from "./authority.ts";
import { abort, namespace, once, provider, statements, traced } from "./postgres.ts";
import { inlineInput } from "../shared/input.ts";

const hello = inlineInput({ content: "hello", whenBusy: "followUp" });

it("concurrent inserts acknowledge one immutable command; mismatch never replaces it", async () => {
  const f = await namespace("receive_race"),
    ns = harnessId(f.ns + "_fresh");
  const controls = await Promise.all(
    Array.from({ length: 5 }, async () => new Control(await Database.connect(provider), provider)),
  );

  try {
    const replies = await Promise.all(
      controls.map((c) =>
        c.receive(
          ns,
          requestId("shared"),
          inlineInput({
            content: "\ud800 \udc00x\u0000 🚀 __proto__",
            whenBusy: "followUp",
          }),
        ),
      ),
    );

    expect(replies.filter((r) => !r.duplicate)).toHaveLength(1);
    expect(new Set(replies.map((r) => JSON.stringify(r.item))).size).toBe(1);

    const before = await f.control.inspectionDb.query("SELECT * FROM inbox WHERE ns=$1", [ns]);

    await expect(
      f.control.receive(
        ns,
        requestId("shared"),
        inlineInput({ content: "different", whenBusy: "followUp" }),
      ),
    ).rejects.toThrow("mismatch");
    expect(await f.control.inspectionDb.query("SELECT * FROM inbox WHERE ns=$1", [ns])).toEqual(
      before,
    );
    expect(before).toHaveLength(1);
  } finally {
    await Promise.all(controls.map((c) => c.close()));
    await f.finish();
  }
});

for (const invalid of [
  "canonical-hash",
  "noncanonical-json",
  "inline-limit",
  "invalid-kind",
  "object-payload",
  "invalid-namespace",
] as const)
  it(`raw receive rejects ${invalid} before any SQL or receipt exists`, async () => {
    const f = await namespace("raw_invalid"),
      { control, trace } = await traced();
    const json = '{"content":"hello","whenBusy":"followUp"}',
      large = canonical({ content: "x".repeat(MAX_INLINE_INPUT_BYTES), whenBusy: "followUp" });
    let ns: HarnessId = f.ns,
      accepted: AcceptedInput = hello;

    if (invalid === "canonical-hash") accepted = { ...hello, hash: sha("different input") };
    if (invalid === "noncanonical-json")
      accepted = { ...hello, payload: { kind: "inline", json: ` ${json} ` } };
    if (invalid === "inline-limit")
      accepted = { hash: large.sha256, payload: { kind: "inline", json: large.json } };
    if (invalid === "invalid-kind")
      accepted = { ...hello, payload: { kind: "unexpected", json } } as unknown as AcceptedInput;
    if (invalid === "object-payload")
      accepted = {
        ...hello,
        payload: {
          kind: "s3",
          key: `p/${f.ns}/00000000-0000-4000-8000-000000000000`,
          sha256: hello.hash,
          bytes: MAX_INLINE_INPUT_BYTES + 1,
        },
      } as unknown as AcceptedInput;
    if (invalid === "invalid-namespace") ns = "bad/namespace" as HarnessId;

    try {
      await expect(control.receive(ns, requestId("bad"), accepted)).rejects.toThrow();
      expect(trace).toEqual([]);
      expect(await f.inbox()).toEqual([]);
    } finally {
      await control.close();
      await f.finish();
    }
  });

it("receive detaches caller-owned input before its first await", async () => {
  const f = await namespace("detach"),
    original = inlineInput({ content: "original", whenBusy: "followUp" }),
    replacement = inlineInput({ content: "replacement", whenBusy: "steer" });
  const caller: { hash: Sha256; payload: { kind: "inline"; json: string } } = {
    hash: original.hash,
    payload: { ...original.payload },
  };
  let mutations = 0;
  const { control } = await traced(async (phase) => {
    if (phase !== "before") return;
    mutations++;
    caller.hash = replacement.hash;
    caller.payload.json = replacement.payload.json;
  });

  try {
    const reply = await control.receive(f.ns, requestId("detach"), caller);

    expect(mutations).toBeGreaterThan(0);
    expect(reply.item).toMatchObject({ hash: original.hash, payload: original.payload });
    expect(await f.control.get(f.ns, requestId("detach"))).toEqual(reply.item);
  } finally {
    await control.close();
    await f.finish();
  }
});

it("a confirmed INSERT abort re-reads authority before repeating the insert", async () => {
  const f = await namespace("abort");
  const { control, trace } = await traced(once("before", "INSERT INTO inbox", abort));

  try {
    const reply = await control.receive(f.ns, requestId("one"), hello);
    const sql = trace.map((q) => q.sql);
    const inserts = sql.flatMap((s, i) => (s.startsWith("INSERT INTO inbox") ? [i] : []));

    expect(reply.duplicate).toBe(false);
    expect(inserts).toHaveLength(2);
    expect(
      sql.slice(inserts[0] + 1, inserts[1]).some((s) => s.startsWith("SELECT * FROM inbox")),
    ).toBe(true);
    expect(await f.inbox()).toHaveLength(1);
  } finally {
    await control.close();
    await f.finish();
  }
});

for (const winner of ["identical", "different"] as const)
  it(`the re-read after a confirmed abort observes a concurrent ${winner} winner`, async () => {
    const f = await namespace("abort_winner"),
      other = inlineInput({ content: "concurrent winner", whenBusy: "followUp" });
    const { control, trace } = await traced(
      once("before", "INSERT INTO inbox", async () => {
        await f.control.receive(f.ns, requestId("one"), winner === "identical" ? hello : other);

        return abort();
      }),
    );

    try {
      const reply = control.receive(f.ns, requestId("one"), hello);

      if (winner === "identical") expect((await reply).duplicate).toBe(true);
      else await expect(reply).rejects.toThrow("mismatch");
      expect(statements(trace, "INSERT INTO inbox")).toHaveLength(1);
      expect((await f.control.get(f.ns, requestId("one")))?.hash).toBe(
        winner === "identical" ? hello.hash : other.hash,
      );
    } finally {
      await control.close();
      await f.finish();
    }
  });

it("persistent confirmed aborts stop after three re-read attempts without a receipt", async () => {
  const f = await namespace("abort_bound");
  const causes: unknown[] = [];
  const { control, trace } = await traced(async (phase, sql) => {
    if (phase !== "before" || !sql.startsWith("INSERT INTO inbox")) return;
    causes.push(abort());

    throw causes.at(-1);
  });

  try {
    const failed = await control.receive(f.ns, requestId("one"), hello).catch((error) => error);

    expect(failed).toBe(causes[2]);
    expect(causes).toHaveLength(3);
    expect(statements(trace, "INSERT INTO inbox")).toHaveLength(3);
    expect(await f.inbox()).toEqual([]);
  } finally {
    await control.close();
    await f.finish();
  }
});

for (const applied of [true, false])
  it(`an unknown INSERT outcome (applied=${applied}) is never retried and never asserts non-acceptance`, async () => {
    const f = await namespace("unknown_insert");
    const { control, trace } = await traced(
      once(applied ? "after" : "before", "INSERT INTO inbox", () =>
        Error("unknown insert acknowledgement"),
      ),
    );

    try {
      await expect(control.receive(f.ns, requestId("unknown"), hello)).rejects.toThrow(
        "unknown insert acknowledgement",
      );
      expect(statements(trace, "INSERT INTO inbox")).toHaveLength(1);
      expect((await f.control.get(f.ns, requestId("unknown")))?.payload).toEqual(
        applied ? hello.payload : undefined,
      );
      expect((await f.control.receive(f.ns, requestId("unknown"), hello)).duplicate).toBe(applied);
    } finally {
      await control.close();
      await f.finish();
    }
  });

const corruptions = [
  ["noncanonical json", "json=' '||json"],
  ["hash mismatch", `payload_sha256='${sha("other")}'`],
] as const;

for (const [name, assignment] of corruptions)
  it(`rejects a stored receipt with ${name} on read and never treats it as a duplicate`, async () => {
    const f = await namespace("corrupt");

    try {
      await f.control.receive(f.ns, requestId("one"), hello);
      await f.control.inspectionDb.query(`UPDATE inbox SET ${assignment} WHERE ns=$1`, [f.ns]);

      const stored = await f.inbox();

      await expect(f.control.get(f.ns, requestId("one"))).rejects.toThrow(/^(Inbox|Unsupported)/);
      await expect(f.control.receive(f.ns, requestId("one"), hello)).rejects.toThrow(
        /^(Inbox|Unsupported)/,
      );
      expect(await f.inbox()).toEqual(stored);
    } finally {
      await f.finish();
    }
  });

it("close ends only this handle; the immutable receipt survives for a fresh connection", async () => {
  const f = await namespace("close_reopen"),
    first = new Control(await Database.connect(provider), provider);
  const accepted = await first.receive(f.ns, requestId("one"), hello);

  await first.close();
  await expect(first.get(f.ns, requestId("one"))).rejects.toThrow();

  const second = new Control(await Database.connect(provider), provider);

  try {
    expect(await second.get(f.ns, requestId("one"))).toEqual(accepted.item);
    expect(await second.receive(f.ns, requestId("one"), hello)).toEqual({
      duplicate: true,
      item: accepted.item,
    });
  } finally {
    await second.close();
    await f.finish();
  }
});
