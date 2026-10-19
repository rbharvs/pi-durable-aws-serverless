import { expect, it } from "vitest";
import { Database } from "../../src/sql/db.ts";
import {
  computeSha256,
  harnessId,
  requestId,
  type HarnessId,
  type Sha256,
} from "../../src/domain.ts";
import { canonical, MAX_INLINE_INPUT_BYTES } from "../../src/input.ts";
import type { AcceptedInput } from "../../src/authority.ts";
import { InspectableAuthority } from "./authority.ts";
import { confirmedAbort, faultOnce, namespace, provider, statements, traced } from "./postgres.ts";
import { acceptedInput } from "../shared/input.ts";

const hello = acceptedInput({ content: "hello", whenBusy: "followUp" });

it("concurrent inserts acknowledge one immutable command; mismatch never replaces it", async () => {
  const store = await namespace("receive_race");
  const ns = harnessId(store.ns + "_fresh");
  const authorities = await Promise.all(
    Array.from(
      { length: 5 },
      async () => new InspectableAuthority(await Database.connect(provider), provider),
    ),
  );

  try {
    const replies = await Promise.all(
      authorities.map((authority) =>
        authority.receive(
          ns,
          requestId("shared"),
          acceptedInput({
            content: "\ud800 \udc00x\u0000 🚀 __proto__",
            whenBusy: "followUp",
          }),
        ),
      ),
    );

    expect(replies.filter((r) => !r.duplicate)).toHaveLength(1);
    expect(new Set(replies.map((r) => JSON.stringify(r.item))).size).toBe(1);

    const before = await store.authority.inspectionDb.query("SELECT * FROM inbox WHERE ns=$1", [
      ns,
    ]);

    await expect(
      store.authority.receive(
        ns,
        requestId("shared"),
        acceptedInput({ content: "different", whenBusy: "followUp" }),
      ),
    ).rejects.toThrow("mismatch");
    expect(
      await store.authority.inspectionDb.query("SELECT * FROM inbox WHERE ns=$1", [ns]),
    ).toEqual(before);
    expect(before).toHaveLength(1);
  } finally {
    await Promise.all(authorities.map((authority) => authority.close()));
    await store.finish();
  }
});

const helloJson = '{"content":"hello","whenBusy":"followUp"}';

const oversized = canonical({ content: "x".repeat(MAX_INLINE_INPUT_BYTES), whenBusy: "followUp" });

/** Inputs a raw caller could pass by bypassing types; each must fail before any SQL. */
const invalidReceives: { name: string; ns?: HarnessId; accepted: AcceptedInput }[] = [
  { name: "canonical-hash", accepted: { ...hello, hash: computeSha256("different input") } },
  {
    name: "noncanonical-json",
    accepted: { ...hello, payload: { kind: "inline", json: ` ${helloJson} ` } },
  },
  {
    name: "inline-limit",
    accepted: { hash: oversized.sha256, payload: { kind: "inline", json: oversized.json } },
  },
  {
    name: "invalid-kind",
    accepted: {
      ...hello,
      payload: { kind: "unexpected", json: helloJson },
    } as unknown as AcceptedInput,
  },
  {
    name: "foreign-object-namespace",
    accepted: {
      ...hello,
      payload: {
        kind: "s3",
        key: "p/test/00000000-0000-4000-8000-000000000000",
        sha256: hello.hash,
        bytes: MAX_INLINE_INPUT_BYTES + 1,
      },
    } as unknown as AcceptedInput,
  },
  { name: "invalid-namespace", ns: "bad/namespace" as HarnessId, accepted: hello },
];

for (const { name, ns, accepted } of invalidReceives)
  it(`raw receive rejects ${name} before any SQL or receipt exists`, async () => {
    const store = await namespace("raw_invalid");
    const { authority, trace } = await traced();

    try {
      await expect(authority.receive(ns ?? store.ns, requestId("bad"), accepted)).rejects.toThrow();
      expect(trace).toEqual([]);
      expect(await store.inbox()).toEqual([]);
    } finally {
      await authority.close();
      await store.finish();
    }
  });

it("receive detaches caller-owned input before its first await", async () => {
  const store = await namespace("detach");
  const original = acceptedInput({ content: "original", whenBusy: "followUp" });
  const replacement = acceptedInput({ content: "replacement", whenBusy: "steer" });
  const caller: { hash: Sha256; payload: { kind: "inline"; json: string } } = {
    hash: original.hash,
    payload: { ...original.payload },
  };
  let mutations = 0;
  const { authority } = await traced(async (phase) => {
    if (phase !== "before") return;
    mutations++;
    caller.hash = replacement.hash;
    caller.payload.json = replacement.payload.json;
  });

  try {
    const reply = await authority.receive(store.ns, requestId("detach"), caller);

    expect(mutations).toBeGreaterThan(0);
    expect(reply.item).toMatchObject({ hash: original.hash, payload: original.payload });
    expect(await store.authority.get(store.ns, requestId("detach"))).toEqual(reply.item);
  } finally {
    await authority.close();
    await store.finish();
  }
});

it("a confirmed INSERT abort re-reads authority before repeating the insert", async () => {
  const store = await namespace("abort");
  const { authority, trace } = await traced(
    faultOnce("before", "INSERT INTO inbox", confirmedAbort),
  );

  try {
    const reply = await authority.receive(store.ns, requestId("one"), hello);
    const sql = trace.map((q) => q.sql);
    const inserts = sql.flatMap((s, i) => (s.startsWith("INSERT INTO inbox") ? [i] : []));

    expect(reply.duplicate).toBe(false);
    expect(inserts).toHaveLength(2);
    expect(
      sql.slice(inserts[0] + 1, inserts[1]).some((s) => s.startsWith("SELECT * FROM inbox")),
    ).toBe(true);
    expect(await store.inbox()).toHaveLength(1);
  } finally {
    await authority.close();
    await store.finish();
  }
});

for (const winner of ["identical", "different"] as const)
  it(`the re-read after a confirmed abort observes a concurrent ${winner} winner`, async () => {
    const store = await namespace("abort_winner");
    const other = acceptedInput({ content: "concurrent winner", whenBusy: "followUp" });
    const { authority, trace } = await traced(
      faultOnce("before", "INSERT INTO inbox", async () => {
        await store.authority.receive(
          store.ns,
          requestId("one"),
          winner === "identical" ? hello : other,
        );

        return confirmedAbort();
      }),
    );

    try {
      const reply = authority.receive(store.ns, requestId("one"), hello);

      if (winner === "identical") expect((await reply).duplicate).toBe(true);
      else await expect(reply).rejects.toThrow("mismatch");
      expect(statements(trace, "INSERT INTO inbox")).toHaveLength(1);
      expect((await store.authority.get(store.ns, requestId("one")))?.hash).toBe(
        winner === "identical" ? hello.hash : other.hash,
      );
    } finally {
      await authority.close();
      await store.finish();
    }
  });

it("persistent confirmed aborts stop after three re-read attempts without a receipt", async () => {
  const store = await namespace("abort_bound");
  const causes: unknown[] = [];
  const { authority, trace } = await traced(async (phase, sql) => {
    if (phase !== "before" || !sql.startsWith("INSERT INTO inbox")) return;
    causes.push(confirmedAbort());

    throw causes.at(-1);
  });

  try {
    const failed = await authority
      .receive(store.ns, requestId("one"), hello)
      .catch((error) => error);

    expect(failed).toBe(causes[2]);
    expect(causes).toHaveLength(3);
    expect(statements(trace, "INSERT INTO inbox")).toHaveLength(3);
    expect(await store.inbox()).toEqual([]);
  } finally {
    await authority.close();
    await store.finish();
  }
});

for (const applied of [true, false])
  it(`an unknown INSERT outcome (applied=${applied}) is never retried and never asserts non-acceptance`, async () => {
    const store = await namespace("unknown_insert");
    const { authority, trace } = await traced(
      faultOnce(applied ? "after" : "before", "INSERT INTO inbox", () =>
        Error("unknown insert acknowledgement"),
      ),
    );

    try {
      await expect(authority.receive(store.ns, requestId("unknown"), hello)).rejects.toThrow(
        "unknown insert acknowledgement",
      );
      expect(statements(trace, "INSERT INTO inbox")).toHaveLength(1);
      expect((await store.authority.get(store.ns, requestId("unknown")))?.payload).toEqual(
        applied ? hello.payload : undefined,
      );
      expect((await store.authority.receive(store.ns, requestId("unknown"), hello)).duplicate).toBe(
        applied,
      );
    } finally {
      await authority.close();
      await store.finish();
    }
  });

const corruptions = [
  ["noncanonical json", "json=' '||json"],
  ["hash mismatch", `payload_sha256='${computeSha256("other")}'`],
] as const;

for (const [name, assignment] of corruptions)
  it(`rejects a stored receipt with ${name} on read and never treats it as a duplicate`, async () => {
    const store = await namespace("corrupt");

    try {
      await store.authority.receive(store.ns, requestId("one"), hello);
      await store.authority.inspectionDb.query(`UPDATE inbox SET ${assignment} WHERE ns=$1`, [
        store.ns,
      ]);

      const stored = await store.inbox();

      await expect(store.authority.get(store.ns, requestId("one"))).rejects.toThrow(
        "Inbox payload integrity",
      );
      await expect(store.authority.receive(store.ns, requestId("one"), hello)).rejects.toThrow(
        "Inbox payload integrity",
      );
      expect(await store.inbox()).toEqual(stored);
    } finally {
      await store.finish();
    }
  });

it("close ends only this handle; the immutable receipt survives for a fresh connection", async () => {
  const store = await namespace("close_reopen");
  const first = new InspectableAuthority(await Database.connect(provider), provider);
  const stored = await first.receive(store.ns, requestId("one"), hello);

  await first.close();
  await expect(first.get(store.ns, requestId("one"))).rejects.toThrow();

  const second = new InspectableAuthority(await Database.connect(provider), provider);

  try {
    expect(await second.get(store.ns, requestId("one"))).toEqual(stored.item);
    expect(await second.receive(store.ns, requestId("one"), hello)).toEqual({
      duplicate: true,
      item: stored.item,
    });
  } finally {
    await second.close();
    await store.finish();
  }
});
