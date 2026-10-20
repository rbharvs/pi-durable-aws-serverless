import { it, expect, vi } from "vitest";
import { createMemoryBackend, type MemoryBackendOptions, type Lease } from "./index.ts";
import { memoryFixture } from "../tests/shared/memory.ts";
import { input } from "../tests/shared/storage.contract.ts";
import { check, requestId, harnessId } from "./domain.ts";
import { idFromNumber } from "./ids.ts";
import { runtimeDependencies } from "./runtime.ts";
import { worker } from "./worker.ts";
import { objectFixture } from "../tests/shared/objects.ts";
import { loseAcknowledgement } from "../tests/shared/faults.ts";
import { createModels } from "@earendil-works/pi-ai";
import {
  MemoryStorage,
  StorageRequestError,
  createRegistry,
  type ConversationId,
  type EntryId,
  type Storage,
} from "@earendil-works/pi-durable";
import { MemoryStorage as SubpathMemory } from "@earendil-works/pi-durable/storage/memory";
import { BACKGROUND_CONTEXT as context } from "@earendil-works/chord/context";

it("pinned public Pi prepare/apply, commit and detached reads do not yield before publication/snapshot", async () => {
  expect(MemoryStorage).toBe(SubpathMemory);
  const engine = new SubpathMemory(),
    root = idFromNumber<ConversationId>(1);
  try {
    const prepared = engine.prepareCommit([{ type: "conversation", value: { id: root } }]);
    const before = engine.scanConversations({}, 100, undefined, context);
    expect(prepared.apply()).toBe(1);
    expect(prepared.apply()).toBe(1);
    const after = engine.scanConversations({}, 100, undefined, context);
    // The public Promise-returning allocator must consume ID 2 before this later commit
    // raises its high-water mark. No private counter or prepared-state inspection.
    const minted = engine.mintId();
    const pending = engine.commit(
      [
        {
          type: "entry",
          value: { id: idFromNumber<EntryId>(20), conversationId: root, kind: "sync" },
        },
      ],
      context,
    );
    const entry = engine.entry(idFromNumber<EntryId>(20), context);
    expect((await before).items).toEqual([]);
    expect((await after).items).toEqual([{ id: 1 }]);
    expect(await minted).toBe(2);
    expect(await pending).toBe(2);
    expect(await engine.mintId()).toBe(21);
    expect(await entry).toMatchObject({ entry: { kind: "sync" }, commitSeq: 2 });
    await expect(
      engine.scanTasks({ order: "invalid" as never }, 1, undefined, context),
    ).rejects.toBeInstanceOf(StorageRequestError);
  } finally {
    await engine.close(context);
  }
});

it("strict injected expiry permits takeover, never revokes the current owner's writes", async () => {
  const f = await memoryFixture(true),
    other = createMemoryBackend({ now: f.time });
  const foreign = await other.connectAuthority();
  const s = await f.backend.openStorage(f.lease);
  try {
    await f.authority.receive(f.ns, requestId("pending"), input);
    f.advance(f.lease.until - f.time());
    expect(await f.authority.claim(f.ns, 45000)).toEqual({ kind: "held" });
    expect(await f.authority.eligible()).toEqual([]);
    f.advance(1);
    expect(await f.authority.eligible()).toEqual([f.ns]);
    expect(await s.mintId()).toBe(f.firstId);
    await s.commit([], context);
    await foreign.receive(f.ns, requestId("one"), input);
    const claim = await foreign.claim(f.ns, 45000);
    check(claim.kind === "claimed", "foreign claim");
    await expect(f.backend.openStorage(claim.lease)).rejects.toThrow("Stale");
    await expect(other.openStorage(f.lease)).rejects.toThrow("Stale");
    expect(await f.authority.release(f.lease)).toBe("released");
    expect(await f.authority.claim(f.ns, 45000)).toEqual({ kind: "held" });
    f.advance(1);
    const next = await f.authority.claim(f.ns, 45000);
    check(next.kind === "claimed", "next claim");
    expect(next.lease.epoch).toBe(f.lease.epoch + 1);
    await expect(s.commit([], context)).rejects.toThrow("Stale");
    await expect(s.mintId()).rejects.toThrow("failed");
    expect(await f.authority.release(f.lease)).toBe("lost");
  } finally {
    await s.close(context);
    await foreign.close();
    await f.finish();
  }
});

it("no asynchronous gap between fence and Pi apply: the next turn observes work before takeover", async () => {
  const f = await memoryFixture(true),
    s = await f.backend.openStorage(f.lease);
  try {
    await f.expire();
    const pending = s.commit(
      [
        {
          type: "submission",
          value: {
            id: idFromNumber(30),
            conversationId: idFromNumber(1),
            type: "input",
            status: "queued",
          },
        },
      ],
      context,
    );
    const nextTurn = Promise.resolve().then(() => {
      const work = f.authority.hasWork(f.ns);
      const claim = f.authority.claim(f.ns, 45000);
      return Promise.all([work, claim]);
    });
    await pending;
    const [work, claim] = await nextTurn;
    expect(work).toBe(true);
    check(claim.kind === "claimed", "takeover");
    const current = await f.backend.openStorage(claim.lease);
    try {
      expect(await current.submission(idFromNumber(30), context)).toMatchObject({
        status: "queued",
      });
      await expect(s.commit([], context)).rejects.toThrow("Stale");
    } finally {
      await current.close(context);
    }
  } finally {
    await s.close(context);
    await f.finish();
  }
});

it("clock options retain their method receiver and backing state cannot escape through handle reflection", async () => {
  class Clock implements MemoryBackendOptions {
    time = 10000;
    now() {
      return this.time;
    }
  }
  const clock = new Clock(),
    backend = createMemoryBackend(clock),
    a = await backend.connectAuthority();
  const ns = harnessId("clock");
  try {
    expect((await a.receive(ns, requestId("one"), input)).item.receivedAt).toBe(clock.time);
    const claim = await a.claim(ns, 45000);
    check(claim.kind === "claimed", "claim");
    expect(Object.isFrozen(claim.lease)).toBe(true);
    const s = await backend.openStorage(claim.lease);
    try {
      expect(Reflect.ownKeys(s)).toEqual([]);
      expect(() => Reflect.get(s, "prepareCommit")).toThrow("Unknown");
      expect(() => Reflect.get(s, "state")).toThrow("Unknown");
    } finally {
      await s.close(context);
    }
  } finally {
    await a.close();
  }
});

it("close drains a held public Pi read but rejects queued read/commit/mint without late effects", async () => {
  const f = await memoryFixture(true),
    s = await f.backend.openStorage(f.lease);
  const reached = Promise.withResolvers<void>(),
    proceed = Promise.withResolvers<void>();
  const scan = SubpathMemory.prototype.scanConversations;
  // Gate the public method's returned snapshot, not the adapter's gate or private backing fields.
  const spy = vi.spyOn(SubpathMemory.prototype, "scanConversations").mockImplementation(function (
    this: SubpathMemory,
    ...args
  ) {
    const snapshot = scan.apply(this, args);
    reached.resolve();
    return proceed.promise.then(() => snapshot);
  });
  let active: ReturnType<Storage["scanConversations"]> | undefined;
  let closing: Promise<void> | undefined;
  try {
    const seq = await s.commit([], context);
    active = s.scanConversations({}, 10, undefined, context);
    await reached.promise;
    const queued = Promise.allSettled([
      s.conversation(idFromNumber<ConversationId>(1), context),
      s.commit(
        [{ type: "conversation", value: { id: idFromNumber<ConversationId>(50) } }],
        context,
      ),
      s.mintId(),
    ]);
    let closed = false;
    closing = s.close(context).then(() => {
      closed = true;
    });
    await Promise.resolve();
    expect(closed).toBe(false);
    proceed.resolve();
    expect((await active).items).toEqual([]);
    const results = await queued;
    expect(results.map((r) => r.status)).toEqual(["rejected", "rejected", "rejected"]);
    for (const result of results) {
      check(result.status === "rejected", "queued operation rejected");
      expect(result.reason).toMatchObject({ message: expect.stringContaining("closed") });
    }
    await closing;
    const cold = await f.backend.openStorage(f.lease);
    try {
      expect(await cold.conversation(idFromNumber<ConversationId>(50), context)).toBeUndefined();
      expect(await cold.mintId()).toBe(f.firstId);
      expect(await cold.commit([], context)).toBe(seq + 1);
    } finally {
      await cold.close(context);
    }
  } finally {
    proceed.resolve();
    await active?.catch(() => {});
    await closing;
    spy.mockRestore();
    await s.close(context);
    await f.finish();
  }
});

it("invalid clock values reject receive/claim/eligible without changing namespace or lease", async () => {
  for (const bad of [NaN, -1, 1.5, Number.MAX_SAFE_INTEGER + 1, "5"]) {
    let time = 1000;
    const backend = createMemoryBackend({ now: () => time }),
      a = await backend.connectAuthority();
    const ns = harnessId("valid-clock"),
      fresh = harnessId("invalid-clock");
    try {
      const receipt = (await a.receive(ns, requestId("seed"), input)).item;
      const claim = await a.claim(ns, 45000);
      check(claim.kind === "claimed", "valid claim");
      time = bad as number;
      const results = await Promise.allSettled([
        a.receive(fresh, requestId("fresh"), input),
        a.receive(ns, requestId("invalid"), input),
        a.claim(ns, 45000),
        a.eligible(),
      ]);
      time = claim.lease.until + 1;
      expect(
        results.map((r) => r.status),
        `clock ${String(bad)}`,
      ).toEqual(Array(4).fill("rejected"));
      for (const result of results) {
        check(result.status === "rejected", "clock rejected");
        expect(result.reason).toMatchObject({ message: "Invalid authority clock" });
      }
      expect(await a.get(fresh, requestId("fresh"))).toBeUndefined();
      await expect(a.hasWork(fresh)).rejects.toThrow("Unknown namespace");
      await expect(a.claim(fresh, 45000)).rejects.toThrow("Unknown namespace");
      expect(await a.get(ns, requestId("invalid"))).toBeUndefined();
      expect(await a.get(ns, requestId("seed"))).toEqual(receipt);
      expect(await a.eligible()).toEqual([ns]);
      const storage = await backend.openStorage(claim.lease);
      try {
        expect(await storage.mintId()).toBe(2);
      } finally {
        await storage.close(context);
      }
      const next = await a.claim(ns, 45000);
      check(next.kind === "claimed", "valid takeover");
      expect(next.lease.epoch).toBe(claim.lease.epoch + 1);
      expect(next.lease.owner).not.toBe(claim.lease.owner);
    } finally {
      await a.close();
    }
  }
});

it("copied issued lease with identical owner and epoch cannot open Storage", async () => {
  const f = await memoryFixture(true);
  const copy = { ...f.lease } as Lease;
  let unexpected: Storage | undefined;
  try {
    expect(copy).toEqual(f.lease);
    expect(copy).not.toBe(f.lease);
    const result = await f.backend.openStorage(copy).then(
      (storage) => {
        unexpected = storage;
        return "opened";
      },
      (error: unknown) => error,
    );
    expect(result).toBeInstanceOf(Error);
    expect(result).toMatchObject({ message: "Stale owner epoch or sequence" });
    const issued = await f.backend.openStorage(f.lease);
    try {
      expect(await issued.mintId()).toBe(f.firstId);
    } finally {
      await issued.close(context);
    }
  } finally {
    await unexpected?.close(context);
    await f.finish();
  }
});

for (const operation of ["mintId", "commit"] as const)
  for (const applied of [false, true])
    it(`memory worker boundary unknown ${operation} applied=${applied} retains ownership`, async () => {
      const f = await memoryFixture();
      await f.authority.release(f.lease);
      f.advance(1);
      const objects = objectFixture(),
        events: string[] = [];
      let wrapped: Storage | undefined;
      const backend = {
        ...f.backend,
        openStorage: async (lease: Parameters<typeof f.backend.openStorage>[0]) =>
          (wrapped = loseAcknowledgement(await f.backend.openStorage(lease), operation, applied)),
      };
      const deps = runtimeDependencies({
        backend,
        harnessOptions: { models: createModels(), registry: createRegistry() },
        objects: objects.objects,
        event: (name) => events.push(name),
        rootAgent: {},
      });
      try {
        await expect(worker(f.ns, () => 45000, deps)).rejects.toThrow("unknown");
        expect(events).not.toContain("release");
        expect(await f.authority.claim(f.ns, 45000)).toEqual({ kind: "held" });
        expect(await f.authority.hasWork(f.ns)).toBe(true);
        await f.expire();
        const c = await f.authority.claim(f.ns, 45000);
        check(c.kind === "claimed", "claim");
        const current = await f.backend.openStorage(c.lease);
        try {
          expect(await current.mintId()).toBeGreaterThanOrEqual(applied ? 3 : 2);
        } finally {
          await current.close(context);
        }
        expect(objects.openHandles()).toBe(0);
      } finally {
        await wrapped?.close(context);
        await f.finish();
      }
    });
