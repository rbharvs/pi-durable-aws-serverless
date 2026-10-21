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
  const engine = new SubpathMemory();
  const root = idFromNumber<ConversationId>(1);
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
  const fixture = await memoryFixture(true);
  const other = createMemoryBackend({ now: fixture.time });
  const foreign = await other.connectAuthority();
  const storage = await fixture.backend.openStorage(fixture.lease);
  try {
    await fixture.authority.receive(fixture.ns, requestId("pending"), input);
    fixture.advance(fixture.lease.until - fixture.time());
    expect(await fixture.authority.claim(fixture.ns, 45000)).toEqual({ kind: "held" });
    expect(await fixture.authority.eligible()).toEqual([]);
    fixture.advance(1);
    expect(await fixture.authority.eligible()).toEqual([fixture.ns]);
    expect(await storage.mintId()).toBe(fixture.firstId);
    await storage.commit([], context);
    await foreign.receive(fixture.ns, requestId("one"), input);
    const claim = await foreign.claim(fixture.ns, 45000);
    check(claim.kind === "claimed", "foreign claim");
    await expect(fixture.backend.openStorage(claim.lease)).rejects.toThrow("Stale");
    await expect(other.openStorage(fixture.lease)).rejects.toThrow("Stale");
    expect(await fixture.authority.release(fixture.lease)).toBe("released");
    expect(await fixture.authority.claim(fixture.ns, 45000)).toEqual({ kind: "held" });
    fixture.advance(1);
    const next = await fixture.authority.claim(fixture.ns, 45000);
    check(next.kind === "claimed", "next claim");
    expect(next.lease.epoch).toBe(fixture.lease.epoch + 1);
    await expect(storage.commit([], context)).rejects.toThrow("Stale");
    await expect(storage.mintId()).rejects.toThrow("failed");
    expect(await fixture.authority.release(fixture.lease)).toBe("lost");
  } finally {
    await storage.close(context);
    await foreign.close();
    await fixture.finish();
  }
});

it("no asynchronous gap between fence and Pi apply: the next turn observes work before takeover", async () => {
  const fixture = await memoryFixture(true);
  const storage = await fixture.backend.openStorage(fixture.lease);
  try {
    await fixture.expire();
    const pending = storage.commit(
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
      const work = fixture.authority.hasWork(fixture.ns);
      const claim = fixture.authority.claim(fixture.ns, 45000);
      return Promise.all([work, claim]);
    });
    await pending;
    const [work, claim] = await nextTurn;
    expect(work).toBe(true);
    check(claim.kind === "claimed", "takeover");
    const current = await fixture.backend.openStorage(claim.lease);
    try {
      expect(await current.submission(idFromNumber(30), context)).toMatchObject({
        status: "queued",
      });
      await expect(storage.commit([], context)).rejects.toThrow("Stale");
    } finally {
      await current.close(context);
    }
  } finally {
    await storage.close(context);
    await fixture.finish();
  }
});

it("clock options retain their method receiver and backing state cannot escape through handle reflection", async () => {
  class Clock implements MemoryBackendOptions {
    time = 10000;
    now() {
      return this.time;
    }
  }
  const clock = new Clock();
  const backend = createMemoryBackend(clock);
  const authority = await backend.connectAuthority();
  const ns = harnessId("clock");
  try {
    expect((await authority.receive(ns, requestId("one"), input)).item.receivedAt).toBe(clock.time);
    const claim = await authority.claim(ns, 45000);
    check(claim.kind === "claimed", "claim");
    expect(Object.isFrozen(claim.lease)).toBe(true);
    const storage = await backend.openStorage(claim.lease);
    try {
      expect(Reflect.ownKeys(storage)).toEqual([]);
      expect(() => Reflect.get(storage, "prepareCommit")).toThrow("Unknown");
      expect(() => Reflect.get(storage, "state")).toThrow("Unknown");
    } finally {
      await storage.close(context);
    }
  } finally {
    await authority.close();
  }
});

it("close drains a held public Pi read but rejects queued read/commit/mint without late effects", async () => {
  const fixture = await memoryFixture(true);
  const storage = await fixture.backend.openStorage(fixture.lease);
  const reached = Promise.withResolvers<void>();
  const proceed = Promise.withResolvers<void>();
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
    const seq = await storage.commit([], context);
    active = storage.scanConversations({}, 10, undefined, context);
    await reached.promise;
    const queued = Promise.allSettled([
      storage.conversation(idFromNumber<ConversationId>(1), context),
      storage.commit(
        [{ type: "conversation", value: { id: idFromNumber<ConversationId>(50) } }],
        context,
      ),
      storage.mintId(),
    ]);
    let closed = false;
    closing = storage.close(context).then(() => {
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
    const cold = await fixture.backend.openStorage(fixture.lease);
    try {
      expect(await cold.conversation(idFromNumber<ConversationId>(50), context)).toBeUndefined();
      expect(await cold.mintId()).toBe(fixture.firstId);
      expect(await cold.commit([], context)).toBe(seq + 1);
    } finally {
      await cold.close(context);
    }
  } finally {
    proceed.resolve();
    await active?.catch(() => {});
    await closing;
    spy.mockRestore();
    await storage.close(context);
    await fixture.finish();
  }
});

it("invalid clock values reject receive/claim/eligible without changing namespace or lease", async () => {
  for (const bad of [NaN, -1, 1.5, Number.MAX_SAFE_INTEGER + 1, "5"]) {
    let time = 1000;
    const backend = createMemoryBackend({ now: () => time });
    const authority = await backend.connectAuthority();
    const ns = harnessId("valid-clock");
    const fresh = harnessId("invalid-clock");
    try {
      const receipt = (await authority.receive(ns, requestId("seed"), input)).item;
      const claim = await authority.claim(ns, 45000);
      check(claim.kind === "claimed", "valid claim");
      time = bad as number;
      const results = await Promise.allSettled([
        authority.receive(fresh, requestId("fresh"), input),
        authority.receive(ns, requestId("invalid"), input),
        authority.claim(ns, 45000),
        authority.eligible(),
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
      expect(await authority.get(fresh, requestId("fresh"))).toBeUndefined();
      await expect(authority.hasWork(fresh)).rejects.toThrow("Unknown namespace");
      await expect(authority.claim(fresh, 45000)).rejects.toThrow("Unknown namespace");
      expect(await authority.get(ns, requestId("invalid"))).toBeUndefined();
      expect(await authority.get(ns, requestId("seed"))).toEqual(receipt);
      expect(await authority.eligible()).toEqual([ns]);
      const storage = await backend.openStorage(claim.lease);
      try {
        expect(await storage.mintId()).toBe(2);
      } finally {
        await storage.close(context);
      }
      const next = await authority.claim(ns, 45000);
      check(next.kind === "claimed", "valid takeover");
      expect(next.lease.epoch).toBe(claim.lease.epoch + 1);
      expect(next.lease.owner).not.toBe(claim.lease.owner);
    } finally {
      await authority.close();
    }
  }
});

it("copied issued lease with identical owner and epoch cannot open Storage", async () => {
  const fixture = await memoryFixture(true);
  const copy = { ...fixture.lease } as Lease;
  let unexpected: Storage | undefined;
  try {
    expect(copy).toEqual(fixture.lease);
    expect(copy).not.toBe(fixture.lease);
    const result = await fixture.backend.openStorage(copy).then(
      (storage) => {
        unexpected = storage;
        return "opened";
      },
      (error: unknown) => error,
    );
    expect(result).toBeInstanceOf(Error);
    expect(result).toMatchObject({ message: "Stale owner epoch or sequence" });
    const issued = await fixture.backend.openStorage(fixture.lease);
    try {
      expect(await issued.mintId()).toBe(fixture.firstId);
    } finally {
      await issued.close(context);
    }
  } finally {
    await unexpected?.close(context);
    await fixture.finish();
  }
});

for (const operation of ["mintId", "commit"] as const) {
  for (const applied of [false, true]) {
    it(`memory worker boundary unknown ${operation} applied=${applied} retains ownership`, async () => {
      const fixture = await memoryFixture();
      await fixture.authority.release(fixture.lease);
      fixture.advance(1);
      const objects = objectFixture();
      const events: string[] = [];
      let wrapped: Storage | undefined;
      const backend = {
        ...fixture.backend,
        openStorage: async (lease: Parameters<typeof fixture.backend.openStorage>[0]) =>
          (wrapped = loseAcknowledgement(
            await fixture.backend.openStorage(lease),
            operation,
            applied,
          )),
      };
      const deps = runtimeDependencies({
        backend,
        harnessOptions: { models: createModels(), registry: createRegistry() },
        objects: objects.objects,
        event: (name) => events.push(name),
        rootAgent: {},
      });
      try {
        await expect(worker(fixture.ns, () => 45000, deps)).rejects.toThrow("unknown");
        expect(events).not.toContain("release");
        expect(await fixture.authority.claim(fixture.ns, 45000)).toEqual({ kind: "held" });
        expect(await fixture.authority.hasWork(fixture.ns)).toBe(true);
        await fixture.expire();
        const c = await fixture.authority.claim(fixture.ns, 45000);
        check(c.kind === "claimed", "claim");
        const current = await fixture.backend.openStorage(c.lease);
        try {
          expect(await current.mintId()).toBeGreaterThanOrEqual(applied ? 3 : 2);
        } finally {
          await current.close(context);
        }
        expect(objects.openHandles()).toBe(0);
      } finally {
        await wrapped?.close(context);
        await fixture.finish();
      }
    });
  }
}
