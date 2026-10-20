import { it, expect } from "vitest";
import {
  apiHandler,
  createMemoryBackend,
  harnessId,
  reconcile,
  runtimeDependencies,
  worker,
} from "../../src/index.ts";
import { requestId, check } from "../../src/domain.ts";
import { BACKGROUND_CONTEXT as context } from "@earendil-works/chord/context";
import { ROOT_CONVERSATION_ID } from "@earendil-works/pi-durable";
import { objectFixture } from "../shared/objects.ts";
import { controlledWork } from "../shared/scheduling.ts";

const ns = harnessId("app");
const command = { kind: "inline", input: { content: "hello", whenBusy: "followUp" } };

function fixture(tools = false) {
  let time = 10000;
  const backend = createMemoryBackend({ now: () => time }),
    objects = objectFixture(),
    work = controlledWork(tools);
  const hints: (typeof ns)[] = [];
  const dispatch = {
    wake: async (id: typeof ns) => {
      hints.push(id);
    },
  };
  const event = () => {};
  const options = {
    backend,
    harnessOptions: work,
    objects: objects.objects,
    event,
    rootAgent: {
      model: { provider: "faux", modelId: "faux-1" },
      ...(tools ? { tools: work.tools } : {}),
    },
  };
  const deps = runtimeDependencies(options);
  const apiDeps = {
    connectAuthority: backend.connectAuthority,
    objects: objects.objects,
    dispatch,
    event,
  };
  return {
    backend,
    work,
    objects,
    hints,
    options,
    deps,
    apiDeps,
    advance: (ms = 1) => {
      time += ms;
    },
    api: apiHandler(apiDeps),
  };
}

it("memory real Harness: lost hint, reconcile, explicit delivery, output and cold continuation", async () => {
  const f = fixture(),
    a = await f.backend.connectAuthority();
  try {
    const lost = apiHandler({
      ...f.apiDeps,
      dispatch: {
        wake: async () => {
          throw Error("lost hint");
        },
      },
    });
    expect(await lost({ ns, request: "one", command })).toMatchObject({ ok: true });
    expect(f.hints).toEqual([]);
    expect(await reconcile(f.apiDeps)).toBe(1);
    expect(f.hints).toEqual([ns]);
    expect(await worker(f.hints.shift()!, () => 45000, f.deps)).toBe("released");
    expect(await a.hasWork(ns)).toBe(false);
    f.advance();
    expect(await f.api({ ns, request: "two", command })).toMatchObject({ ok: true });
    expect(await worker(f.hints.shift()!, () => 45000, runtimeDependencies(f.options))).toBe(
      "released",
    );
    f.advance();
    const c = await a.claim(ns, 45000);
    check(c.kind === "claimed", "inspection claim");
    const storage = await f.backend.openStorage(c.lease);
    try {
      const submissions = (await storage.scanSubmissions({}, 100, undefined, context)).items;
      expect(submissions.map((s) => s.status)).toEqual(["done", "done"]);
      const entries = (
        await storage.scanEntries({ conversationId: ROOT_CONVERSATION_ID }, 100, undefined, context)
      ).items;
      expect(entries.filter((e) => e.kind === "pi.user")).toHaveLength(2);
      expect(entries.filter((e) => e.kind === "pi.assistant").map((e) => e.model)).toMatchObject([
        [{ role: "assistant", content: [{ type: "text", text: "done" }] }],
        [{ role: "assistant", content: [{ type: "text", text: "done" }] }],
      ]);
    } finally {
      await storage.close(context);
    }
    expect(f.objects.openHandles()).toBe(0);
  } finally {
    await a.close();
  }
});

it("memory submit-before-mark cut recovers one real submission and user entry after takeover", async () => {
  const f = fixture(),
    a = await f.backend.connectAuthority();
  const connect = f.deps.connectAuthority;
  let cut = true;
  f.deps.connectAuthority = async () => {
    const control = await connect();
    return {
      ...control,
      markAdmitted: async (...args) => {
        if (cut) {
          cut = false;
          throw Error("submit-mark cut");
        }
        return control.markAdmitted(...args);
      },
    };
  };
  try {
    await f.api({ ns, request: "one", command });
    await expect(worker(ns, () => 45000, f.deps)).rejects.toThrow("submit-mark cut");
    expect((await a.get(ns, requestId("one")))?.kind).toBe("received");
    expect(await a.claim(ns, 45000)).toEqual({ kind: "held" });
    f.advance(900001);
    expect(await worker(ns, () => 45000, f.deps)).toBe("released");
    expect(await a.hasWork(ns)).toBe(false);
    f.advance();
    const c = await a.claim(ns, 45000);
    check(c.kind === "claimed", "inspection claim");
    const s = await f.backend.openStorage(c.lease);
    try {
      const submissions = (await s.scanSubmissions({}, 100, undefined, context)).items;
      expect(submissions).toHaveLength(1);
      expect(await a.get(ns, requestId("one"))).toMatchObject({
        kind: "admitted",
        submission: submissions[0]!.id,
      });
      expect(
        (
          await s.scanEntries({ conversationId: ROOT_CONVERSATION_ID }, 100, undefined, context)
        ).items.filter((e) => e.kind === "pi.user"),
      ).toHaveLength(1);
    } finally {
      await s.close(context);
    }
  } finally {
    await a.close();
  }
});

it("memory safe tool restarts twice, unsafe never replays; held competitors cannot disturb it", async () => {
  const f = fixture(true),
    a = await f.backend.connectAuthority();
  let opened: Awaited<ReturnType<typeof f.deps.open>> | undefined;
  const open = f.deps.open;
  f.deps.open = async (lease) => (opened = await open(lease));
  let running: Promise<unknown> | undefined;
  try {
    await f.api({ ns, request: "tools", command });
    for (let epoch = 0; epoch < 3; epoch++) {
      let budget = 45000;
      running = worker(ns, () => budget, f.deps);
      await f.work.started(epoch + 2);
      expect(
        await Promise.all(Array.from({ length: 5 }, () => worker(ns, () => 45000, f.deps))),
      ).toEqual(Array(5).fill("held"));
      if (epoch === 2) {
        f.work.starts.at(-1)!.finish();
        await opened!.root.waitForIdle(context);
      } else budget = 5000;
      expect(await running).toBe("released");
      expect(await a.hasWork(ns)).toBe(epoch !== 2);
      f.advance();
    }
    expect(f.work.starts.filter((s) => s.name === "safe")).toHaveLength(3);
    expect(f.work.starts.filter((s) => s.name === "unsafe")).toHaveLength(1);
    const c = await a.claim(ns, 45000);
    check(c.kind === "claimed", "inspection claim");
    const s = await f.backend.openStorage(c.lease);
    try {
      expect(
        JSON.stringify(
          (await s.scanEntries({ conversationId: ROOT_CONVERSATION_ID }, 100, undefined, context))
            .items,
        ),
      ).toContain("interrupted");
    } finally {
      await s.close(context);
    }
  } finally {
    for (const start of f.work.starts) start.finish();
    await opened?.harness.close(context);
    await running?.catch(() => {});
    await a.close();
  }
});
