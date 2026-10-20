import { it, expect } from "vitest";
// A source-pinned consumer: only our supported entry and public Pi exports.
import {
  apiHandler,
  createMemoryBackend,
  harnessId,
  reconcile,
  runtimeDependencies,
  worker,
  type CommandObjects,
  type HarnessId,
  type RuntimeOptions,
  type Backend,
  type Authority,
  type Lease,
} from "./index.ts";
import { createRegistry, type Storage } from "@earendil-works/pi-durable";
import { createModels, fauxProvider, fauxAssistantMessage } from "@earendil-works/pi-ai";

class CountingBackend implements Backend {
  connects = 0;
  opens = 0;

  constructor(private readonly inner: Backend) {}

  connectAuthority(): Promise<Authority> {
    this.connects++;

    return this.inner.connectAuthority();
  }

  openStorage(lease: Lease): Promise<Storage> {
    this.opens++;

    return this.inner.openStorage(lease);
  }
}

const inlineOnly = (_ns: HarnessId): CommandObjects & ReturnType<RuntimeOptions["objects"]> => ({
  getStaged: async () => {
    throw Error("staging unsupported");
  },
  put: async () => {
    throw Error("large input unsupported");
  },
  get: async () => {
    throw Error("no objects");
  },
  close: () => {},
});

it("public named-options consumer completes admission reconcile and a real Pi memory worker", async () => {
  let now = 1000000;
  const backend = createMemoryBackend({ now: () => now }),
    faux = fauxProvider(),
    models = createModels();

  faux.setResponses([fauxAssistantMessage("done")]);
  models.setProvider(faux.provider);

  const wakes: HarnessId[] = [],
    dispatch = {
      wake: async (ns: HarnessId) => {
        wakes.push(ns);
      },
    },
    event = () => {};
  const api = apiHandler({
      connectAuthority: backend.connectAuthority,
      objects: inlineOnly,
      dispatch,
      event,
    }),
    ns = harnessId("consumer");

  expect(
    await api({
      ns,
      request: "r1",
      command: {
        kind: "inline",
        input: { content: "hi", whenBusy: "followUp" },
      },
    }),
  ).toMatchObject({ ok: true, duplicate: false });
  wakes.shift();
  now += 10;
  expect(
    await reconcile({
      connectAuthority: backend.connectAuthority,
      dispatch,
      event,
    }),
  ).toBe(1);
  expect(wakes).toEqual([ns]);

  const options: RuntimeOptions = {
    backend,
    harnessOptions: { models, registry: createRegistry() },
    objects: inlineOnly,
    event,
    rootAgent: { model: { provider: "faux", modelId: "faux-1" } },
  };

  expect(await worker(ns, () => 45000, runtimeDependencies(options))).toBe("released");

  const authority = await backend.connectAuthority();

  try {
    expect(await authority.hasWork(ns)).toBe(false);
  } finally {
    await authority.close();
  }
});

it("public runtime preserves this for a class-based Backend decorator through actual worker completion", async () => {
  const backend = new CountingBackend(createMemoryBackend({ now: () => 1000000 })),
    faux = fauxProvider(),
    models = createModels();

  faux.setResponses([fauxAssistantMessage("done")]);
  models.setProvider(faux.provider);

  const ns = harnessId("bound-decorator"),
    api = apiHandler({
      connectAuthority: () => backend.connectAuthority(),
      objects: inlineOnly,
      dispatch: { wake: async () => {} },
      event: () => {},
    });

  expect(
    await api({
      ns,
      request: "r",
      command: {
        kind: "inline",
        input: { content: "hi", whenBusy: "followUp" },
      },
    }),
  ).toMatchObject({ ok: true });

  const deps = runtimeDependencies({
    backend,
    harnessOptions: { models, registry: createRegistry() },
    objects: inlineOnly,
    event: () => {},
    rootAgent: { model: { provider: "faux", modelId: "faux-1" } },
  });

  await expect(worker(ns, () => 45000, deps)).resolves.toBe("released");
  expect(backend.connects).toBe(2);
  expect(backend.opens).toBe(1);

  const authority = await backend.connectAuthority();

  try {
    expect(await authority.hasWork(ns)).toBe(false);
  } finally {
    await authority.close();
  }
});
