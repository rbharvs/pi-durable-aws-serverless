import { expect, it } from "vitest";
import { createSqlBackend } from "../../src/sql/backend.ts";
import {
  context,
  deferred,
  historyFixture,
  memoryObjects,
  queryGate,
  dataEntry,
  rootId,
} from "./history-support.ts";
import { provider, wrap } from "./postgres.ts";

for (const failureAt of ["objects", "connect", "open", "none"] as const)
  it(`owns exactly the resources acquired when ${failureAt} fails`, async () => {
    const f = await historyFixture("backend_acquire", { objects: memoryObjects });
    let connects = 0,
      ends = 0,
      closes = 0;
    const failure = Error(failureAt);
    const backend = createSqlBackend(
      {
        ...provider,
        async connect() {
          connects++;
          if (failureAt === "connect") throw failure;
          const client = await provider.connect();
          const end = client.end.bind(client);
          client.end = async () => {
            ends++;
            await end();
          };
          return wrap(client, async (phase, sql) => {
            if (failureAt === "open" && phase === "before" && sql.startsWith("SELECT epoch"))
              throw failure;
          });
        },
      },
      () => {
        if (failureAt === "objects") throw failure;
        return {
          ...f.objects,
          async close() {
            closes++;
          },
        };
      },
    );
    try {
      if (failureAt === "none") {
        const storage = await backend.openStorage(f.lease);
        await Promise.all([storage.close(context), storage.close(context)]);
        await storage.close(context);
        await expect(storage.mintId()).rejects.toThrow("closed");
      } else await expect(backend.openStorage(f.lease)).rejects.toBe(failure);
      expect(connects).toBe(failureAt === "objects" ? 0 : 1);
      expect(ends).toBe(failureAt === "none" || failureAt === "open" ? 1 : 0);
      expect(closes).toBe(failureAt === "objects" ? 0 : 1);
      expect((await f.head()).owner).toBe(f.lease.owner);
    } finally {
      await f.finish();
    }
  });

for (const duringOpen of [false, true])
  it(`preserves database and asynchronous object cleanup failures during ${duringOpen ? "open" : "close"}`, async () => {
    const f = await historyFixture("backend_errors", { objects: memoryObjects });
    const opening = Error("open failed"),
      ending = Error("end failed"),
      object = Error("object close failed");
    let ends = 0,
      closes = 0;
    const backend = createSqlBackend(
      {
        ...provider,
        async connect() {
          const client = await provider.connect();
          const end = client.end.bind(client);
          client.end = async () => {
            ends++;
            await end();
            throw ending;
          };
          return wrap(client, async (phase, sql) => {
            if (duringOpen && phase === "before" && sql.startsWith("SELECT epoch")) throw opening;
          });
        },
      },
      () => ({
        ...f.objects,
        async close() {
          closes++;
          throw object;
        },
      }),
    );
    try {
      const operation = duringOpen
        ? backend.openStorage(f.lease)
        : (await backend.openStorage(f.lease)).close(context);
      await expect(operation).rejects.toMatchObject({
        cause: duringOpen ? opening : ending,
        errors: duringOpen ? [opening, ending, object] : [ending, object],
      });
      expect(ends).toBe(1);
      expect(closes).toBe(1);
    } finally {
      await f.finish();
    }
  });

it("joins in-flight uploads and SQL before closing objects once, and waits for async object cleanup", async () => {
  const f = await historyFixture("backend_join", { objects: memoryObjects });
  const uploadStarted = deferred(),
    uploadRelease = deferred(),
    cleanupStarted = deferred(),
    cleanupRelease = deferred();
  const sequence: string[] = [];
  const backend = createSqlBackend(
    {
      ...provider,
      async connect() {
        const client = await provider.connect();
        const end = client.end.bind(client);
        client.end = async () => {
          sequence.push("db close");
          await end();
        };
        return client;
      },
    },
    () => ({
      ...f.objects,
      async put(json) {
        uploadStarted.resolve();
        await uploadRelease.promise;
        sequence.push("upload");
        return f.objects.put(json);
      },
      async close() {
        sequence.push("objects close");
        cleanupStarted.resolve();
        await cleanupRelease.promise;
      },
    }),
  );
  const storage = await backend.openStorage(f.lease);
  try {
    const commit = storage.commit([dataEntry(20, "x".repeat(300 * 1024))], context);
    await uploadStarted.promise;
    let closed = false;
    const closing = storage.close(context).then(() => {
      closed = true;
    });
    const again = storage.close(context);
    await expect(storage.mintId()).rejects.toThrow("closed");
    expect(sequence).toEqual([]);
    uploadRelease.resolve();
    await commit;
    await cleanupStarted.promise;
    expect(sequence).toEqual(["upload", "db close", "objects close"]);
    expect(closed).toBe(false);
    cleanupRelease.resolve();
    await Promise.all([closing, again]);
    expect(closed).toBe(true);
  } finally {
    uploadRelease.resolve();
    cleanupRelease.resolve();
    await storage.close(context);
    await f.finish();
  }
});

it("joined close also waits for a read snapshot and authority close does not release ownership", async () => {
  const f = await historyFixture("backend_read_close");
  const gate = queryGate("after", (sql) => sql.includes("FROM records"));
  const sequence: string[] = [];
  const backend = createSqlBackend(
    { ...provider, connect: async () => wrap(await provider.connect(), gate.hook) },
    () => ({
      ...f.objects,
      close() {
        sequence.push("objects");
      },
    }),
  );
  const storage = await backend.openStorage(f.lease);
  try {
    gate.arm();
    const read = storage.conversation(rootId, context);
    await gate.reached;
    const close = storage.close(context);
    expect(sequence).toEqual([]);
    gate.release();
    await read;
    await close;
    expect(sequence).toEqual(["objects"]);
    const authority = await backend.connectAuthority();
    expect(await authority.hasWork(f.ns)).toBe(true);
    await authority.close();
    expect((await f.head()).owner).toBe(f.lease.owner);
  } finally {
    gate.release();
    await storage.close(context);
    await f.finish();
  }
});
