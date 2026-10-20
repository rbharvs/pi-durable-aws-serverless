import { expect, it } from "vitest";
import { BACKGROUND_CONTEXT as context } from "@earendil-works/chord/context";
import { ROOT_CONVERSATION_ID } from "@earendil-works/pi-durable";
import { check, requestId } from "../../src/domain.ts";
import {
  runtimeDependencies,
  worker,
  type Lease,
  type WorkerDependencies,
} from "../../src/index.ts";
import { workerFixture } from "./worker-support.ts";

it("rejects an actual foreign receipt before resolution or admission and retains both owners", async () => {
  const local = await workerFixture("worker_local"),
    foreign = await workerFixture("worker_foreign");
  let lease: Lease | undefined,
    resolves = 0,
    reads = 0,
    mappings = 0;
  const deps = runtimeDependencies({
    ...local.options,
    objects(ns) {
      const reader = foreign.objects.objects(ns),
        get = reader.get.bind(reader);
      reader.get = (ref) => {
        reads++;
        return get(ref);
      };
      return reader;
    },
  });
  try {
    await local.receive();
    const staged = foreign.objects.stage(
      foreign.ns,
      JSON.stringify({ content: "foreign ".repeat(40000), whenBusy: "followUp" }),
    );
    expect(
      await foreign.api({ ns: foreign.ns, request: "foreign", command: { kind: "s3", ...staged } }),
    ).toMatchObject({ ok: true });
    const receipt = await foreign.authority.get(foreign.ns, requestId("foreign"));
    check(receipt?.kind === "received", "Expected actual foreign receipt");
    expect((await foreign.authority.claim(foreign.ns, 45000)).kind).toBe("claimed");
    const foreignHead = await foreign.head();
    const ports: WorkerDependencies = {
      ...deps,
      async open(owned) {
        lease = owned;
        return deps.open(owned);
      },
      async resolve(item) {
        resolves++;
        return deps.resolve(item);
      },
      async connectAuthority() {
        const authority = await deps.connectAuthority(),
          mark = authority.markAdmitted.bind(authority);
        // Faulty adapter returns a genuine durable receipt from a different namespace.
        authority.received = async () => [receipt];
        authority.markAdmitted = async (item, id) => {
          mappings++;
          await mark(item, id);
        };
        return authority;
      },
    };
    const outcome = await worker(local.ns, () => 45000, ports).then(
      (result) => ({ result }),
      (error) => ({ error }),
    );
    check(lease, "Expected local ownership");
    const storage = await local.backend.openStorage(lease);
    try {
      const entries = await storage.scanEntries(
        { conversationId: ROOT_CONVERSATION_ID },
        100,
        undefined,
        context,
      );
      expect(entries.items.filter((e) => e.kind === "pi.user").map((e) => e.kind)).toEqual([]);
      expect((await storage.scanSubmissions({}, 100, undefined, context)).items).toEqual([]);
    } finally {
      await storage.close(context);
    }
    expect(outcome).toEqual({ error: Error("Foreign receipt namespace") });
    expect(resolves).toBe(0);
    expect(reads).toBe(0);
    expect(mappings).toBe(0);
    expect(await local.head()).toMatchObject({
      owner: lease.owner,
      epoch: String(lease.epoch),
      lease_until: String(lease.until),
    });
    expect(await foreign.head()).toEqual(foreignHead);
    expect(await foreign.authority.get(foreign.ns, receipt.request)).toEqual(receipt);
    expect((await local.authority.get(local.ns, requestId("one")))?.kind).toBe("received");
    expect(
      await foreign.authority.inspectionDb.query("SELECT id FROM records WHERE ns=$1", [
        foreign.ns,
      ]),
    ).toEqual([]);
    expect(local.events.some((e) => e.event === "release")).toBe(false);
  } finally {
    await local.finish();
    await foreign.finish();
  }
});
