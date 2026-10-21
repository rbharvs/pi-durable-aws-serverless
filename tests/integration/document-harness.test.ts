import { expect, it } from "vitest";
import { createModels } from "@earendil-works/pi-ai";
import { fauxAssistantMessage, fauxProvider } from "@earendil-works/pi-ai/providers/faux";
import {
  Harness,
  MemoryStorage,
  SessionFailed,
  createRegistry,
  defineDoc,
  type Storage,
} from "@earendil-works/pi-durable";
import { check, requestId } from "../../src/domain.ts";
import { context, conversationId, entryId, rootId } from "./history-support.ts";
import { documentFixture } from "./document-support.ts";

const options = () => ({ models: createModels(), registry: createRegistry() });

it("actual Session fails once on corrupt materialization and never publishes a callback that catches the failed read", async () => {
  const f = await documentFixture("pi_corrupt_read");
  const token = defineDoc<{ count: number }>({
    kind: "app.poison",
    version: 1,
    scope: "conversation",
    history: "rewindable",
    fork: "asOf",
    initial: () => ({ count: 0 }),
  });
  let h: Harness | undefined;
  const reports: unknown[] = [];
  try {
    h = await Harness.open(await f.openStorage(), options(), context);
    const root = await h.root(context);
    await root.commit(async (tx) => {
      await tx.doc(token, root.id);
    }, context);
    await h.close(context);
    const stored = await f.openStorage();
    const record = await stored.findDocument(
      { kind: "app.poison", scope: { kind: "conversation", conversationId: root.id } },
      "current",
      context,
    );
    check(record, "Missing poison document");
    await stored.close(context);
    await f.control.query(
      "UPDATE inline_payloads SET json='[]' WHERE ns=$1 AND payload_ref=(SELECT payload_ref FROM revisions WHERE ns=$1 AND document_id=$2 ORDER BY seq DESC LIMIT 1)",
      [f.ns, record.id],
    );
    const before = await f.snapshot();
    h = await Harness.open(
      await f.openStorage(),
      { ...options(), onReport: (error) => reports.push(error) },
      context,
    );
    let observed: unknown;
    await expect(
      h.commit(async (tx) => {
        try {
          await tx.doc(token, root.id);
        } catch (error) {
          observed = error;
        }
      }, context),
    ).rejects.toBeInstanceOf(SessionFailed);
    expect(await h.closed).toEqual({ reason: "failed", error: observed });
    expect(reports).toEqual([observed]);
    expect(await f.snapshot()).toEqual(before);
    await expect(h.snapshot(token, root.id, context)).rejects.toBeInstanceOf(SessionFailed);
  } finally {
    await h?.close(context);
    await f.finish();
  }
});

for (const version of [1, 2, Number.MAX_SAFE_INTEGER]) {
  it(`accepts actual Pi document version ${version}, with the same delta and cold value as public MemoryStorage`, async () => {
    const f = await documentFixture("pi_document_version");
    const token = defineDoc<{ count: number }>({
      kind: "app.version",
      version,
      scope: "conversation",
      history: "rewindable",
      fork: "asOf",
      initial: () => ({ count: 0 }),
      checkpointWhen: () => false,
    });
    try {
      for (const storage of [new MemoryStorage(), await f.openStorage()]) {
        const h = await Harness.open(storage, options(), context);
        try {
          const root = await h.root(context);
          await root.commit(async (tx) => {
            (await tx.doc(token, root.id)).count = 1;
          }, context);
          await root.commit(async (tx) => {
            (await tx.doc(token, root.id)).count = 2;
          }, context);
          expect(await h.snapshot(token, root.id, context)).toEqual({ count: 2 });
          const record = await storage.findDocument(
            { kind: "app.version", scope: { kind: "conversation", conversationId: root.id } },
            "current",
            context,
          );
          check(record, "Missing actual Pi document");
          expect(await storage.document(record.id, "current", context)).toMatchObject({
            version,
            value: { count: 2 },
            deltasSinceBase: 1,
          });
        } finally {
          await h.close(context);
        }
      }
      const cold = await Harness.open(await f.openStorage(), options(), context);
      try {
        expect(await cold.snapshot(token, rootId, context)).toEqual({ count: 2 });
      } finally {
        await cold.close(context);
      }
    } finally {
      await f.finish();
    }
  });
}

it("actual Harness forks as-of document state, retires/replaces an address, and keeps cold source history", async () => {
  const f = await documentFixture("pi_document_fork");
  const token = defineDoc<{ count: number }>({
    kind: "app.history",
    version: 1,
    scope: "conversation",
    history: "rewindable",
    fork: "asOf",
    initial: () => ({ count: 0 }),
    checkpointWhen: () => false,
  });
  let h: Harness | undefined;
  try {
    h = await Harness.open(await f.openStorage(), options(), context);
    const root = await h.root(context);
    const marker = await root.commit(async (tx) => {
      (await tx.doc(token, root.id)).count = 1;
      return tx.appendEntry(root.id, { kind: "app.marker" });
    }, context);
    await root.commit(async (tx) => {
      (await tx.doc(token, root.id)).count = 2;
    }, context);
    const fork = await root.fork(marker.id, { ownership: { kind: "ownerless" } }, context);
    expect(await h.snapshot(token, fork.id, context)).toEqual({ count: 1 });
    await root.commit((tx) => tx.retireDoc(token, root.id), context);
    await root.commit(async (tx) => {
      (await tx.doc(token, root.id)).count = 3;
    }, context);
    await h.close(context);
    h = await Harness.open(await f.openStorage(), options(), context);
    expect(await h.snapshot(token, root.id, context)).toEqual({ count: 3 });
    expect(await h.snapshotAsOf(token, root.id, marker.id, context)).toEqual({ count: 1 });
    expect(await h.snapshot(token, fork.id, context)).toEqual({ count: 1 });
  } finally {
    await h?.close(context);
    await f.finish();
  }
});

it("real Pi submit/resume/wait reaches durable done and maps the immutable receipt across reopen", async () => {
  const f = await documentFixture("pi_submission");
  const faux = fauxProvider({ tokensPerSecond: Infinity });
  faux.setResponses([fauxAssistantMessage("durable answer")]);
  const models = createModels();
  models.setProvider(faux.provider);
  const opts = { models, registry: createRegistry() };
  let h: Harness | undefined;
  const authority = await f.backend.connectAuthority();
  try {
    const receipt = await authority.get(f.ns, requestId("fixture"));
    check(receipt?.kind === "received", "Missing receipt");
    h = await Harness.open(await f.openStorage(), opts, context);
    const root = await h.root(context, {
      agent: { model: { provider: "faux", modelId: "faux-1" } },
    });
    const submitted = await root.submit(
      { type: "input", content: "history fixture", requestId: receipt.request },
      context,
    );
    h.resume();
    await authority.markAdmitted(receipt, submitted.id);
    const done = await submitted.wait(context);
    expect(done.status).toBe("done");
    await h.close(context);
    const raw = await f.openStorage();
    expect(await raw.submission(submitted.id, context)).toEqual(done);
    await raw.close(context);
    h = await Harness.open(await f.openStorage(), opts, context);
    h.resume();
    const coldRoot = await h.root(context);
    const repeated = await coldRoot.submit(
      { type: "input", content: "history fixture", requestId: receipt.request },
      context,
    );
    expect(repeated.id).toBe(submitted.id);
    expect(await repeated.wait(context)).toEqual(done);
    expect(faux.state.callCount).toBe(1);
    expect(await authority.get(f.ns, receipt.request)).toEqual({
      ...receipt,
      kind: "admitted",
      submission: submitted.id,
    });
    expect(await authority.received(f.ns)).toEqual([]);
    expect(await authority.hasWork(f.ns)).toBe(false);
  } finally {
    await h?.close(context);
    await authority.close();
    await f.finish();
  }
});

it("actual Pi callback and initialization failures consume IDs but publish no records or document fragments", async () => {
  const f = await documentFixture("pi_callback_ids");
  let h: Harness | undefined;
  try {
    h = await Harness.open(await f.openStorage(), options(), context);
    const root = await h.root(context);
    const before = await f.snapshot();
    let callbackId = 0;
    await expect(
      root.commit(async (tx) => {
        callbackId = (await tx.createConversation({ ownership: { kind: "ownerless" } })).id;
        throw new Error("callback cut");
      }, context),
    ).rejects.toThrow("callback cut");
    const after = await f.snapshot();
    expect({ ...after, harness: undefined }).toEqual({ ...before, harness: undefined });
    let initId = 0;
    await expect(
      h.createConversation(
        {
          ownership: { kind: "ownerless" },
          init: (_tx, id) => {
            initId = id;
            throw new Error("init cut");
          },
        },
        context,
      ),
    ).rejects.toThrow("init cut");
    expect(initId).toBeGreaterThan(callbackId);
    await h.close(context);
    const cold = await f.openStorage();
    expect(await cold.conversation(conversationId(callbackId), context)).toBeUndefined();
    expect(await cold.conversation(conversationId(initId), context)).toBeUndefined();
    expect(await cold.mintId()).toBeGreaterThan(initId);
  } finally {
    await h?.close(context);
    await f.finish();
  }
});

for (const operation of ["mint", "commit"] as const) {
  for (const applied of [false, true]) {
    it(`actual Session seals once after unknown ${operation} applied=${applied}, closes, and retains its lease`, async () => {
      const failure = new Error("outcome unknown");
      let armed = false;
      let fenced = false;
      const f = await documentFixture("pi_poison", {
        hook: async (phase, sql) => {
          if (phase === "after" && sql.startsWith("UPDATE harness SET next_seq")) fenced = true;
          const target =
            operation === "mint"
              ? sql.startsWith("UPDATE harness SET next_id=")
              : fenced && sql === "COMMIT";
          if (armed && target && phase === (applied ? "after" : "before")) {
            armed = false;
            throw failure;
          }
        },
      });
      let h: Harness | undefined;
      const reports: unknown[] = [];
      let storage: Storage | undefined;
      try {
        storage = await f.openStorage();
        h = await Harness.open(
          storage,
          { ...options(), onReport: (error) => reports.push(error) },
          context,
        );
        const root = await h.root(context);
        const nextId = Number((await f.head()).next_id);
        f.trace.length = 0;
        fenced = false;
        armed = true;
        await expect(
          root.commit((tx) => tx.appendEntry(root.id, { kind: "app.unknown" }), context),
        ).rejects.toBe(failure);
        expect(await h.closed).toEqual({ reason: "failed", error: failure });
        expect(reports).toEqual([failure]);
        await expect(
          root.commit((tx) => tx.appendEntry(root.id, { kind: "app.later" }), context),
        ).rejects.toBeInstanceOf(SessionFailed);
        expect((await f.head()).owner).toBe(f.lease.owner);
        await expect(storage.mintId()).rejects.toThrow("closed");
        if (operation === "commit") {
          const businessCommit = f.trace.findIndex(
            (q) => q.sql === "COMMIT" && q.error === failure,
          );
          expect(f.trace.slice(businessCommit + 1).some((q) => q.sql === "ROLLBACK")).toBe(false);
        }
        const cold = await f.openStorage();
        expect(await cold.mintId()).toBe(nextId + (operation === "commit" || applied ? 1 : 0));
        expect((await cold.entry(entryId(nextId), context)) !== undefined).toBe(
          operation === "commit" && applied,
        );
      } finally {
        await h?.close(context);
        await f.finish();
      }
    });
  }
}
