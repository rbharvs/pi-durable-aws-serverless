import { afterEach, expect, it, vi } from "vitest";
import { ROOT_CONVERSATION_ID } from "@earendil-works/pi-durable";
import { BACKGROUND_CONTEXT as context } from "@earendil-works/chord/context";
import { createHandler as apiRoot } from "../../src/aws/api.ts";
import { createHandler as reconcilerRoot } from "../../src/aws/reconciler.ts";
import { createHandler as workerRoot, createAwsHandler } from "../../src/aws/worker.ts";
import * as common from "../../src/aws/common.ts";
import * as s3 from "../../src/aws/s3.ts";
import { createSqlBackend } from "../../src/sql/backend.ts";
import { runtimeDependencies } from "../../src/runtime.ts";
import { canonical } from "../../src/input.ts";
import { check, computeSha256, requestId, type HarnessId } from "../../src/domain.ts";
import { DB_NOW_MS } from "../../src/sql/authority.ts";
import { aws, lambdaContext as ctx } from "../shared/aws.ts";
import { sdkObjects } from "../shared/aws-objects.ts";
import { largeImage, imageFixture } from "../shared/image.ts";
import { namespace, provider } from "./postgres.ts";
import { modelWork } from "./worker-support.ts";

afterEach(() => vi.restoreAllMocks());

async function fixture(label: string, hold = false) {
  const f = await namespace(label);
  const objects = sdkObjects();
  const backend = createSqlBackend(provider, (ns) => objects.objects(ns));
  const work = modelWork(hold);
  const hints: HarnessId[] = [];
  const events: string[] = [];
  const dispatch = {
    async wake(ns: HarnessId) {
      expect(this).toBe(dispatch);
      hints.push(ns);
    },
  };
  const options = {
    backend,
    harnessOptions: work,
    rootAgent: { model: { provider: "faux", modelId: "faux-1" } },
    objects: (ns: HarnessId) => objects.objects(ns),
    event: (name: string) => {
      events.push(name);
    },
  };
  const deps = {
    async connectAuthority() {
      expect(this).toBe(deps);
      return backend.connectAuthority();
    },
    objects: options.objects,
    dispatch,
    event: options.event,
  };
  return {
    ...f,
    objects,
    backend,
    work,
    hints,
    events,
    options,
    deps,
    api: apiRoot(() => deps),
    reconcile: reconcilerRoot(() => deps),
    async output() {
      const claimed = await f.authority.claim(f.ns, 45000);
      check(claimed.kind === "claimed", "Inspection requires released lease");
      const store = await backend.openStorage(claimed.lease);
      try {
        return {
          entries: (
            await store.scanEntries(
              { conversationId: ROOT_CONVERSATION_ID, order: "ascending" },
              100,
              undefined,
              context,
            )
          ).items,
          submissions: (await store.scanSubmissions({}, 100, undefined, context)).items,
          tasks: (await store.scanTasks({}, 100, undefined, context)).items,
        };
      } finally {
        await store.close(context);
        await f.authority.release(claimed.lease);
      }
    },
    expire: () =>
      f.authority.inspectionDb.query(`UPDATE harness SET lease_until=${DB_NOW_MS}-1 WHERE ns=$1`, [
        f.ns,
      ]),
    async finish() {
      work.finish();
      await f.finish();
      expect(objects.openHandles()).toBe(0);
    },
  };
}

it("injected AWS roots: PG receipt -> captured delivery -> actual Harness -> cold continuation -> missed-wake reconciliation", async () => {
  const f = await fixture("aws_roots");
  const command = (request: string) => ({
    ns: f.ns,
    request,
    command: { kind: "inline", input: { content: request, whenBusy: "followUp" } },
  });
  try {
    const first = await f.api(command("one"), ctx);
    expect(first).toMatchObject({ ok: true, duplicate: false });
    expect(f.hints).toEqual([f.ns]);
    expect((await f.authority.get(f.ns, requestId("one")))?.kind).toBe("received");
    expect(
      await f.authority.inspectionDb.query("SELECT * FROM records WHERE ns=$1", [f.ns]),
    ).toEqual([]);
    const run = workerRoot(() => runtimeDependencies(f.options));
    expect(await run({ ns: f.hints.shift()! }, ctx)).toBe("released");
    expect(f.hints).toEqual([]);
    const firstOutput = await f.output();
    expect(firstOutput.submissions.map((s) => s.status)).toEqual(["done"]);
    expect(firstOutput.entries.find((e) => e.kind === "pi.assistant")?.model).toMatchObject([
      { role: "assistant", content: [{ type: "text", text: "ok" }] },
    ]);
    expect(f.events.indexOf("closed")).toBeLessThan(f.events.indexOf("release"));

    const missed = apiRoot(() => ({
      ...f.deps,
      connectAuthority: () => f.backend.connectAuthority(),
      dispatch: {
        async wake() {
          throw new Error("lost hint");
        },
      },
    }));
    expect(await missed(command("two"), ctx)).toMatchObject({ ok: true });
    expect(f.hints).toEqual([]);
    expect(await f.reconcile({}, ctx)).toBe(1);
    expect(f.hints).toEqual([f.ns]);
    // A new handler, real models/registry and backend are constructed for cold continuation.
    vi.spyOn(common, "configuration").mockReturnValue({
      aws,
      provider,
      connectAuthority: () => f.backend.connectAuthority(),
    });
    vi.spyOn(s3, "objectFactory").mockReturnValue((ns) => f.objects.objects(ns));
    vi.spyOn(console, "log").mockImplementation(() => {});
    const cold = createAwsHandler(modelWork(), f.options.rootAgent);
    expect(await cold({ ns: f.hints.shift()! }, ctx)).toBe("released");
    expect(f.hints).toEqual([]);
    await f.expire();
    expect(await f.reconcile({}, ctx)).toBe(0);
    const second = await f.output();
    expect(second.submissions.map((s) => s.status)).toEqual(["done", "done"]);
    expect(second.entries.slice(0, firstOutput.entries.length)).toEqual(firstOutput.entries);
    expect(second.entries.filter((e) => e.kind === "pi.user")).toHaveLength(2);
    expect(second.tasks.every((t) => t.state.status === "terminal")).toBe(true);
    expect(f.objects.openHandles()).toBe(0);
  } finally {
    await f.finish();
  }
});

it("actual immutable SDK adapter publishes a >6 MiB image; PG worker handoff cold-reopens after staging deletion", async () => {
  const f = await fixture("aws_large", true);
  let remaining = 45000;
  let mapped = 0;
  const dependencies = runtimeDependencies(f.options);
  const connect = dependencies.connectAuthority;
  dependencies.connectAuthority = async () => {
    const authority = await connect();
    const mark = authority.markAdmitted.bind(authority);
    authority.markAdmitted = async (receipt, id) => {
      await mark(receipt, id);
      if (++mapped === 2) remaining = 5000;
    };
    return authority;
  };
  try {
    const input = canonical({
      content: [{ type: "image", data: largeImage, mimeType: "image/png" }],
      whenBusy: "followUp",
    });
    expect(input.bytes).toBeGreaterThan(6 * 1024 * 1024);
    expect(
      await f.api(
        {
          ns: f.ns,
          request: "000-busy",
          command: { kind: "inline", input: { content: "hold", whenBusy: "followUp" } },
        },
        ctx,
      ),
    ).toMatchObject({ ok: true });
    const staged = f.objects.stage(f.ns, input.json);
    expect(
      await f.api({ ns: f.ns, request: "001-image", command: { kind: "s3", ...staged } }, ctx),
    ).toMatchObject({ ok: true });
    f.objects.bytes.delete(staged.key);
    const receipt = await f.authority.get(f.ns, requestId("001-image"));
    expect(receipt?.payload.kind).toBe("s3");
    expect(f.objects.calls.filter((c) => c.name === "PutObjectCommand")).toHaveLength(1);
    const run = workerRoot(() => dependencies);
    expect(
      await run({ ns: f.hints.shift()! }, { ...ctx, getRemainingTimeInMillis: () => remaining }),
    ).toBe("released");
    expect(mapped).toBe(2);
    expect(await f.authority.hasWork(f.ns)).toBe(true);
    f.hints.length = 0; // Lose all advisory API delivery duplicates.
    expect(await f.reconcile({}, ctx)).toBe(1);
    const fresh = modelWork();
    let images = 0;
    const stream = fresh.models.streamSimple.bind(fresh.models);
    fresh.models.streamSimple = (model, transcript, options) => {
      for (const message of transcript.messages) {
        if (message.role === "user" && Array.isArray(message.content)) {
          for (const block of message.content) {
            if (block.type === "image") {
              expect(computeSha256(block.data)).toBe(imageFixture.base64Sha256);
              images++;
            }
          }
        }
      }
      return stream(model, transcript, options);
    };
    const cold = workerRoot(() =>
      runtimeDependencies({
        ...f.options,
        backend: createSqlBackend(provider, (ns) => f.objects.objects(ns)),
        harnessOptions: fresh,
      }),
    );
    expect(await cold({ ns: f.hints.shift()! }, ctx)).toBe("released");
    expect(images).toBeGreaterThan(0);
    expect(await f.authority.hasWork(f.ns)).toBe(false);
    const output = await f.output();
    expect(output.submissions.map((s) => s.status)).toEqual(["done", "done"]);
    const stored = output.entries
      .flatMap((e) => e.model ?? [])
      .filter((m) => m.role === "user")
      .flatMap((m) => (Array.isArray(m.content) ? m.content : []))
      .filter((b) => b.type === "image");
    expect(stored).toHaveLength(1);
    expect(computeSha256(stored[0].data)).toBe(imageFixture.base64Sha256);
    expect(f.objects.streams.every((s) => s.destroyed)).toBe(true);
    expect(f.objects.openHandles()).toBe(0);
  } finally {
    await f.finish();
  }
});
