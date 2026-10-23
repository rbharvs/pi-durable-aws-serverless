import { expect, it } from "vitest";
import { BACKGROUND_CONTEXT as context } from "@earendil-works/chord/context";
import { createRegistry } from "@earendil-works/pi-durable";
import { fauxAssistantMessage } from "@earendil-works/pi-ai";
import { AssistantMessageEventStream } from "@earendil-works/pi-ai/utils/event-stream";
import { apiHandler, resolveInput } from "../../src/submit.ts";
import { canonical } from "../../src/input.ts";
import { computeSha256, requestId, check } from "../../src/domain.ts";
import { createSqlBackend } from "../../src/sql/backend.ts";
import { createProviderModels } from "../../src/providers.ts";
import { runtimeDependencies } from "../../src/runtime.ts";
import { worker } from "../../src/worker.ts";
import { objectFixture } from "../shared/objects.ts";
import { namespace, provider } from "./postgres.ts";

for (const [kind, retired] of [
  ["inline", { provider: "openai", modelId: "gpt-retired-example" }],
  ["inline", { provider: "retired-provider", modelId: "retired-model" }],
  ["s3", { provider: "openai", modelId: "gpt-retired-example" }],
] as const) {
  it(`PG historical ${kind} ${retired.provider} receipt stays readable, retryable and drainable`, async () => {
    const f = await namespace("retired_model");
    const objects = objectFixture();
    const input = {
      content: kind === "s3" ? "x".repeat(300000) : "historical input",
      whenBusy: "followUp",
      model: retired,
    };
    // Exact canonical bytes written by a prior bundle, independent of today's parser/catalog.
    const json = JSON.stringify(input);
    const hash = computeSha256(json);
    const request = requestId("historical");
    const store = objects.objects(f.ns);
    try {
      if (kind === "inline") {
        const seed = canonical({ content: "seed", whenBusy: "followUp" });
        await f.authority.receive(f.ns, request, {
          hash: seed.sha256,
          payload: { kind: "inline", json: seed.json },
        });
        // Fixture-only SQL models a receipt accepted before the catalog changed.
        await f.authority.inspectionDb.query(
          "UPDATE inbox SET json=$3,payload_sha256=$4 WHERE ns=$1 AND request_id=$2",
          [f.ns, request, json, hash],
        );
      } else {
        const published = await store.put(json);
        await f.authority.receive(f.ns, request, {
          hash,
          payload: published,
        });
      }
      const original = await f.authority.get(f.ns, request);
      expect(original).toMatchObject({ kind: "received", hash });
      check(original, "Historical receipt exists");
      expect(
        await f.authority.receive(f.ns, request, {
          hash,
          payload: original.payload,
        }),
      ).toMatchObject({ duplicate: true, item: original });
      const received = await f.authority.received(f.ns);
      expect(received).toHaveLength(1);
      expect(await resolveInput(received[0]!, store)).toEqual(input);

      const backend = createSqlBackend(provider, objects.objects);
      const api = apiHandler({
        connectAuthority: backend.connectAuthority,
        objects: objects.objects,
        dispatch: { wake: async () => {} },
        event: () => {},
      });
      function command(value: typeof input) {
        return kind === "inline"
          ? { kind: "inline", input: value }
          : {
              kind: "s3",
              ...objects.stage(f.ns, JSON.stringify(value)),
            };
      }
      const retry = () => api({ ns: f.ns, request, command: command(input) });
      expect(await retry()).toMatchObject({
        ok: true,
        duplicate: true,
        receivedAt: original.receivedAt,
      });
      expect(
        await api({
          ns: f.ns,
          request,
          command: command({ ...input, content: input.content + "changed" }),
        }),
      ).toMatchObject({ ok: false, code: "request-conflict" });
      expect(
        await api({ ns: f.ns, request: "new-unknown", command: command(input) }),
      ).toMatchObject({
        ok: false,
        code: kind === "inline" ? "invalid-input" : "staged-content-invalid",
      });
      expect(await f.authority.get(f.ns, requestId("new-unknown"))).toBeUndefined();

      const next = { provider: "anthropic", modelId: "claude-sonnet-4-5" };
      expect(
        await api({
          ns: f.ns,
          request: "next",
          command: {
            kind: "inline",
            input: { content: "next input", whenBusy: "followUp", model: next },
          },
        }),
      ).toMatchObject({ ok: true, duplicate: false });
      // One retired row must not poison the entire received() SQL batch.
      expect((await f.authority.received(f.ns)).map((item) => item.request)).toEqual([
        request,
        "next",
      ]);
      const models = createProviderModels();
      const calls: string[] = [];
      models.streamSimple = (model) => {
        calls.push(`${model.provider}/${model.id}`);
        const stream = new AssistantMessageEventStream();
        stream.push({
          type: "done",
          reason: "stop",
          message: fauxAssistantMessage("synthetic answer"),
        });
        stream.end();
        return stream;
      };
      const deps = runtimeDependencies({
        backend,
        objects: objects.objects,
        rootAgent: {},
        event: () => {},
        harnessOptions: { models, registry: createRegistry() },
      });
      const deadline = Date.now() + 4000;
      expect(await worker(f.ns, () => 5000 + Math.max(0, deadline - Date.now()), deps)).toBe(
        "released",
      );
      expect(calls).toEqual(["anthropic/claude-sonnet-4-5"]);
      expect(await f.authority.hasWork(f.ns)).toBe(false);
      expect(await f.authority.received(f.ns)).toEqual([]);
      expect(await retry()).toMatchObject({
        ok: true,
        duplicate: true,
        receivedAt: original.receivedAt,
      });
      expect(await f.authority.get(f.ns, request)).toMatchObject({
        kind: "admitted",
        hash,
        payload: original.payload,
      });

      const claim = await f.authority.claim(f.ns, 45000);
      check(claim.kind === "claimed", "Inspection claim");
      const live = await deps.open(claim.lease);
      try {
        expect(
          await live.root.commit((tx) => tx.submissionByRequest(live.root.id, request), context),
        ).toMatchObject({ status: "unanswered", reason: "no_model" });
        expect(
          await live.root.commit((tx) => tx.submissionByRequest(live.root.id, "next"), context),
        ).toMatchObject({ status: "done" });
        expect((await live.root.agent(context)).model).toEqual(next);
      } finally {
        await live.harness.close(context);
        await f.authority.release(claim.lease);
      }
    } finally {
      store.close();
      await f.finish();
      expect(objects.openHandles()).toBe(0);
    }
  });
}
