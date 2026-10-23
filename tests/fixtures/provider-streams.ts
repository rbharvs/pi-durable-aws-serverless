import assert from "node:assert/strict";
import http from "node:http";
import https from "node:https";
import net from "node:net";
import { createProviderModels } from "../../src/providers.ts";
import { createRegistry } from "@earendil-works/pi-durable";
import { BACKGROUND_CONTEXT as context } from "@earendil-works/chord/context";
import {
  createMemoryBackend,
  apiHandler,
  runtimeDependencies,
  harnessId,
} from "../../src/index.ts";
import { createHandler } from "../../src/aws/worker.ts";
import { objectFixture } from "../shared/objects.ts";

// Bundle and run in an owned child process with a clean environment and synthetic keys.
// These in-process stubs cover the exercised paths; they are not OS-level network isolation.
const denied = () => {
  throw new Error("Network is denied in provider contract tests");
};
globalThis.fetch = denied;
http.request = denied;
https.request = denied;
net.connect = denied;
net.createConnection = denied;

async function main() {
  const models = createProviderModels();
  assert.deepEqual(
    models.getProviders().map((p) => p.id),
    ["openai", "anthropic"],
  );
  const text = "synthetic SDK response";
  const message = {
    id: "msg_test",
    type: "message",
    role: "assistant",
    status: "completed",
    content: [{ type: "output_text", text, annotations: [] }],
  };
  const openaiEvents = [
    { type: "response.created", response: { id: "resp_test" } },
    { type: "response.output_item.added", output_index: 0, item: { ...message, content: [] } },
    { type: "response.output_text.delta", output_index: 0, delta: text },
    { type: "response.output_item.done", output_index: 0, item: message },
    {
      type: "response.completed",
      response: {
        id: "resp_test",
        status: "completed",
        output: [message],
        usage: { input_tokens: 1, output_tokens: 2, total_tokens: 3 },
      },
    },
  ];
  const anthropicEvents = [
    {
      type: "message_start",
      message: {
        id: "msg_test",
        model: "claude-sonnet-4-5",
        role: "assistant",
        content: [],
        usage: { input_tokens: 1, output_tokens: 0 },
      },
    },
    { type: "content_block_start", index: 0, content_block: { type: "text", text: "" } },
    { type: "content_block_delta", index: 0, delta: { type: "text_delta", text } },
    { type: "content_block_stop", index: 0 },
    { type: "message_delta", delta: { stop_reason: "end_turn" }, usage: { output_tokens: 2 } },
    { type: "message_stop" },
  ];
  for (const [provider, modelId, path, key, events] of [
    [
      "openai",
      "gpt-4.1",
      "https://api.openai.com/v1/responses",
      "synthetic-openai-only",
      openaiEvents,
    ],
    [
      "anthropic",
      "claude-sonnet-4-5",
      "https://api.anthropic.com/v1/messages?beta=true",
      "synthetic-anthropic-only",
      anthropicEvents,
    ],
  ] as const) {
    const model = models.getModel(provider, modelId);
    assert.ok(model);
    assert.deepEqual((await models.getAuth(model))?.auth, { apiKey: key });
    let requests = 0;
    globalThis.fetch = async (url, init) => {
      requests++;
      assert.equal(String(url), path);
      const headers = new Headers(init?.headers);
      assert.equal(
        headers.get(provider === "openai" ? "authorization" : "x-api-key"),
        provider === "openai" ? `Bearer ${key}` : key,
      );
      const body = JSON.parse(String(init?.body));
      assert.equal(body.model, modelId);
      assert.equal(body.stream, true);
      return new Response(
        events.map((event) => `event: ${event.type}\ndata: ${JSON.stringify(event)}\n\n`).join(""),
        {
          headers: { "content-type": "text/event-stream" },
        },
      );
    };
    const result = await models.completeSimple(
      model,
      {
        messages: [{ role: "user", content: "hello", timestamp: 1 }],
      },
      { maxRetries: 0 },
    );
    assert.equal(requests, 1);
    assert.equal(result.stopReason, "stop", result.errorMessage);
    assert.equal(result.provider, provider);
    assert.equal(result.model, modelId);
    assert.equal(result.content[0]?.type, "text");
    assert.ok(result.content[0]?.type === "text" && result.content[0].text === text);

    // End to end through API receipt -> worker Lambda adapter -> real Pi -> real provider SDK,
    // with only HTTP responses and persistence kept local. No stream implementation is replaced.
    let now = 10000;
    const backend = createMemoryBackend({ now: () => now++ });
    const objects = objectFixture();
    const ns = harnessId(`sdk_${provider}`);
    const deps = runtimeDependencies({
      backend,
      objects: objects.objects,
      rootAgent: {},
      event: () => {},
      harnessOptions: {
        models,
        registry: createRegistry(),
        settings: { retry: { enabled: false } },
      },
    });
    const api = apiHandler({
      connectAuthority: backend.connectAuthority,
      objects: objects.objects,
      dispatch: { wake: async () => {} },
      event: () => {},
    });
    assert.equal(
      (
        await api({
          ns,
          request: "one",
          command: {
            kind: "inline",
            input: {
              content: "hello",
              whenBusy: "followUp",
              model: { provider, modelId },
            },
          },
        })
      ).ok,
      true,
    );
    const deadline = Date.now() + 10000;
    assert.equal(
      await createHandler(() => deps)(
        { ns },
        {
          awsRequestId: "synthetic-provider-invocation",
          getRemainingTimeInMillis: () => deadline - Date.now(),
        },
      ),
      "released",
    );
    assert.equal(requests, 2);
    const control = await backend.connectAuthority();
    try {
      const claimed = await control.claim(ns, 45000);
      assert.equal(claimed.kind, "claimed");
      assert.ok(claimed.kind === "claimed");
      const live = await deps.open(claimed.lease);
      try {
        const entries = (await live.root.entries({ order: "ascending" }, 100, undefined, context))
          .items;
        const answer = entries.find((entry) => entry.kind === "pi.assistant");
        assert.ok(answer);
        assert.match(JSON.stringify(answer.model), /synthetic SDK response/);
        assert.equal((await live.root.agent(context)).model?.provider, provider);
      } finally {
        await live.harness.close(context);
        await control.release(claimed.lease);
      }
    } finally {
      await control.close();
    }
    assert.equal(objects.openHandles(), 0);
    globalThis.fetch = denied;
  }

  // Models are constructed without reading credentials; resolution uses the current environment.
  delete process.env.OPENAI_API_KEY;
  delete process.env.ANTHROPIC_API_KEY;
  for (const [provider, modelId] of [
    ["openai", "gpt-4.1"],
    ["anthropic", "claude-sonnet-4-5"],
  ]) {
    assert.equal(await models.getAuth(models.getModel(provider!, modelId!)!), undefined);
  }
  console.log("Official OpenAI Responses and Anthropic Messages streams passed; HTTP mocked");
}
void main().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
