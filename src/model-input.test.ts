import { expect, it } from "vitest";
import { apiHandler } from "./submit.ts";
import { createMemoryBackend } from "./memory.ts";
import { harnessId, requestId, computeSha256 } from "./domain.ts";
import { canonical, canonicalStagedInput } from "./input.ts";
import { objectFixture } from "../tests/shared/objects.ts";

const model = { provider: "openai", modelId: "gpt-4.1" };
const input = { content: "hello", whenBusy: "followUp", model };

it("API durably accepts model selection, dedupes equivalent staged input, and rejects a changed model", async () => {
  const backend = createMemoryBackend();
  const objects = objectFixture();
  const api = apiHandler({
    connectAuthority: backend.connectAuthority,
    objects: objects.objects,
    dispatch: { wake: async () => {} },
    event: () => {},
  });
  const ns = harnessId("model-input");
  const request = requestId("one");
  expect(await api({ ns, request, command: { kind: "inline", input } })).toMatchObject({
    ok: true,
    duplicate: false,
  });
  const authority = await backend.connectAuthority();
  try {
    expect(await authority.get(ns, request)).toMatchObject({
      hash: canonical(input).sha256,
      payload: { kind: "inline", json: canonical(input).json },
    });
    expect(
      await api({
        ns,
        request,
        command: {
          kind: "inline",
          input: {
            ...input,
            model: { modelId: model.modelId, provider: model.provider },
          },
        },
      }),
    ).toMatchObject({ ok: true, duplicate: true });
    const staged = objects.stage(
      ns,
      JSON.stringify({
        model: { modelId: model.modelId, provider: model.provider },
        content: input.content,
        whenBusy: input.whenBusy,
      }),
    );
    expect(await api({ ns, request, command: { kind: "s3", ...staged } })).toMatchObject({
      ok: true,
      duplicate: true,
    });
    expect(
      await api({
        ns,
        request,
        command: {
          kind: "inline",
          input: {
            ...input,
            model: { provider: "anthropic", modelId: "claude-sonnet-4-5" },
          },
        },
      }),
    ).toMatchObject({ ok: false, code: "request-conflict" });
    const invalid = { ...input, model: { provider: "openai", modelId: "unknown-model" } };
    expect(
      await api({ ns, request: "invalid", command: { kind: "inline", input: invalid } }),
    ).toMatchObject({ ok: false, code: "invalid-input" });
    expect(await authority.get(ns, requestId("invalid"))).toBeUndefined();
    expect(
      await api({
        ns,
        request: "invalid-staged",
        command: {
          kind: "s3",
          ...objects.stage(ns, JSON.stringify(invalid), "invalid"),
        },
      }),
    ).toMatchObject({ ok: false, code: "staged-content-invalid" });
  } finally {
    await authority.close();
  }
});

it("model canonicalization is shared by inline and staged input without changing old receipt hashes", () => {
  const legacy = { content: "hello", whenBusy: "followUp" };
  expect(canonical(legacy).json).toBe('{"content":"hello","whenBusy":"followUp"}');
  expect(canonical(legacy).sha256).toBe(computeSha256(JSON.stringify(legacy)));
  expect(
    canonicalStagedInput(
      Buffer.from(
        JSON.stringify({
          model: { modelId: model.modelId, provider: model.provider },
          whenBusy: input.whenBusy,
          content: input.content,
        }),
      ),
    ),
  ).toEqual(canonical(input));
  const mutable = { ...input, model: { ...model } };
  const accepted = canonical(mutable);
  mutable.model.modelId = "changed";
  expect(accepted.input).toEqual(input);
});

for (const retired of [
  { provider: "openai", modelId: "gpt-retired-example" },
  { provider: "retired-provider", modelId: "retired-model" },
]) {
  it(`keeps historical ${retired.provider} identity and duplicates while rejecting new unsupported input`, async () => {
    const backend = createMemoryBackend();
    const authority = await backend.connectAuthority();
    const ns = harnessId("retired-memory");
    const value = { ...input, model: retired };
    const json = JSON.stringify(value);
    const hash = computeSha256(json);
    const api = apiHandler({
      connectAuthority: backend.connectAuthority,
      dispatch: { wake: async () => {} },
      event: () => {},
    });
    try {
      expect(canonical(value)).toMatchObject({ json, sha256: hash });
      expect(canonicalStagedInput(Buffer.from(json))).toEqual(canonical(value));
      const command = { kind: "inline", input: value };
      expect(await api({ ns, request: "new-unknown", command })).toMatchObject({
        ok: false,
        code: "invalid-input",
      });
      expect(await authority.get(ns, requestId("new-unknown"))).toBeUndefined();
      // The storage adapter accepts historical structural bytes, not current catalog policy.
      const original = await authority.receive(ns, requestId("historical"), {
        hash,
        payload: { kind: "inline", json },
      });
      expect(await authority.get(ns, requestId("historical"))).toEqual(original.item);
      expect(await authority.received(ns)).toEqual([original.item]);
      expect(await api({ ns, request: "historical", command })).toMatchObject({
        ok: true,
        duplicate: true,
        receivedAt: original.item.receivedAt,
      });
      expect(
        await api({
          ns,
          request: "historical",
          command: {
            kind: "inline",
            input: { ...value, content: "changed" },
          },
        }),
      ).toMatchObject({ ok: false, code: "request-conflict" });
      expect(await authority.get(ns, requestId("historical"))).toEqual(original.item);
    } finally {
      await authority.close();
    }
  });
}

for (const invalid of [
  null,
  {},
  [],
  "openai/gpt-4.1",
  { provider: "", modelId: "gpt-4.1" },
  { provider: "x".repeat(33), modelId: "gpt-4.1" },
  { provider: "openai", modelId: "" },
  { provider: "openai", modelId: "x".repeat(257) },
  { provider: "openai", modelId: "gpt-4.1", apiKey: "synthetic-only" },
  { provider: "openai", modelId: 1 },
]) {
  it(`structural parsing rejects malformed model selection ${JSON.stringify(invalid)}`, () => {
    expect(() => canonical({ ...input, model: invalid })).toThrow();
  });
}
