import { expect, it } from "vitest";
import { canonical, runtimeDependencies, worker } from "../../src/index.ts";
import { computeSha256 } from "../../src/domain.ts";
import { imageFixture, largeImage } from "../shared/image.ts";
import { modelWork, workerFixture } from "./worker-support.ts";

it("another owner cold-recovers two full-size PNG inputs from durable objects after staging deletion", async () => {
  const f = await workerFixture("worker_large", true);
  let remaining = 45000,
    mapped = 0;
  const connect = f.deps.connectAuthority;
  f.deps.connectAuthority = async () => {
    const c = await connect(),
      mark = c.markAdmitted.bind(c);
    c.markAdmitted = async (item, id) => {
      await mark(item, id);
      if (++mapped === 3) remaining = 5000;
    };
    return c;
  };
  try {
    expect(imageFixture.pngBytes).toBe(4.5 * 1024 * 1024);
    expect(Buffer.from(largeImage, "base64").subarray(0, 8)).toEqual(
      Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]),
    );
    const input = canonical({
      content: [{ type: "image", data: largeImage, mimeType: "image/png" }],
      whenBusy: "followUp",
    });
    expect(input.bytes).toBeGreaterThan(6 * 1024 * 1024);
    await f.receive("000-busy", "hold the model");
    for (let i = 1; i <= 2; i++) {
      const staged = f.objects.stage(f.ns, input.json, `image-${i}`);
      expect(
        await f.api({ ns: f.ns, request: `00${i}-image`, command: { kind: "s3", ...staged } }),
      ).toMatchObject({ ok: true });
      f.objects.deleteStage(staged.key);
    }
    expect(f.hints).toEqual([f.ns, f.ns, f.ns]);
    expect(await worker(f.hints.shift()!, () => remaining, f.deps)).toBe("released");
    expect(mapped).toBe(3);
    expect(await f.authority.hasWork(f.ns)).toBe(true);
    const old = await f.head();
    expect(old.owner).toBeNull();

    const fresh = modelWork();
    let contextImages = 0;
    const stream = fresh.models.streamSimple.bind(fresh.models);
    fresh.models.streamSimple = (model, transcript, options) => {
      for (const message of transcript.messages) {
        if (message.role !== "user" || !Array.isArray(message.content)) continue;
        for (const block of message.content)
          if (block.type === "image") {
            expect(computeSha256(block.data)).toBe(imageFixture.base64Sha256);
            contextImages++;
          }
      }
      return stream(model, transcript, options);
    };
    const deps = runtimeDependencies({ ...f.options, harnessOptions: fresh });
    expect(await worker(f.ns, () => 45000, deps)).toBe("released");
    expect(contextImages).toBeGreaterThanOrEqual(2);
    expect((await f.head()).epoch).not.toBe(old.epoch);
    expect(await f.authority.hasWork(f.ns)).toBe(false);
    const output = await f.output();
    expect(output.submissions.map((s) => s.status)).toEqual(["done", "done", "done"]);
    let images = 0;
    for (const entry of output.entries)
      for (const message of entry.model ?? []) {
        if (message.role !== "user" || !Array.isArray(message.content)) continue;
        for (const block of message.content)
          if (block.type === "image") {
            expect(computeSha256(block.data)).toBe(imageFixture.base64Sha256);
            images++;
          }
      }
    expect(images).toBe(2);
    expect(f.objects.openHandles()).toBe(0);
  } finally {
    await f.finish();
  }
});
