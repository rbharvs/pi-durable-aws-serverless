import { afterEach, expect, it, vi } from "vitest";
import { Readable } from "node:stream";
import {
  GetObjectCommand,
  HeadObjectCommand,
  PutObjectCommand,
  S3Client,
  NoSuchKey,
  S3ServiceException,
} from "@aws-sdk/client-s3";
import { ImmutableObjects } from "./s3.ts";
import { aws, lambdaContext } from "../../tests/shared/aws.ts";
import { sdkObjects } from "../../tests/shared/aws-objects.ts";
import { computeSha256, harnessId } from "../domain.ts";
import { stagedReference } from "../input.ts";
import { readAtMost } from "../object-data.ts";
import { reference } from "../s3-payload.ts";
import { LIMITS } from "../limits.ts";
import { createHandler } from "./api.ts";
import { createMemoryBackend } from "../memory.ts";

const ns = harnessId("objects");
const json = '{"content":"hello","whenBusy":"followUp"}';
const ref = stagedReference(ns, {
  key: `in/${ns}/input`,
  bytes: Buffer.byteLength(json),
  sha256: computeSha256(json),
});
const checksum = Buffer.from(ref.sha256, "hex").toString("base64");
function clientFixture() {
  const client = new S3Client({ ...aws, maxAttempts: 1 });
  const send = vi.spyOn(client, "send");
  const destroy = vi.spyOn(client, "destroy");
  const store = new ImmutableObjects("service-payloads", ns, aws, client);
  return { client, send, destroy, store };
}
afterEach(() => {
  vi.restoreAllMocks();
  vi.useRealTimers();
});

it("actual immutable SDK adapter keeps published bytes independent of staging and cold handles", async () => {
  const f = sdkObjects();
  const staged = f.stage(ns, json);
  const first = f.objects(ns);
  const raw = await readAtMost(await first.getStaged(staged), staged.bytes);
  expect(raw?.toString()).toBe(json);
  const published = await first.put(json);
  first.close();
  f.bytes.set(staged.key, Buffer.from("replaced"));
  const cold = f.objects(ns);
  try {
    expect(await cold.get(published)).toBe(json);
  } finally {
    cold.close();
  }
  expect(f.openHandles()).toBe(0);
  expect(f.streams.every((body) => body.destroyed)).toBe(true);
});

for (const outcome of ["success", "missing-checksum", "lost-ack", "conditional-retry"] as const)
  it(`publication ${outcome} uses one conditional key, confirmed bytes, no overwrite or application retry`, async () => {
    const f = clientFixture();
    const primary = new S3ServiceException({
      name: outcome === "conditional-retry" ? "PreconditionFailed" : "TimeoutError",
      $fault: "client",
      $metadata: { httpStatusCode: outcome === "conditional-retry" ? 412 : undefined },
    });
    if (outcome === "success")
      f.send.mockResolvedValueOnce({ $metadata: {}, ChecksumSHA256: checksum } as never);
    else if (outcome === "missing-checksum")
      f.send.mockResolvedValueOnce({ $metadata: {} } as never);
    else f.send.mockRejectedValueOnce(primary);
    f.send.mockResolvedValueOnce({
      $metadata: {},
      ContentLength: ref.bytes,
      ChecksumSHA256: checksum,
    } as never);
    try {
      const published = await f.store.put(json);
      expect(published).toMatchObject({ kind: "s3", bytes: ref.bytes, sha256: ref.sha256 });
      const put = f.send.mock.calls[0][0] as PutObjectCommand;
      expect(put).toBeInstanceOf(PutObjectCommand);
      expect(put.input).toMatchObject({
        Key: published.key,
        IfNoneMatch: "*",
        ChecksumSHA256: checksum,
        ContentLength: ref.bytes,
      });
      expect(put.input.Body).toEqual(Buffer.from(json));
      expect(f.send).toHaveBeenCalledTimes(outcome === "success" ? 1 : 2);
      if (outcome !== "success") {
        expect(f.send.mock.calls[1][0]).toBeInstanceOf(HeadObjectCommand);
        expect((f.send.mock.calls[1][0] as HeadObjectCommand).input).toEqual({
          Bucket: "service-payloads",
          Key: published.key,
          ChecksumMode: "ENABLED",
        });
      }
    } finally {
      f.store.close();
    }
  });

for (const head of ["missing", "wrong-size", "wrong-checksum"] as const)
  it(`uncertain PUT with ${head} HEAD stays uncertain and preserves the primary failure`, async () => {
    const f = clientFixture();
    const primary = Error("write acknowledgment unknown");
    f.send.mockRejectedValueOnce(primary);
    if (head === "missing") f.send.mockRejectedValueOnce(Error("head unavailable"));
    else
      f.send.mockResolvedValueOnce({
        $metadata: {},
        ContentLength: head === "wrong-size" ? ref.bytes + 1 : ref.bytes,
        ChecksumSHA256: head === "wrong-checksum" ? "bad" : checksum,
      } as never);
    try {
      await expect(f.store.put(json)).rejects.toBe(primary);
      expect(f.send).toHaveBeenCalledTimes(2);
    } finally {
      f.store.close();
    }
  });

it("validates namespace, size, JSON and closed handles before SDK I/O", async () => {
  const f = clientFixture();
  try {
    expect(() =>
      f.store.getStaged({ ...ref, key: "in/foreign/input" as typeof ref.key }),
    ).toThrow();
    const published = reference(
      {
        object_key: "p/foreign/00000000-0000-4000-8000-000000000000",
        sha256: ref.sha256,
        bytes: ref.bytes,
      },
      harnessId("foreign"),
    );
    await expect(f.store.get(published)).rejects.toThrow();
    await expect(f.store.put("invalid")).rejects.toThrow();
    await expect(f.store.put(JSON.stringify("x".repeat(LIMITS.maxObjectBytes)))).rejects.toThrow();
    // Oversized malformed JSON is rejected by the resource cap before parsing.
    await expect(f.store.put("!".repeat(LIMITS.maxObjectBytes + 1))).rejects.toThrow(
      "Object exceeds",
    );
    f.store.close();
    f.store.close();
    await expect(f.store.put(json)).rejects.toThrow("closed");
    await expect(f.store.getStaged(ref)).rejects.toThrow("closed");
    expect(f.send).not.toHaveBeenCalled();
    expect(f.destroy).toHaveBeenCalledTimes(1);
  } finally {
    f.store.close();
  }
});

for (const fault of ["missing", "denied", "server", "transfer"] as const)
  it(`staged ${fault} has truthful material-versus-transport classification through the API root`, async () => {
    const f = clientFixture();
    const backend = createMemoryBackend();
    let error: Error;
    if (fault === "missing")
      error = new NoSuchKey({ $metadata: { httpStatusCode: 404 }, message: "missing" });
    else
      error = new S3ServiceException({
        name: fault === "denied" ? "AccessDenied" : "ServiceUnavailable",
        $fault: "server",
        $metadata: { httpStatusCode: fault === "denied" ? 403 : 503 },
        message: "private provider message",
      });
    if (fault === "transfer") {
      const stream = Readable.from(
        (async function* () {
          yield Buffer.from(json.slice(0, 10));
          throw error;
        })(),
      );
      f.send.mockResolvedValue({ $metadata: {}, Body: stream, ContentLength: ref.bytes } as never);
    } else f.send.mockRejectedValue(error);
    const events: unknown[] = [];
    const api = createHandler(() => ({
      connectAuthority: () => backend.connectAuthority(),
      objects: () => f.store,
      dispatch: {
        wake: async () => {
          throw Error("unexpected wake");
        },
      },
      event: (name, fields) => events.push({ name, ...fields }),
    }));
    expect(
      await api({ ns, request: "one", command: { kind: "s3", ...ref } }, lambdaContext),
    ).toEqual({
      ok: false,
      code: fault === "missing" ? "staged-content-invalid" : "unavailable",
      retryable: fault !== "missing",
    });
    expect(events).toEqual([
      {
        name: "submit-failed",
        code: fault === "missing" ? "staged-content-invalid" : "unavailable",
        phase: fault === "missing" ? "validation-or-internal" : "staged-read",
      },
    ]);
    expect(f.destroy).toHaveBeenCalledTimes(1);
  });

for (const kind of ["no-body", "wrong-length", "overflow", "short"] as const)
  it(`body boundary rejects ${kind} and destroys every acquired stream`, async () => {
    const f = clientFixture();
    const body = Readable.from([
      Buffer.from(kind === "overflow" ? json + "x" : kind === "short" ? json.slice(1) : json),
    ]);
    f.send.mockResolvedValue({
      $metadata: {},
      Body: kind === "no-body" ? undefined : body,
      ContentLength: kind === "wrong-length" ? ref.bytes + 1 : undefined,
    } as never);
    const backend = createMemoryBackend();
    const api = createHandler(() => ({
      connectAuthority: () => backend.connectAuthority(),
      objects: () => f.store,
      dispatch: { wake: async () => {} },
      event: () => {},
    }));
    try {
      expect(
        await api({ ns, request: "one", command: { kind: "s3", ...ref } }, lambdaContext),
      ).toEqual(
        kind === "no-body"
          ? { ok: false, code: "unavailable", retryable: true }
          : { ok: false, code: "staged-content-invalid", retryable: false },
      );
      if (kind !== "no-body") expect(body.destroyed).toBe(true);
      expect(f.destroy).toHaveBeenCalledTimes(1);
    } finally {
      body.destroy();
      f.store.close();
    }
  });

it("consumer early return destroys the SDK body; close also destroys a never-consumed body", async () => {
  const f = clientFixture();
  const bodies = [Readable.from([Buffer.from(json)]), Readable.from([Buffer.from(json)])];
  f.send
    .mockResolvedValueOnce({ $metadata: {}, Body: bodies[0] } as never)
    .mockResolvedValueOnce({ $metadata: {}, Body: bodies[1] } as never);
  const stream = await f.store.getStaged(ref);
  for await (const _chunk of stream) break;
  expect(bodies[0].destroyed).toBe(true);
  await f.store.getStaged(ref);
  f.store.close();
  expect(bodies[1].destroyed).toBe(true);
  expect(f.destroy).toHaveBeenCalledTimes(1);
});

it("a stalled SDK body has a finite abort deadline, releases transport, and remains retryable", async () => {
  vi.useFakeTimers();
  const f = clientFixture();
  const body = new Readable({ read() {} });
  f.send.mockResolvedValue({ $metadata: {}, Body: body } as never);
  const stream = await f.store.getStaged(ref);
  const read = readAtMost(stream, ref.bytes);
  const rejected = expect(read).rejects.toThrow("aborted");
  await vi.advanceTimersByTimeAsync(20000);
  await rejected;
  expect(body.destroyed).toBe(true);
  const signal = (f.send.mock.calls[0][1] as { abortSignal: AbortSignal }).abortSignal;
  expect(signal.aborted).toBe(true);
  f.store.close();
});

for (const kind of ["hash", "utf8", "json", "bom"] as const)
  it(`published reads independently verify ${kind} after bounded transfer`, async () => {
    const f = clientFixture();
    const bytes =
      kind === "utf8"
        ? Buffer.from([0xff])
        : Buffer.from(kind === "json" ? "!" : kind === "bom" ? "\ufeff{}" : "{}");
    const published = reference(
      {
        object_key: `p/${ns}/00000000-0000-4000-8000-000000000000`,
        sha256: kind === "hash" ? computeSha256("wrong") : computeSha256(bytes),
        bytes: bytes.length,
      },
      ns,
    );
    const body = Readable.from([bytes]);
    f.send.mockResolvedValue({ $metadata: {}, Body: body } as never);
    try {
      await expect(f.store.get(published)).rejects.toThrow();
      expect(body.destroyed).toBe(true);
    } finally {
      f.store.close();
    }
  });

it("GET requests carry only the fixed bucket, validated key and checksum mode", async () => {
  const f = clientFixture();
  f.send.mockResolvedValue({ $metadata: {}, Body: Readable.from([Buffer.from(json)]) } as never);
  try {
    expect((await readAtMost(await f.store.getStaged(ref), ref.bytes))?.toString()).toBe(json);
    expect(f.send.mock.calls[0][0]).toBeInstanceOf(GetObjectCommand);
    expect((f.send.mock.calls[0][0] as GetObjectCommand).input).toEqual({
      Bucket: "service-payloads",
      Key: ref.key,
      ChecksumMode: "ENABLED",
    });
  } finally {
    f.store.close();
  }
});
