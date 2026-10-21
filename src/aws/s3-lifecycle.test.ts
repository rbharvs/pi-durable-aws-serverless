import { afterEach, expect, it, vi } from "vitest";
import { createServer, type RequestListener } from "node:http";
import { Readable } from "node:stream";
import { setImmediate as turn } from "node:timers/promises";
import { S3Client } from "@aws-sdk/client-s3";
import { ImmutableObjects } from "./s3.ts";
import { createHandler } from "./api.ts";
import { REQUEST_MS, transport } from "./config.ts";
import { computeSha256, harnessId } from "../domain.ts";
import { stagedReference } from "../input.ts";
import { readAtMost } from "../object-data.ts";
import { reference } from "../s3-payload.ts";
import { createMemoryBackend } from "../memory.ts";
import { aws, lambdaContext } from "../../tests/shared/aws.ts";

const ns = harnessId("wire_lifecycle");
const raw = Buffer.from(JSON.stringify({ content: "hello", whenBusy: "followUp" }));
const ref = stagedReference(ns, {
  key: `in/${ns}/input`,
  bytes: raw.length,
  sha256: computeSha256(raw),
});

async function wire(respond: RequestListener) {
  const server = createServer(respond);
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (!address || typeof address === "string") throw Error("Missing loopback address");
  const client = new S3Client({
    ...aws,
    maxAttempts: 1,
    forcePathStyle: true,
    endpoint: `http://127.0.0.1:${address.port}`,
    requestHandler: transport,
  });
  const destroy = client.destroy.bind(client);
  const store = new ImmutableObjects("service-payloads", ns, aws, client);
  const send = vi.spyOn(client, "send");
  return {
    store,
    client,
    signal: () => (send.mock.calls[0][1] as { abortSignal: AbortSignal }).abortSignal,
    async close() {
      store.close();
      destroy();
      server.closeAllConnections();
      await new Promise<void>((resolve, reject) =>
        server.close((error) => (error ? reject(error) : resolve())),
      );
    },
  };
}

const stalled: RequestListener = (_request, response) => {
  response.writeHead(200, { "content-length": raw.length });
  response.write(raw.subarray(0, 1));
};

afterEach(() => {
  vi.restoreAllMocks();
  vi.useRealTimers();
});

for (const fault of ["caller-hash", "content-length", "missing", "sdk-checksum"] as const)
  it(`loopback SDK staging classifies ${fault} through the public API`, async () => {
    const missing = fault === "missing";
    const body = missing ? Buffer.from("<Error><Code>NoSuchKey</Code></Error>") : raw;
    const f = await wire((_request, response) => {
      response.writeHead(missing ? 404 : 200, {
        connection: "close",
        "content-length": body.length + (fault === "content-length" ? 1 : 0),
        ...(!missing
          ? {
              "x-amz-checksum-sha256": Buffer.from(
                computeSha256(fault === "sdk-checksum" ? "wrong" : body),
                "hex",
              ).toString("base64"),
            }
          : {}),
      });
      response.end(body);
    });
    const backend = createMemoryBackend();
    const acquire = vi.fn(() => backend.connectAuthority());
    const wake = vi.fn(async () => {});
    const api = createHandler(() => ({
      connectAuthority: acquire,
      objects: () => f.store,
      dispatch: { wake },
      event: () => {},
    }));
    try {
      expect(
        await api(
          {
            ns,
            request: "one",
            command: {
              kind: "s3",
              ...ref,
              ...(fault === "caller-hash" ? { sha256: computeSha256("wrong") } : {}),
            },
          },
          lambdaContext,
        ),
      ).toEqual({
        ok: false,
        code: fault === "sdk-checksum" ? "unavailable" : "staged-content-invalid",
        retryable: fault === "sdk-checksum",
      });
      expect(acquire).not.toHaveBeenCalled();
      expect(wake).not.toHaveBeenCalled();
    } finally {
      await f.close();
    }
  });

it("loopback published reads reject a caller hash mismatch despite a valid SDK checksum", async () => {
  const f = await wire((_request, response) => {
    response.writeHead(200, {
      "content-length": raw.length,
      "x-amz-checksum-sha256": Buffer.from(computeSha256(raw), "hex").toString("base64"),
    });
    response.end(raw);
  });
  try {
    const published = reference(
      {
        object_key: `p/${ns}/00000000-0000-4000-8000-000000000000`,
        bytes: raw.length,
        sha256: computeSha256("wrong"),
      },
      ns,
    );
    await expect(f.store.get(published)).rejects.toThrow("Object integrity mismatch");
  } finally {
    await f.close();
  }
});

for (const closing of [false, true])
  it(`loopback body is destroyed by ${closing ? "close" : "deadline"} without SDK destroy assistance`, async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    const f = await wire(stalled);
    const destroy = vi.spyOn(f.client, "destroy").mockImplementation(() => {});
    try {
      const body = (await f.store.getStaged(ref)) as Readable;
      // Do not consume: cleanup must not depend on a consumer ever starting iteration.
      if (closing) f.store.close();
      else await vi.advanceTimersByTimeAsync(REQUEST_MS);
      expect(f.signal().aborted).toBe(true);
      expect(body.destroyed).toBe(true);
      await turn(); // queued error/close is safe even without an iterator or error observer
      await expect(readAtMost(body, ref.bytes)).rejects.toMatchObject({ name: "AbortError" });
      f.store.close();
      f.store.close();
      expect(destroy).toHaveBeenCalledTimes(1);
      await expect(f.store.getStaged(ref)).rejects.toThrow("closed");
    } finally {
      await f.close();
    }
  });

it("an SDK response held past close returns an already-aborted body safely", async () => {
  const f = await wire(stalled);
  const arrived = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  f.client.middlewareStack.add(
    (next) => async (args) => {
      const response = await next(args);
      arrived.resolve();
      await release.promise;
      return response;
    },
    { step: "initialize", name: "holdResponse" },
  );
  vi.spyOn(f.client, "destroy").mockImplementation(() => {});
  try {
    const pending = f.store.getStaged(ref);
    await arrived.promise;
    f.store.close();
    release.resolve();
    const body = (await pending) as Readable;
    expect(body.destroyed).toBe(true);
    await turn();
    await expect(readAtMost(body, ref.bytes)).rejects.toMatchObject({ name: "AbortError" });
  } finally {
    release.resolve();
    await f.close();
  }
});

it("completed loopback reads release their deadline and active close ownership", async () => {
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
  const f = await wire((_request, response) => {
    response.writeHead(200, { "content-length": raw.length });
    response.end(raw);
  });
  try {
    expect(await readAtMost(await f.store.getStaged(ref), ref.bytes)).toEqual(raw);
    await turn();
    f.store.close();
    await vi.advanceTimersByTimeAsync(REQUEST_MS);
    expect(f.signal().aborted).toBe(false);
  } finally {
    await f.close();
  }
});

it("an unread SDK body error survives completion cleanup as the primary read error", async () => {
  const f = await wire(stalled);
  const primary = Error("transfer failed");
  try {
    const body = (await f.store.getStaged(ref)) as Readable;
    body.destroy(primary);
    await turn();
    f.store.close();
    expect(f.signal().aborted).toBe(false);
    await expect(readAtMost(body, ref.bytes)).rejects.toBe(primary);
  } finally {
    await f.close();
  }
});
