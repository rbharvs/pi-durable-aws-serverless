import { afterEach, expect, it, vi } from "vitest";
import { Readable } from "node:stream";
import { createServer } from "node:http";
import type { Socket, AddressInfo } from "node:net";
import { setImmediate as tick } from "node:timers/promises";
import { HeadObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { ImmutableObjects } from "./s3.ts";
import { REQUEST_MS, transport } from "./config.ts";
import { harnessId } from "../domain.ts";
import { aws } from "../../tests/shared/aws.ts";

afterEach(() => {
  vi.restoreAllMocks();
  vi.useRealTimers();
});

it("stalled confirmation HEAD shares the PUT deadline and preserves the primary uncertain-write error", async () => {
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
  const putStarted = Promise.withResolvers<void>();
  const finishPut = Promise.withResolvers<void>();
  const headStarted = Promise.withResolvers<void>();
  const finishHead = Promise.withResolvers<never>();
  const calls: { method: string; path: string; signal?: AbortSignal }[] = [];
  let primary: unknown;
  const client = new S3Client({
    ...aws,
    maxAttempts: 1,
    requestHandler: {
      async handle(
        request: Parameters<S3Client["config"]["requestHandler"]["handle"]>[0],
        options?: Parameters<S3Client["config"]["requestHandler"]["handle"]>[1],
      ) {
        const signal = options && (Reflect.get(options, "abortSignal") as AbortSignal | undefined);
        calls.push({ method: request.method, path: request.path, signal });
        if (request.method === "PUT") {
          putStarted.resolve();
          await finishPut.promise;
          return {
            response: {
              statusCode: 503,
              headers: { "content-type": "application/xml" },
              body: Readable.from([
                "<Error><Code>SlowDown</Code><Message>uncertain PUT</Message></Error>",
              ]),
            },
          };
        }
        const abort = () => finishHead.reject(Error("HEAD aborted"));
        signal?.addEventListener("abort", abort, { once: true });
        if (signal?.aborted) abort();
        headStarted.resolve();
        try {
          return await finishHead.promise;
        } finally {
          signal?.removeEventListener("abort", abort);
        }
      },
    },
  });
  client.middlewareStack.add(
    (next, context) => async (args) => {
      try {
        return await next(args);
      } catch (error) {
        if (context.commandName === "PutObjectCommand") primary = error;
        throw error;
      }
    },
    { step: "initialize", name: "capturePrimary" },
  );
  const store = new ImmutableObjects("service-payloads", harnessId("deadline"), aws, client);
  let settled = false;
  const outcome = store
    .put('{"ok":true}')
    .then(
      (value) => ({ value }),
      (error: unknown) => ({ error }),
    )
    .finally(() => {
      settled = true;
    });
  try {
    await putStarted.promise;
    await vi.advanceTimersByTimeAsync(5000);
    finishPut.resolve();
    await headStarted.promise;
    await vi.advanceTimersByTimeAsync(REQUEST_MS - 5001);
    expect(settled).toBe(false);
    await vi.advanceTimersByTimeAsync(1);
    await tick();
    expect(settled).toBe(true);
    const result = await outcome;
    expect("error" in result && result.error).toBe(primary);
    expect(primary).toMatchObject({
      name: "SlowDown",
      $metadata: { httpStatusCode: 503, attempts: 1 },
    });
    expect(calls.map((c) => c.method)).toEqual(["PUT", "HEAD"]);
    expect(calls[1].path).toBe(calls[0].path);
    expect(calls[1].signal).toBe(calls[0].signal);
    expect(calls[1].signal?.aborted).toBe(true);
  } finally {
    finishPut.resolve();
    finishHead.reject(Error("test cleanup"));
    store.close();
    await outcome;
  }
});

it("configured SDK requestTimeout rejects a stalled loopback request without an adapter AbortController", async () => {
  const received = Promise.withResolvers<void>();
  const sockets = new Set<Socket>();
  const server = createServer((request) => {
    request.resume();
    received.resolve();
  });
  server.on("connection", (socket) => {
    sockets.add(socket);
    socket.once("close", () => sockets.delete(socket));
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const client = new S3Client({
    ...aws,
    maxAttempts: 1,
    forcePathStyle: true,
    endpoint: `http://127.0.0.1:${(server.address() as AddressInfo).port}`,
    requestHandler: transport,
  });
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
  let settled = false;
  const outcome = client
    .send(new HeadObjectCommand({ Bucket: "service-payloads", Key: "test" }))
    .then(
      (value) => ({ value }),
      (error: unknown) => ({ error }),
    )
    .finally(() => {
      settled = true;
    });
  try {
    await received.promise;
    await vi.advanceTimersByTimeAsync(REQUEST_MS);
    await tick();
    expect(settled).toBe(true);
    expect(await outcome).toMatchObject({
      error: { name: "TimeoutError", code: "ETIMEDOUT", $metadata: { attempts: 1 } },
    });
  } finally {
    client.destroy();
    await Promise.all(
      [...sockets].map(
        (socket) =>
          new Promise<void>((resolve) => {
            socket.once("close", resolve);
            socket.destroy();
          }),
      ),
    );
    await new Promise<void>((resolve, reject) =>
      server.close((error) => (error ? reject(error) : resolve())),
    );
    await outcome;
    vi.useRealTimers();
  }
  expect(sockets.size).toBe(0);
});
