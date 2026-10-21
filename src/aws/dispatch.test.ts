import { afterEach, expect, it, vi } from "vitest";
import { Readable } from "node:stream";
import { InvokeCommand, LambdaClient } from "@aws-sdk/client-lambda";
import { aws } from "../../tests/shared/aws.ts";
import { harnessId } from "../domain.ts";
import { dispatcher } from "./dispatch.ts";

afterEach(() => {
  vi.restoreAllMocks();
  vi.useRealTimers();
});

it("async dispatch uses the real signed Lambda request and correlation without inline work", async () => {
  const requests: {
    hostname: string;
    path: string;
    headers: Record<string, string>;
    body?: unknown;
  }[] = [];
  const client = new LambdaClient({
    ...aws,
    maxAttempts: 1,
    requestHandler: {
      async handle(request: Parameters<LambdaClient["config"]["requestHandler"]["handle"]>[0]) {
        requests.push(request);
        return {
          response: {
            statusCode: 202,
            headers: { "x-amzn-requestid": "offline" },
            body: Readable.from([]),
          },
        };
      },
    },
  });
  const events: unknown[] = [];
  const dispatch = dispatcher(
    "service-worker:live",
    aws,
    (name, fields) => {
      events.push({ name, ...fields });
      throw Error("observer");
    },
    "parent",
    client,
  );
  try {
    await dispatch.wake(harnessId("dispatch"));
  } finally {
    dispatch.close();
  }
  expect(requests).toHaveLength(1);
  const request = requests[0];
  expect(request.hostname).toBe("lambda.eu-west-1.amazonaws.com");
  expect(request.path).toContain("service-worker%3Alive");
  expect(request.headers["x-amz-invocation-type"]).toBe("Event");
  expect(request.headers.authorization).toContain("/eu-west-1/lambda/aws4_request");
  const input = JSON.parse(Buffer.from(request.body as Uint8Array).toString());
  expect(input).toEqual({
    ns: "dispatch",
    dispatchId: expect.stringMatching(/^[0-9a-f-]{36}$/),
    parentInvocation: "parent",
  });
  expect(events).toEqual([
    {
      name: "dispatch-send",
      ns: "dispatch",
      target: "service-worker:live",
      dispatchId: input.dispatchId,
      parentInvocation: "parent",
    },
    {
      name: "dispatch-accepted",
      ns: "dispatch",
      dispatchId: input.dispatchId,
      parentInvocation: "parent",
      requestId: "offline",
    },
  ]);
});

for (const status of [200, 204, undefined])
  it(`status ${status} is not an acknowledged async delivery`, async () => {
    const client = new LambdaClient({ ...aws, maxAttempts: 1 });
    const send = vi
      .spyOn(client, "send")
      .mockResolvedValue({ $metadata: {}, StatusCode: status } as never);
    const dispatch = dispatcher("service-worker", aws, () => {}, "parent", client);
    try {
      await expect(dispatch.wake(harnessId("dispatch"))).rejects.toThrow("not accepted");
      expect(send).toHaveBeenCalledTimes(1);
    } finally {
      dispatch.close();
    }
  });

it("close explicitly aborts active dispatch even when injected transport destroy does nothing", async () => {
  const started = Promise.withResolvers<AbortSignal>();
  const response = Promise.withResolvers<never>();
  const abort = Error("explicit close abort");
  const transportDestroy = vi.fn();
  const client = new LambdaClient({
    ...aws,
    maxAttempts: 1,
    requestHandler: {
      handle(
        _request: Parameters<LambdaClient["config"]["requestHandler"]["handle"]>[0],
        options?: Parameters<LambdaClient["config"]["requestHandler"]["handle"]>[1],
      ) {
        const signal = Reflect.get(options!, "abortSignal") as AbortSignal;
        signal.addEventListener("abort", () => response.reject(abort), { once: true });
        started.resolve(signal);
        return response.promise;
      },
      destroy: transportDestroy,
    },
  });
  const dispatch = dispatcher("service-worker", aws, () => {}, "parent", client);
  const outcome = dispatch.wake(harnessId("dispatch")).catch((error) => error);
  try {
    const signal = await started.promise;
    dispatch.close();
    dispatch.close();
    expect(signal.aborted).toBe(true);
    expect(await outcome).toBe(abort);
    expect(transportDestroy).toHaveBeenCalledTimes(1);
  } finally {
    response.reject(abort);
    await outcome;
    dispatch.close();
  }
});

it("dispatch has a finite abort, no application retry, receiver-safe close, and no use after close", async () => {
  vi.useFakeTimers();
  let signal: AbortSignal | undefined;
  const client = new LambdaClient({
    ...aws,
    maxAttempts: 1,
    requestHandler: {
      handle(
        _request: Parameters<LambdaClient["config"]["requestHandler"]["handle"]>[0],
        options?: Parameters<LambdaClient["config"]["requestHandler"]["handle"]>[1],
      ) {
        signal = options && (Reflect.get(options, "abortSignal") as AbortSignal);
        return new Promise((_resolve, reject) => {
          signal!.addEventListener("abort", () => reject(Error("offline abort")), { once: true });
        });
      },
    },
  });
  const send = vi.spyOn(client, "send");
  const destroy = vi.spyOn(client, "destroy");
  const dispatch = dispatcher("service-worker", aws, () => {}, "parent", client);
  const pending = dispatch.wake(harnessId("dispatch"));
  const rejected = expect(pending).rejects.toThrow("offline abort");
  await vi.advanceTimersByTimeAsync(20000);
  await rejected;
  expect(signal?.aborted).toBe(true);
  expect(send).toHaveBeenCalledTimes(1);
  expect(send.mock.calls[0][0]).toBeInstanceOf(InvokeCommand);
  dispatch.close();
  dispatch.close();
  expect(destroy).toHaveBeenCalledTimes(1);
  await expect(dispatch.wake(harnessId("dispatch"))).rejects.toThrow("closed");
  expect(send).toHaveBeenCalledTimes(1);
});
