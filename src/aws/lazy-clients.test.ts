import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { LambdaClient } from "@aws-sdk/client-lambda";
import { handler as api } from "./api.ts";
import { handler as reconcile } from "./reconciler.ts";
import { dispatcher } from "./dispatch.ts";
import * as common from "./common.ts";
import { createMemoryBackend } from "../memory.ts";
import { harnessId } from "../domain.ts";
import { aws, lambdaContext as ctx } from "../../tests/shared/aws.ts";

const counts = vi.hoisted(() => ({ lambda: 0, s3: 0, lambdaCloses: 0, s3Closes: 0 }));
// Observe actual SDK construction, not counterfeit clients or destroy-as-construction guesses.
vi.mock("@aws-sdk/client-lambda", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@aws-sdk/client-lambda")>();
  return {
    ...actual,
    LambdaClient: class extends actual.LambdaClient {
      constructor(...args: ConstructorParameters<typeof actual.LambdaClient>) {
        super(...args);
        counts.lambda++;
      }
      override destroy() {
        counts.lambdaCloses++;
        super.destroy();
      }
    },
  };
});
vi.mock("@aws-sdk/client-s3", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@aws-sdk/client-s3")>();
  return {
    ...actual,
    S3Client: class extends actual.S3Client {
      constructor(...args: ConstructorParameters<typeof actual.S3Client>) {
        super(...args);
        counts.s3++;
      }
      override destroy() {
        counts.s3Closes++;
        super.destroy();
      }
    },
  };
});

beforeEach(() => {
  Object.assign(counts, { lambda: 0, s3: 0, lambdaCloses: 0, s3Closes: 0 });
  vi.stubEnv("WORKER_NAME", "service-worker");
  vi.stubEnv("PAYLOAD_BUCKET", "service-payloads");
  vi.spyOn(console, "log").mockImplementation(() => {});
});
afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
});

function configuration() {
  const backend = createMemoryBackend();
  const config = {
    aws,
    provider: {
      connect: async () => {
        throw new Error("unexpected SQL");
      },
      isRetryableAbort: () => false,
    },
    connectAuthority: () => backend.connectAuthority(),
  };
  const spy = vi.spyOn(common, "configuration").mockReturnValue(config);
  return { config, spy };
}

it("actual invalid API, failed receipt, and empty reconciler roots construct no Lambda or S3 clients", async () => {
  const f = configuration();
  expect(await api(null, ctx)).toEqual({ ok: false, code: "invalid-input", retryable: false });
  expect(await reconcile({}, ctx)).toBe(0);
  f.spy.mockReturnValue({
    ...f.config,
    connectAuthority: async () => {
      throw new Error("offline acquisition");
    },
  });
  expect(
    await api(
      {
        ns: "lazy",
        request: "r",
        command: { kind: "inline", input: { content: "hello", whenBusy: "followUp" } },
      },
      ctx,
    ),
  ).toEqual({ ok: false, code: "unavailable", retryable: true });
  expect(counts).toEqual({ lambda: 0, s3: 0, lambdaCloses: 0, s3Closes: 0 });
});

it("normal API and nonempty reconciliation construct and destroy one real client per used invocation", async () => {
  configuration();
  const send = vi
    .spyOn(LambdaClient.prototype, "send")
    .mockResolvedValue({ $metadata: {}, StatusCode: 202 } as never);
  expect(
    await api(
      {
        ns: "lazy",
        request: "r",
        command: { kind: "inline", input: { content: "hello", whenBusy: "followUp" } },
      },
      ctx,
    ),
  ).toMatchObject({ ok: true });
  expect(counts).toEqual({ lambda: 1, lambdaCloses: 1, s3: 0, s3Closes: 0 });
  expect(await reconcile({}, ctx)).toBe(1);
  expect(counts).toEqual({ lambda: 2, lambdaCloses: 2, s3: 0, s3Closes: 0 });
  expect(send).toHaveBeenCalledTimes(2);
});

it("concurrent wakes share exactly one lazily constructed client and close cannot acquire it", async () => {
  const finish = Promise.withResolvers<void>();
  const instances: unknown[] = [];
  vi.spyOn(LambdaClient.prototype, "send").mockImplementation(async function (this: LambdaClient) {
    instances.push(this);
    await finish.promise;
    return { $metadata: {}, StatusCode: 202 } as never;
  });
  const unused = dispatcher("service-worker", aws, () => {}, "parent");
  unused.close();
  unused.close();
  await expect(unused.wake(harnessId("lazy"))).rejects.toThrow("closed");
  expect(counts.lambda).toBe(0);
  const used = dispatcher("service-worker", aws, () => {}, "parent");
  expect(counts.lambda).toBe(0);
  const pending = [used.wake(harnessId("one")), used.wake(harnessId("two"))];
  try {
    expect(counts.lambda).toBe(1);
    expect(instances).toHaveLength(2);
    expect(instances[0]).toBe(instances[1]);
  } finally {
    finish.resolve();
    await Promise.all(pending);
    used.close();
    used.close();
  }
  expect(counts.lambdaCloses).toBe(1);
});

it("an injected real client is owned and closed once even without a wake", async () => {
  const client = new LambdaClient(aws);
  const dispatch = dispatcher("service-worker", aws, () => {}, "parent", client);
  dispatch.close();
  dispatch.close();
  await expect(dispatch.wake(harnessId("lazy"))).rejects.toThrow("closed");
  expect(counts.lambda).toBe(1);
  expect(counts.lambdaCloses).toBe(1);
  expect(counts.s3).toBe(0);
});
