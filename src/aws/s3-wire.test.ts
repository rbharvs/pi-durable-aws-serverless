import { afterEach, expect, it, vi } from "vitest";
import { Readable } from "node:stream";
import { S3Client } from "@aws-sdk/client-s3";
import { ImmutableObjects } from "./s3.ts";
import { createHandler } from "./api.ts";
import { createMemoryBackend } from "../memory.ts";
import { computeSha256, harnessId } from "../domain.ts";
import { stagedReference } from "../input.ts";
import { aws, lambdaContext } from "../../tests/shared/aws.ts";

const ns = harnessId("multipart");
const small = Buffer.from(JSON.stringify({ content: "hello", whenBusy: "followUp" }));
const digest = (bytes: Uint8Array) => Buffer.from(computeSha256(bytes), "hex");

/** S3's SHA-256 of concatenated binary part hashes, not the SHA-256 of the full body. */
function composite(body: Buffer, partSize = 5 * 1024 * 1024) {
  const hashes: Buffer[] = [];
  for (let i = 0; i < body.length; i += partSize)
    hashes.push(digest(body.subarray(i, i + partSize)));
  return `${digest(Buffer.concat(hashes)).toString("base64")}-${hashes.length}`;
}

function wire(body: Buffer, headers: Record<string, string>, status = 200, transportError?: Error) {
  const methods: string[] = [];
  const client = new S3Client({
    ...aws,
    maxAttempts: 1,
    requestHandler: {
      async handle(request: Parameters<S3Client["config"]["requestHandler"]["handle"]>[0]) {
        methods.push(request.method);
        if (transportError) throw transportError;
        if (request.method === "PUT")
          return {
            response: {
              statusCode: 200,
              headers: { "x-amz-checksum-sha256": request.headers["x-amz-checksum-sha256"] },
              body: Readable.from([]),
            },
          };
        return {
          response: {
            statusCode: status,
            headers: { "content-length": String(body.length), ...headers },
            body: Readable.from([body]),
          },
        };
      },
    },
  });
  const store = new ImmutableObjects("service-payloads", ns, aws, client);
  const backend = createMemoryBackend();
  const api = createHandler(() => ({
    connectAuthority: () => backend.connectAuthority(),
    objects: () => store,
    dispatch: { wake: async () => {} },
    event: () => {},
  }));
  return {
    methods,
    store,
    submit(expected: Buffer) {
      const ref = stagedReference(ns, {
        key: `in/${ns}/input`,
        bytes: expected.length,
        sha256: computeSha256(expected),
      });
      return api({ ns, request: "one", command: { kind: "s3", ...ref } }, lambdaContext);
    },
  };
}

afterEach(() => vi.restoreAllMocks());

for (const large of [false, true])
  it(`real SDK wire accepts valid composite SHA256 staging large=${large}`, async () => {
    const body = large
      ? Buffer.from(
          JSON.stringify({ content: "x".repeat(5 * 1024 * 1024 + 32), whenBusy: "followUp" }),
        )
      : small;
    const checksum = composite(body);
    expect(checksum).not.toBe(digest(body).toString("base64"));
    const f = wire(body, { "x-amz-checksum-sha256": checksum, "x-amz-checksum-type": "COMPOSITE" });
    try {
      expect(await f.submit(body)).toMatchObject({ ok: true, duplicate: false });
      expect(f.methods).toEqual(large ? ["GET", "PUT"] : ["GET"]);
    } finally {
      f.store.close();
    }
  });

it("caller hash, not composite-looking S3 checksum metadata, decides matching staged bytes", async () => {
  for (const type of [undefined, "FULL_OBJECT"]) {
    const f = wire(small, {
      "x-amz-checksum-sha256": composite(small),
      ...(type ? { "x-amz-checksum-type": type } : {}),
    });
    try {
      expect(await f.submit(small)).toMatchObject({ ok: true });
    } finally {
      f.store.close();
    }
  }
});

for (const checksum of [composite(small), "malformed-1"])
  it(`caller full raw hash rejects tampering even when SDK skips composite checksum ${checksum}`, async () => {
    const tampered = Buffer.from(small.toString().replace("hello", "other"));
    const f = wire(tampered, {
      "x-amz-checksum-sha256": checksum,
      "x-amz-checksum-type": "COMPOSITE",
    });
    try {
      expect(await f.submit(small)).toEqual({
        ok: false,
        code: "staged-content-invalid",
        retryable: false,
      });
      expect(f.methods).toEqual(["GET"]);
    } finally {
      f.store.close();
    }
  });

for (const kind of [
  "full",
  "none",
  "mismatch",
  "malformed-suffix",
  "missing",
  "transport",
] as const)
  it(`real SDK staging wire retains ${kind} behavior`, async () => {
    const body =
      kind === "missing"
        ? Buffer.from("<Error><Code>NoSuchKey</Code><Message>missing</Message></Error>")
        : small;
    const checksum =
      kind === "mismatch"
        ? digest(Buffer.from("wrong")).toString("base64")
        : kind === "malformed-suffix"
          ? composite(small) + "junk"
          : digest(small).toString("base64");
    const f = wire(
      body,
      kind === "none" || kind === "missing" ? {} : { "x-amz-checksum-sha256": checksum },
      kind === "missing" ? 404 : 200,
      kind === "transport" ? Error("offline transport") : undefined,
    );
    try {
      const response = await f.submit(small);
      if (kind === "full" || kind === "none" || kind === "malformed-suffix")
        expect(response).toMatchObject({ ok: true });
      else
        expect(response).toEqual({
          ok: false,
          code: kind === "missing" ? "staged-content-invalid" : "unavailable",
          retryable: kind !== "missing",
        });
      expect(f.methods).toEqual(["GET"]);
    } finally {
      f.store.close();
    }
  });

it("staged integrity uses the raw digest before canonical JSON, even with a valid SDK checksum", async () => {
  const raw = Buffer.from(JSON.stringify({ whenBusy: "followUp", content: "hello" }));
  expect(raw.length).toBe(small.length);
  expect(digest(raw)).not.toEqual(digest(small));
  for (const expected of [raw, small]) {
    const f = wire(raw, { "x-amz-checksum-sha256": digest(raw).toString("base64") });
    try {
      expect(await f.submit(expected)).toMatchObject(
        expected === raw
          ? { ok: true }
          : { ok: false, code: "staged-content-invalid", retryable: false },
      );
    } finally {
      f.store.close();
    }
  }
});

it("composite PUT and HEAD checksums cannot confirm our single-part immutable publication", async () => {
  const methods: string[] = [];
  const client = new S3Client({
    ...aws,
    maxAttempts: 1,
    requestHandler: {
      async handle(request: Parameters<S3Client["config"]["requestHandler"]["handle"]>[0]) {
        methods.push(request.method);
        return {
          response: {
            statusCode: 200,
            headers: {
              "content-length": String(small.length),
              "x-amz-checksum-sha256": composite(small),
              "x-amz-checksum-type": "COMPOSITE",
            },
            body: Readable.from([]),
          },
        };
      },
    },
  });
  const store = new ImmutableObjects("service-payloads", ns, aws, client);
  try {
    await expect(store.put(small.toString())).rejects.toThrow("did not confirm checksum");
    expect(methods).toEqual(["PUT", "HEAD"]);
  } finally {
    store.close();
  }
});
